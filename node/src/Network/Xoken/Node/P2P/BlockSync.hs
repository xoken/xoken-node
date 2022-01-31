{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.P2P.BlockSync where

import Codec.Serialise
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (AsyncCancelled, mapConcurrently, mapConcurrently_, race_)
import Control.Concurrent.Async.Lifted as LA (async, concurrently, concurrently_, race)
import Control.Concurrent.Event as EV
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TQueue as TB
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Lens
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import Control.Monad.Trans.Control
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16 (decode, encode)
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import Data.HashTable as CHT
import qualified Data.HashTable.IO as H
import Data.IORef
import Data.Int
import qualified Data.IntMap as I
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Pool
import Data.Serialize
import Data.Serialize as S
import Data.String
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as DTE
import Data.Time.Calendar
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import qualified Database.Bolt as BT
import Database.XCQL.Protocol as Q
import qualified ListT as LT
import qualified Network.Socket as NS
import Network.Socket
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Address
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data as DA
import Network.Xoken.Node.Data.Allegory
import qualified Network.Xoken.Node.Data.Allegory as AL (Index(..))
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types as Type
import Network.Xoken.Node.Service.Chain
import Network.Xoken.Script.Standard
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import Numeric.Lens (base)
import StmContainers.Map as SM
import StmContainers.Set as SS
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Xoken
import Xoken.NodeConfig

produceGetDataMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> m (Message)
produceGetDataMessage peer = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    debug lg $ LG.msg $ "Block - produceGetDataMessage - called." ++ (show peer)
    bl <- liftIO $ takeMVar (blockFetchQueue peer)
    trace lg $ LG.msg $ "took mvar.. " ++ (show bl) ++ (show peer)
    let gd = GetData $ [InvVector InvBlock $ getBlockHash $ biBlockHash bl]
    debug lg $ LG.msg $ "GetData req: " ++ show gd
    return (MGetData gd)

sendRequestMessages :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> Message -> m ()
sendRequestMessages pr msg = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    debug lg $ LG.msg $ val "Block - sendRequestMessages - called."
    case (bpSocket pr) of
        Just s -> do
            let em = runPut . putMessage net $ msg
            res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
            case res of
                Right () -> return ()
                Left (e :: SomeException) -> do
                    case fromException e of
                        Just (t :: AsyncCancelled) -> throw e
                        otherwise -> do
                            err lg $ LG.msg ("Error BS, sending out data: " ++ show e)
                            liftIO $ atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress pr))
                            liftIO $ Network.Socket.close s
                            throw e
            debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
        Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"

peerBlockSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> m ()
peerBlockSync peer =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        trace lg $ LG.msg $ ("peer block sync : " ++ (show peer))
        !tm <- liftIO $ getCurrentTime
        let tracker = statsTracker peer
        fw <- liftIO $ readIORef $ ptBlockFetchWindow tracker
        recvtm <- liftIO $ readIORef $ ptLastTxRecvTime tracker
        sendtm <- liftIO $ readIORef $ ptLastGetDataSent tracker
        let staleTime = fromInteger $ fromIntegral (unresponsivePeerConnTimeoutSecs $ nodeConfig bp2pEnv)
        case recvtm of
            Just rt -> do
                if (fw == 0 && (diffUTCTime tm rt < staleTime))
                    then do
                        msg <- produceGetDataMessage peer
                        res <- LE.try $ sendRequestMessages peer msg
                        case res of
                            Right () -> do
                                debug lg $ LG.msg $ val "updating state."
                                liftIO $ writeIORef (ptLastGetDataSent tracker) $ Just tm
                                liftIO $ atomicModifyIORef' (ptBlockFetchWindow tracker) (\z -> (z + 1, ()))
                            Left (e :: SomeException) -> do
                                err lg $ LG.msg ("[ERROR] peerBlockSync " ++ show e)
                                ------------
                                throw e
                    else if (diffUTCTime tm rt > staleTime)
                             then do
                                 debug lg $ LG.msg ("Removing unresponsive peer. (1)" ++ show peer)
                                 case bpSocket peer of
                                     Just sock -> liftIO $ NS.close $ sock
                                     Nothing -> return ()
                                 liftIO $ atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress peer))
                                 throw UnresponsivePeerException
                             else return ()
            Nothing -> do
                case sendtm of
                    Just st -> do
                        if (diffUTCTime tm st > staleTime)
                            then do
                                debug lg $ LG.msg ("Removing unresponsive peer. (2)" ++ show peer)
                                case bpSocket peer of
                                    Just sock -> liftIO $ NS.close $ sock
                                    Nothing -> return ()
                                liftIO $ atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress peer))
                                throw UnresponsivePeerException
                            else return ()
                    Nothing -> do
                        if (fw == 0)
                            then do
                                msg <- produceGetDataMessage peer
                                res <- LE.try $ sendRequestMessages peer msg
                                case res of
                                    Right () -> do
                                        debug lg $ LG.msg $ val "updating state."
                                        liftIO $ writeIORef (ptLastGetDataSent tracker) $ Just tm
                                        liftIO $ atomicModifyIORef' (ptBlockFetchWindow tracker) (\z -> (z + 1, ()))
                                    Left (e :: SomeException) -> do
                                        err lg $ LG.msg ("[ERROR] peerBlockSync " ++ show e)
                                        throw e
                            else return ()
        liftIO $ threadDelay (100000) -- 0.1 sec
        return ()

runPeerSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runPeerSync =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        dbe' <- getDB
        let net = bitcoinNetwork $ nodeConfig bp2pEnv
        allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        let connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
        if L.length connPeers < (maxBitcoinPeerCount $ nodeConfig bp2pEnv)
            then do
                liftIO $
                    mapConcurrently_
                        (\(_, pr) ->
                             case (bpSocket pr) of
                                 Just s -> do
                                     let em = runPut . putMessage net $ (MGetAddr)
                                     debug lg $ LG.msg ("sending GetAddr to " ++ show pr)
                                     res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
                                     case res of
                                         Right () -> liftIO $ threadDelay (60 * 1000000)
                                         Left (e :: SomeException) -> do
                                             err lg $ LG.msg ("[ERROR] runPeerSync " ++ show e)
                                             liftIO $
                                                 atomically $
                                                 modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress pr))
                                 Nothing -> err lg $ LG.msg $ val "Error sending, no connections available")
                        (connPeers)
            else liftIO $ threadDelay (60 * 1000000)

markBestSyncedBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BlockHash -> Int32 -> XCqlClientState -> m ()
markBestSyncedBlock hash height conn = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    liftIO $ atomically $ writeTVar (bestSyncedBlock bp2pEnv) (Just $ BlockInfo hash $ fromIntegral height)
    let q :: Q.QueryString Q.W (Text, (Maybe Bool, Int32, Maybe Int64, Text)) ()
        q = Q.QueryString "insert INTO xoken.misc_store (key, value) values (? , ?)"
        p :: Q.QueryParams (Text, (Maybe Bool, Int32, Maybe Int64, Text))
        p = getSimpleQueryParam ("best-synced", (Nothing, height, Nothing, (blockHashToHex hash)))
    res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query q p)
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg ("Error: Marking [Best-Synced] blockhash failed: " ++ show e)
            throw KeyValueDBInsertException

checkBlocksFullySynced :: (HasLogger m, MonadIO m) => XCqlClientState -> m Bool
checkBlocksFullySynced conn = do
    lg <- getLogger
    let qstr :: Q.QueryString Q.R (Text, Text) (Identity (Maybe Bool, Int32, Maybe Int64, Text))
        qstr = "SELECT value FROM xoken.misc_store WHERE key IN (?,?)"
        par = getSimpleQueryParam (T.pack "best-synced", T.pack "best_chain_tip")
    res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right results ->
            if L.length results /= 2
                then do
                    err lg $
                        LG.msg $
                        val $
                        C.pack $
                        "checkBlocksFullySynced: misc_store missing entries for best-synced, best_chain_tip or both"
                    return False
                else do
                    let Identity (_, h1, _, _) = results !! 0
                        Identity (_, h2, _, _) = results !! 1
                    return (h1 == h2)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "checkBlocksFullySynced: error while querying DB: " ++ show e
            return False

getBatchSizeMainnet :: Int32 -> Int32 -> [Int32]
getBatchSizeMainnet peerCount n
    | n < 200000 =
        if peerCount > 8
            then [1 .. 4]
            else [1 .. 2]
    | n >= 200000 && n < 540000 =
        if peerCount > 4
            then [1 .. 2]
            else [1]
    | otherwise = [1]

getBatchSizeTestnet :: Int32 -> Int32 -> [Int32]
getBatchSizeTestnet peerCount n
    | peerCount > 4 = [1 .. 2]
    | otherwise = [1]

getBatchSize :: Network -> Int32 -> Int32 -> [Int32]
getBatchSize net peerCount n
    | (getNetworkName net == "bsvtest") = getBatchSizeTestnet peerCount n
    | otherwise = getBatchSizeMainnet peerCount n

runBlockCacheQueue :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runBlockCacheQueue =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        dbe <- getDB
        !tm <- liftIO $ getCurrentTime
        trace lg $ LG.msg $ val "runBlockCacheQueue loop..."
        let nc = nodeConfig bp2pEnv
            net = bitcoinNetwork $ nc
            conn = xCqlClientState dbe
        allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        let connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
        syt' <- liftIO $ TSH.toList (blockSyncStatusMap bp2pEnv)
        let syt = L.sortBy (\(_, (_, h)) (_, (_, h')) -> compare h h') syt'
            sysz = fromIntegral $ L.length syt
        fullySynced <- liftIO $ readTVarIO $ indexUnconfirmedTx bp2pEnv
        -- check any retries
        retn1 <-
            do trace lg $ LG.msg $ val "Checking for retries"
               let unsent = L.filter (\x -> (fst $ snd x) == RequestQueued) syt
               let sent =
                       L.filter
                           (\x ->
                                case fst $ snd x of
                                    RequestSent _ -> True
                                    otherwise -> False)
                           syt
               let recvNotStarted =
                       L.filter
                           (\(_, ((RequestSent t), _)) ->
                                (diffUTCTime tm t > (fromIntegral $ getDataResponseTimeout nc)))
                           sent
               let receiveInProgress =
                       L.filter
                           (\x ->
                                case fst $ snd x of
                                    RecentTxReceiveTime _ -> True
                                    otherwise -> False)
                           syt
               let recvTimedOut =
                       L.filter
                           (\(_, ((RecentTxReceiveTime (t, c)), _)) ->
                                (diffUTCTime tm t > (fromIntegral $ recentTxReceiveTimeout nc)))
                           receiveInProgress
               let recvComplete =
                       L.filter
                           (\x ->
                                case fst $ snd x of
                                    BlockReceiveComplete _ -> True
                                    otherwise -> False)
                           syt
               let processingIncomplete =
                       L.filter
                           (\(_, ((BlockReceiveComplete t), _)) ->
                                (diffUTCTime tm t > (fromIntegral $ blockProcessingTimeout nc)))
                           recvComplete
                -- all blocks received, empty the cache, cache-miss gracefully
               if L.length processingIncomplete > 0
                   then return $ mkBlkInf $ getHead processingIncomplete
                   else if L.length recvTimedOut > 0
                            then return $ mkBlkInf $ getHead recvTimedOut
                            else if L.length recvNotStarted > 0
                                     then return $ mkBlkInf $ getHead recvNotStarted
                                     else if L.length unsent > 0
                                              then return $ mkBlkInf $ getHead unsent
                                              else return Nothing
        -- reload cache
        retn <-
            case retn1 of
                Just x -> return $ Just x
                Nothing -> do
                    if sysz < (blocksFetchWindow $ nodeConfig bp2pEnv)
                        then do
                            (hash, ht) <- fetchBestSyncedBlock
                            let fetchMore = (blocksFetchWindow $ nodeConfig bp2pEnv) - sysz
                            let cacheInd = [1 .. fetchMore] -- getBatchSize net (fromIntegral $ L.length connPeers) ht
                            let !bks = map (\x -> ht + (fromIntegral x)) cacheInd
                            let qstr :: Q.QueryString Q.R (Identity [Int32]) ((Int32, T.Text))
                                qstr =
                                    "SELECT block_height, block_hash from xoken.blocks_by_height where block_height in ?"
                                p = getSimpleQueryParam $ Identity (bks)
                            res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
                            case res of
                                Left (e :: SomeException) -> do
                                    err lg $ LG.msg ("Error: runBlockCacheQueue: " ++ show e)
                                    throw e
                                Right (op) -> do
                                    if L.length op == 0
                                        then do
                                            trace lg $ LG.msg $ val "Synced fully!"
                                            return (Nothing)
                                        else do
                                            p <-
                                                mapM
                                                    (\x ->
                                                         case (hexToBlockHash $ snd x) of
                                                             Just h -> do
                                                                 val <-
                                                                     liftIO $ TSH.lookup (blockSyncStatusMap bp2pEnv) h
                                                                 case val of
                                                                     Nothing -> do
                                                                         return $
                                                                             Just
                                                                                 ( h
                                                                                 , (RequestQueued, fromIntegral $ fst x))
                                                                     Just v -> return Nothing
                                                             Nothing -> return Nothing)
                                                    (op)
                                            let cmp = catMaybes p
                                            if L.null cmp
                                                then do
                                                    trace lg $ LG.msg $ val "Nothing to add."
                                                    return Nothing
                                                else do
                                                    debug lg $ LG.msg $ val "Adding to cache."
                                                    mapM
                                                        (\(k, v) -> do
                                                             liftIO $ TSH.insert (blockSyncStatusMap bp2pEnv) k v
                                                             liftIO $
                                                                 catch
                                                                     (liftIO $
                                                                      TSH.new 32 >>= \pie ->
                                                                          liftIO $
                                                                          TSH.insert (protocolInfo bp2pEnv) k pie)
                                                                     (\(e :: SomeException) ->
                                                                          err lg $
                                                                          LG.msg $
                                                                          "Error: Failed to insert into protocolInfo TSH (key " <>
                                                                          (show k) <> "): " <> (show e)))
                                                        (cmp)
                                                    let e = cmp !! 0
                                                    return (Just $ BlockInfo (fst e) (snd $ snd e))
                        else return Nothing
        --
        mapM_
            (\(bsh, (_, ht)) -> do
                 valx <- liftIO $ TSH.lookup (blockTxProcessingLeftMap bp2pEnv) (bsh)
                 case valx of
                     Just xv -> do
                         siza <- liftIO $ TSH.toList (fst xv)
                         if ((sum $ snd $ unzip siza) == snd xv)
                             then do
                                 liftIO $ TSH.insert (blockSyncStatusMap bp2pEnv) (bsh) (BlockProcessingComplete, ht)
                             else return ()
                     Nothing -> return ())
            (syt)
        --
        trace lg $ LG.msg $ ("blockSyncStatusMap (list): " ++ (show syt))
        syncList <- liftIO $ TSH.toList (blockSyncStatusMap bp2pEnv)
        let sortedList = L.sortOn (snd . snd) syncList
        trace lg $ LG.msg $ ("sorted blockSyncStatusMap (list): " ++ (show sortedList))
        let compl = L.takeWhile (\x -> (fst $ snd x) == BlockProcessingComplete) sortedList
        if not $ L.null compl
            then do
                let lelm = last $ compl
                debug lg $ LG.msg $ "Marking best synced block: " <> show lelm
                markBestSyncedBlock (fst $ lelm) (fromIntegral $ snd $ snd $ lelm) conn
                mapM
                    (\(bsh, (_, ht)) -> do
                         trace lg $ LG.msg $ "deleting from blockSyncStatusMap: " <> show bsh
                         liftIO $ TSH.delete (blockSyncStatusMap bp2pEnv) bsh
                         liftIO $ TSH.delete (blockTxProcessingLeftMap bp2pEnv) bsh
                         --
                         LA.async $
                             liftIO $
                             catch
                                 (do v <- liftIO $ TSH.lookup (protocolInfo bp2pEnv) bsh
                                     case v of
                                         Just v' -> do
                                             pi <- liftIO $ TSH.toList v'
                                             debug lg $
                                                 LG.msg $
                                                 "Number of protocols for block: " <>
                                                 show (Prelude.length pi) <> " height: " <> show ht
                                             pres <-
                                                 liftIO $
                                                 try $ do
                                                     runInBatch
                                                         (\(protocol, (props, blockInf)) -> do
                                                              TSH.delete v' protocol
                                                              withResource'
                                                                  (pool $ graphDB dbe)
                                                                  (`BT.run` insertProtocolWithBlockInfo
                                                                                protocol
                                                                                props
                                                                                blockInf))
                                                         pi
                                                         (maxInsertsProtocol $ nodeConfig bp2pEnv)
                                                     TSH.delete (protocolInfo bp2pEnv) bsh
                                             case pres of
                                                 Right rt -> return ()
                                                 Left (e :: SomeException) -> do
                                                     err lg $
                                                         LG.msg $
                                                         "Error: Failed to insert protocol with blockInfo:" <>
                                                         (show (bsh, ht)) <> ": " <> (show e)
                                                     throw MerkleSubTreeDBInsertException
                                         Nothing -> do
                                             debug lg $
                                                 LG.msg $ "Debug: No Information available for block hash " ++ show bsh
                                             return ()
                                     return ())
                                 (\(e :: SomeException) ->
                                      err lg $
                                      LG.msg $
                                      "Error: Failed to insert into graph DB block " <>
                                      (show (bsh, ht)) <> ": " <> (show e))
                         --
                     )
                    compl
                return ()
            else return ()
        --                                                   
        case retn of
            Just bbi -> do
                latest <- liftIO $ newIORef True
                sortedPeers <- liftIO $ sortPeers (snd $ unzip connPeers)
                mapM_
                    (\pr -> do
                         ltst <- liftIO $ readIORef latest
                         if ltst
                             then do
                                 trace lg $ LG.msg $ "try putting mvar.. " ++ (show bbi)
                                 fl <- liftIO $ tryPutMVar (blockFetchQueue pr) bbi
                                 if fl
                                     then do
                                         trace lg $ LG.msg $ "done putting mvar.. " ++ (show bbi)
                                         !tm <- liftIO $ getCurrentTime
                                         liftIO $
                                             TSH.insert
                                                 (blockSyncStatusMap bp2pEnv)
                                                 (biBlockHash bbi)
                                                 (RequestSent tm, biBlockHeight bbi)
                                         liftIO $ writeIORef latest False
                                     else return ()
                             else return ())
                    (sortedPeers)
            Nothing -> do
                trace lg $ LG.msg $ "nothing yet" ++ ""
        --
        liftIO $ threadDelay (100000) -- 0.1 sec
        return ()
  where
    getHead l = head $ L.sortOn (snd . snd) (l)
    mkBlkInf h = Just $ BlockInfo (fst h) (snd $ snd h)

sortPeersRandom :: [BitcoinPeer] -> IO ([BitcoinPeer])
sortPeersRandom peers = do
    mark <- randomRIO (0, (L.length peers - 1))
    let parts = L.splitAt mark peers
    return $ snd parts ++ fst parts

sortPeers :: [BitcoinPeer] -> IO ([BitcoinPeer])
sortPeers peers = do
    let longlongago = UTCTime (ModifiedJulianDay 1) 1
    ts <-
        mapM
            (\p -> do
                 lstr <- liftIO $ readIORef $ ptLastTxRecvTime $ statsTracker p
                 case lstr of
                     Just lr -> return lr
                     Nothing -> return longlongago)
            peers
    return $ snd $ unzip $ L.sortBy (\(a, _) (b, _) -> compare a b) (zip ts peers)

commitScriptHashOutputs ::
       (HasXokenNodeEnv env m, MonadIO m)
    => XCqlClientState
    -> Text
    -> (Text, Int32)
    -> Maybe (Text, Int32, Int32)
    -> m ()
commitScriptHashOutputs conn sh output blockInfo = do
    lg <- getLogger
    tm <- liftIO getCurrentTime
    blocksSynced <- checkBlocksFullySynced conn
    nominalTxIndex <- generateNominalTxIndex blockInfo (snd output)
    let qstrAddrOuts :: Q.QueryString Q.W (Text, Int64, (Text, Int32)) ()
        qstrAddrOuts = "INSERT INTO xoken.script_hash_outputs (script_hash, nominal_tx_index, output) VALUES (?,?,?)"
        parAddrOuts = getSimpleQueryParam (sh, nominalTxIndex, output)
    queryId <- liftIO $ queryPrepared conn (Q.RqPrepare (Q.Prepare qstrAddrOuts))
    resAddrOuts <- liftIO $ try $ write conn (Q.RqExecute (Q.Execute queryId parAddrOuts))
    case resAddrOuts of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into 'script_hash_outputs': " ++ show e
            throw KeyValueDBInsertException

insertTxIdOutputs ::
       (HasXokenNodeEnv env m, MonadIO m)
    => (Text, Int32)
    -> Text
    -> Text
    -> Bool
    -> Maybe (Text, Int32, Int32)
    -> [((Text, Int32), Int32, (Text, Int64))]
    -> Int64
    -> m ()
insertTxIdOutputs (txid, outputIndex) address scriptHash isRecv blockInfo other value = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
    res <-
        case blockInfo of
            Nothing ->
                let queryStr =
                        fromString $
                        "UPDATE xoken.txid_outputs SET address=?, script_hash=?, other=?, value=? " ++
                        "WHERE txid=? AND output_index=? AND is_recv=?" :: Q.QueryString Q.W ( Text
                                                                                             , Text
                                                                                             , [( (Text, Int32)
                                                                                                , Int32
                                                                                                , (Text, Int64))]
                                                                                             , Int64
                                                                                             , Text
                                                                                             , Int32
                                                                                             , Bool) ()
                    queryPar = getSimpleQueryParam (address, scriptHash, other, value, txid, outputIndex, isRecv)
                 in liftIO $
                    queryPrepared conn (Q.RqPrepare (Q.Prepare queryStr)) >>= \queryId ->
                        liftIO $ try $ write conn (Q.RqExecute (Q.Execute queryId queryPar))
            Just bi ->
                let queryStr =
                        fromString $
                        "UPDATE xoken.txid_outputs SET address=?, script_hash=?, block_info=?, other=?, value=? " ++
                        "WHERE txid=? AND output_index=? AND is_recv=?" :: Q.QueryString Q.W ( Text
                                                                                             , Text
                                                                                             , (Text, Int32, Int32)
                                                                                             , [( (Text, Int32)
                                                                                                , Int32
                                                                                                , (Text, Int64))]
                                                                                             , Int64
                                                                                             , Text
                                                                                             , Int32
                                                                                             , Bool) ()
                    queryPar = getSimpleQueryParam (address, scriptHash, bi, other, value, txid, outputIndex, isRecv)
                 in liftIO $
                    queryPrepared conn (Q.RqPrepare (Q.Prepare queryStr)) >>= \queryId ->
                        liftIO $ try $ write conn (Q.RqExecute (Q.Execute queryId queryPar))
    case res of
        Left (e :: SomeException) -> do
            err lg $
                LG.msg $
                C.pack $
                "[ERROR] While inserting outputs for " <>
                (T.unpack txid) <> ":" <> (show outputIndex) <> ": " <> (show e)
            throw e
        _ ->
            debug lg $
            LG.msg $ "Updated/Inserted outputs (block " ++ show blockInfo ++ ") for tx:" ++ show (txid, outputIndex)

commitTxPage ::
       (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m)
    => [TxHash]
    -> BlockHash
    -> Int32
    -> m ()
commitTxPage txhash bhash page = do
    dbe' <- getDB
    lg <- getLogger
    let conn = xCqlClientState $ dbe'
        txids = txHashToHex <$> txhash
        qstr :: Q.QueryString Q.W (Text, Int32, [Text]) ()
        qstr = "insert INTO xoken.blockhash_txids (block_hash, page_number, txids) values (?, ?, ?)"
        par = getSimpleQueryParam (blockHashToHex bhash, page, txids)
    res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            liftIO $ err lg $ LG.msg ("Error: INSERTing into 'xoken.blockhash_txids': " ++ show e)
            throw KeyValueDBInsertException

processConfTransaction ::
       (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BlockIngestState -> Tx -> BlockHash -> Int -> Int -> m ()
processConfTransaction bis tx bhash blkht txind = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let !txhs = txHash tx
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        conn = xCqlClientState dbe'
    debug lg $ LG.msg $ "Processing confirmed transaction <begin> :" ++ show txhs
    let !inAddrs = zip (txIn tx) [0 :: Int32 ..]
    let !outAddrs =
            zip3
                (map (\y ->
                          case scriptToAddressBS $ scriptOutput y of
                              Left e -> "" -- avoid null values in Cassandra wherever possible
                              Right os ->
                                  case addrToString net os of
                                      Nothing -> ""
                                      Just addr -> addr)
                     (txOut tx))
                (txOut tx)
                [0 :: Int32 ..]
    --
    -- lookup into tx outputs value cache if cache-miss, fetch from DB
    !inputs <-
        mapM
            (\(b, j) -> do
                 tuple <- return Nothing
                    --  liftIO $
                    --  TSH.lookup
                    --      (txOutputValuesCache bp2pEnv)
                    --      (getTxShortHash (outPointHash $ prevOutput b) (txOutputValuesCacheKeyBits $ nodeConfig bp2pEnv))
                 val <-
                     case tuple of
                         Just (ftxh, indexvals) ->
                             if ftxh == (outPointHash $ prevOutput b)
                                 then do
                                     trace lg $ LG.msg $ C.pack $ "txOutputValuesCache: cache-hit"
                                     let rr =
                                             head $
                                             filter
                                                 (\x -> fst x == (fromIntegral $ outPointIndex $ prevOutput b))
                                                 indexvals
                                     return $ snd $ rr
                                 else do
                                     if (outPointHash nullOutPoint) == (outPointHash $ prevOutput b)
                                         then return
                                                  ( ""
                                                  , ""
                                                  , fromIntegral $ computeSubsidy net $ (fromIntegral blkht :: Word32))
                                         else do
                                             debug lg $ LG.msg $ C.pack $ "txOutputValuesCache: cache-miss (1)"
                                             dbRes <-
                                                 liftIO $
                                                 LE.try $
                                                 getSatsValueFromOutpoint
                                                     conn
                                                     (txSynchronizer bp2pEnv)
                                                     lg
                                                     net
                                                     (prevOutput b)
                                                     (5)
                                                     (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                                                     True
                                             case dbRes of
                                                 Right v -> return $ v
                                                 Left (e :: SomeException) -> do
                                                     err lg $
                                                         LG.msg $
                                                         "Error: [pCT calling gSVFO] WHILE Processing TxID " ++
                                                         show (txHashToHex txhs) ++
                                                         ", getting value for dependent input (TxID,Index): (" ++
                                                         show (txHashToHex $ outPointHash (prevOutput b)) ++
                                                         ", " ++ show (outPointIndex $ prevOutput b) ++ ")"
                                                     throw e
                         Nothing -> do
                             if (outPointHash nullOutPoint) == (outPointHash $ prevOutput b)
                                 then return
                                          ("", "", fromIntegral $ computeSubsidy net $ (fromIntegral blkht :: Word32))
                                 else do
                                     debug lg $ LG.msg $ C.pack $ "txOutputValuesCache: cache-miss (2)"
                                     debug lg $
                                         LG.msg $
                                         " outputindex: " ++
                                         show
                                             ( txHashToHex $ outPointHash $ prevOutput b
                                             , fromIntegral $ outPointIndex $ prevOutput b)
                                     dbRes <-
                                         liftIO $
                                         LE.try $
                                         getSatsValueFromOutpoint
                                             conn
                                             (txSynchronizer bp2pEnv)
                                             lg
                                             net
                                             (prevOutput b)
                                             (5)
                                             (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                                             True
                                     case dbRes of
                                         Right v -> do
                                             debug lg $
                                                 LG.msg $ " value returned from getStatsValueFromOutpoint: " ++ show v
                                             return $ v
                                         Left (e :: SomeException) -> do
                                             err lg $
                                                 LG.msg $
                                                 "Error: [pCT calling gSVFO] WHILE Processing TxID " ++
                                                 show (txHashToHex txhs) ++
                                                 ", getting value for dependent input (TxID,Index): (" ++
                                                 show (txHashToHex $ outPointHash (prevOutput b)) ++
                                                 ", " ++ show (outPointIndex $ prevOutput b) ++ ")"
                                             throw e
                 return
                     ((txHashToHex $ outPointHash $ prevOutput b, fromIntegral $ outPointIndex $ prevOutput b), j, val))
            inAddrs
    -- calculate Tx fees
    let !ipSum = foldl (+) 0 $ (\(_, _, (_, _, val)) -> val) <$> inputs
        !opSum = foldl (+) 0 $ (\(_, o, _) -> fromIntegral $ outValue o) <$> outAddrs
        !fees =
            if txind == 0 -- coinbase tx
                then 0
                else ipSum - opSum
        serbs = runPutLazy $ putLazyByteString $ S.encodeLazy tx
        count = BSL.length serbs
    --
    debug lg $ LG.msg $ "Processing confirmed transaction <fetched inputs> :" ++ show txhs
    trace lg $ LG.msg $ "processing Tx " ++ show txhs ++ ": calculated fees"
    trace lg $ LG.msg $ "processing Tx " ++ show txhs ++ ": fetched input(s): " ++ show inputs
    -- handle allegory
    eres <- LE.try $ handleIfAllegoryTx tx True True
    case eres of
        Right (flg) -> return ()
        Left (e :: SomeException) -> err lg $ LG.msg ("Error: " ++ show e)
    --
    -- cache compile output values
    -- imp: order is (address, scriptHash, value)
    let ovs =
            map
                (\(a, o, i) ->
                     ( fromIntegral $ i
                     , (a, (txHashToHex $ TxHash $ sha256 (scriptOutput o)), fromIntegral $ outValue o)))
                outAddrs
    trace lg $ LG.msg $ "processing Tx " ++ show txhs ++ ": compiled output value(s): " ++ (show ovs)
    -- liftIO $
    --     TSH.insert
    --         (txOutputValuesCache bp2pEnv)
    --         (getTxShortHash (txHash tx) (txOutputValuesCacheKeyBits $ nodeConfig bp2pEnv))
    --         (txHash tx, ovs)
    trace lg $ LG.msg $ "processing Tx " ++ show txhs ++ ": added outputvals to cache"
    -- update outputs and scripthash tables
    mapM_
        (\(a, o, i) -> do
             let sh = txHashToHex $ TxHash $ sha256 (scriptOutput o)
             let bi = (blockHashToHex bhash, fromIntegral blkht, fromIntegral txind)
             let output = (txHashToHex txhs, i)
             let bsh = B16.encode $ scriptOutput o
             let (op, rem) = B.splitAt 2 bsh
             let (op_false, op_return, remD) =
                     if op == "6a"
                         then ("00", op, rem)
                         else (\(a, b) -> (op, a, b)) $ B.splitAt 2 rem
             outputsExist <- checkOutputDataExists output
             unless outputsExist $ do
                 commitScriptHashOutputs conn sh output (Just bi)
                 when (op_false == "00" && op_return == "6a") $ do
                     props <-
                         case runGet (getPropsG 3) (fst $ B16.decode remD) of
                             Right p -> return p
                             Left str -> do
                                 liftIO $
                                     err lg $
                                     LG.msg
                                         ("Error: Getting protocol name " ++
                                          show str ++
                                          " for height: " ++
                                          show blkht ++ " with bsh: " ++ show bsh ++ " with remD: " ++ show remD)
                                 return []
                     when (isJust (headMaybe props)) $ do
                         let protocol = snd <$> props
                         ts <- (blockTimestamp' . DA.blockHeader . head) <$> xGetChainHeaders (fromIntegral blkht) 1
                         let epd = utctDay $ posixSecondsToUTCTime 0
                         let cd = utctDay $ posixSecondsToUTCTime (fromIntegral ts)
                         let sec = diffUTCTime (posixSecondsToUTCTime (fromIntegral ts)) (posixSecondsToUTCTime 0)
                         let (m, d) = diffGregorianDurationClip cd epd
                         let (y', _, _) = toGregorian cd
                         let (y'', _, _) = toGregorian epd
                         let y = y' - y''
                         let h = div (fromEnum $ sec / (10 ^ 12)) (60 * 60)
                         let curBi =
                                 BlockPInfo
                                     { height = blkht
                                     , hash = T.pack $ show bhash
                                     , timestamp = fromIntegral ts
                                     , hour = h
                                     , day =
                                           let hh = div h 24
                                            in if hh == 0
                                                   then 1
                                                   else hh
                                     , absoluteHour = Prelude.rem h 24
                                     , month = 1 + fromIntegral m
                                     , year = fromIntegral y
                                     , fees = fromIntegral fees
                                     , bytes = binBlockSize bis
                                     , count = 1
                                     }
                         let upf b =
                                 BlockPInfo
                                     blkht
                                     (Type.hash b)
                                     (Type.timestamp b)
                                     (hour b)
                                     (day b)
                                     (month b)
                                     (year b)
                                     ((fromIntegral fees) + (Type.fees b))
                                     ((binBlockSize bis) + (Type.bytes b))
                                     ((Type.count b) + 1)
                                     (absoluteHour b)
                         let fn x =
                                 case x of
                                     Just (p, bi) -> (Just (props, upf bi), ())
                                     Nothing -> (Just (props, curBi), ())
                             prot = tail $ L.inits protocol
                         mapM_
                             (\p ->
                                  commitScriptOutputProtocol
                                      conn
                                      (T.intercalate "." p)
                                      output
                                      (Just bi)
                                      fees
                                      (fromIntegral count))
                             prot
                         v <- liftIO $ TSH.lookup (protocolInfo bp2pEnv) bhash
                         case v of
                             Just v' -> liftIO $ TSH.mutate v' (T.intercalate "_" protocol) fn
                             Nothing -> debug lg $ LG.msg $ "No ProtocolInfo Available for: " ++ show bhash
             insertTxIdOutputs output a sh True (Just bi) (stripScriptHash <$> inputs) (fromIntegral $ outValue o))
        outAddrs
    debug lg $ LG.msg $ "Processing confirmed transaction <committed outputs> :" ++ show txhs
    mapM_
        (\((o, i), (a, sh)) -> do
             let bi = (blockHashToHex bhash, fromIntegral blkht, fromIntegral txind)
             let blockHeight = fromIntegral blkht
             let prevOutpoint = (txHashToHex $ outPointHash $ prevOutput o, fromIntegral $ outPointIndex $ prevOutput o)
             let spendInfo = (\ov -> ((txHashToHex txhs, fromIntegral $ fst $ ov), i, snd $ ov)) <$> ovs
             if a == "" || sh == "" -- likely coinbase txns
                 then return ()
                 else do
                     insertTxIdOutputs prevOutpoint a sh False (Just bi) (stripScriptHash <$> spendInfo) 0)
        (zip (inAddrs) (map (\x -> (fst3 $ thd3 x, snd3 $ thd3 $ x)) inputs))
    debug lg $ LG.msg $ "Processing confirmed transaction <updated inputs> :" ++ show txhs
    --
    trace lg $ LG.msg $ "processing Tx " ++ show txhs ++ ": updated spend info for inputs"
    -- persist tx
    let qstr :: Q.QueryString Q.W ((Text, Int32, Int32), Blob, [((Text, Int32), Int32, (Text, Int64))], Int64, Text) ()
        qstr = "UPDATE xoken.transactions SET block_info=?, tx_serialized=?, inputs=?, fees=? WHERE tx_id=?"
        smb a = a * 16 * 1000 * 1000
        segments =
            let (d, m) = divMod count (smb 1)
             in d +
                (if m == 0
                     then 0
                     else 1)
        fst =
            if segments > 1
                then (LC.replicate 32 'f') <> (BSL.fromStrict $ DTE.encodeUtf8 $ T.pack $ show $ segments)
                else serbs
    let par =
            getSimpleQueryParam
                ( (blockHashToHex bhash, fromIntegral blkht, fromIntegral txind)
                , Blob fst
                , (stripScriptHash <$> inputs)
                , fees
                , txHashToHex txhs)
    queryI <- liftIO $ queryPrepared conn (Q.RqPrepare $ Q.Prepare qstr)
    res <- liftIO $ try $ write conn (Q.RqExecute $ Q.Execute queryI par)
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            liftIO $ err lg $ LG.msg ("Error: INSERTing into 'xoken.transactions': " ++ show e)
            throw KeyValueDBInsertException
    when (segments > 1) $ do
        let segmentsData = chunksOf (smb 1) serbs
        mapM_
            (\(seg, i) -> do
                 let par =
                         getSimpleQueryParam
                             ( (blockHashToHex bhash, fromIntegral blkht, fromIntegral txind)
                             , Blob seg
                             , []
                             , fees
                             , (txHashToHex txhs) <> (T.pack $ show i))
                 queryI <- liftIO $ queryPrepared conn (Q.RqPrepare $ Q.Prepare qstr)
                 res <- liftIO $ try $ write conn (Q.RqExecute $ Q.Execute queryI par)
                 case res of
                     Right _ -> return ()
                     Left (e :: SomeException) -> do
                         liftIO $ err lg $ LG.msg ("Error: INSERTing into 'xoken.transactions': " ++ show e)
                         throw KeyValueDBInsertException)
            (zip segmentsData [1 ..])
    --
    trace lg $ LG.msg $ "processing Tx " ++ show txhs ++ ": persisted in DB"
    --
    trace lg $ LG.msg $ "processing Tx " ++ show txhs ++ ": handled Allegory Tx"
    -- signal 'done' event for tx's that were processed out of sequence
    --
    vall <- liftIO $ TSH.lookup (txSynchronizer bp2pEnv) txhs
    case vall of
        Just ev -> liftIO $ EV.signal ev
        Nothing -> return ()
    debug lg $ LG.msg $ "Processing confirmed transaction <end> :" ++ show txhs

--
--
getSatsValueFromOutpoint ::
       XCqlClientState
    -> TSH.TSHashTable TxHash EV.Event
    -> Logger
    -> Network
    -> OutPoint
    -> Int
    -> Int
    -> Bool
    -> IO ((Text, Text, Int64))
getSatsValueFromOutpoint conn txSync lg net outPoint wait maxWait confirmedOnly = do
    let qstr :: Q.QueryString Q.R (Text, Int32, Bool) (Text, Text, Int64, Maybe (Text, Int32, Int32))
        qstr =
            "SELECT address, script_hash, value, block_info FROM xoken.txid_outputs WHERE txid=? AND output_index=? AND is_recv=?"
        par = getSimpleQueryParam (txHashToHex $ outPointHash outPoint, fromIntegral $ outPointIndex outPoint, True)
    res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right results -> do
            if (L.length results == 0) ||
               ((\r ->
                     case r of
                         [] -> False
                         (_, _, _, bi):_ -> isNothing bi)
                    results &&
                confirmedOnly)
                then do
                    debug lg $
                        LG.msg $
                        "getSatsValueFromOutpoint txid: " ++
                        (show outPoint) ++
                        ", results: " ++
                        (show $ L.length results) ++
                        ", blockInfo: " ++
                        (show $ (\(_, _, _, bi) -> bi) <$> results) ++ ", confirmed: " ++ (show confirmedOnly)
                    debug lg $
                        LG.msg $
                        "Tx not found: " ++ (show $ txHashToHex $ outPointHash outPoint) ++ " _waiting_ for event"
                    valx <- liftIO $ TSH.lookup txSync (outPointHash outPoint)
                    event <-
                        case valx of
                            Just evt -> return evt
                            Nothing -> EV.new
                    liftIO $ TSH.insert txSync (outPointHash outPoint) event
                    tofl <- waitTimeout event (1000000 * (fromIntegral wait))
                    if tofl == False
                        then if ((2 * wait) < maxWait)
                                 then do
                                     getSatsValueFromOutpoint
                                         conn
                                         txSync
                                         lg
                                         net
                                         outPoint
                                         (2 * wait)
                                         maxWait
                                         confirmedOnly
                                 else do
                                     liftIO $ TSH.delete txSync (outPointHash outPoint)
                                     debug lg $
                                         LG.msg $
                                         "TxIDNotFoundException: While querying txid_outputs for (TxID, Index): " ++
                                         (show $ txHashToHex $ outPointHash outPoint) ++
                                         ", " ++ show (outPointIndex outPoint) ++ ")"
                                     throw $
                                         TxIDNotFoundException
                                             ( show $ txHashToHex $ outPointHash outPoint
                                             , Just $ fromIntegral $ outPointIndex outPoint)
                                             "getSatsValueFromOutpoint"
                        else do
                            debug lg $
                                LG.msg $ "event received _available_: " ++ (show $ txHashToHex $ outPointHash outPoint)
                            getSatsValueFromOutpoint conn txSync lg net outPoint wait maxWait confirmedOnly
                else do
                    let (addr, scriptHash, val, blockInfo) = head $ results
                    return $ (addr, scriptHash, val)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getSatsValueFromOutpoint: " ++ show e
            throw e

--
getScriptHashFromOutpoint ::
       XCqlClientState -> TSH.TSHashTable TxHash EV.Event -> Logger -> Network -> OutPoint -> Int -> IO (Maybe Text)
getScriptHashFromOutpoint conn txSync lg net outPoint waitSecs = do
    let str = "SELECT tx_serialized from xoken.transactions where tx_id = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Identity Blob)
        p = getSimpleQueryParam $ Identity $ txHashToHex $ outPointHash outPoint
    res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg ("Error: getScriptHashFromOutpoint: " ++ show e)
            throw e
        Right (iop) -> do
            if L.length iop == 0
                then do
                    debug lg $
                        LG.msg ("TxID not found: (waiting for event) " ++ (show $ txHashToHex $ outPointHash outPoint))
                    --
                    -- tmap <- liftIO $ takeMVar (txSync)
                    valx <- liftIO $ TSH.lookup txSync (outPointHash outPoint)
                    event <-
                        case valx of
                            Just evt -> return evt
                            Nothing -> EV.new
                    -- liftIO $ putMVar (txSync) (M.insert (outPointHash outPoint) event tmap)
                    liftIO $ TSH.insert txSync (outPointHash outPoint) event
                    tofl <- waitTimeout event (1000000 * (fromIntegral waitSecs))
                    if tofl == False -- False indicates a timeout occurred.
                            -- tmap <- liftIO $ takeMVar (txSync)
                            -- liftIO $ putMVar (txSync) (M.delete (outPointHash outPoint) tmap)
                        then do
                            liftIO $ TSH.delete txSync (outPointHash outPoint)
                            debug lg $ LG.msg ("TxIDNotFoundException" ++ (show $ txHashToHex $ outPointHash outPoint))
                            throw $
                                TxIDNotFoundException
                                    ( show $ txHashToHex $ outPointHash outPoint
                                    , Just $ fromIntegral $ outPointIndex outPoint)
                                    "getScriptHashFromOutpoint"
                        else getScriptHashFromOutpoint conn txSync lg net outPoint waitSecs -- if being signalled, try again to success
                    --
                    return Nothing
                else do
                    let txbyt = runIdentity $ iop !! 0
                    ctxbyt <-
                        if isSegmented (fromBlob txbyt)
                            then liftIO $
                                 getCompleteTx
                                     conn
                                     (txHashToHex $ outPointHash outPoint)
                                     (getSegmentCount (fromBlob txbyt))
                            else pure $ fromBlob txbyt
                    case runGetLazy (getConfirmedTx) ctxbyt of
                        Left e -> do
                            debug lg $ LG.msg (encodeHex $ BSL.toStrict ctxbyt)
                            throw DBTxParseException
                        Right (txd) -> do
                            case txd of
                                Just tx ->
                                    if (fromIntegral $ outPointIndex outPoint) > (L.length $ txOut tx)
                                        then throw InvalidOutpointException
                                        else do
                                            let output = (txOut tx) !! (fromIntegral $ outPointIndex outPoint)
                                            return $ Just $ txHashToHex $ TxHash $ sha256 (scriptOutput output)
                                Nothing -> return Nothing

processBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => DefBlock -> m ()
processBlock dblk = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    debug lg $ LG.msg ("processing deflated Block! " ++ show dblk)
    -- liftIO $ signalQSem (blockFetchBalance bp2pEnv)
    return ()

handleIfAllegoryTx :: (HasXokenNodeEnv env m, MonadIO m) => Tx -> Bool -> Bool -> m (Bool)
handleIfAllegoryTx tx revert confirmed = do
    dbe <- getDB
    lg <- getLogger
    trace lg $ LG.msg $ val $ "Checking for Allegory OP_RETURN"
    let op_return = head (txOut tx)
        hexstr = B16.encode (scriptOutput op_return)
        txid = T.unpack . txHashToHex . txHash $ tx
    if "006a0f416c6c65676f72792f416c6c506179" `L.isPrefixOf` (C.unpack hexstr)
        then do
            liftIO $ print (hexstr)
            case decodeOutputScript $ scriptOutput op_return of
                Right (script) -> do
                    liftIO $ print (script)
                    case last $ scriptOps script of
                        (OP_PUSHDATA payload _) -> do
                            case (deserialiseOrFail $ BSL.fromStrict payload) of
                                Right (allegory) -> do
                                    liftIO $ print (allegory)
                                    checkNodes <- LE.try $ nodesExist (txHash tx) allegory confirmed
                                    nodesAlreadyExist <-
                                        case checkNodes of
                                            Left (e :: SomeException) -> do
                                                err lg $
                                                    LG.msg $
                                                    C.pack $
                                                    "[ERROR] Failed to check if nUTXO nodes exist for outputs of transaction " <>
                                                    txid
                                                throw e
                                            Right res -> return res
                                    if nodesAlreadyExist
                                        then do
                                            debug lg $
                                                LG.msg $
                                                C.pack $
                                                "handleIfAllegoryTx: nUTXO nodes already exist for outputs of transaction " <>
                                                txid
                                            return True
                                        else do
                                            if revert
                                                then do
                                                    _ <- resdb lg dbe revertAllegoryStateTree tx allegory
                                                    resdb lg dbe (updateAllegoryStateTrees confirmed) tx allegory
                                                else resdb lg dbe (updateAllegoryStateTrees confirmed) tx allegory
                                Left (e :: DeserialiseFailure) -> do
                                    err lg $ LG.msg $ "error deserialising OP_RETURN CBOR data" ++ show e
                                    throw e
                Left (e) -> do
                    err lg $ LG.msg $ "error decoding op_return data:" ++ show e
                    throw MessageParsingException
        else do
            return False
  where
    resdb lg db fn tx al = do
        let txid = T.unpack . txHashToHex . txHash $ tx
        eres <- liftIO $ try $ withResource' (pool $ graphDB db) (`BT.run` fn tx al)
        case eres of
            Right () -> return True
            Left (SomeException e) -> do
                err lg $
                    LG.msg $
                    "[ERROR] While handling Allegory metadata for txid " <>
                    txid <> " : failed to update graph (" <> show e <> ")"
                throw e

nodesExist :: (HasXokenNodeEnv env m, MonadIO m) => TxHash -> Allegory -> Bool -> m Bool
nodesExist txId allegory confirmed = do
    lg <- getLogger
    gdb <- graphDB <$> getDB
    let txHash = T.unpack $ txHashToHex txId
        outputIndices = getOutputIndices allegory
        outpoints = (\index -> txHash ++ ":" ++ (show index)) <$> outputIndices
    opsExist <- liftIO $ sequence $ nodeExists lg gdb confirmed <$> T.pack <$> outpoints
    return $ L.all (== True) opsExist
  where
    nodeExists lg gdb oc op = do
        res <-
            catch (withResource' (pool gdb) (`BT.run` nUtxoNodeByOutpoint op oc)) $ \(e :: SomeException) -> do
                err lg $ LG.msg $ C.pack $ "[ERROR] nodesExist: While running Neo4j query: " <> show e
                throw e
        return . not . L.null $ res
    getOutputIndices al =
        AL.index <$>
        case action al of
            OwnerAction _ oo _ -> [owner oo]
            ProducerAction _ po poo es ->
                (producer po) :
                (maybe mempty (\o -> [owner o]) poo) ++
                ((\e ->
                      case e of
                          OwnerExtension oeo _ -> owner oeo
                          ProducerExtension peo _ -> producer peo) <$>
                 es)

commitScriptOutputProtocol ::
       (HasXokenNodeEnv env m, MonadIO m)
    => XCqlClientState
    -> Text
    -> (Text, Int32)
    -> Maybe (Text, Int32, Int32)
    -> Int64
    -> Int32
    -> m ()
commitScriptOutputProtocol conn protocol (txid, output_index) blockInfo fees size = do
    lg <- getLogger
    tm <- liftIO getCurrentTime
    blocksSynced <- checkBlocksFullySynced conn
    nominalTxIndex <- generateNominalTxIndex blockInfo output_index
    let qstrAddrOuts :: Q.QueryString Q.W (Text, Text, Int64, Int32, Int32, Int64) ()
        qstrAddrOuts =
            "INSERT INTO xoken.script_output_protocol (proto_str, txid, fees, size, output_index, nominal_tx_index) VALUES (?,?,?,?,?,?)"
        parAddrOuts = getSimpleQueryParam (protocol, txid, fees, size, output_index, nominalTxIndex)
    queryId <- liftIO $ queryPrepared conn (Q.RqPrepare (Q.Prepare qstrAddrOuts))
    resAddrOuts <- liftIO $ try $ write conn (Q.RqExecute (Q.Execute queryId parAddrOuts))
    case resAddrOuts of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into 'script_output_protocol: " ++ show e
            throw KeyValueDBInsertException

diffGregorianDurationClip :: Day -> Day -> (Integer, Integer)
diffGregorianDurationClip day2 day1 =
    let (y1, m1, d1) = toGregorian day1
        (y2, m2, d2) = toGregorian day2
        ym1 = y1 * 12 + toInteger m1
        ym2 = y2 * 12 + toInteger m2
        ymdiff = ym2 - ym1
        ymAllowed =
            if day2 >= day1
                then if d2 >= d1
                         then ymdiff
                         else ymdiff - 1
                else if d2 <= d1
                         then ymdiff
                         else ymdiff + 1
        dayAllowed = addGregorianMonthsClip ymAllowed day1
     in (ymAllowed, diffDays day2 dayAllowed)

checkOutputDataExists :: (HasXokenNodeEnv env m, MonadIO m) => (Text, Int32) -> m Bool
checkOutputDataExists (txid, outputIndex) = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        queryStr :: Q.QueryString Q.R (T.Text, Int32, Bool) (Identity Bool)
        queryStr = "SELECT is_recv FROM xoken.txid_outputs WHERE txid=? AND output_index=? AND is_recv=?"
        queryPar = getSimpleQueryParam (txid, outputIndex, True)
    res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query queryStr queryPar)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ C.pack $ "[ERROR] Querying database while checking if output exists: " <> (show e)
            throw KeyValueDBLookupException
        Right res -> return $ not . L.null $ res

updateBlockInfo :: (HasXokenNodeEnv env m, MonadIO m) => (Text, Int32) -> Bool -> (Text, Int32, Int32) -> m ()
updateBlockInfo (txid, outputIndex) isRecv blockInfo = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        queryStr :: Q.QueryString Q.W ((Text, Int32, Int32), Text, Int32, Bool) ()
        queryStr = "UPDATE xoken.txid_outputs SET block_info=? WHERE txid=? AND output_index=? AND is_recv=?"
        queryPar = getSimpleQueryParam (blockInfo, txid, outputIndex, isRecv)
    queryId <- liftIO $ queryPrepared conn (Q.RqPrepare (Q.Prepare queryStr))
    res <- liftIO $ try $ write conn (Q.RqExecute (Q.Execute queryId queryPar))
    case res of
        Left (e :: SomeException) -> do
            err lg $
                LG.msg $
                C.pack $
                "[ERROR] While updating spendInfo for confirmed Tx output " <>
                (T.unpack txid) <> ":" <> (show outputIndex) <> ": " <> (show e)
            throw e
        _ -> debug lg $ LG.msg $ "Updated BlockInfo: " ++ show blockInfo ++ " for tx:" ++ show (txid, outputIndex)
