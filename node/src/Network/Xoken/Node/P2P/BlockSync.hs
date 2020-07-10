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

module Network.Xoken.Node.P2P.BlockSync
    ( processBlock
    , processConfTransaction
    , runEgressBlockSync
    , checkBlocksFullySynced
    , runPeerSync
    , getScriptHashFromOutpoint
    , sendRequestMessages
    , handleIfAllegoryTx
    , commitTxPage
    ) where

import Codec.Serialise
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (AsyncCancelled, mapConcurrently, mapConcurrently_, race_)
import Control.Concurrent.Async.Lifted as LA (async)
import Control.Concurrent.Event as EV
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
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
import qualified Data.HashTable.IO as H
import Data.Int
import qualified Data.IntMap as I
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Pool
import Data.Serialize
import Data.Serialize as S
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import qualified Database.Bolt as BT
import qualified Database.CQL.IO as Q
import qualified Database.CQL.IO as Q
import Database.CQL.Protocol
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Address
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Script.Standard
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Xoken
import Xoken.NodeConfig

produceGetDataMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => UTCTime -> m (Maybe Message)
produceGetDataMessage !tm = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    res <- LE.try $ getNextBlockToSync tm
    case res of
        Right (bl) -> do
            case bl of
                Just b -> do
                    mv <- liftIO $ takeMVar (blockSyncStatusMap bp2pEnv)
                    let xm = M.insert (biBlockHash b) (RequestSent tm, biBlockHeight b) mv
                    liftIO $ putMVar (blockSyncStatusMap bp2pEnv) xm
                    let gd = GetData $ [InvVector InvBlock $ getBlockHash $ biBlockHash b]
                    debug lg $ LG.msg $ "GetData req: " ++ show gd
                    return (Just $ MGetData gd)
                Nothing -> do
                    debug lg $ LG.msg $ val "producing - empty ..."
                    liftIO $ threadDelay (200000)
                    return Nothing
        Left (e :: SomeException) -> do
            case fromException e of
                Just (t :: AsyncCancelled) -> do
                    err lg $ LG.msg ("[ERROR] AsyncCancelled " ++ show e)
                    throw e
                otherwise -> do
                    err lg $ LG.msg ("[ERROR] produceGetDataMessage " ++ show e)
                    return Nothing

sendRequestMessages :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> Message -> m ()
sendRequestMessages pr msg = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    debug lg $ LG.msg $ val "sendRequestMessages - called."
    case msg of
        MGetData gd -> do
            case (bpSocket pr) of
                Just s -> do
                    let em = runPut . putMessage net $ msg
                    res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
                    case res of
                        Right () -> return ()
                        Left (e :: SomeException) -> do
                            case fromException e of
                                Just (t :: AsyncCancelled) -> throw e
                                otherwise -> debug lg $ LG.msg $ "Error, sending out data: " ++ show e
                    debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
                Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"
        ___ -> return ()

runEgressBlockSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runEgressBlockSync =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        let net = bitcoinNetwork $ nodeConfig bp2pEnv
        allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        let connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
        debug lg $ LG.msg $ ("Connected peers: " ++ (show $ map (\x -> snd x) connPeers))
        -- sort peers by most recent message received --
        timePeer <-
            mapM
                (\(_, pr) -> do
                     recvtm <- liftIO $ readTVarIO $ bpLastTxRecvTime pr
                     case recvtm of
                         Just rt -> do
                             return (fromIntegral $ diffTimeToPicoseconds $ utctDayTime rt, pr)
                         Nothing -> return (999, pr))
                (connPeers)
        let sortedPeers = L.take 4 $ L.reverse $ L.sortBy (\(a, _) (b, _) -> compare a b) (timePeer)
        !tm <- liftIO $ getCurrentTime
        mapM_
            (\(_, peer) -> do
                 fw <- liftIO $ readTVarIO $ bpBlockFetchWindow peer
                 recvtm <- liftIO $ readTVarIO $ bpLastTxRecvTime peer
                 sendtm <- liftIO $ readTVarIO $ bpLastGetDataSent peer
                 let staleTime = fromInteger $ fromIntegral (unresponsivePeerConnTimeoutSecs $ nodeConfig bp2pEnv)
                 case recvtm of
                     Just rt -> do
                         if (fw == 0) && (diffUTCTime tm rt < staleTime)
                             then do
                                 mmsg <- produceGetDataMessage tm
                                 case mmsg of
                                     Just msg -> do
                                         res <- LE.try $ sendRequestMessages peer msg
                                         case res of
                                             Right () -> do
                                                 debug lg $ LG.msg $ val "updating state."
                                                 liftIO $ atomically $ writeTVar (bpLastGetDataSent peer) $ Just tm
                                                 liftIO $
                                                     atomically $ modifyTVar' (bpBlockFetchWindow peer) (\z -> z + 1)
                                             Left (e :: SomeException) ->
                                                 err lg $ LG.msg ("[ERROR] runEgressBlockSync " ++ show e)
                                     Nothing -> return ()
                             else if (diffUTCTime tm rt > staleTime)
                                      then do
                                          debug lg $ LG.msg ("Removing unresponsive peer. (1)" ++ show peer)
                                          case bpSocket peer of
                                              Just sock -> liftIO $ NS.close $ sock
                                              Nothing -> return ()
                                          liftIO $
                                              atomically $
                                              modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress peer))
                                      else liftIO $ threadDelay (100000) -- window is full, but isnt stale either
                     Nothing -- never received a block from this peer
                      -> do
                         case sendtm of
                             Just st -> do
                                 if (diffUTCTime tm st > staleTime)
                                     then do
                                         debug lg $ LG.msg ("Removing unresponsive peer. (2)" ++ show peer)
                                         case bpSocket peer of
                                             Just sock -> liftIO $ NS.close $ sock
                                             Nothing -> return ()
                                         liftIO $
                                             atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress peer))
                                     else liftIO $ threadDelay (100000)
                             Nothing -> do
                                 if (fw == 0)
                                     then do
                                         mmsg <- produceGetDataMessage tm
                                         case mmsg of
                                             Just msg -> do
                                                 res <- LE.try $ sendRequestMessages peer msg
                                                 case res of
                                                     Right () -> do
                                                         debug lg $ LG.msg $ val "updating state."
                                                         liftIO $
                                                             atomically $ writeTVar (bpLastGetDataSent peer) $ Just tm
                                                         liftIO $
                                                             atomically $
                                                             modifyTVar' (bpBlockFetchWindow peer) (\z -> z + 1)
                                                     Left (e :: SomeException) ->
                                                         err lg $ LG.msg ("[ERROR] runEgressBlockSync " ++ show e)
                                             Nothing -> return ()
                                     else return ())
                --
            sortedPeers
        liftIO $ threadDelay (500000) -- 0.5 sec
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
                                         Right () -> liftIO $ threadDelay (120 * 1000000)
                                         Left (e :: SomeException) -> err lg $ LG.msg ("[ERROR] runPeerSync " ++ show e)
                                 Nothing -> err lg $ LG.msg $ val "Error sending, no connections available")
                        (connPeers)
            else liftIO $ threadDelay (120 * 1000000)

markBestSyncedBlock :: (HasLogger m, MonadIO m) => Text -> Int32 -> Q.ClientState -> m ()
markBestSyncedBlock hash height conn = do
    lg <- getLogger
    let str = "insert INTO xoken.misc_store (key, value) values (? , ?)"
        qstr = str :: Q.QueryString Q.W (Text, (Maybe Bool, Int32, Maybe Int64, Text)) ()
        par = Q.defQueryParams Q.One ("best-synced", (Nothing, height, Nothing, hash))
    res <- liftIO $ try $ Q.runClient conn (Q.write (Q.prepared qstr) par)
    case res of
        Right () -> return ()
        Left (e :: SomeException) ->
            err lg $
            LG.msg ("Error: Marking [Best-Synced] blockhash failed: " ++ show e) >> throw KeyValueDBInsertException

checkBlocksFullySynced :: (HasLogger m, MonadIO m) => Q.ClientState -> m Bool
checkBlocksFullySynced conn = do
    lg <- getLogger
    let str = "SELECT value FROM xoken.misc_store WHERE key IN (?,?)"
        qstr = str :: Q.QueryString Q.R (Text, Text) (Identity (Maybe Bool, Int32, Maybe Int64, Text))
        par = Q.defQueryParams Q.One $ (T.pack "best-synced", T.pack "best_chain_tip")
    res <- liftIO $ try $ Q.runClient conn (Q.query qstr par)
    case res of
        Right results ->
            if L.length results /= 2
                then do
                    err lg $ LG.msg $ val $ C.pack $ "checkFullySynced: Missing entries best-synced or best_chain_tip"
                    throw KeyValueDBLookupException
                else do
                    let Identity (_, h1, _, _) = results !! 0
                        Identity (_, h2, _, _) = results !! 1
                    return (h1 == h2)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "checkFullySynced: " ++ show e
            throw e

getBatchSize n
    | n < 180000 = [1 .. 16]
    | n >= 180000 && n < 400000 = [1 .. 8]
    | n >= 400000 && n < 500000 = [1 .. 4]
    | n >= 500000 && n < 640000 = [1 .. 2]
    | otherwise = [1]

getNextBlockToSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => UTCTime -> m (Maybe BlockInfo)
getNextBlockToSync tm = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    conn <- keyValDB <$> getDB
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    sy <- liftIO $ takeMVar (blockSyncStatusMap bp2pEnv)
    -- reload cache
    if M.size sy == 0
        then do
            (hash, ht) <- fetchBestSyncedBlock conn net
            let cacheInd = getBatchSize ht
            let !bks = map (\x -> ht + x) cacheInd
            let str = "SELECT block_height, block_hash from xoken.blocks_by_height where block_height in ?"
                qstr = str :: Q.QueryString Q.R (Identity [Int32]) ((Int32, T.Text))
                p = Q.defQueryParams Q.One $ Identity (bks)
            res <- liftIO $ try $ Q.runClient conn (Q.query qstr p)
            case res of
                Left (e :: SomeException) -> do
                    liftIO $ putMVar (blockSyncStatusMap bp2pEnv) sy
                    err lg $ LG.msg ("Error: getNextBlockToSync: " ++ show e)
                    throw e
                Right (op) -> do
                    if L.length op == 0
                        then do
                            debug lg $ LG.msg $ val "Synced fully!"
                            liftIO $ putMVar (blockSyncStatusMap bp2pEnv) sy
                            return (Nothing)
                        else if L.length op == (fromIntegral $ last cacheInd)
                                 then do
                                     debug lg $ LG.msg $ val "Reloading cache."
                                     let !p =
                                             catMaybes $
                                             map
                                                 (\x ->
                                                      case (hexToBlockHash $ snd x) of
                                                          Just h -> Just (h, (RequestQueued, fromIntegral $ fst x))
                                                          Nothing -> Nothing)
                                                 (op)
                                     liftIO $ putMVar (blockSyncStatusMap bp2pEnv) (M.fromList p)
                                     let e = p !! 0
                                     return (Just $ BlockInfo (fst e) (snd $ snd e))
                                 else do
                                     debug lg $ LG.msg $ val "Still loading block headers, try again!"
                                     return (Nothing)
        else do
            let unsent = M.filter (\x -> fst x == RequestQueued) sy
            let sent =
                    M.filter
                        (\x ->
                             case fst x of
                                 RequestSent _ -> True
                                 otherwise -> False)
                        sy
            let receiveInProgress =
                    M.filter
                        (\x ->
                             case fst x of
                                 RecentTxReceiveTime _ -> True
                                 otherwise -> False)
                        sy
            let recvTimedOut =
                    M.filter (\((RecentTxReceiveTime (t, c)), _) -> (diffUTCTime tm t > 30)) receiveInProgress
            -- all blocks received, empty the cache, cache-miss gracefully
            debug lg $ LG.msg $ ("recv in progress, awaiting: " ++ show receiveInProgress)
            if M.size sent == 0 && M.size unsent == 0 && M.size receiveInProgress == 0
                then do
                    let !lelm = last $ L.sortOn (snd . snd) (M.toList sy)
                    debug lg $ LG.msg $ ("DEBUG, marking best synced " ++ show (blockHashToHex $ fst $ lelm))
                    markBestSyncedBlock (blockHashToHex $ fst $ lelm) (fromIntegral $ snd $ snd $ lelm) conn
                    liftIO $ putMVar (blockSyncStatusMap bp2pEnv) M.empty
                    return Nothing
                else do
                    liftIO $ putMVar (blockSyncStatusMap bp2pEnv) sy
                    if M.size recvTimedOut > 0
                             -- TODO: can be replaced with minimum using Ord instance based on (snd . snd)
                        then do
                            let !sortRecvTimedOutHead = head $ L.sortOn (snd . snd) (M.toList recvTimedOut)
                            return (Just $ BlockInfo (fst sortRecvTimedOutHead) (snd $ snd sortRecvTimedOutHead))
                        else if M.size sent > 0
                                 then do
                                     let !recvNotStarted =
                                             M.filter (\((RequestSent t), _) -> (diffUTCTime tm t > 10)) sent
                                     if M.size recvNotStarted > 0
                                         then do
                                             let !sortRecvNotStartedHead =
                                                     head $ L.sortOn (snd . snd) (M.toList recvNotStarted)
                                             return
                                                 (Just $
                                                  BlockInfo
                                                      (fst $ sortRecvNotStartedHead)
                                                      (snd $ snd $ sortRecvNotStartedHead))
                                         else if M.size unsent > 0
                                                  then do
                                                      let !sortUnsentHead =
                                                              head $ L.sortOn (snd . snd) (M.toList unsent)
                                                      return
                                                          (Just $
                                                           BlockInfo (fst sortUnsentHead) (snd $ snd sortUnsentHead))
                                                  else return Nothing
                                 else if M.size unsent > 0
                                          then do
                                              let !sortUnsentHead = head $ L.sortOn (snd . snd) (M.toList unsent)
                                              return (Just $ BlockInfo (fst sortUnsentHead) (snd $ snd sortUnsentHead))
                                          else return Nothing

fetchBestSyncedBlock :: (HasLogger m, MonadIO m) => Q.ClientState -> Network -> m ((BlockHash, Int32))
fetchBestSyncedBlock conn net = do
    lg <- getLogger
    let str = "SELECT value from xoken.misc_store where key = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Identity (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text))
        p = Q.defQueryParams Q.One $ Identity "best-synced"
    iop <- Q.runClient conn (Q.query qstr p)
    if L.length iop == 0
        then do
            debug lg $ LG.msg $ val "Best-synced-block is genesis."
            return ((headerHash $ getGenesisHeader net), 0)
        else do
            let record = runIdentity $ iop !! 0
            debug lg $ LG.msg $ "Best-synced-block from DB: " ++ (show record)
            case getTextVal record of
                Just tx -> do
                    case (hexToBlockHash $ tx) of
                        Just x -> do
                            case getIntVal record of
                                Just y -> return (x, y)
                                Nothing -> throw InvalidMetaDataException
                        Nothing -> throw InvalidBlockHashException
                Nothing -> throw InvalidMetaDataException

commitScriptHashOutputs ::
       (HasLogger m, MonadIO m)
    => Q.ClientState
    -> Text -- address
    -> (Text, Int32) -- output (txid, index)
    -> (Text, Int32, Int32) -- blockInfo (blockHash, blockHeight, blockTxIndex)
    -> Bool
    -> m ()
commitScriptHashOutputs conn sh output blockInfo isRecv = do
    lg <- getLogger
    let blkHeight = fromIntegral $ snd3 blockInfo
        txIndex = fromIntegral $ thd3 blockInfo
        nominalTxIndex = blkHeight * 1000000000 + txIndex
        strAddrOuts =
            "INSERT INTO xoken.script_hash_outputs (script_hash, nominal_tx_index, output, is_recv) VALUES (?,?,?,?)"
        qstrAddrOuts = strAddrOuts :: Q.QueryString Q.W (Text, Int64, (Text, Int32), Bool) ()
        parAddrOuts = Q.defQueryParams Q.One (sh, nominalTxIndex, output, isRecv)
    resAddrOuts <- liftIO $ try $ Q.runClient conn (Q.write (qstrAddrOuts) parAddrOuts)
    case resAddrOuts of
        Right () -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into 'script_hash_outputs': " ++ show e
            throw KeyValueDBInsertException

insertTxIdOutputs ::
       (HasLogger m, MonadIO m)
    => Q.ClientState
    -> (Text, Int32) -- (txid, output_index)
    -> Maybe Text -- address
    -> Bool -- is_recv
    -> (Text, Int32, Int32) -- block_info (blockHash, blockHeight, blockTxIndex) {spender's block info}
    -> [((Text, Int32), Int32, (Maybe Text, Int64))] -- (prevOutpoint, inputIndex, value) {spender's info}
    -> Int64 -- value
    -> m ()
insertTxIdOutputs conn (txid, outputIndex) address isRecv blockInfo other value = do
    lg <- getLogger
    let str =
            "INSERT INTO xoken.txid_outputs (txid,output_index,address,is_recv,block_info,other,value) VALUES (?,?,?,?,?,?,?)"
        qstr =
            str :: Q.QueryString Q.W ( Text
                                     , Int32
                                     , Maybe Text
                                     , Bool
                                     , (Text, Int32, Int32)
                                     , [((Text, Int32), Int32, (Maybe Text, Int64))]
                                     , Int64) ()
        par = Q.defQueryParams Q.One (txid, outputIndex, address, isRecv, blockInfo, other, value)
    res <- liftIO $ try $ Q.runClient conn $ (Q.write qstr par)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into: txid_outputs " ++ show e
            throw KeyValueDBInsertException

commitTxPage ::
       (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m)
    => [TxHash]
    -> BlockHash
    -> Int32
    -> m ()
commitTxPage txhash bhash page = do
    dbe' <- getDB
    lg <- getLogger
    let conn = keyValDB $ dbe'
        txids = txHashToHex <$> txhash
        str = "insert INTO xoken.blockhash_txids (block_hash, page_number, txids) values (?, ?, ?)"
        qstr = str :: Q.QueryString Q.W (Text, Int32, [Text]) ()
        par = Q.defQueryParams Q.One (blockHashToHex bhash, page, txids)
    res <- liftIO $ try $ Q.runClient conn (Q.write qstr par)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> do
            liftIO $ err lg $ LG.msg ("Error: INSERTing into 'xoken.blockhash_txids': " ++ show e)
            throw KeyValueDBInsertException

processConfTransaction :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> BlockHash -> Int -> Int -> m ()
processConfTransaction tx bhash txind blkht = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let conn = keyValDB $ dbe'
    debug lg $ LG.msg ("processing Transaction: " ++ show (txHash tx))
    let inAddrs = zip (txIn tx) [0 :: Int32 ..]
    let outAddrs =
            zip3
                (map (\y ->
                          case scriptToAddressBS $ scriptOutput y of
                              Left e -> Nothing
                              Right os -> addrToString net os)
                     (txOut tx))
                (txOut tx)
                [0 :: Int32 ..]
    --
    -- lookup into tx outputs value cache if cache-miss, fetch from DB
    inputs <-
        mapM
            (\(b, j) -> do
                 tuple <-
                     liftIO $
                     H.lookup
                         (txOutputValuesCache bp2pEnv)
                         (getTxShortHash (txHash tx) (txOutputValuesCacheKeyBits $ nodeConfig bp2pEnv))
                 val <-
                     case tuple of
                         Just (ftxh, indexvals) ->
                             if ftxh == (outPointHash $ prevOutput b)
                                 then do
                                     let rr =
                                             head $
                                             filter
                                                 (\x -> fst x == (fromIntegral $ outPointIndex $ prevOutput b))
                                                 indexvals
                                     return $ snd $ rr
                                 else do
                                     if (outPointHash nullOutPoint) == (outPointHash $ prevOutput b)
                                         then return
                                                  ( Nothing :: Maybe Text
                                                  , fromIntegral $ computeSubsidy net $ (fromIntegral blkht :: Word32))
                                         else do
                                             dbRes <-
                                                 liftIO $
                                                 LE.try $
                                                 getSatValuesFromOutpoint
                                                     conn
                                                     (txSynchronizer bp2pEnv)
                                                     lg
                                                     net
                                                     (prevOutput b)
                                                     (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                                             case dbRes of
                                                 Right v -> return $ v
                                                 Left (e :: SomeException) -> do
                                                     err lg $
                                                         LG.msg $
                                                         "Error: [pCT calling gSVFO] WHILE Processing TxID " ++
                                                         show (txHashToHex $ txHash tx) ++
                                                         ", getting value for dependent input (TxID,Index): (" ++
                                                         show (txHashToHex $ outPointHash (prevOutput b)) ++
                                                         ", " ++ show (outPointIndex $ prevOutput b) ++ ")"
                                                     throw e
                         Nothing -> do
                             if (outPointHash nullOutPoint) == (outPointHash $ prevOutput b)
                                 then return
                                          ( Nothing :: Maybe Text
                                          , fromIntegral $ computeSubsidy net $ (fromIntegral blkht :: Word32))
                                 else do
                                     dbRes <-
                                         liftIO $
                                         LE.try $
                                         getSatValuesFromOutpoint
                                             conn
                                             (txSynchronizer bp2pEnv)
                                             lg
                                             net
                                             (prevOutput b)
                                             (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                                     case dbRes of
                                         Right v -> return $ v
                                         Left (e :: SomeException) -> do
                                             err lg $
                                                 LG.msg $
                                                 "Error: [pCT calling gSVFO] WHILE Processing TxID " ++
                                                 show (txHashToHex $ txHash tx) ++
                                                 ", getting value for dependent input (TxID,Index): (" ++
                                                 show (txHashToHex $ outPointHash (prevOutput b)) ++
                                                 ", " ++ show (outPointIndex $ prevOutput b) ++ ")"
                                             throw e
                 return
                     ((txHashToHex $ outPointHash $ prevOutput b, fromIntegral $ outPointIndex $ prevOutput b), j, val))
            inAddrs
    trace lg $ LG.msg $ "processing Transaction " ++ show (txHash tx) ++ ": fetched input(s): " ++ show inputs
    --
    -- cache compile output values 
    let ovs = map (\(a, o, i) -> (fromIntegral $ i, (a, fromIntegral $ outValue o))) outAddrs
    trace lg $ LG.msg $ "processing Transaction " ++ show (txHash tx) ++ ": compiled output value(s): " ++ (show ovs)
    liftIO $
        H.insert
            (txOutputValuesCache bp2pEnv)
            (getTxShortHash (txHash tx) (txOutputValuesCacheKeyBits $ nodeConfig bp2pEnv))
            (txHash tx, ovs)
    --
    trace lg $ LG.msg $ "processing Transaction " ++ show (txHash tx) ++ ": added outputvals to cache"
    -- update outputs and scripthash tables
    mapM_
        (\(a, o, i) -> do
             let sh = txHashToHex $ TxHash $ sha256 (scriptOutput o)
             let bi = (blockHashToHex bhash, fromIntegral blkht, fromIntegral txind)
             let output = (txHashToHex $ txHash tx, i)
             insertTxIdOutputs conn output a True bi inputs (fromIntegral $ outValue o)
             commitScriptHashOutputs
                 conn -- connection
                 sh -- scriptHash
                 output
                 bi
                 True)
        outAddrs
    trace lg $ LG.msg $ "processing Transaction " ++ show (txHash tx) ++ ": committed scripthash,txid_outputs tables"
    mapM_
        (\((o, i), a) -> do
             let bi = (blockHashToHex bhash, fromIntegral blkht, fromIntegral txind)
             let blockHeight = fromIntegral blkht
             let prevOutpoint = (txHashToHex $ outPointHash $ prevOutput o, fromIntegral $ outPointIndex $ prevOutput o)
             let spendInfo = (\ov -> ((txHashToHex $ txHash tx, fromIntegral $ fst $ ov), i, snd $ ov)) <$> ovs
             insertTxIdOutputs conn prevOutpoint a False bi spendInfo 0
             if a == Nothing
                 then return ()
                 else case convertToScriptHash net (T.unpack $ fromJust a) of
                          Nothing -> return ()
                          Just sh -> commitScriptHashOutputs conn (T.pack sh) prevOutpoint bi False)
        (zip (inAddrs) (map (\x -> fst $ thd3 x) inputs))
    --
    trace lg $ LG.msg $ "processing Transaction " ++ show (txHash tx) ++ ": updated spend info for inputs"
    -- calculate Tx fees
    let ipSum = foldl (+) 0 $ (\(_, _, (_, val)) -> val) <$> inputs
        opSum = foldl (+) 0 $ (\(_, o, _) -> fromIntegral $ outValue o) <$> outAddrs
        fees = ipSum - opSum
    --
    trace lg $ LG.msg $ "processing Transaction " ++ show (txHash tx) ++ ": calculated fees"
    -- persist tx
    let str = "insert INTO xoken.transactions ( tx_id, block_info, tx_serialized , inputs, fees) values (?, ?, ?, ?, ?)"
        qstr =
            str :: Q.QueryString Q.W ( Text
                                     , (Text, Int32, Int32)
                                     , Blob
                                     , [((Text, Int32), Int32, (Maybe Text, Int64))]
                                     , Int64) ()
        par =
            Q.defQueryParams
                Q.One
                ( txHashToHex $ txHash tx
                , (blockHashToHex bhash, fromIntegral blkht, fromIntegral txind)
                , Blob $ runPutLazy $ putLazyByteString $ S.encodeLazy tx
                , inputs
                , fees)
    res <- liftIO $ try $ Q.runClient conn (Q.write (qstr) par)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> do
            liftIO $ err lg $ LG.msg ("Error: INSERTing into 'xoken.transactions': " ++ show e)
            throw KeyValueDBInsertException
    --
    trace lg $ LG.msg $ "processing Transaction " ++ show (txHash tx) ++ ": persisted in DB"
    -- handle allegory
    eres <- LE.try $ handleIfAllegoryTx tx True
    case eres of
        Right (flg) -> return ()
        Left (e :: SomeException) -> err lg $ LG.msg ("Error: " ++ show e)
    --
    trace lg $ LG.msg $ "processing Transaction " ++ show (txHash tx) ++ ": handled Allegory Tx"
    -- signal 'done' event for tx's that were processed out of sequence 
    --
    txSyncMap <- liftIO $ readMVar (txSynchronizer bp2pEnv)
    case (M.lookup (txHash tx) txSyncMap) of
        Just ev -> liftIO $ EV.signal $ ev
        Nothing -> return ()
    trace lg $ LG.msg $ "processing Transaction " ++ show (txHash tx) ++ ": end of processing signaled"

getSatValuesFromOutpoint ::
       Q.ClientState
    -> (MVar (M.Map TxHash EV.Event))
    -> Logger
    -> Network
    -> OutPoint
    -> Int
    -> IO ((Maybe Text, Int64))
getSatValuesFromOutpoint conn txSync lg net outPoint waitSecs = do
    let str = "SELECT address, value FROM xoken.txid_outputs WHERE txid=? AND output_index=?"
        qstr = str :: Q.QueryString Q.R (Text, Int32) (Maybe Text, Int64)
        par = Q.defQueryParams Q.One $ (txHashToHex $ outPointHash outPoint, fromIntegral $ outPointIndex outPoint)
    res <- liftIO $ try $ Q.runClient conn (Q.query qstr par)
    case res of
        Right results -> do
            if L.length results == 0
                then do
                    debug lg $
                        LG.msg $
                        "Tx not found: " ++ (show $ txHashToHex $ outPointHash outPoint) ++ "... waiting for event"
                    tmap <- liftIO $ takeMVar (txSync)
                    event <-
                        case (M.lookup (outPointHash outPoint) tmap) of
                            Just evt -> return evt
                            Nothing -> EV.new
                    liftIO $ putMVar (txSync) (M.insert (outPointHash outPoint) event tmap)
                    tofl <- waitTimeout event (1000000 * (fromIntegral waitSecs))
                    if tofl == False
                        then do
                            tmap <- liftIO $ takeMVar (txSync)
                            liftIO $ putMVar (txSync) (M.delete (outPointHash outPoint) tmap)
                            debug lg $
                                LG.msg $
                                "TxIDNotFoundException: While querying txid_outputs for (TxID, Index): " ++
                                (show $ txHashToHex $ outPointHash outPoint) ++
                                ", " ++ show (outPointIndex outPoint) ++ ")"
                            throw TxIDNotFoundException
                        else getSatValuesFromOutpoint conn txSync lg net outPoint waitSecs
                else do
                    let (scrHash, val) = head $ results
                    return $ (scrHash, val)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getSatValuesFromOutpoint: " ++ show e
            throw e

--
--
--
getScriptHashFromOutpoint ::
       Q.ClientState -> (MVar (M.Map TxHash EV.Event)) -> Logger -> Network -> OutPoint -> Int -> IO (Maybe Text)
getScriptHashFromOutpoint conn txSync lg net outPoint waitSecs = do
    let str = "SELECT tx_serialized from xoken.transactions where tx_id = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Identity Blob)
        p = Q.defQueryParams Q.One $ Identity $ txHashToHex $ outPointHash outPoint
    res <- liftIO $ try $ Q.runClient conn (Q.query qstr p)
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
                    tmap <- liftIO $ takeMVar (txSync)
                    event <-
                        case (M.lookup (outPointHash outPoint) tmap) of
                            Just evt -> return evt
                            Nothing -> EV.new
                    liftIO $ putMVar (txSync) (M.insert (outPointHash outPoint) event tmap)
                    tofl <- waitTimeout event (1000000 * (fromIntegral waitSecs))
                    if tofl == False -- False indicates a timeout occurred.
                        then do
                            tmap <- liftIO $ takeMVar (txSync)
                            liftIO $ putMVar (txSync) (M.delete (outPointHash outPoint) tmap)
                            debug lg $ LG.msg ("TxIDNotFoundException" ++ (show $ txHashToHex $ outPointHash outPoint))
                            throw TxIDNotFoundException
                        else getScriptHashFromOutpoint conn txSync lg net outPoint waitSecs -- if being signalled, try again to success
                    --
                    return Nothing
                else do
                    let txbyt = runIdentity $ iop !! 0
                    case runGetLazy (getConfirmedTx) (fromBlob txbyt) of
                        Left e -> do
                            debug lg $ LG.msg (encodeHex $ BSL.toStrict $ fromBlob txbyt)
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

handleIfAllegoryTx :: (HasXokenNodeEnv env m, MonadIO m) => Tx -> Bool -> m (Bool)
handleIfAllegoryTx tx revert = do
    dbe <- getDB
    lg <- getLogger
    debug lg $ LG.msg $ val $ "Checking for Allegory OP_RETURN"
    let op_return = head (txOut tx)
    let hexstr = B16.encode (scriptOutput op_return)
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
                                    if revert
                                        then do
                                            _ <- resdb dbe revertAllegoryStateTree tx allegory
                                            resdb dbe updateAllegoryStateTrees tx allegory
                                        else resdb dbe updateAllegoryStateTrees tx allegory
                                Left (e :: DeserialiseFailure) -> do
                                    err lg $ LG.msg $ "error deserialising OP_RETURN CBOR data" ++ show e
                                    throw e
                Left (e) -> do
                    err lg $ LG.msg $ "error decoding op_return data:" ++ show e
                    throw MessageParsingException
        else do
            return False
  where
    resdb db fn tx al = do
        eres <- liftIO $ try $ withResource (pool $ graphDB db) (`BT.run` fn tx al)
        case eres of
            Right () -> return True
            Left (SomeException e) -> throw e

convertToScriptHash :: Network -> String -> Maybe String
convertToScriptHash net s = do
    let addr = stringToAddr net (T.pack s)
    (T.unpack . txHashToHex . TxHash . sha256 . addressToScriptBS) <$> addr
