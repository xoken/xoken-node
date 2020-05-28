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
    , runPeerSync
    , getAddressFromOutpoint
    , sendRequestMessages
    ) where

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
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import Data.Int
import qualified Data.IntMap as I
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Serialize
import Data.Serialize as S
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
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
import Xoken.NodeConfig

produceGetDataMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m (Maybe Message)
produceGetDataMessage = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    res <- LE.try $ getNextBlockToSync
    case res of
        Right (bl) -> do
            case bl of
                Just b -> do
                    t <- liftIO $ getCurrentTime
                    liftIO $
                        atomically $
                        modifyTVar'
                            (blockSyncStatusMap bp2pEnv)
                            (M.insert (biBlockHash b) $ (RequestSent t, biBlockHeight b))
                    let gd = GetData $ [InvVector InvBlock $ getBlockHash $ biBlockHash b]
                    debug lg $ LG.msg $ "GetData req: " ++ show gd
                    return (Just $ MGetData gd)
                Nothing -> do
                    debug lg $ LG.msg $ val "producing - empty ..."
                    liftIO $ threadDelay (1000000 * 1)
                    return Nothing
        Left (e :: SomeException) -> do
            case fromException e of
                Just (t :: AsyncCancelled) -> throw e
                otherwise -> do
                    err lg $ LG.msg ("[ERROR] produceGetDataMessage " ++ show e)
                    return Nothing

sendRequestMessages :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Message -> m ()
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
        if L.null connPeers
            then liftIO $ threadDelay (5 * 1000000)
            else do
                mapM_
                    (\(_, peer) -> do
                         tm <- liftIO $ getCurrentTime
                         fw <- liftIO $ readTVarIO $ bpBlockFetchWindow peer
                         recvtm <- liftIO $ readTVarIO $ bpLastTxRecvTime peer
                         sendtm <- liftIO $ readTVarIO $ bpLastGetDataSent peer
                         let staleTime =
                                 fromInteger $ fromIntegral (unresponsivePeerConnTimeoutSecs $ nodeConfig bp2pEnv)
                         case recvtm of
                             Just rt -> do
                                 if (fw == 0) && (diffUTCTime tm rt < staleTime)
                                     then do
                                         rnd <- liftIO $ randomRIO (1, L.length connPeers) -- dynamic peer shuffle logic
                                         if rnd /= 1
                                             then return ()
                                             else do
                                                 mmsg <- produceGetDataMessage
                                                 case mmsg of
                                                     Just msg -> do
                                                         res <- LE.try $ sendRequestMessages peer msg
                                                         case res of
                                                             Right () -> do
                                                                 debug lg $ LG.msg $ val "updating state."
                                                                 liftIO $
                                                                     atomically $
                                                                     writeTVar (bpLastGetDataSent peer) $ Just tm
                                                                 liftIO $
                                                                     atomically $
                                                                     modifyTVar' (bpBlockFetchWindow peer) (\z -> z + 1)
                                                             Left (e :: SomeException) ->
                                                                 err lg $
                                                                 LG.msg ("[ERROR] runEgressBlockSync " ++ show e)
                                                     Nothing -> return ()
                                     else if (diffUTCTime tm rt > staleTime)
                                              then do
                                                  debug lg $ msg ("Removing unresponsive peer. (1)" ++ show peer)
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
                                                 debug lg $ msg ("Removing unresponsive peer. (2)" ++ show peer)
                                                 case bpSocket peer of
                                                     Just sock -> liftIO $ NS.close $ sock
                                                     Nothing -> return ()
                                                 liftIO $
                                                     atomically $
                                                     modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress peer))
                                             else liftIO $ threadDelay (100000)
                                     Nothing -> do
                                         rnd <- liftIO $ randomRIO (1, L.length connPeers) -- dynamic peer shuffle logic
                                         if rnd /= 1
                                             then return ()
                                             else do
                                                 mmsg <- produceGetDataMessage
                                                 case mmsg of
                                                     Just msg -> do
                                                         res <- LE.try $ sendRequestMessages peer msg
                                                         case res of
                                                             Right () -> do
                                                                 debug lg $ LG.msg $ val "updating state."
                                                                 liftIO $
                                                                     atomically $
                                                                     writeTVar (bpLastGetDataSent peer) $ Just tm
                                                                 liftIO $
                                                                     atomically $
                                                                     modifyTVar' (bpBlockFetchWindow peer) (\z -> z + 1)
                                                             Left (e :: SomeException) ->
                                                                 err lg $
                                                                 LG.msg ("[ERROR] runEgressBlockSync " ++ show e)
                                                     Nothing -> return ())
                    (connPeers)

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

getBatchSize n
    | n < 200000 = [1 .. 400]
    | n >= 200000 && n < 400000 = [1 .. 100]
    | n >= 400000 && n < 500000 = [1 .. 50]
    | n >= 500000 && n < 600000 = [1 .. 25]
    | otherwise = [1, 12]

getNextBlockToSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m (Maybe BlockInfo)
getNextBlockToSync = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    conn <- keyValDB <$> getDB
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    sy <- liftIO $ readTVarIO $ blockSyncStatusMap bp2pEnv
    tm <- liftIO $ getCurrentTime
    -- reload cache
    if M.size sy == 0
        then do
            (hash, ht) <- fetchBestSyncedBlock conn net
            let cacheInd = getBatchSize ht
            let !bks = map (\x -> ht + x) cacheInd -- cache size of 200
            let str = "SELECT block_height, block_hash from xoken.blocks_by_height where block_height in ?"
                qstr = str :: Q.QueryString Q.R (Identity [Int32]) ((Int32, T.Text))
                p = Q.defQueryParams Q.One $ Identity (bks)
            res <- liftIO $ try $ Q.runClient conn (Q.query qstr p)
            case res of
                Left (e :: SomeException) -> do
                    err lg $ LG.msg ("Error: getNextBlockToSync: " ++ show e)
                    throw e
                Right (op) -> do
                    if L.length op == 0
                        then do
                            debug lg $ LG.msg $ val "Synced fully!"
                            return (Nothing)
                        else do
                            debug lg $ LG.msg $ val "Reloading cache."
                            let !p =
                                    catMaybes $
                                    map
                                        (\x ->
                                             case (hexToBlockHash $ snd x) of
                                                 Just h -> Just (h, (RequestQueued, fromIntegral $ fst x))
                                                 Nothing -> Nothing)
                                        (op)
                            liftIO $ atomically $ writeTVar (blockSyncStatusMap bp2pEnv) (M.fromList p)
                            let e = p !! 0
                            return (Just $ BlockInfo (fst e) (snd $ snd e))
        else do
            let unsent = M.filter (\x -> fst x == RequestQueued) sy
            -- let received = M.filter (\x -> fst x == BlockReceiveComplete) sy
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
                    M.filter (\((RecentTxReceiveTime (t, c)), _) -> (diffUTCTime tm t > 66)) receiveInProgress
            -- all blocks received, empty the cache, cache-miss gracefully
            debug lg $ LG.msg $ ("recv in progress, awaiting: " ++ show receiveInProgress)
            if M.size sent == 0 && M.size unsent == 0 && M.size receiveInProgress == 0
                then do
                    let !lelm = last $ L.sortOn (snd . snd) (M.toList sy)
                    liftIO $ atomically $ writeTVar (blockSyncStatusMap bp2pEnv) M.empty
                    -- debug lg $ LG.msg $ ("DEBUG, marking best synced " ++ show (blockHashToHex $ fst $ lelm))
                    markBestSyncedBlock (blockHashToHex $ fst $ lelm) (fromIntegral $ snd $ snd $ lelm) conn
                    return Nothing
                else if M.size recvTimedOut > 0
                             -- TODO: can be replaced with minimum using Ord instance based on (snd . snd)
                         then do
                             let !sortRecvTimedOutHead = head $ L.sortOn (snd . snd) (M.toList recvTimedOut)
                             return (Just $ BlockInfo (fst sortRecvTimedOutHead) (snd $ snd sortRecvTimedOutHead))
                         else if M.size sent > 0
                                  then do
                                      let !recvNotStarted =
                                              M.filter (\((RequestSent t), _) -> (diffUTCTime tm t > 66)) sent
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

commitAddressOutputs ::
       (HasLogger m, MonadIO m)
    => Q.ClientState
    -> Text
    -> Bool
    -> Maybe Text
    -> (Text, Int32)
    -> ((Text, Int32), Int32)
    -> (Text, Int32)
    -> Int64
    -> m ()
commitAddressOutputs conn addr typeRecv otherAddr output blockInfo prevOutpoint value = do
    lg <- getLogger
    let str =
            "insert INTO xoken.address_outputs ( address, is_type_receive,other_address, output, block_info, prev_outpoint, value, is_block_confirmed, is_output_spent ) values (?, ?, ?, ?, ?, ? ,? ,? ,?)"
        qstr =
            str :: Q.QueryString Q.W ( Text
                                     , Bool
                                     , Maybe Text
                                     , (Text, Int32)
                                     , ((Text, Int32), Int32)
                                     , (Text, Int32)
                                     , Int64
                                     , Bool
                                     , Bool) ()
        par = Q.defQueryParams Q.One (addr, typeRecv, otherAddr, output, blockInfo, prevOutpoint, value, False, False)
    res1 <- liftIO $ try $ Q.runClient conn (Q.write (qstr) par)
    case res1 of
        Right () -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into 'address_outputs': " ++ show e
            throw KeyValueDBInsertException

processConfTransaction :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> BlockHash -> Int -> Int -> m ()
processConfTransaction tx bhash txind blkht = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let conn = keyValDB $ dbe'
        str = "insert INTO xoken.transactions ( tx_id, block_info, tx_serialized ) values (?, ?, ?)"
        qstr = str :: Q.QueryString Q.W (Text, ((Text, Int32), Int32), Blob) ()
        par =
            Q.defQueryParams
                Q.One
                ( txHashToHex $ txHash tx
                , ((blockHashToHex bhash, fromIntegral txind), fromIntegral blkht)
                , Blob $ runPutLazy $ putLazyByteString $ S.encodeLazy tx)
    debug lg $ LG.msg ("processing Transaction: " ++ show (txHash tx))
    res <- liftIO $ try $ Q.runClient conn (Q.write (qstr) par)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> do
            liftIO $ err lg $ LG.msg ("Error: INSERTing into 'xoken.transactions': " ++ show e)
            throw KeyValueDBInsertException
    --
    let inAddrs =
            zip3
                (map (\x -> do
                          case decodeInputBS net $ scriptInput x of
                              Left e -> Nothing
                              Right is ->
                                  case inputAddress is of
                                      Just s -> addrToString net s
                                      Nothing -> Nothing)
                     (txIn tx))
                (txIn tx)
                [0 :: Int32 ..]
    let outAddrs =
            zip3
                (catMaybes $
                 map
                     (\y ->
                          case scriptToAddressBS $ scriptOutput y of
                              Left e -> Nothing
                              Right os -> addrToString net os)
                     (txOut tx))
                (txOut tx)
                [0 :: Int32 ..]
    --
    lookupInAddrs <-
        mapM
            (\(a, b, c) ->
                 case a of
                     Just a -> return $ Just (a, b, c)
                     Nothing -> do
                         if (outPointHash nullOutPoint) == (outPointHash $ prevOutput b)
                             then return Nothing
                             else do
                                 res <-
                                     liftIO $
                                     try $
                                     getAddressFromOutpoint
                                         conn
                                         (txSynchronizer bp2pEnv)
                                         lg
                                         net
                                         (prevOutput b)
                                         (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                                 case res of
                                     Right (ma) -> do
                                         case (ma) of
                                             Just x ->
                                                 case addrToString net x of
                                                     Just as -> return $ Just (as, b, c)
                                                     Nothing -> throw InvalidAddressException
                                             Nothing -> do
                                                 liftIO $
                                                     err lg $ LG.msg $ val "Error: OutpointAddressNotFoundException "
                                                 return Nothing
                                     Left TxIDNotFoundException -- report and ignore
                                      -> do
                                         err lg $ LG.msg $ val "Error: TxIDNotFoundException"
                                         return Nothing)
            inAddrs
    mapM_
        (\(x, a, i) ->
             mapM_
                 (\(y, b, j) ->
                      commitAddressOutputs
                          conn
                          x
                          True
                          y
                          (txHashToHex $ txHash tx, i)
                          ((blockHashToHex bhash, fromIntegral blkht), fromIntegral txind)
                          (txHashToHex $ outPointHash $ prevOutput b, fromIntegral $ outPointIndex $ prevOutput b)
                          (fromIntegral $ outValue a))
                 inAddrs)
        outAddrs
    mapM_
        (\(x, a, i) ->
             mapM_
                 (\(y, b, j) ->
                      commitAddressOutputs
                          conn
                          x
                          False
                          (Just y)
                          (txHashToHex $ txHash tx, i)
                          ((blockHashToHex bhash, fromIntegral blkht), fromIntegral txind)
                          (txHashToHex $ outPointHash $ prevOutput a, fromIntegral $ outPointIndex $ prevOutput a)
                          (fromIntegral $ outValue b))
                 outAddrs)
        (catMaybes lookupInAddrs)
    --
    txSyncMap <- liftIO $ readTVarIO (txSynchronizer bp2pEnv)
    case (M.lookup (txHash tx) txSyncMap) of
        Just ev -> liftIO $ EV.signal $ ev
        Nothing -> return ()

--
--
--
--
getAddressFromOutpoint ::
       Q.ClientState -> (TVar (M.Map TxHash EV.Event)) -> Logger -> Network -> OutPoint -> Int -> IO (Maybe Address)
getAddressFromOutpoint conn txSync lg net outPoint waitSecs = do
    let str = "SELECT tx_serialized from xoken.transactions where tx_id = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Identity Blob)
        p = Q.defQueryParams Q.One $ Identity $ txHashToHex $ outPointHash outPoint
    res <- liftIO $ try $ Q.runClient conn (Q.query qstr p)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg ("Error: getAddressFromOutpoint: " ++ show e)
            throw e
        Right (iop) -> do
            if L.length iop == 0
                then do
                    debug lg $
                        LG.msg ("TxID not found: (waiting for event) " ++ (show $ txHashToHex $ outPointHash outPoint))
                    --
                    event <- EV.new
                    liftIO $ atomically $ modifyTVar' (txSync) (M.insert (outPointHash outPoint) event)
                    tofl <- waitTimeout event (1000000 * (fromIntegral waitSecs))
                    if tofl == False -- False indicates a timeout occurred.
                        then do
                            liftIO $ atomically $ modifyTVar' (txSync) (M.delete (outPointHash outPoint))
                            debug lg $ LG.msg ("TxIDNotFoundException" ++ (show $ txHashToHex $ outPointHash outPoint))
                            throw TxIDNotFoundException
                        else getAddressFromOutpoint conn txSync lg net outPoint waitSecs -- if being signalled, try again to success
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
                                            case scriptToAddressBS $ scriptOutput output of
                                                Left e
                                                    -- debug lg $ LG.msg ("unable to decode addr: " ++ show e)
                                                 -> do
                                                    return Nothing
                                                Right os -> return $ Just os
                                Nothing -> return Nothing

processBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => DefBlock -> m ()
processBlock dblk = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    debug lg $ LG.msg ("processing deflated Block! " ++ show dblk)
    -- liftIO $ signalQSem (blockFetchBalance bp2pEnv)
    return ()
