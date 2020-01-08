{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}

module Network.Xoken.Node.P2P.BlockSync
    ( processBlock
    , processConfTransaction
    , runEgressBlockSync
    , runPeerSync
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently, race_)
import Control.Concurrent.Async.Lifted as LA (async)
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

produceGetDataMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m (Maybe Message)
produceGetDataMessage = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    (fl, bl) <- getNextBlockToSync
    case bl of
        Just b -> do
            t <- liftIO $ getCurrentTime
            liftIO $
                atomically $
                modifyTVar (blockSyncStatusMap bp2pEnv) (M.insert (biBlockHash b) $ (RequestSent t, biBlockHeight b))
            let gd = GetData $ [InvVector InvBlock $ getBlockHash $ biBlockHash b]
            debug lg $ LG.msg $ "GetData req: " ++ show gd
            return (Just $ MGetData gd)
        Nothing -> do
            debug lg $ LG.msg $ val "producing - empty ..."
            liftIO $ threadDelay (1000000 * 1)
            return Nothing

sendRequestMessages :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Maybe Message -> m ()
sendRequestMessages pr mmsg = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    case mmsg of
        Just msg -> do
            debug lg $ LG.msg $ val "sendRequestMessages - called."
            bp2pEnv <- getBitcoinP2P
            case msg of
                MGetData gd -> do
                    case (bpSocket pr) of
                        Just s -> do
                            let em = runPut . putMessage net $ msg
                            res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
                            case res of
                                Right () -> return ()
                                Left (e :: SomeException) -> debug lg $ LG.msg $ "Error, sending out data: " ++ show e
                            debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
                        Nothing -> debug lg $ LG.msg $ val "Error sending, no connections available"
                ___ -> return ()
        Nothing -> do
            debug lg $ LG.msg $ val "graceful ignore..."
            return ()

runEgressBlockSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runEgressBlockSync =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        let net = bncNet $ bitcoinNodeConfig bp2pEnv
        allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        let connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
        debug lg $ LG.msg $ ("Connected peers: " ++ (show $ map (\x -> snd x) connPeers))
        if L.length connPeers == 0
            then liftIO $ threadDelay (5 * 1000000)
            else do
                mapM_
                    (\(_, peer) -> do
                         tm <- liftIO $ getCurrentTime
                         fw <- liftIO $ readTVarIO $ bpBlockFetchWindow peer
                         recvtm <- liftIO $ readTVarIO $ bpLastBlockRecvTime peer
                         sendtm <- liftIO $ readTVarIO $ bpLastGetDataSent peer
                         case recvtm of
                             Just rt -> do
                                 if (fw < 4) && (diffUTCTime tm rt < 30)
                                     then do
                                         msg <- produceGetDataMessage
                                         res <- LE.try $ sendRequestMessages peer msg
                                         case res of
                                             Right () -> do
                                                 debug lg $ LG.msg $ val "updating state."
                                                 liftIO $ atomically $ writeTVar (bpLastGetDataSent peer) $ Just tm
                                                 liftIO $
                                                     atomically $ modifyTVar (bpBlockFetchWindow peer) (\z -> z + 1)
                                             Left (e :: SomeException) ->
                                                 debug lg $ LG.msg ("[ERROR] runEgressBlockSync " ++ show e)
                                     else if (diffUTCTime tm rt > 30) && (fw > 0)
                                              then do
                                                  debug lg $ msg ("Removing unresponsive peer. (1)" ++ show peer)
                                                  case bpSocket peer of
                                                      Just sock -> liftIO $ close' $ sock
                                                      Nothing -> return ()
                                                  liftIO $
                                                      atomically $
                                                      modifyTVar (bitcoinPeers bp2pEnv) (M.delete (bpAddress peer))
                                              else liftIO $ threadDelay (1 * 1000000) -- window is full, but isnt stale either
                             Nothing -- never received a block from this peer
                              -> do
                                 case sendtm of
                                     Just st -> do
                                         if (diffUTCTime tm st > 30)
                                             then do
                                                 debug lg $ msg ("Removing unresponsive peer. (2)" ++ show peer)
                                                 case bpSocket peer of
                                                     Just sock -> liftIO $ close' $ sock
                                                     Nothing -> return ()
                                                 liftIO $
                                                     atomically $
                                                     modifyTVar (bitcoinPeers bp2pEnv) (M.delete (bpAddress peer))
                                             else liftIO $ threadDelay (1 * 1000000)
                                     Nothing -> do
                                         msg <- produceGetDataMessage
                                         res <- LE.try $ sendRequestMessages peer msg
                                         case res of
                                             Right () -> do
                                                 debug lg $ LG.msg $ val "updating state."
                                                 liftIO $ atomically $ writeTVar (bpLastGetDataSent peer) $ Just tm
                                                 liftIO $
                                                     atomically $ modifyTVar (bpBlockFetchWindow peer) (\z -> z + 1)
                                             Left (e :: SomeException) ->
                                                 debug lg $ LG.msg ("[ERROR] runEgressBlockSync " ++ show e))
                    (connPeers)

runPeerSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runPeerSync =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        dbe' <- getDB
        let net = bncNet $ bitcoinNodeConfig bp2pEnv
        allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        mapM_
            (\(_, pr) ->
                 case (bpSocket pr) of
                     Just s -> do
                         let em = runPut . putMessage net $ (MGetAddr)
                         debug lg $ LG.msg ("sending GetAddr to " ++ show pr)
                         res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
                         case res of
                             Right () -> liftIO $ threadDelay (60 * 1000000)
                             Left (e :: SomeException) -> debug lg $ LG.msg ("[ERROR] runPeerSync " ++ show e)
                     Nothing -> debug lg $ LG.msg $ val "Error sending, no connections available")
            (L.filter (\x -> bpConnected (snd x)) (M.toList allPeers))

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
            debug lg $
            LG.msg ("Error: Marking [Best-Synced] blockhash failed: " ++ show e) >> throw KeyValueDBInsertException

getNextBlockToSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m (Bool, Maybe BlockInfo)
getNextBlockToSync = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    dbe' <- getDB
    let conn = keyValDB $ dbe'
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    sy <- liftIO $ readTVarIO $ blockSyncStatusMap bp2pEnv
    tm <- liftIO $ getCurrentTime
    -- reload cache
    if M.size sy == 0
        then do
            (hash, ht) <- fetchBestSyncedBlock conn net
            let cacheInd =
                    if ht < 500000
                        then [1 .. 100]
                        else if ht < 600000
                                 then [1 .. 20]
                                 else [1, 2]
            let bks = map (\x -> ht + x) cacheInd -- cache size of 200
            let str = "SELECT block_height, block_hash from xoken.blocks_by_height where block_height in ?"
                qstr = str :: Q.QueryString Q.R (Identity [Int32]) ((Int32, T.Text))
                p = Q.defQueryParams Q.One $ Identity (bks)
            op <- Q.runClient conn (Q.query qstr p)
            if L.length op == 0
                then do
                    debug lg $ LG.msg $ val "Synced fully!"
                    return (False, Nothing)
                else do
                    debug lg $ LG.msg $ val "Reloading cache."
                    let z =
                            catMaybes $
                            map
                                (\x ->
                                     case (hexToBlockHash $ snd x) of
                                         Just h -> Just (h, fromIntegral $ fst x)
                                         Nothing -> Nothing)
                                (op)
                        p = map (\x -> (fst x, (RequestQueued, snd x))) z
                    liftIO $ atomically $ writeTVar (blockSyncStatusMap bp2pEnv) (M.fromList p)
                    let e = p !! 0
                    return (True, Just $ BlockInfo (fst e) (snd $ snd e))
        else do
            let (unsent, _others) = M.partition (\x -> fst x == RequestQueued) sy
            let (received, _others2) = M.partition (\x -> fst x == BlockReceived) _others
            let (receiveStarted, sent) = M.partition (\x -> fst x == BlockReceiveStarted) _others2
            if M.size sent == 0 && M.size unsent == 0 && M.size receiveStarted == 0
                    -- all blocks received, empty the cache, cache-miss gracefully
                then do
                    let lelm = last $ L.sortOn (snd . snd) (M.toList sy)
                    liftIO $ atomically $ writeTVar (blockSyncStatusMap bp2pEnv) M.empty
                    markBestSyncedBlock (blockHashToHex $ fst $ lelm) (fromIntegral $ snd $ snd $ lelm) conn
                    return (False, Nothing)
                else if M.size unsent > 0
                         then do
                             let sortUnsent = L.sortOn (snd . snd) (M.toList unsent)
                             return (True, Just $ BlockInfo (fst $ head sortUnsent) (snd $ snd $ head sortUnsent))
                         else do
                             let recvNotStarted = M.filter (\((RequestSent t), _) -> (diffUTCTime tm t > 15)) sent
                             if M.size recvNotStarted == 0
                                 then return (False, Nothing)
                                 else return
                                          ( False
                                          , Just $
                                            BlockInfo
                                                (fst $ M.elemAt 0 recvNotStarted)
                                                (snd $ snd $ M.elemAt 0 recvNotStarted))

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
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
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
            liftIO $ err lg $ LG.msg ("Error: INSERTing into 'xoken.transactions': " ++ show par)
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
    lookupInAddrs <-
        mapM
            (\(a, b, c) ->
                 case a of
                     Just a -> return $ Just (a, b, c)
                     Nothing -> do
                         if (outPointHash nullOutPoint) == (outPointHash $ prevOutput b)
                             then return Nothing
                             else do
                                 ma <-
                                     liftIO $
                                     EX.retryBool
                                         (\e ->
                                              case e of
                                                  TxIDNotFoundRetryException -> True
                                                  otherwise -> False)
                                         120
                                         (getAddressFromOutpoint conn net $ prevOutput b)
                                 case (ma) of
                                     Just x ->
                                         case addrToString net x of
                                             Just as -> return $ Just (as, b, c)
                                             Nothing -> throw InvalidAddressException
                                     Nothing -> throw OutpointAddressNotFoundException)
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
    return ()

--
--
--
--
getAddressFromOutpoint :: Q.ClientState -> Network -> OutPoint -> IO (Maybe Address)
getAddressFromOutpoint conn net outPoint = do
    let str = "SELECT tx_serialized from xoken.transactions where tx_id = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Identity Blob)
        p = Q.defQueryParams Q.One $ Identity $ txHashToHex $ outPointHash outPoint
    iop <- Q.runClient conn (Q.query qstr p)
    if L.length iop == 0
            -- debug lg $ LG.msg ("(retry) TxID not found: " ++ (show $ txHashToHex $ outPointHash outPoint))
        then do
            liftIO $ threadDelay (1000000 * 5)
            throw TxIDNotFoundRetryException
        else do
            let txbyt = runIdentity $ iop !! 0
            case runGetLazy (getConfirmedTx) (fromBlob txbyt) of
                Left e
                    -- debug lg $ LG.msg (encodeHex $ BSL.toStrict $ fromBlob txbyt)
                 -> do
                    throw DBTxParseException
                Right (txd) -> do
                    case txd of
                        Just tx ->
                            if (fromIntegral $ outPointIndex outPoint) > (L.length $ txOut tx)
                                then throw InvalidOutpointException
                                else do
                                    let output = (txOut tx) !! (fromIntegral $ outPointIndex outPoint)
                                    case scriptToAddressBS $ scriptOutput output of
                                        Left e -> return Nothing
                                        Right os -> return $ Just os
                        Nothing -> undefined

processBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => DefBlock -> m ()
processBlock dblk = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    debug lg $ LG.msg ("processing deflated Block! " ++ show dblk)
    -- liftIO $ signalQSem (blockFetchBalance bp2pEnv)
    return ()
