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
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently)
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
import System.Random

produceGetDataMessage :: (HasXokenNodeEnv env m, MonadIO m) => m (Maybe Message)
produceGetDataMessage = do
    liftIO $ print ("produceGetDataMessage - called.")
    bp2pEnv <- getBitcoinP2P
    (fl, bl) <- getNextBlockToSync
    case bl of
        Just b -> do
            if fl == True
                then liftIO $ waitQSem (blockFetchBalance bp2pEnv)
                else return ()
            t <- liftIO $ getCurrentTime
            liftIO $
                atomically $
                modifyTVar (blockSyncStatusMap bp2pEnv) (M.insert (biBlockHash b) $ (RequestSent t, biBlockHeight b))
            let gd = GetData $ [InvVector InvBlock $ getBlockHash $ biBlockHash b]
            liftIO $ print ("GetData req: " ++ show gd)
            return (Just $ MGetData gd)
        Nothing -> do
            liftIO $ print ("producing - empty ...")
            liftIO $ threadDelay (1000000 * 1)
            return Nothing

sendRequestMessages :: (HasXokenNodeEnv env m, MonadIO m) => Maybe Message -> m ()
sendRequestMessages mmsg = do
    case mmsg of
        Just msg -> do
            liftIO $ print ("sendRequestMessages - called.")
            bp2pEnv <- getBitcoinP2P
            dbe' <- getDB
            let conn = keyValDB $ dbe'
            let net = bncNet $ bitcoinNodeConfig bp2pEnv
            allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
            let connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
            case msg of
                MGetData gd -> do
                    let fbh = getHash256 $ invHash $ last $ getDataList gd
                        md = BSS.index fbh $ (BSS.length fbh) - 1
                        pd = fromIntegral md `mod` L.length connPeers
                    let pr = snd $ connPeers !! pd
                    case (bpSocket pr) of
                        Just s -> do
                            let em = runPut . putMessage net $ msg
                            res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
                            case res of
                                Right () -> return ()
                                Left (e :: SomeException) -> liftIO $ print ("Error, sending out data: " ++ show e)
                            liftIO $ print ("sending out GetData: " ++ show (bpAddress pr))
                        Nothing -> liftIO $ print ("Error sending, no connections available")
                ___ -> return ()
        Nothing -> do
            liftIO $ print ("graceful ignore...")
            return ()

runEgressBlockSync :: (HasXokenNodeEnv env m, MonadIO m) => m ()
runEgressBlockSync = do
    res <- LE.try $ runStream $ (S.repeatM produceGetDataMessage) & (S.mapM sendRequestMessages)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> liftIO $ print ("[ERROR] runEgressBlockSync " ++ show e)

markBestSyncedBlock :: Text -> Int32 -> Q.ClientState -> IO ()
markBestSyncedBlock hash height conn = do
    let str = "insert INTO xoken.misc_store (key, value) values (? , ?)"
        qstr = str :: Q.QueryString Q.W (Text, (Maybe Bool, Int32, Maybe Int64, Text)) ()
        par = Q.defQueryParams Q.One ("best-synced", (Nothing, height, Nothing, hash))
    res <- try $ Q.runClient conn (Q.write (Q.prepared qstr) par)
    case res of
        Right () -> return ()
        Left (e :: SomeException) ->
            print ("Error: Marking [Best-Synced] blockhash failed: " ++ show e) >> throw KeyValueDBInsertException

getNextBlockToSync :: (HasXokenNodeEnv env m, MonadIO m) => m (Bool, Maybe BlockInfo)
getNextBlockToSync = do
    bp2pEnv <- getBitcoinP2P
    dbe' <- getDB
    let conn = keyValDB $ dbe'
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    sy <- liftIO $ readTVarIO $ blockSyncStatusMap bp2pEnv
    tm <- liftIO $ getCurrentTime
    -- reload cache
    if M.size sy == 0
        then do
            (hash, ht) <- liftIO $ fetchBestSyncedBlock conn net
            let bks = map (\x -> ht + x) [1 .. 100] -- cache size of 200
            let str = "SELECT block_height, block_hash from xoken.blocks_by_height where block_height in ?"
                qstr = str :: Q.QueryString Q.R (Identity [Int32]) ((Int32, T.Text))
                p = Q.defQueryParams Q.One $ Identity (bks)
            op <- Q.runClient conn (Q.query qstr p)
            if L.length op == 0
                then do
                    liftIO $ print ("Synced fully!")
                    return (False, Nothing)
                else do
                    liftIO $ print ("Reloading cache.")
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
            let (received, sent) = M.partition (\x -> fst x == BlockReceived) _others
            if M.size sent == 0 && M.size unsent == 0
                    -- all blocks received, empty the cache, cache-miss gracefully
                then do
                    let lelm = last $ L.sortOn (snd . snd) (M.toList sy)
                    liftIO $ atomically $ writeTVar (blockSyncStatusMap bp2pEnv) M.empty
                    liftIO $ markBestSyncedBlock (blockHashToHex $ fst $ lelm) (fromIntegral $ snd $ snd $ lelm) conn
                    return (False, Nothing)
                else if M.size unsent > 0
                         then return (True, Just $ BlockInfo (fst $ M.elemAt 0 unsent) (snd $ snd $ M.elemAt 0 unsent))
                         else do
                             let expired = M.filter (\((RequestSent t), _) -> (diffUTCTime tm t > 300)) sent
                             if M.size expired == 0
                                 then return (False, Nothing)
                                 else return
                                          ( False
                                          , Just $ BlockInfo (fst $ M.elemAt 0 expired) (snd $ snd $ M.elemAt 0 expired))

fetchBestSyncedBlock :: Q.ClientState -> Network -> IO ((BlockHash, Int32))
fetchBestSyncedBlock conn net = do
    let str = "SELECT value from xoken.misc_store where key = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Identity (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text))
        p = Q.defQueryParams Q.One $ Identity "best-synced"
    iop <- Q.runClient conn (Q.query qstr p)
    if L.length iop == 0
        then print ("Best-synced-block is genesis.") >> return ((headerHash $ getGenesisHeader net), 0)
        else do
            let record = runIdentity $ iop !! 0
            print ("Best-synced-block from DB: " ++ (show record))
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
       Q.ClientState
    -> Text
    -> Bool
    -> Maybe Text
    -> (Text, Int32)
    -> ((Text, Int32), Int32)
    -> (Text, Int32)
    -> Int64
    -> IO ()
commitAddressOutputs conn addr typeRecv otherAddr output blockInfo prevOutpoint value = do
    liftIO $ print ("commitAddressOutputs : ")
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
        Right () -> liftIO $ print ("done commiting.. ") >> return ()
        Left (e :: SomeException) ->
            liftIO $ print ("Error: INSERTing into 'address_outputs': " ++ show e) >>= throw KeyValueDBInsertException

processConfTransaction :: (HasXokenNodeEnv env m, MonadIO m) => Tx -> BlockHash -> Int -> Int -> m ()
processConfTransaction tx bhash txind blkht = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    liftIO $ print ("processing Transaction! " ++ show tx)
    let conn = keyValDB $ dbe'
        str = "insert INTO xoken.transactions ( tx_id, block_info, tx_serialized ) values (?, ?, ?)"
        qstr = str :: Q.QueryString Q.W (Text, ((Text, Int32), Int32), Blob) ()
        par =
            Q.defQueryParams
                Q.One
                ( txHashToHex $ txHash tx
                , ((blockHashToHex bhash, fromIntegral txind), fromIntegral blkht)
                , Blob $ runPutLazy $ putLazyByteString $ S.encodeLazy tx)
    liftIO $ print (show (txHash tx))
    liftIO $ LC.putStrLn $ A.encode tx
    res <- liftIO $ try $ Q.runClient conn (Q.write (qstr) par)
    case res of
        Right () -> return ()
        Left (e :: SomeException) ->
            liftIO $
            print ("Error: INSERTing into 'xoken.transactions': " ++ show e) >>= throw KeyValueDBInsertException
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
             liftIO $
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
             liftIO $
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
    liftIO $ print ("inAddrs : " ++ show inAddrs)
    liftIO $ print ("outAddrs : " ++ show outAddrs)
    return ()

getAddressFromOutpoint :: Q.ClientState -> Network -> OutPoint -> IO (Maybe Address)
getAddressFromOutpoint conn net outPoint = do
    let str = "SELECT tx_serialized from xoken.transactions where tx_id = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Identity Blob)
        p = Q.defQueryParams Q.One $ Identity $ txHashToHex $ outPointHash outPoint
    iop <- Q.runClient conn (Q.query qstr p)
    if L.length iop == 0
        then do
            print ("(retry) TxID not found: " ++ (show $ txHashToHex $ outPointHash outPoint))
            liftIO $ threadDelay (1000000 * 1)
            throw TxIDNotFoundRetryException
        else do
            let txbyt = runIdentity $ iop !! 0
            case runGetLazy (getConfirmedTx) (fromBlob txbyt) of
                Left e -> do
                    print (encodeHex $ BSL.toStrict $ fromBlob txbyt)
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

processBlock :: (HasXokenNodeEnv env m, MonadIO m) => DefBlock -> m ()
processBlock dblk = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    liftIO $ print ("processing deflated Block! " ++ show dblk)
    liftIO $ signalQSem (blockFetchBalance bp2pEnv)
    return ()
    -- if (L.length $ headersList hdrs) == 0
    --     then liftIO $ print "Nothing to process!" >> throw EmptyHeadersMessageException
    --     else liftIO $ print $ "Processing Headers with " ++ show (L.length $ headersList hdrs) ++ " entries."
    -- case undefined of
    --     True -> do
    --         let net = bncNet $ bitcoinNodeConfig bp2pEnv
    --             genesisHash = blockHashToHex $ headerHash $ getGenesisHeader net
    --             conn = keyValDB $ dbHandles dbe'
    --             headPrevHash = (blockHashToHex $ prevBlock $ fst $ head $ headersList hdrs)
    --         bb <- liftIO $ fetchBestSyncedBlock conn net
    --         if (blockHashToHex $ fst bb) == genesisHash
    --             then liftIO $ print ("First Headers set from genesis")
    --             else if (blockHashToHex $ fst bb) == headPrevHash
    --                      then liftIO $ print ("Links okay!")
    --                      else liftIO $ print ("Does not match DB best-block") >> throw BlockHashNotFoundException -- likely a previously sync'd block
    --         let indexed = zip [((snd bb) + 1) ..] (headersList hdrs)
    --             str1 = "insert INTO xoken.blocks_hash (blockhash, header, height, transactions) values (?, ? , ? , ?)"
    --             qstr1 = str1 :: Q.QueryString Q.W (Text, Text, Int32, Maybe Text) ()
    --             str2 = "insert INTO xoken.blocks_by_height (height, blockhash, header, transactions) values (?, ? , ? , ?)"
    --             qstr2 = str2 :: Q.QueryString Q.W (Int32, Text, Text, Maybe Text) ()
    --         liftIO $ print ("indexed " ++ show (L.length indexed))
    --         mapM_
    --             (\y -> do
    --                  let hdrHash = blockHashToHex $ headerHash $ fst $ snd y
    --                      hdrJson = T.pack $ LC.unpack $ A.encode $ fst $ snd y
    --                      par1 = Q.defQueryParams Q.One (hdrHash, hdrJson, fst y, Nothing)
    --                      par2 = Q.defQueryParams Q.One (fst y, hdrHash, hdrJson, Nothing)
    --                  res1 <- liftIO $ try $ Q.runClient conn (Q.write (Q.prepared qstr1) par1)
    --                  case res1 of
    --                      Right () -> return ()
    --                      Left (e :: SomeException) ->
    --                          liftIO $
    --                          print ("Error: INSERT into 'blocks_hash' failed: " ++ show e) >>=
    --                          throw KeyValueDBInsertException
    --                  res2 <- liftIO $ try $ Q.runClient conn (Q.write (Q.prepared qstr2) par2)
    --                  case res2 of
    --                      Right () -> return ()
    --                      Left (e :: SomeException) ->
    --                          liftIO $
    --                          print ("Error: INSERT into 'blocks_by_height' failed: " ++ show e) >>=
    --                          throw KeyValueDBInsertException)
    --             indexed
    --         liftIO $ print ("done..")
    --         liftIO $ do
    --             markBestSyncedBlock (blockHashToHex $ headerHash $ fst $ snd $ last $ indexed) (fst $ last indexed) conn
    --
    --         return ()
    --     False -> liftIO $ print ("Error: BlocksNotChainedException") >> throw BlocksNotChainedException
