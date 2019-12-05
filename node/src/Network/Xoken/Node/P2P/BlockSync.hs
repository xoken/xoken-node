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
import Network.Xoken.Transaction.Common
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Random

produceGetDataMessage :: (HasService env m) => m (Maybe Message)
produceGetDataMessage = do
    liftIO $ print ("produceGetDataMessage - called.")
    bp2pEnv <- asks getBitcoinP2PEnv
    (fl, bl) <- getNextBlockToSync
    case bl of
        Just b -> do
            if fl == True
                then liftIO $ waitQSem (blockFetchBalance bp2pEnv)
                else return ()
            t <- liftIO $ getCurrentTime
            liftIO $ atomically $ modifyTVar (blockSyncStatus bp2pEnv) (M.insert b $ Just (False, t))
            let gd = GetData $ [InvVector InvBlock $ getBlockHash b]
            liftIO $ print ("GetData req: " ++ show gd)
            return (Just $ MGetData gd)
        Nothing -> do
            liftIO $ print ("producing - empty ...")
            liftIO $ threadDelay (1000000 * 10)
            return Nothing

sendRequestMessages :: (HasService env m) => Maybe Message -> m ()
sendRequestMessages mmsg = do
    case mmsg of
        Just msg -> do
            liftIO $ print ("sendRequestMessages - called.")
            bp2pEnv <- asks getBitcoinP2PEnv
            dbe' <- asks getDBEnv
            let conn = keyValDB $ dbHandles dbe'
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
                            res <- liftIO $ try $ sendEncMessage (bpSockLock pr) s (BSL.fromStrict em)
                            case res of
                                Right () -> return ()
                                Left (e :: SomeException) -> liftIO $ print ("Error, sending out data: " ++ show e)
                            liftIO $ print ("sending out GetData: " ++ show (bpAddress pr))
                        Nothing -> liftIO $ print ("Error sending, no connections available")
                ___ -> return ()
        Nothing -> do
            liftIO $ print ("graceful ignore...")
            return ()

runEgressBlockSync :: (HasService env m) => m ()
runEgressBlockSync = do
    res <- LE.try $ runStream $ (S.repeatM produceGetDataMessage) & (S.mapM sendRequestMessages)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> liftIO $ print ("[ERROR] runEgressBlockSync " ++ show e)
        -- S.mergeBy msgOrder (S.repeatM produceGetDataMessage) (S.repeatM produceGetDataMessage) &
        --    S.mapM   sendRequestMessages

-- markBestSyncedBlock :: Text -> Int32 -> Q.ClientState -> IO ()
-- markBestSyncedBlock hash height conn = do
--     let str = "insert INTO xoken.misc_store (name, strval, numval, boolval) values (? ,? ,?, ?)"
--         qstr = str :: Q.QueryString Q.W (Text, Text, Int32, Maybe Bool) ()
--         par = Q.defQueryParams Q.One ("best-synced", hash, height :: Int32, Nothing)
--     res <- try $ Q.runClient conn (Q.write (Q.prepared qstr) par)
--     case res of
--         Right () -> return ()
--         Left (e :: SomeException) ->
--             print ("Error: Marking [Best] blockhash failed: " ++ show e) >> throw KeyValueDBInsertException
getNextBlockToSync :: (HasService env m) => m (Bool, Maybe BlockHash)
getNextBlockToSync = do
    bp2pEnv <- asks getBitcoinP2PEnv
    dbe' <- asks getDBEnv
    let conn = keyValDB $ dbHandles dbe'
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    sy <- liftIO $ readTVarIO $ blockSyncStatus bp2pEnv
    tm <- liftIO $ getCurrentTime
    -- reload cache
    if M.size sy == 0
        then do
            (hash, ht) <- liftIO $ fetchBestSyncedBlock conn net
            let bks = map (\x -> ht + x) [1 .. 200] -- cache size of 200
            let str = "SELECT block_height, block_hash from xoken.blocks_by_height where block_height in ?"
                qstr = str :: Q.QueryString Q.R (Identity [Int32]) ((Int32, T.Text))
                p = Q.defQueryParams Q.One $ Identity (bks)
            op <- Q.runClient conn (Q.query qstr p)
            if L.length op == 0
                then do
                    liftIO $ print ("Synced fully!")
                    return (False, Nothing)
                else do
                    liftIO $ print ("Cache reload: " ++ (show $ op))
                    let z = catMaybes $ map (\x -> hexToBlockHash $ snd x) (reverse op)
                        p = map (\x -> (x, Nothing)) z
                    liftIO $ atomically $ writeTVar (blockSyncStatus bp2pEnv) (M.fromList p)
                    return (True, Just $ fst $ p !! 0)
        else do
            let (sent, unsent) =
                    M.partition
                        (\x ->
                             case x of
                                 Just z -> not $ fst z
                                 Nothing -> False)
                        (sy)
            if M.size sent == 0
                then return (True, Just $ fst (M.elemAt 0 unsent))
                else do
                    let expired =
                            M.filter
                                (\x ->
                                     case x of
                                         Just z ->
                                             (not $ fst z) && ((diffUTCTime tm (snd z) > (300 :: NominalDiffTime)))
                                         Nothing -> False)
                                (sent)
                    if M.size expired == 0
                        then if M.size unsent == 0
                                 -- empty the cache, cache-miss gracefully
                                 then do
                                     liftIO $ atomically $ writeTVar (blockSyncStatus bp2pEnv) M.empty
                                     return (False, Nothing)
                                 else return (True, Just $ fst (M.elemAt 0 unsent))
                        else return (False, Just $ fst (M.elemAt 0 expired))

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

processConfTransaction :: (HasService env m) => Tx -> BlockHash -> Int -> Int -> m ()
processConfTransaction tx bhash blkht txind = do
    dbe' <- asks getDBEnv
    bp2pEnv <- asks getBitcoinP2PEnv
    liftIO $ print ("processing Transaction! " ++ show tx)
    let conn = keyValDB $ dbHandles dbe'
        str1 = "insert INTO xoken.transactions ( tx_id, block_info, tx_serialized ) values (?, ?, ?)"
            -- blockhash, txindex, txid, serialized, deserialized
        qstr1 = str1 :: Q.QueryString Q.W (Text, ((Text, Int32), Int32), Blob) ()
        -- blkref = BlockRef 100 (fromIntegral txind) -- TODO: replace dummy value
        -- sps = map (\x -> Spender (outPointHash $ prevOutput x) (outPointIndex $ prevOutput x)) (txIn tx)
        -- txdt = TxData blkref tx I.empty False False (fromIntegral 999)
        par1 =
            Q.defQueryParams
                Q.One
                ( txHashToHex $ txHash tx
                , ((blockHashToHex bhash, fromIntegral txind), fromIntegral blkht)
                , Blob $ runPutLazy $ putLazyByteString $ S.encodeLazy tx)
                -- , Blob $ A.encode tx)
    liftIO $ print (show (txHash tx))
    liftIO $ LC.putStrLn $ A.encode tx
    res1 <- liftIO $ try $ Q.runClient conn (Q.write (qstr1) par1)
    case res1 of
        Right () -> return ()
        Left (e :: SomeException) ->
            liftIO $ print ("Error: INSERTing into 'blocks_by_hash': " ++ show e) >>= throw KeyValueDBInsertException
    return ()

processBlock :: (HasService env m) => DefBlock -> m ()
processBlock dblk = do
    dbe' <- asks getDBEnv
    bp2pEnv <- asks getBitcoinP2PEnv
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
