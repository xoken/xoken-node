{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.BlockSync
    ( processBlock
    , runEgressBlockSync
    ) where

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
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Serialize
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import qualified Database.CQL.IO as Q
import Network.Socket
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Random

produceGetHeadersMessage :: (HasService env m) => m (Message)
produceGetHeadersMessage = do
    liftIO $ print ("producing - called.")
    bp2pEnv <- asks getBitcoinP2PEnv
    dbe' <- asks getDBEnv
    liftIO $ waitQSem (blockFetchBalance bp2pEnv)
    let conn = keyValDB $ dbHandles dbe'
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    bl <- liftIO $ getNextBatch conn net
    let gd = GetData $ map (\x -> InvVector InvBlock $ getBlockHash x) bl
    liftIO $ print ("block-locator: " ++ show bl)
    return (MGetData gd)

sendRequestMessages :: (HasService env m) => Message -> m ()
sendRequestMessages msg = do
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
                        Left (e :: SomeException) -> do
                            liftIO $ print ("Error, sending out data: " ++ show e)
                    liftIO $ print ("sending out GetData: " ++ show (bpAddress pr))
                Nothing -> liftIO $ print ("Error sending, no connections available")
            return ()
        ___ -> undefined

runEgressBlockSync :: (HasService env m) => m ()
runEgressBlockSync = do
    res <- LE.try $ runStream $ (S.repeatM produceGetHeadersMessage) & (S.mapM sendRequestMessages)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> liftIO $ print ("[ERROR] runEgressChainSync " ++ show e)
        -- S.mergeBy msgOrder (S.repeatM produceGetHeadersMessage) (S.repeatM produceGetHeadersMessage) &
        --    S.mapM   sendRequestMessages

markBestSyncedBlock :: Text -> Int32 -> Q.ClientState -> IO ()
markBestSyncedBlock hash height conn = do
    let str = "insert INTO xoken.proc_summary (name, strval, numval, boolval) values (? ,? ,?, ?)"
        qstr = str :: Q.QueryString Q.W (Text, Text, Int32, Maybe Bool) ()
        par = Q.defQueryParams Q.One ("best-synced", hash, height :: Int32, Nothing)
    res <- try $ Q.runClient conn (Q.write (Q.prepared qstr) par)
    case res of
        Right () -> return ()
        Left (e :: SomeException) ->
            print ("Error: Marking [Best] blockhash failed: " ++ show e) >> throw KeyValueDBInsertException

getNextBatch :: Q.ClientState -> Network -> IO ([BlockHash])
getNextBatch conn net = do
    (hash, ht) <- fetchBestSyncedBlock conn net
    let bt = L.insert ht $ map (\x -> ht + x) [1 .. 4]
        str = "SELECT height, blockhash from xoken.blocks_height where height in ?"
        qstr = str :: Q.QueryString Q.R (Identity [Int32]) ((Int32, T.Text))
        p = Q.defQueryParams Q.One $ Identity bt
    op <- Q.runClient conn (Q.query qstr p)
    if L.length op == 0
        then return [headerHash $ getGenesisHeader net]
        else do
            print ("Best-block from DB: " ++ (show $ op))
            return $
                catMaybes $
                (map (\x ->
                          case (hexToBlockHash $ snd x) of
                              Just y -> Just y
                              Nothing -> Nothing)
                     (reverse op))

fetchBestSyncedBlock :: Q.ClientState -> Network -> IO ((BlockHash, Int32))
fetchBestSyncedBlock conn net = do
    let str = "SELECT strval, numval from xoken.proc_summary where name = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) ((T.Text, Int32))
        p = Q.defQueryParams Q.One $ Identity "best-synced"
    op <- Q.runClient conn (Q.query qstr p)
    if L.length op == 0
        then print ("Best-synced-block is genesis.") >> return ((headerHash $ getGenesisHeader net), 0)
        else do
            print ("Best-synced-block from DB: " ++ show ((op !! 0)))
            case (hexToBlockHash $ fst (op !! 0)) of
                Just x -> return (x, snd (op !! 0))
                Nothing -> do
                    print ("block hash seems invalid, startin over from genesis") -- not optimal, but unforseen case.
                    return ((headerHash $ getGenesisHeader net), 0)

processBlock :: (HasService env m) => DefBlock -> m ()
processBlock dblk = do
    dbe' <- asks getDBEnv
    bp2pEnv <- asks getBitcoinP2PEnv
    liftIO $ print ("Nothing to process!" ++ show dblk)
    -- if (L.length $ headersList hdrs) == 0
    --     then liftIO $ print "Nothing to process!" >> throw EmptyHeadersMessage
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
    --                      else liftIO $ print ("Does not match DB best-block") >> throw BlockHashNotFoundInDB -- likely a previously sync'd block
    --         let indexed = zip [((snd bb) + 1) ..] (headersList hdrs)
    --             str1 = "insert INTO xoken.blocks_hash (blockhash, header, height, transactions) values (?, ? , ? , ?)"
    --             qstr1 = str1 :: Q.QueryString Q.W (Text, Text, Int32, Maybe Text) ()
    --             str2 = "insert INTO xoken.blocks_height (height, blockhash, header, transactions) values (?, ? , ? , ?)"
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
    --                          print ("Error: INSERT into 'blocks_height' failed: " ++ show e) >>=
    --                          throw KeyValueDBInsertException)
    --             indexed
    --         liftIO $ print ("done..")
    --         liftIO $ do
    --             markBestSyncedBlock (blockHashToHex $ headerHash $ fst $ snd $ last $ indexed) (fst $ last indexed) conn
    --             signalQSem (blockFetchBalance bp2pEnv)
    --         return ()
    --     False -> liftIO $ print ("Error: BlocksNotChainedException") >> throw BlocksNotChainedException
    return ()
