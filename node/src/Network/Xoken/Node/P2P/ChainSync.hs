{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.P2P.ChainSync
    ( runEgressChainSync
    , processHeaders
    ) where

import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.Async.Lifted as LA (async)
import Control.Concurrent.MVar
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
import Network.Xoken.Network.Common -- (GetData(..), MessageCommand(..), NetworkAddress(..))
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
    liftIO $ print ("produceGetHeadersMessage - called.")
    bp2pEnv <- asks getBitcoinP2PEnv
    dbe' <- asks getDBEnv
    liftIO $ takeMVar (bestBlockUpdated bp2pEnv) -- be blocked until a new best-block is updated in DB.
    let conn = keyValDB $ dbHandles dbe'
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    bl <- liftIO $ getBlockLocator conn net
    let gh =
            GetHeaders
                { getHeadersVersion = myVersion
                , getHeadersBL = bl
                , getHeadersHashStop = "0000000000000000000000000000000000000000000000000000000000000000"
                }
    liftIO $ print ("block-locator: " ++ show bl)
    return (MGetHeaders gh)

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
        MGetHeaders hdr -> do
            let fbh = getHash256 $ getBlockHash $ (getHeadersBL hdr) !! 0
                md = BSS.index fbh $ (BSS.length fbh) - 1
                pds =
                    map
                        (\p -> (fromIntegral (md + p) `mod` L.length connPeers))
                        [1 .. fromIntegral (L.length connPeers)]
                indices =
                    case L.length (getHeadersBL hdr) of
                        x
                            | x >= 19 -> take 4 pds -- 2^19 = blk ht 524288
                            | x < 19 -> take 1 pds
            res <-
                liftIO $
                try $
                mapM_
                    (\z -> do
                         let pr = snd $ connPeers !! z
                         case (bpSocket pr) of
                             Just q -> do
                                 let em = runPut . putMessage net $ msg
                                 liftIO $ sendEncMessage (bpSockLock pr) q (BSL.fromStrict em)
                                 liftIO $ print ("sending out GetHeaders: " ++ show (bpAddress pr))
                             Nothing -> liftIO $ print ("Error sending, no connections available"))
                    indices
            case res of
                Right () -> return ()
                Left (e :: SomeException) -> do
                    liftIO $ print ("Error, sending out data: " ++ show e)
        ___ -> undefined

msgOrder :: Message -> Message -> Ordering
msgOrder m1 m2 = do
    if msgType m1 == MCGetHeaders
        then LT
        else GT

runEgressChainSync :: (HasService env m) => m ()
runEgressChainSync = do
    res <- LE.try $ runStream $ (S.repeatM produceGetHeadersMessage) & (S.mapM sendRequestMessages)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> liftIO $ print ("[ERROR] runEgressChainSync " ++ show e)
        -- S.mergeBy msgOrder (S.repeatM produceGetHeadersMessage) (S.repeatM produceGetHeadersMessage) &
        --    S.mapM   sendRequestMessages

validateChainedBlockHeaders :: Headers -> Bool
validateChainedBlockHeaders hdrs = do
    let xs = headersList hdrs
        pairs = zip xs (drop 1 xs)
        res = map (\x -> (headerHash $ fst (fst x)) == (prevBlock $ fst (snd x))) pairs
    if all (== True) res
        then True
        else False

markBestBlock :: Text -> Int32 -> Q.ClientState -> IO ()
markBestBlock hash height conn = do
    let str = "insert INTO xoken.misc_store (key, value) values (? , ?)"
        qstr = str :: Q.QueryString Q.W (Text, (Maybe Bool, Int32, Maybe Int64, Text)) ()
        par = Q.defQueryParams Q.One ("best-block", (Nothing, height, Nothing, hash))
    res <- try $ Q.runClient conn (Q.write (Q.prepared qstr) par)
    case res of
        Right () -> return ()
        Left (e :: SomeException) ->
            print ("Error: Marking [Best] blockhash failed: " ++ show e) >> throw KeyValueDBInsertException

getBlockLocator :: Q.ClientState -> Network -> IO ([BlockHash])
getBlockLocator conn net = do
    (hash, ht) <- fetchBestBlock conn net
    let bl = L.insert ht $ filter (> 0) $ takeWhile (< ht) $ map (\x -> ht - (2 ^ x)) [0 .. 20] -- [1,2,4,8,16,32,64,... ,262144,524288,1048576]
        str = "SELECT block_height, block_hash from xoken.blocks_by_height where block_height in ?"
        qstr = str :: Q.QueryString Q.R (Identity [Int32]) ((Int32, T.Text))
        p = Q.defQueryParams Q.One $ Identity bl
    op <- Q.runClient conn (Q.query qstr p)
    if L.length op == 0
        then return [headerHash $ getGenesisHeader net]
        else do
            print ("Best-block from DB: " ++ (show $ last op))
            return $
                catMaybes $
                (map (\x ->
                          case (hexToBlockHash $ snd x) of
                              Just y -> Just y
                              Nothing -> Nothing)
                     (reverse op))

fetchBestBlock :: Q.ClientState -> Network -> IO ((BlockHash, Int32))
fetchBestBlock conn net = do
    let str = "SELECT value from xoken.misc_store where key = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Identity (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text))
        p = Q.defQueryParams Q.One $ Identity "best-block"
    iop <- Q.runClient conn (Q.query qstr p)
    if L.length iop == 0
        then print ("Bestblock is genesis.") >> return ((headerHash $ getGenesisHeader net), 0)
        else do
            let record = runIdentity $ iop !! 0
            print ("Best-block from DB: " ++ show (record))
            case getTextVal record of
                Just tx -> do
                    case (hexToBlockHash $ tx) of
                        Just x -> do
                            case getIntVal record of
                                Just y -> return (x, y)
                                Nothing -> throw InvalidMetaDataException
                        Nothing -> throw InvalidBlockHashException
                Nothing -> throw InvalidMetaDataException

processHeaders :: (HasService env m) => Headers -> m ()
processHeaders hdrs = do
    dbe' <- asks getDBEnv
    bp2pEnv <- asks getBitcoinP2PEnv
    if (L.length $ headersList hdrs) == 0
        then liftIO $ print "Nothing to process!" >>= throw EmptyHeadersMessageException
        else liftIO $ print $ "Processing Headers with " ++ show (L.length $ headersList hdrs) ++ " entries."
    case validateChainedBlockHeaders hdrs of
        True -> do
            let net = bncNet $ bitcoinNodeConfig bp2pEnv
                genesisHash = blockHashToHex $ headerHash $ getGenesisHeader net
                conn = keyValDB $ dbHandles dbe'
                headPrevHash = (blockHashToHex $ prevBlock $ fst $ head $ headersList hdrs)
            bb <- liftIO $ fetchBestBlock conn net
            if (blockHashToHex $ fst bb) == genesisHash
                then liftIO $ print ("First Headers set from genesis")
                else if (blockHashToHex $ fst bb) == headPrevHash
                         then liftIO $ print ("Links okay!")
                         else liftIO $ print ("Does not match DB best-block") >> throw BlockHashNotFoundException -- likely a previously sync'd block
            let indexed = zip [((snd bb) + 1) ..] (headersList hdrs)
                str1 = "insert INTO xoken.blocks_by_hash (block_hash, block_header, block_height) values (?, ? , ? )"
                qstr1 = str1 :: Q.QueryString Q.W (Text, Text, Int32) ()
                str2 = "insert INTO xoken.blocks_by_height (block_height, block_hash, block_header) values (?, ? , ? )"
                qstr2 = str2 :: Q.QueryString Q.W (Int32, Text, Text) ()
            liftIO $ print ("indexed " ++ show (L.length indexed))
            mapM_
                (\y -> do
                     let hdrHash = blockHashToHex $ headerHash $ fst $ snd y
                         hdrJson = T.pack $ LC.unpack $ A.encode $ fst $ snd y
                         par1 = Q.defQueryParams Q.One (hdrHash, hdrJson, fst y)
                         par2 = Q.defQueryParams Q.One (fst y, hdrHash, hdrJson)
                     res1 <- liftIO $ try $ Q.runClient conn (Q.write (Q.prepared qstr1) par1)
                     case res1 of
                         Right () -> return ()
                         Left (e :: SomeException) ->
                             liftIO $
                             print ("Error: INSERT into 'blocks_hash' failed: " ++ show e) >>=
                             throw KeyValueDBInsertException
                     res2 <- liftIO $ try $ Q.runClient conn (Q.write (Q.prepared qstr2) par2)
                     case res2 of
                         Right () -> return ()
                         Left (e :: SomeException) ->
                             liftIO $
                             print ("Error: INSERT into 'blocks_by_height' failed: " ++ show e) >>=
                             throw KeyValueDBInsertException)
                indexed
            liftIO $ print ("done..")
            liftIO $ do
                markBestBlock (blockHashToHex $ headerHash $ fst $ snd $ last $ indexed) (fst $ last indexed) conn
                putMVar (bestBlockUpdated bp2pEnv) True
            return ()
        False -> liftIO $ print ("Error: BlocksNotChainedException") >> throw BlocksNotChainedException
    return ()

logMessage :: (HasService env m) => MessageCommand -> m ()
logMessage mg = do
    liftIO $ print ("processed: " ++ show mg)
    return ()
