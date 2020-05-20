{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.P2P.ChainSync
    ( runEgressChainSync
    , processHeaders
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.Async.Lifted as LA (async, race)
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
import System.Logger as LG
import System.Logger.Message
import System.Random

produceGetHeadersMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m (Message)
produceGetHeadersMessage = do
    lg <- getLogger
    debug lg $ LG.msg $ val "produceGetHeadersMessage - called."
    bp2pEnv <- getBitcoinP2P
    -- be blocked until a new best-block is updated in DB, or a set timeout.
    LA.race (liftIO $ takeMVar (bestBlockUpdated bp2pEnv)) (liftIO $ threadDelay (15 * 1000000))
    conn <- keyValDB <$> getDB
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    bl <- getBlockLocator conn net
    let gh =
            GetHeaders
                { getHeadersVersion = myVersion
                , getHeadersBL = bl
                , getHeadersHashStop = "0000000000000000000000000000000000000000000000000000000000000000"
                }
    debug lg $ LG.msg ("block-locator: " ++ show bl)
    return (MGetHeaders gh)

sendRequestMessages :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Message -> m ()
sendRequestMessages msg = do
    lg <- getLogger
    debug lg $ LG.msg $ val ("sendRequestMessages - called.")
    bp2pEnv <- getBitcoinP2P
    dbe' <- getDB
    let conn = keyValDB $ dbe'
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    case msg of
        MGetHeaders hdr -> do
            blockedPeers <- liftIO $ readTVarIO (blacklistedPeers bp2pEnv)
            allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
            let connPeers = L.filter (\x -> bpConnected (snd x) && not (M.member (fst x) blockedPeers)) (M.toList allPeers)

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
                                 liftIO $ sendEncMessage (bpWriteMsgLock pr) q (BSL.fromStrict em)
                                 debug lg $ LG.msg ("sending out GetHeaders: " ++ show (bpAddress pr))
                             Nothing -> debug lg $ LG.msg $ val "Error sending, no connections available")
                    indices
            case res of
                Right () -> return ()
                Left (e :: SomeException) -> do
                    err lg $ LG.msg ("Error, sending out data: " ++ show e)
        ___ -> undefined

msgOrder :: Message -> Message -> Ordering
msgOrder m1 m2 = do
    if msgType m1 == MCGetHeaders
        then LT
        else GT

runEgressChainSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runEgressChainSync = do
    lg <- getLogger
    res <- LE.try $ S.drain $ (S.repeatM produceGetHeadersMessage) & (S.mapM sendRequestMessages)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> err lg $ LG.msg $ "[ERROR] runEgressChainSync " ++ show e

validateChainedBlockHeaders :: Headers -> Bool
validateChainedBlockHeaders hdrs = do
    let xs = headersList hdrs
        pairs = zip xs (drop 1 xs)
    L.foldl' (\ac x -> ac && (headerHash $ fst (fst x)) == (prevBlock $ fst (snd x))) True pairs

markBestBlock :: (HasLogger m, MonadIO m) => Text -> Int32 -> Q.ClientState -> m ()
markBestBlock hash height conn = do
    lg <- getLogger
    let str = "insert INTO xoken.misc_store (key, value) values (? , ?)"
        qstr = str :: Q.QueryString Q.W (Text, (Maybe Bool, Int32, Maybe Int64, Text)) ()
        par = Q.defQueryParams Q.One ("best_chain_tip", (Nothing, height, Nothing, hash))
    res <- liftIO $ try $ Q.runClient conn (Q.write (Q.prepared qstr) par)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg ("Error: Marking [Best] blockhash failed: " ++ show e)
            throw KeyValueDBInsertException

getBlockLocator :: (HasLogger m, MonadIO m) => Q.ClientState -> Network -> m ([BlockHash])
getBlockLocator conn net = do
    lg <- getLogger
    (hash, ht) <- fetchBestBlock conn net
    let bl = L.insert ht $ filter (> 0) $ takeWhile (< ht) $ map (\x -> ht - (2 ^ x)) [0 .. 20] -- [1,2,4,8,16,32,64,... ,262144,524288,1048576]
        str = "SELECT block_height, block_hash from xoken.blocks_by_height where block_height in ?"
        qstr = str :: Q.QueryString Q.R (Identity [Int32]) ((Int32, T.Text))
        p = Q.defQueryParams Q.One $ Identity bl
    op <- Q.runClient conn (Q.query qstr p)
    if L.null op
        then return [headerHash $ getGenesisHeader net]
        else do
            debug lg $ LG.msg $ "Best-block from DB: " ++ (show $ last op)
            return $
                reverse $
                    catMaybes $ map (hexToBlockHash . snd) op

fetchBestBlock :: (HasLogger m, MonadIO m) => Q.ClientState -> Network -> m ((BlockHash, Int32))
fetchBestBlock conn net = do
    lg <- getLogger
    let str = "SELECT value from xoken.misc_store where key = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Identity (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text))
        p = Q.defQueryParams Q.One $ Identity "best_chain_tip"
    iop <- Q.runClient conn (Q.query qstr p)
    if L.null iop
        then do
            debug lg $ LG.msg $ val "Bestblock is genesis."
            return ((headerHash $ getGenesisHeader net), 0)
        else do
            let record = runIdentity $ iop !! 0
            debug lg $ LG.msg $ "Best-block from DB: " ++ show (record)
            case getTextVal record of
                Just tx -> do
                    case (hexToBlockHash $ tx) of
                        Just x -> do
                            case getIntVal record of
                                Just y -> return (x, y)
                                Nothing -> throw InvalidMetaDataException
                        Nothing -> throw InvalidBlockHashException
                Nothing -> throw InvalidMetaDataException

processHeaders :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Headers -> m ()
processHeaders hdrs = do
    dbe' <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    if (L.null $ headersList hdrs)
        then do
            debug lg $ LG.msg $ val "Nothing to process!"
            throw EmptyHeadersMessageException
        else debug lg $ LG.msg $ "Processing Headers with " ++ show (L.length $ headersList hdrs) ++ " entries."
    case validateChainedBlockHeaders hdrs of
        True -> do
            let net = bncNet $ bitcoinNodeConfig bp2pEnv
                genesisHash = blockHashToHex $ headerHash $ getGenesisHeader net
                conn = keyValDB $ dbe'
                headPrevHash = (blockHashToHex $ prevBlock $ fst $ head $ headersList hdrs)
                hdrHash y = headerHash $ fst y
                validate m = validateWithCheckPoint net (fromIntegral m) (hdrHash <$> (headersList hdrs))
            bb <- fetchBestBlock conn net
            -- TODO: throw exception if it's a bitcoin cash block
            indexed <-
                if (blockHashToHex $ fst bb) == genesisHash
                    then do
                        debug lg $ LG.msg $ val "First Headers set from genesis"
                        return $ zip [((snd bb) + 1) ..] (headersList hdrs)
                    else if (blockHashToHex $ fst bb) == headPrevHash
                             then do
                                 unless (validate (snd bb)) $ throw InvalidBlocksException
                                 debug lg $ LG.msg $ val "Building on current Best block"
                                 return $ zip [((snd bb) + 1) ..] (headersList hdrs)
                             else do
                                 if ((fst bb) == (headerHash $ fst $ last $ headersList hdrs))
                                     then do
                                         debug lg $ LG.msg $ LG.val ("Does not match best-block, redundant Headers msg")
                                         return [] -- already synced
                                     else do
                                         res <- fetchMatchBlockOffset conn net headPrevHash
                                         case res of
                                             Just (matchBHash, matchBHt) -> do
                                                 unless (validate matchBHt) $ throw InvalidBlocksException
                                                 if ((snd bb) >
                                                     (matchBHt + fromIntegral (L.length $ headersList hdrs) + 12) -- reorg limit of 12 blocks
                                                     )
                                                     then do
                                                         debug lg $
                                                             LG.msg $
                                                             LG.val
                                                                 ("Does not match best-block, assuming stale Headers msg")
                                                         return [] -- assuming its stale/redundant and ignore
                                                     else do
                                                         debug lg $
                                                             LG.msg $
                                                             LG.val
                                                                 ("Does not match best-block, potential block re-org ")
                                                         return $ zip [(matchBHt + 1) ..] (headersList hdrs) -- potential re-org
                                             Nothing -> throw BlockHashNotFoundException
            let str1 = "insert INTO xoken.blocks_by_hash (block_hash, block_header, block_height) values (?, ? , ? )"
                qstr1 = str1 :: Q.QueryString Q.W (Text, Text, Int32) ()
                str2 = "insert INTO xoken.blocks_by_height (block_height, block_hash, block_header) values (?, ? , ? )"
                qstr2 = str2 :: Q.QueryString Q.W (Int32, Text, Text) ()
            debug lg $ LG.msg $ "indexed " ++ show (L.length indexed)
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
                             liftIO $ do
                                 err lg $ LG.msg ("Error: INSERT into 'blocks_hash' failed: " ++ show e)
                                 throw KeyValueDBInsertException
                     res2 <- liftIO $ try $ Q.runClient conn (Q.write (Q.prepared qstr2) par2)
                     case res2 of
                         Right () -> return ()
                         Left (e :: SomeException) -> do
                             err lg $ LG.msg ("Error: INSERT into 'blocks_by_height' failed: " ++ show e)
                             throw KeyValueDBInsertException)
                indexed
            unless (L.null indexed) $ do
                    markBestBlock (blockHashToHex $ headerHash $ fst $ snd $ last $ indexed) (fst $ last indexed) conn
                    liftIO $ putMVar (bestBlockUpdated bp2pEnv) True
        False -> do
            err lg $ LG.msg $ val "Error: BlocksNotChainedException"
            throw BlocksNotChainedException

fetchMatchBlockOffset :: (HasLogger m, MonadIO m) => Q.ClientState -> Network -> Text -> m (Maybe (Text, Int32))
fetchMatchBlockOffset conn net hashes = do
    lg <- getLogger
    let str = "SELECT block_height, block_hash from xoken.blocks_by_hash where block_hash = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Int32, Text)
        p = Q.defQueryParams Q.One $ Identity hashes
    iop <- Q.runClient conn (Q.query qstr p)
    if L.null iop
        then return Nothing
        else do
            let (maxHt, bhash) = iop !! 0
            return $ Just (bhash, maxHt)
