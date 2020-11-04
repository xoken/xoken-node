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
import Control.Concurrent.Async (mapConcurrently_)
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
import qualified Data.Aeson as A (decode, eitherDecode, encode)
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
import qualified Data.Text.Encoding as DTE
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import qualified Database.XCQL.Protocol as Q
import Network.Socket
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common -- (GetData(..), MessageCommand(..), NetworkAddress(..))
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Service.Block
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Xoken.NodeConfig

produceGetHeadersMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m (Message)
produceGetHeadersMessage = do
    lg <- getLogger
    debug lg $ LG.msg $ val "produceGetHeadersMessage - called."
    bp2pEnv <- getBitcoinP2P
    -- be blocked until a new best-block is updated in DB, or a set timeout.
    LA.race (liftIO $ threadDelay (15 * 1000000)) (liftIO $ takeMVar (bestBlockUpdated bp2pEnv))
    dbe <- getDB
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        conn = xCqlClientState dbe
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
    debug lg $ LG.msg $ val ("Chain - sendRequestMessages - called.")
    bp2pEnv <- getBitcoinP2P
    dbe' <- getDB
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    case msg of
        MGetHeaders hdr -> do
            allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
            let connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
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
                    throw e
        ___ -> undefined

msgOrder :: Message -> Message -> Ordering
msgOrder m1 m2 = do
    if msgType m1 == MCGetHeaders
        then LT
        else GT

runEgressChainSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runEgressChainSync = do
    lg <- getLogger
    res1 <- LE.try $ forever $ do produceGetHeadersMessage >>= sendRequestMessages
    case res1 of
        Right () -> return ()
        Left (e :: SomeException) -> err lg $ LG.msg $ "[ERROR] runEgressChainSync " ++ show e

validateChainedBlockHeaders :: Headers -> Bool
validateChainedBlockHeaders hdrs = do
    let xs = headersList hdrs
        pairs = zip xs (drop 1 xs)
    L.foldl' (\ac x -> ac && (headerHash $ fst (fst x)) == (prevBlock $ fst (snd x))) True pairs

markBestBlock :: (HasLogger m, MonadIO m) => Text -> Int32 -> XCqlClientState -> m ()
markBestBlock hash height conn = do
    lg <- getLogger
    let q :: Q.QueryString Q.W (Text, (Maybe Bool, Int32, Maybe Int64, Text)) ()
        q = Q.QueryString "insert INTO xoken.misc_store (key, value) values (? , ?)"
        p :: Q.QueryParams (Text, (Maybe Bool, Int32, Maybe Int64, Text))
        p = getSimpleQueryParam ("best_chain_tip", (Nothing, height, Nothing, hash))
    res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query q p)
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg ("Error: Marking [Best] blockhash failed: " ++ show e)
            throw KeyValueDBInsertException

getBlockLocator :: (HasLogger m, MonadIO m) => XCqlClientState -> Network -> m ([BlockHash])
getBlockLocator conn net = do
    lg <- getLogger
    (hash, ht) <- fetchBestBlock conn net
    let bl = L.insert ht $ filter (> 0) $ takeWhile (< ht) $ map (\x -> ht - (2 ^ x)) [0 .. 20] -- [1,2,4,8,16,32,64,... ,262144,524288,1048576]
        qstr :: Q.QueryString Q.R (Identity [Int32]) ((Int32, T.Text))
        qstr = "SELECT block_height, block_hash from xoken.blocks_by_height where block_height in ?"
        p = getSimpleQueryParam $ Identity bl
    op <- liftIO $ query conn (Q.RqQuery $ Q.Query qstr p)
    if L.null op
        then return [headerHash $ getGenesisHeader net]
        else do
            debug lg $ LG.msg $ "Best-block from DB: " ++ (show $ last op)
            return $ reverse $ catMaybes $ map (hexToBlockHash . snd) op

fetchBestBlock :: (HasLogger m, MonadIO m) => XCqlClientState -> Network -> m ((BlockHash, Int32))
fetchBestBlock conn net = do
    lg <- getLogger
    let qstr :: Q.QueryString Q.R (Identity Text) (Identity (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text))
        qstr = "SELECT value from xoken.misc_store where key = ?"
        p = getSimpleQueryParam "best_chain_tip"
    res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
    case res of
        (Right iop) -> do
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
        Left (e :: SomeException) -> throw InvalidMetaDataException

fetchBestSynced :: (HasLogger m, MonadIO m) => XCqlClientState -> Network -> m (BlockHash, Int32)
fetchBestSynced conn net = do
    lg <- getLogger
    let qstr :: Q.QueryString Q.R (Identity Text) (Identity (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text))
        qstr = "SELECT value from xoken.misc_store where key = ?"
        p = getSimpleQueryParam "best-synced"
    res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
    case res of
        (Right iop) -> do
            if L.null iop
                then do
                    return ((headerHash $ getGenesisHeader net), 0)
                else do
                    let record = runIdentity $ iop !! 0
                    case getTextVal record of
                        Just tx -> do
                            case (hexToBlockHash $ tx) of
                                Just x -> do
                                    case getIntVal record of
                                        Just y -> return (x, y)
                                        Nothing -> throw InvalidMetaDataException
                                Nothing -> throw InvalidBlockHashException
                        Nothing -> throw InvalidMetaDataException
        Left (e :: SomeException) -> throw InvalidMetaDataException

setBestSynced :: (HasLogger m, MonadIO m) => XCqlClientState -> Network -> Int32 -> T.Text -> m ()
setBestSynced conn net bsHeight bsHash = do
    lg <- getLogger
    let qstr :: Q.QueryString Q.W (Identity (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text)) ()
        qstr = "UPDATE xoken.misc_store SET value = (?,?,?,?) WHERE key = 'best-synced'"
        par = getSimpleQueryParam (Identity (Nothing, Just bsHeight, Nothing, Just bsHash))
    queryId <- liftIO $ queryPrepared conn (Q.RqPrepare (Q.Prepare qstr))
    res <- liftIO $ try $ write conn (Q.RqExecute $ Q.Execute queryId par)
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into misc_store (best-synced): " <> (show e)
            throw KeyValueDBInsertException

processHeaders :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Headers -> m ()
processHeaders hdrs = do
    dbe' <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        conn = xCqlClientState dbe'
    if (L.null $ headersList hdrs)
        then do
            debug lg $ LG.msg $ val "Nothing to process!"
            throw EmptyHeadersMessageException
        else debug lg $ LG.msg $ "Processing Headers with " ++ show (L.length $ headersList hdrs) ++ " entries."
    case validateChainedBlockHeaders hdrs of
        True -> do
            let net = bitcoinNetwork $ nodeConfig bp2pEnv
                genesisHash = blockHashToHex $ headerHash $ getGenesisHeader net
                conn = xCqlClientState dbe'
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
                                                         let reOrgDiff = zip [(matchBHt + 1) ..] (headersList hdrs) -- potential re-org
                                                         bestSynced <- fetchBestSynced conn net
                                                         if snd bestSynced >= (fst $ head reOrgDiff)
                                                             then do
                                                                 setBestSynced
                                                                     conn
                                                                     net
                                                                     (fst $ head reOrgDiff)
                                                                     (blockHashToHex $
                                                                      headerHash $ fst $ snd $ head reOrgDiff)
                                                                 return reOrgDiff
                                                             else return reOrgDiff
                                             Nothing -> throw BlockHashNotFoundException
            let q1 :: Q.QueryString Q.W (Text, Text, Int32, Text) ()
                q1 =
                    Q.QueryString
                        "insert INTO xoken.blocks_by_hash (block_hash, block_header, block_height, next_block_hash) values (?, ? , ?, ?)"
                q2 :: Q.QueryString Q.W (Int32, Text, Text, Text) ()
                q2 =
                    Q.QueryString
                        "insert INTO xoken.blocks_by_height (block_height, block_hash, block_header, next_block_hash) values (?, ? , ?, ?)"
                q3 :: Q.QueryString Q.W (Text, Text) ()
                q3 = Q.QueryString "UPDATE xoken.blocks_by_hash SET next_block_hash=? where block_hash=?"
                q4 :: Q.QueryString Q.W (Text, Int32) ()
                q4 = Q.QueryString "UPDATE xoken.blocks_by_height SET next_block_hash=? where block_height=?"
                lenIndexed = L.length indexed
            debug lg $ LG.msg $ "indexed " ++ show (lenIndexed)
            liftIO $
                mapConcurrently_
                    (\(ind, y) -> do
                         let hdrHash = blockHashToHex $ headerHash $ fst $ snd y
                             nextHdrHash =
                                 if ind == (lenIndexed - 1)
                                     then ""
                                     else blockHashToHex $ headerHash $ fst $ snd $ (indexed !! (ind + 1))
                             hdrJson = T.pack $ LC.unpack $ A.encode $ fst $ snd y
                         let p1 = getSimpleQueryParam (hdrHash, hdrJson, fst y, nextHdrHash)
                             p2 = getSimpleQueryParam (fst y, hdrHash, hdrJson, nextHdrHash)
                         res1 <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query q1 p1)
                         case res1 of
                             Right _ -> return ()
                             Left (e :: SomeException) ->
                                 liftIO $ do
                                     err lg $ LG.msg ("Error: INSERT into 'blocks_hash' failed: " ++ show e)
                                     throw KeyValueDBInsertException
                         res2 <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query q2 p2)
                         case res2 of
                             Right _ -> return ()
                             Left (e :: SomeException) -> do
                                 err lg $ LG.msg ("Error: INSERT into 'blocks_by_height' failed: " ++ show e)
                                 throw KeyValueDBInsertException)
                    (zip [0 ..] indexed)
            unless (L.null indexed) $ do
                let height = (fst $ head indexed) - 1
                if height == 0
                    then return ()
                    else do
                        blk <- xGetBlockHeight height
                        case blk of
                            Just b -> do
                                let nextHdrHash = blockHashToHex $ headerHash $ fst $ snd $ head indexed
                                    p3 = getSimpleQueryParam (nextHdrHash, T.pack $ rbHash b)
                                    p4 = getSimpleQueryParam (nextHdrHash, height)
                                res3 <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query q3 p3)
                                case res3 of
                                    Right _ -> return ()
                                    Left (e :: SomeException) ->
                                        liftIO $ do
                                            err lg $ LG.msg ("Error: UPDATE into 'blocks_by_hash' failed: " ++ show e)
                                            throw KeyValueDBInsertException
                                res4 <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query q4 p4)
                                case res4 of
                                    Right _ -> return ()
                                    Left (e :: SomeException) -> do
                                        err lg $ LG.msg ("Error: UPDATE into 'blocks_by_height' failed: " ++ show e)
                                        throw KeyValueDBInsertException
                            Nothing -> do
                                err lg $ LG.msg ("Error: SELECT from 'blocks_by_height' for height :" ++ show height)
                                throw KeyValueDBLookupException
                updateChainWork indexed conn
                markBestBlock (blockHashToHex $ headerHash $ fst $ snd $ last $ indexed) (fst $ last indexed) conn
                liftIO $ putMVar (bestBlockUpdated bp2pEnv) True
        False -> do
            err lg $ LG.msg $ val "Error: BlocksNotChainedException"
            throw BlocksNotChainedException

fetchMatchBlockOffset :: (HasLogger m, MonadIO m) => XCqlClientState -> Network -> Text -> m (Maybe (Text, Int32))
fetchMatchBlockOffset conn net hashes = do
    lg <- getLogger
    let qstr :: Q.QueryString Q.R (Identity Text) (Int32, Text)
        qstr = "SELECT block_height, block_hash from xoken.blocks_by_hash where block_hash = ?"
        p = getSimpleQueryParam $ Identity hashes
    iop <- liftIO $ query conn (Q.RqQuery $ Q.Query qstr p)
    if L.null iop
        then return Nothing
        else do
            let (maxHt, bhash) = iop !! 0
            return $ Just (bhash, maxHt)

updateChainWork :: (HasLogger m, MonadIO m) => [(Int32, BlockHeaderCount)] -> XCqlClientState -> m ()
updateChainWork indexed conn = do
    lg <- getLogger
    if L.length indexed == 0
        then do
            debug lg $ LG.msg $ val "updateChainWork: Input list empty. Nothing updated."
            return ()
        else do
            let qstr :: Q.QueryString Q.R (Identity Text) (Identity (Maybe Bool, Int32, Maybe Int64, T.Text))
                qstr = "SELECT value from xoken.misc_store where key = ?"
                qstr1 :: Q.QueryString Q.W (Text, (Maybe Bool, Int32, Maybe Int64, Text)) ()
                qstr1 = "insert INTO xoken.misc_store (key, value) values (?, ?)"
                par = getSimpleQueryParam $ Identity "chain-work"
                lenInd = L.length indexed
                lenOffset =
                    fromIntegral $
                    if lenInd <= 10
                        then (10 - lenInd)
                        else 0
                lagIndexed = take (lenInd - 10) indexed
                indexedCW = foldr (\x y -> y + (convertBitsToBlockWork $ blockBits $ fst $ snd $ x)) 0 lagIndexed
            res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr par)
            case res of
                Right iop -> do
                    let (_, height, _, chainWork) =
                            case L.length iop of
                                0 -> (Nothing, 0, Nothing, "4295032833") -- 4295032833 (0x100010001) is chainwork for genesis block
                                _ -> runIdentity $ iop !! 0
                        lag = [(height + 1) .. ((fst $ head indexed) - (1 + lenOffset))]
                    (par1, ub) <-
                        if L.null lag
                            then do
                                let updatedBlock = fst $ last lagIndexed
                                return $
                                    ( getSimpleQueryParam
                                          ("chain-work", (Nothing, updatedBlock, Nothing, T.pack $ show $ indexedCW))
                                    , updatedBlock)
                            else do
                                lagCW <- calculateChainWork lag conn
                                let updatedChainwork = T.pack $ show $ lagCW + indexedCW + (read . T.unpack $ chainWork)
                                    updatedBlock =
                                        if L.null lagIndexed
                                            then last lag
                                            else fst $ last lagIndexed
                                return
                                    ( getSimpleQueryParam
                                          ("chain-work", (Nothing, updatedBlock, Nothing, updatedChainwork))
                                    , updatedBlock)
                    res1 <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr1 par1)
                    case res1 of
                        Right _ -> do
                            debug lg $ LG.msg $ "updateChainWork: updated till block: " ++ show ub
                            return ()
                        Left (e :: SomeException) -> do
                            err lg $ LG.msg ("Error: INSERT 'chain-work' into 'misc_store' failed: " ++ show e)
                            throw KeyValueDBInsertException
                Left (e :: SomeException) -> do
                    err lg $ LG.msg ("Error: SELECT from 'misc_store' failed: " ++ show e)
                    throw KeyValueDBLookupException
