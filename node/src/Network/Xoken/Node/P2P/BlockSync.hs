{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}

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
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import qualified Database.Bolt as BT
import Database.XCQL.Protocol as Q
import qualified ListT as LT
import Network.Socket
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
import Network.Xoken.Node.Data as DA
import Network.Xoken.Node.Data.Allegory
import qualified Network.Xoken.Node.Data.Allegory as AL (Index (..))
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
import Streamly.Prelude (nil, (|:))
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Xoken
import Xoken.NodeConfig

produceGetDataMessages :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> m ([Message])
produceGetDataMessages peer = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    trace lg $ LG.msg $ "Block - produceGetDataMessages - called." ++ (show peer)
    !tm <- liftIO $ getCurrentTime
    pendingReqs <- liftIO $ TSH.toList (bpPendingRequests peer)
    let pend = L.length pendingReqs
    (blkhash, blkht) <- fetchBestSyncedBlock
    let fw = getFetchWindowSize (blocksFetchWindow $ nodeConfig bp2pEnv) (fromIntegral blkht)
    blkinfo <- fetchBlockCacheQueue (fw - (fromIntegral pend))
    gds <-
        mapM
            ( \bi -> do
                va <- liftIO $ TSH.lookup (bpPendingRequests peer) (biBlockHash bi)
                case va of
                    Just _ -> do
                        debug lg $
                            LG.msg $
                                " produceGetDataMessages: pending request FOUND: "
                                    ++ (show $ biBlockHash bi)
                                    ++ ", peer: "
                                    ++ (show peer)
                        return Nothing -- wasting the dequeued msg now, but next time a new peer is expected to dequeue
                    Nothing -> do
                        debug lg $
                            LG.msg $
                                " produceGetDataMessages: pending request NOT-FOUND: "
                                    ++ (show $ biBlockHash bi)
                                    ++ ", peer: "
                                    ++ (show peer)
                        liftIO $ TSH.insert (fetchBlockPeerMap bp2pEnv) (biBlockHash bi) peer
                        liftIO $ TSH.insert (bpPendingRequests peer) (biBlockHash bi) (tm)
                        return $ Just $ MGetData $ GetData $ [InvVector InvBlock $ getBlockHash $ biBlockHash bi]
            )
            blkinfo
    if L.null gds
        then return ()
        else debug lg $ LG.msg $ "GetData reqs: " ++ (show $ catMaybes gds)
    return $ catMaybes gds

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
                            -- liftIO $ TSH.delete (parallelBlockProcessingMap bp2pEnv) (show $ bpAddress pr)
                            liftIO $ Network.Socket.close s
                            throw e
            debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
        Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"

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
                        ( \(_, pr) ->
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
                                --  liftIO $
                                --      TSH.delete (parallelBlockProcessingMap bp2pEnv) (show $ bpAddress pr)
                                Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"
                        )
                        (connPeers)
            else return ()
        !ct <- liftIO $ getCurrentTime
        isemp <- liftIO $ atomically $ isEmptyTQueue (peerFetchQueue bp2pEnv)
        mapM_
            ( \(_, pr) -> do
                pendingReqs <- liftIO $ TSH.toList (bpPendingRequests pr)
                let val =
                        catMaybes $
                            map
                                ( \(blk, stm) ->
                                    if ( diffUTCTime ct stm
                                            > (fromIntegral $ unresponsivePeerConnTimeoutSecs $ nodeConfig bp2pEnv)
                                       )
                                        then Nothing -- req has timed-out
                                        else Just ()
                                )
                                pendingReqs
                if (L.null val == True) && (L.null pendingReqs == False) -- all pending requests have timed-out
                    then do
                        err lg $ LG.msg ("Error: Closing stale peer: " ++ show pr)
                        case (bpSocket pr) of
                            Just sock -> liftIO $ Network.Socket.close sock
                            Nothing -> return ()
                        liftIO $ atomically $ do modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress pr))
                    else
                        if (L.null pendingReqs && isemp) -- no pending reqs on peer and main queue is empty
                            then do
                                liftIO $ atomically $ writeTQueue (peerFetchQueue bp2pEnv) pr
                            else return ()
            )
            connPeers
        liftIO $ threadDelay (5 * 1000000)

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

getSleepIntervalMainnet :: Int32 -> Int32
getSleepIntervalMainnet n
    | n < 200000 = 100000
    | n >= 200000 && n < 300000 = 200000
    | n >= 300000 && n < 400000 = 300000
    | n >= 400000 && n < 500000 = 500000
    | otherwise = 1000000

getSleepIntervalTestnet :: Int32 -> Int32
getSleepIntervalTestnet n
    | n < 500000 = 100000
    | otherwise = 500000

getSleepInterval :: Network -> Int32 -> Int32
getSleepInterval net n
    | (getNetworkName net == "bsvtest") = getSleepIntervalTestnet n
    | otherwise = getSleepIntervalMainnet n

getFetchWindowSize :: [BlockFetchWindow] -> Int8 -> Int8
getFetchWindowSize fwlst ht = do
    let fw' =
            L.filter
                ( \bf -> do
                    if ((fromIntegral $ heightBegin bf) <= ht && ht <= (fromIntegral $ heightEnd bf))
                        then True
                        else False
                )
                fwlst
    if L.null fw'
        then 1 :: Int8 --default of 1
        else fromIntegral $ windowSize $ fw' !! 0

fetchBlockCacheQueue :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Int8 -> m ([BlockInfo])
fetchBlockCacheQueue gdSize = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    dbe <- getDB
    !tm <- liftIO $ getCurrentTime
    trace lg $ LG.msg $ val "fetchBlockCacheQueue syncronized block ..."
    let nc = nodeConfig bp2pEnv
        net = bitcoinNetwork $ nc
        conn = xCqlClientState dbe
    syt' <- liftIO $ TSH.toList (blockSyncStatusMap bp2pEnv)
    let syt = L.sortBy (\(_, (_, h)) (_, (_, h')) -> compare h h') syt'
        sysz = fromIntegral $ L.length syt
    fullySynced <- liftIO $ readTVarIO $ indexUnconfirmedTx bp2pEnv
    -- check any retries
    ffc <-
        do
            trace lg $ LG.msg $ val "Checking for retries"
            let queuedAndRecving =
                    L.filter
                        ( \x ->
                            case fst $ snd x of
                                RequestQueuedFW _ -> True
                                RecentTxReceiveTime _ -> True
                                otherwise -> False
                        )
                        syt
            -- check if any peers were prematurely lost and needs preemt re-fetching of block
            fxv <-
                mapM
                    ( \(blkhash, x) -> do
                        peer <- liftIO $ TSH.lookup (fetchBlockPeerMap bp2pEnv) (blkhash) --  peer that was fetching block
                        case peer of
                            Just pr -> do
                                if bpConnected pr
                                    then return Nothing -- block is hopefully still being received by some peer
                                    else return $ Just (blkhash, x) -- old peer was lost, assign new peer for this block
                            Nothing -> return $ Nothing
                    )
                    (queuedAndRecving)
            let preemptBlk = catMaybes fxv
            --
            if not (L.null preemptBlk)
                then return preemptBlk
                else do
                    if L.length syt == 0
                        then return []
                        else do
                            let leaderBlock = head syt
                                remainingBlocks = L.tail syt
                            let cached =
                                    L.filter
                                        ( \x ->
                                            case fst $ snd x of
                                                RequestCached -> True
                                                otherwise -> False
                                        )
                                        remainingBlocks
                            let finalCacheList =
                                    case (fst $ snd leaderBlock) of
                                        RequestQueuedFW t ->
                                            if (diffUTCTime tm t > (fromIntegral $ getDataResponseTimeout nc))
                                                then [leaderBlock] ++ cached
                                                else cached -- leader recv not started, dont get the rest yet, peers could get tied up!
                                        RecentTxReceiveTime t ->
                                            if (diffUTCTime tm t > (fromIntegral $ recentTxReceiveTimeout nc))
                                                then [leaderBlock] ++ cached
                                                else cached
                                        RequestCached -> [leaderBlock] ++ cached
                                        otherwise -> cached
                            return $ finalCacheList
    let fetchFromCache = map (\i -> mkBlkInf i) ffc
    if L.length syt > 0
        then do
            let shortListed = L.take (fromIntegral $ gdSize) $ catMaybes (fetchFromCache)
            mapM_
                ( \(bi) -> do
                    liftIO $
                        TSH.insert
                            (blockSyncStatusMap bp2pEnv)
                            (biBlockHash bi)
                            (RequestQueuedFW tm, fromIntegral $ biBlockHeight bi)
                )
                (shortListed)
            return shortListed
        else do
            (hash, ht) <- fetchBestSyncedBlock
            let !bks = map (\x -> ht + (fromIntegral x)) [1 .. (parallelBlockProcessing nc)]
            let qstr :: Q.QueryString Q.R (Identity [Int32]) ((Int32, T.Text))
                qstr = "SELECT block_height, block_hash from xoken.blocks_by_height where block_height in ?"
                p = getSimpleQueryParam $ Identity (bks)
            res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
            case res of
                Left (e :: SomeException) -> do
                    err lg $ LG.msg ("Error: fetchBlockCacheQueue: " ++ show e)
                    return ()
                Right (op) -> do
                    if L.length op == 0
                        then trace lg $ LG.msg $ val "Synced fully!"
                        else do
                            mapM_
                                ( \x ->
                                    case (hexToBlockHash $ snd x) of
                                        Just h -> do
                                            __ <-
                                                liftIO $
                                                    TSH.getOrCreate
                                                        (blockSyncStatusMap bp2pEnv)
                                                        h
                                                        (return (RequestCached, fromIntegral $ fst x))
                                            liftIO $
                                                catch
                                                    ( liftIO $
                                                        TSH.new 32 >>= \pie ->
                                                            liftIO $
                                                                TSH.insert
                                                                    (protocolInfo bp2pEnv)
                                                                    (fromJust $ hexToBlockHash $ snd x)
                                                                    pie
                                                    )
                                                    ( \(e :: SomeException) ->
                                                        err lg $
                                                            LG.msg $
                                                                "Error: Failed to insert into protocolInfo TSH (key "
                                                                    <> (show $ snd x)
                                                                    <> "): "
                                                                    <> (show e)
                                                    )
                                            return ()
                                        Nothing -> return ()
                                )
                                (op)
            updatedSyncList <- liftIO $ TSH.toList (blockSyncStatusMap bp2pEnv)
            let cached =
                    L.filter
                        ( \x ->
                            case fst $ snd x of
                                RequestCached -> True
                                otherwise -> False
                        )
                        updatedSyncList
            let sortCached = L.sortBy (\(_, (_, h)) (_, (_, h')) -> compare h h') cached
                sortCachedBinf = map (\i -> mkBlkInf i) sortCached
            let shortListed = L.take (fromIntegral $ gdSize) $ catMaybes (sortCachedBinf)
            mapM_
                ( \(bi) -> do
                    liftIO $
                        TSH.insert
                            (blockSyncStatusMap bp2pEnv)
                            (biBlockHash bi)
                            (RequestQueuedFW tm, fromIntegral $ biBlockHeight bi)
                )
                (shortListed)
            return shortListed
  where
    mkBlkInf h = Just $ BlockInfo (fst h) (snd $ snd h)

commitBlockCacheQueue :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
commitBlockCacheQueue =
    forever $ do
        liftIO $ threadDelay (100000) -- 0.1 sec
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        dbe <- getDB
        !tm <- liftIO $ getCurrentTime
        debug lg $ LG.msg $ val "commitBlockCacheQueue loop..."
        let nc = nodeConfig bp2pEnv
            net = bitcoinNetwork $ nc
            conn = xCqlClientState dbe
        syt' <- liftIO $ TSH.toList (blockSyncStatusMap bp2pEnv)
        let syt = L.sortBy (\(_, (_, h)) (_, (_, h')) -> compare h h') syt'
        --
        mapM_
            ( \(bsh, (_, ht)) -> do
                valx <- liftIO $ TSH.lookup (blockTxProcessingLeftMap bp2pEnv) (bsh)
                debug lg $ LG.msg $ val "blockTxProcessingLeftMap : "
                case valx of
                    Just xv -> do
                        siza <- liftIO $ TSH.toList (fst xv)
                        debug lg $
                            LG.msg $
                                "blockTxProcessingLeftMap lhs: "
                                    ++ (show siza)
                                    ++ " sum: "
                                    ++ (show $ (sum $ snd $ unzip siza))
                                    ++ " rhs: "
                                    ++ (show $ snd xv)
                        if ((sum $ snd $ unzip siza) == snd xv)
                            then do
                                debug lg $ LG.msg $ "inserting BlockProcessingComplete : " ++ (show bsh)
                                liftIO $ TSH.insert (blockSyncStatusMap bp2pEnv) (bsh) (BlockProcessingComplete, ht)
                                peer <- liftIO $ TSH.lookup (fetchBlockPeerMap bp2pEnv) (bsh)
                                case peer of
                                    Just pr -> do
                                        debug lg $
                                            LG.msg $ "ABCD - commitBlockCacheQueue, clearing maps" ++ (show (bsh, pr))
                                        --  liftIO $ TSH.delete (bpPendingRequests pr) (bsh)
                                        --  liftIO $ atomically $ unGetTQueue (peerFetchQueue bp2pEnv) pr -- queueing at the front, for recency!
                                        -- liftIO $ atomically $ writeTQueue (peerFetchQueue bp2pEnv) pr
                                        liftIO $ TSH.delete (fetchBlockPeerMap bp2pEnv) (bsh)
                                    Nothing -> return ()
                            else return ()
                    Nothing -> return ()
            )
            (syt)
        trace lg $ LG.msg $ ("blockSyncStatusMap (list): " ++ (show syt))
        syncList <- liftIO $ TSH.toList (blockSyncStatusMap bp2pEnv)
        let sortedList = L.sortOn (snd . snd) syncList
        debug lg $ LG.msg $ ("sorted blockSyncStatusMap (list): " ++ (show sortedList))
        let compl = L.takeWhile (\x -> (fst $ snd x) == BlockProcessingComplete) sortedList
        if not $ L.null compl
            then do
                let lelm = last $ compl
                debug lg $ LG.msg $ "Marking best synced block: " <> show lelm
                markBestSyncedBlock (fst $ lelm) (fromIntegral $ snd $ snd $ lelm) conn
                mapM
                    ( \(bsh, (_, ht)) -> do
                        trace lg $ LG.msg $ "deleting from blockSyncStatusMap: " <> show bsh
                        liftIO $ TSH.delete (blockSyncStatusMap bp2pEnv) bsh
                        liftIO $ TSH.delete (blockTxProcessingLeftMap bp2pEnv) bsh
                        --
                        LA.async $
                            liftIO $
                                catch
                                    ( do
                                        v <- liftIO $ TSH.lookup (protocolInfo bp2pEnv) bsh
                                        case v of
                                            Just v' -> do
                                                pi <- liftIO $ TSH.toList v'
                                                debug lg $
                                                    LG.msg $
                                                        "Number of protocols for block: "
                                                            <> show (Prelude.length pi)
                                                            <> " height: "
                                                            <> show ht
                                                pres <-
                                                    liftIO $
                                                        try $ do
                                                            runInBatch
                                                                ( \(protocol, (props, blockInf)) -> do
                                                                    TSH.delete v' protocol
                                                                    withResource'
                                                                        (pool $ graphDB dbe)
                                                                        ( `BT.run`
                                                                            insertProtocolWithBlockInfo
                                                                                protocol
                                                                                props
                                                                                blockInf
                                                                        )
                                                                )
                                                                pi
                                                                (maxInsertsProtocol $ nodeConfig bp2pEnv)
                                                            TSH.delete (protocolInfo bp2pEnv) bsh
                                                case pres of
                                                    Right rt -> return ()
                                                    Left (e :: SomeException) -> do
                                                        err lg $
                                                            LG.msg $
                                                                "Error: Failed to insert protocol with blockInfo:"
                                                                    <> (show (bsh, ht))
                                                                    <> ": "
                                                                    <> (show e)
                                                        throw MerkleSubTreeDBInsertException
                                            Nothing -> do
                                                debug lg $
                                                    LG.msg $ "Debug: No Information available for block hash " ++ show bsh
                                                return ()
                                        return ()
                                    )
                                    ( \(e :: SomeException) ->
                                        err lg $
                                            LG.msg $
                                                "Error: Failed to insert into graph DB block "
                                                    <> (show (bsh, ht))
                                                    <> ": "
                                                    <> (show e)
                                    )
                                    --
                    )
                    compl
                return ()
            else return ()
        return ()

commitScriptHashOutputs ::
    (HasXokenNodeEnv env m, MonadIO m) =>
    XCqlClientState ->
    Text ->
    (Text, Int32) ->
    Maybe (Text, Int32, Int32) ->
    m ()
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

insertTxIdOutputs :: (HasXokenNodeEnv env m, MonadIO m) => (Text, Int32) -> Text -> Text -> Int64 -> m ()
insertTxIdOutputs (txid, outputIndex) address script value = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
    let queryStr =
            fromString $
                "INSERT INTO xoken.txid_outputs (txid , output_index, address, script,  value) VALUES (?,?,?,?,?) " ::
                Q.QueryString
                    Q.W
                    ( Text
                    , Int32
                    , Text
                    , Text
                    , Int64
                    )
                    ()
    let queryPar = getSimpleQueryParam (txid, outputIndex, address, script, value)
    res <-
        liftIO $
            queryPrepared conn (Q.RqPrepare (Q.Prepare queryStr)) >>= \queryId ->
                liftIO $ try $ write conn (Q.RqExecute (Q.Execute queryId queryPar))
    case res of
        Left (e :: SomeException) -> do
            err lg $
                LG.msg $
                    C.pack $
                        "[ERROR] While inserting outputs for "
                            <> (T.unpack txid)
                            <> ":"
                            <> (show outputIndex)
                            <> ": "
                            <> (show e)
            throw e
        _ -> debug lg $ LG.msg $ "Insert outputs for tx:" ++ show (txid, outputIndex)

updateSpendInfoOutputs :: (HasXokenNodeEnv env m, MonadIO m) => Text -> Int32 -> (Text, Int32) -> m ()
updateSpendInfoOutputs txid outputIndex spendInfo = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    debug lg $ LG.msg $ "updateSpendInfoOutputs :" ++ show spendInfo ++ " " ++ show txid ++ " " ++ show outputIndex
    let conn = xCqlClientState dbe
    let queryStr =
            fromString $ "UPDATE xoken.txid_outputs SET spend_info=? WHERE txid=? AND output_index=? " ::
                Q.QueryString
                    Q.W
                    ( ( Text
                      , Int32
                      )
                    , Text
                    , Int32
                    )
                    ()
    let queryPar = getSimpleQueryParam (spendInfo, txid, outputIndex)
    res <-
        liftIO $
            queryPrepared conn (Q.RqPrepare (Q.Prepare queryStr)) >>= \queryId ->
                liftIO $ try $ write conn (Q.RqExecute (Q.Execute queryId queryPar))
    case res of
        Left (e :: SomeException) -> do
            err lg $
                LG.msg $
                    C.pack $
                        "[ERROR] While updating outputs for "
                            <> (T.unpack txid)
                            <> ":"
                            <> (show outputIndex)
                            <> ": "
                            <> (show e)
            throw e
        _ -> debug lg $ LG.msg $ "Updated spend-info for output:" ++ show spendInfo

commitTxPage ::
    (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m) =>
    [TxHash] ->
    BlockHash ->
    Int32 ->
    m ()
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
    debug lg $ LG.msg $ "(Wrapper) Processing lock begin :" ++ show txhs
    -- lock begin
    -- lockv <- liftIO $ TSH.getOrCreate (txSynchronizer bp2pEnv) (txhs, ParentTxProcessing) (liftIO $ newMVar ())
    -- liftIO $ takeMVar lockv
    --
    res <- LE.try $ processConfTransaction' bis tx bhash blkht txind -- get the Inv message
    case res of
        Right mm ->
            -- liftIO $ putMVar lockv () -- lock end/1
            do
                debug lg $ LG.msg $ "(Wrapper) Processing lock ends :" ++ show txhs
                --
                vall <- liftIO $ TSH.lookup (txSynchronizer bp2pEnv) (txhs, ChildTxWaiting)
                case vall of
                    Just ev -> do
                        liftIO $ tryPutMVar ev ()
                        return ()
                    Nothing -> return ()
        --
        Left (e :: SomeException) ->
            -- liftIO $ putMVar lockv () -- lock end/2
            do
                liftIO $ err lg $ LG.msg ("Error: while processConfTransaction: " ++ show e)
                throw e

--

processConfTransaction' ::
    (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BlockIngestState -> Tx -> BlockHash -> Int -> Int -> m ()
processConfTransaction' bis tx bhash blkht txind = do
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
                ( map
                    ( \y ->
                        case scriptToAddressBS $ scriptOutput y of
                            Left e -> "" -- avoid null values in Cassandra wherever possible
                            Right os ->
                                case addrToString net os of
                                    Nothing -> ""
                                    Just addr -> addr
                    )
                    (txOut tx)
                )
                (txOut tx)
                [0 :: Int32 ..]
    --
    -- update outputs and scripthash tables
    mapM_
        ( \(a, o, i) -> do
            let sh = txHashToHex $ TxHash $ sha256 (scriptOutput o)
            let script =
                    if a == ""
                        then (scriptOutput o)
                        else ""
            let bi = (blockHashToHex bhash, fromIntegral blkht, fromIntegral txind)
            let output = (txHashToHex txhs, i)
            let bsh = B16.encode $ scriptOutput o
            let (op, rem) = B.splitAt 2 bsh
            let (op_false, op_return, remD) =
                    if op == "6a"
                        then ("00", op, rem)
                        else (\(a, b) -> (op, a, b)) $ B.splitAt 2 rem
            insertTxIdOutputs output a (T.pack $ C.unpack script) (fromIntegral $ outValue o)
        )
        outAddrs
    -- lookup into tx outputs value cache if cache-miss, fetch from DB
    !inputs <-
        mapM
            ( \(b, j) -> do
                tuple <- return Nothing
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
                                        then
                                            return
                                                ( ""
                                                , ""
                                                , fromIntegral $ computeSubsidy net $ (fromIntegral blkht :: Word32)
                                                )
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
                                                            (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                                            case dbRes of
                                                Right v -> return $ v
                                                Left (e :: SomeException) -> do
                                                    err lg $
                                                        LG.msg $
                                                            "Error: [pCT calling gSVFO] WHILE Processing TxID "
                                                                ++ show (txHashToHex txhs)
                                                                ++ ", getting value for dependent input (TxID,Index): ("
                                                                ++ show (txHashToHex $ outPointHash (prevOutput b))
                                                                ++ ", "
                                                                ++ show (outPointIndex $ prevOutput b)
                                                                ++ ")"
                                                    throw e
                        Nothing -> do
                            if (outPointHash nullOutPoint) == (outPointHash $ prevOutput b)
                                then
                                    return
                                        ("", "", fromIntegral $ computeSubsidy net $ (fromIntegral blkht :: Word32))
                                else do
                                    debug lg $ LG.msg $ C.pack $ "txOutputValuesCache: cache-miss (2)"
                                    debug lg $
                                        LG.msg $
                                            " outputindex: "
                                                ++ show
                                                    ( txHashToHex $ outPointHash $ prevOutput b
                                                    , fromIntegral $ outPointIndex $ prevOutput b
                                                    )
                                    dbRes <-
                                        liftIO $
                                            LE.try $
                                                getSatsValueFromOutpoint
                                                    conn
                                                    (txSynchronizer bp2pEnv)
                                                    lg
                                                    net
                                                    (prevOutput b)
                                                    (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                                    case dbRes of
                                        Right v -> do
                                            debug lg $
                                                LG.msg $ " value returned from getStatsValueFromOutpoint: " ++ show v
                                            return $ v
                                        Left (e :: SomeException) -> do
                                            err lg $
                                                LG.msg $
                                                    "Error: [pCT calling gSVFO] WHILE Processing TxID "
                                                        ++ show (txHashToHex txhs)
                                                        ++ ", getting value for dependent input (TxID,Index): ("
                                                        ++ show (txHashToHex $ outPointHash (prevOutput b))
                                                        ++ ", "
                                                        ++ show (outPointIndex $ prevOutput b)
                                                        ++ ")"
                                            throw e
                return
                    ((txHashToHex $ outPointHash $ prevOutput b, fromIntegral $ outPointIndex $ prevOutput b), j, val)
            )
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
    mapM_
        ( \(a, o, i) -> do
            let sh = txHashToHex $ TxHash $ sha256 (scriptOutput o)
            let script =
                    if a == ""
                        then (scriptOutput o)
                        else ""
            let bi = (blockHashToHex bhash, fromIntegral blkht, fromIntegral txind)
            let output = (txHashToHex txhs, i)
            let bsh = B16.encode $ scriptOutput o
            let (op, rem) = B.splitAt 2 bsh
            let (op_false, op_return, remD) =
                    if op == "6a"
                        then ("00", op, rem)
                        else (\(a, b) -> (op, a, b)) $ B.splitAt 2 rem
            -- outputsExist <- checkOutputDataExists output
            -- unless outputsExist $ do

            commitScriptHashOutputs conn sh output (Just bi)
            when (op_false == "00" && op_return == "6a") $ do
                props <-
                    case runGet (getPropsG 3) (fst $ B16.decode remD) of
                        Right p -> return p
                        Left str -> do
                            liftIO $
                                err lg $
                                    LG.msg
                                        ( "Error: Getting protocol name "
                                            ++ show str
                                            ++ " for height: "
                                            ++ show blkht
                                            ++ " with bsh: "
                                            ++ show bsh
                                            ++ " with remD: "
                                            ++ show remD
                                        )
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
                                Just (p, bi) -> return (Just (props, upf bi), ())
                                Nothing -> return (Just (props, curBi), ())
                        prot = tail $ L.inits protocol
                    mapM_
                        ( \p ->
                            commitScriptOutputProtocol
                                conn
                                (T.intercalate "." p)
                                output
                                (Just bi)
                                fees
                                (fromIntegral count)
                        )
                        prot
                    v <- liftIO $ TSH.lookup (protocolInfo bp2pEnv) bhash
                    case v of
                        Just v' -> liftIO $ TSH.mutate v' (T.intercalate "_" protocol) fn
                        Nothing -> debug lg $ LG.msg $ "No ProtocolInfo Available for: " ++ show bhash
        )
        outAddrs
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
                ( \(a, o, i) ->
                    ( fromIntegral $ i
                    , (a, (txHashToHex $ TxHash $ sha256 (scriptOutput o)), fromIntegral $ outValue o)
                    )
                )
                outAddrs
    trace lg $ LG.msg $ "processing Tx " ++ show txhs ++ ": compiled output value(s): " ++ (show ovs)
    trace lg $ LG.msg $ "processing Tx " ++ show txhs ++ ": added outputvals to cache"
    -- <<
    -- >>
    debug lg $ LG.msg $ "Processing confirmed transaction <committed outputs> :" ++ show txhs
    mapM_
        ( \((o, i), (a, sh)) -> do
            let bi = (blockHashToHex bhash, fromIntegral blkht, fromIntegral txind)
            let blockHeight = fromIntegral blkht
            let prevOutpoint = (txHashToHex $ outPointHash $ prevOutput o, fromIntegral $ outPointIndex $ prevOutput o)
            let spendInfo = (\ov -> ((txHashToHex txhs, fromIntegral $ fst $ ov), i, snd $ ov)) <$> ovs
            -- this will fail to update for coinbase txns, its cool
            updateSpendInfoOutputs (fst prevOutpoint) (snd prevOutpoint) (txHashToHex txhs, i)
        )
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
             in d
                    + ( if m == 0
                            then 0
                            else 1
                      )
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
                , txHashToHex txhs
                )
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
            ( \(seg, i) -> do
                let par =
                        getSimpleQueryParam
                            ( (blockHashToHex bhash, fromIntegral blkht, fromIntegral txind)
                            , Blob seg
                            , []
                            , fees
                            , (txHashToHex txhs) <> (T.pack $ show i)
                            )
                queryI <- liftIO $ queryPrepared conn (Q.RqPrepare $ Q.Prepare qstr)
                res <- liftIO $ try $ write conn (Q.RqExecute $ Q.Execute queryI par)
                case res of
                    Right _ -> return ()
                    Left (e :: SomeException) -> do
                        liftIO $ err lg $ LG.msg ("Error: INSERTing into 'xoken.transactions': " ++ show e)
                        throw KeyValueDBInsertException
            )
            (zip segmentsData [1 ..])
    --
    trace lg $ LG.msg $ "processing Tx " ++ show txhs ++ ": persisted in DB"
    --
    trace lg $ LG.msg $ "processing Tx " ++ show txhs ++ ": handled Allegory Tx"
    -- signal 'done' event for tx's that were processed out of sequence
    --
    debug lg $ LG.msg $ "Processing confirmed transaction <end> :" ++ show txhs

--
--
getSatsValueFromOutpoint ::
    XCqlClientState ->
    TSH.TSHashTable (TxHash, DependentTxStatus) (MVar ()) -> -- EV.Event
    Logger ->
    Network ->
    OutPoint ->
    Int ->
    IO ((Text, Text, Int64))
getSatsValueFromOutpoint conn txSync lg net outPoint maxWait = do
    return ((T.pack "16Rcy7RYM3xkPEJr4tvUtL485Fuobi8S7o"), (T.pack "1234123412341234"), 2147483647 )

--
--
getSatsValueFromOutpoint'' ::
    XCqlClientState ->
    TSH.TSHashTable (TxHash, DependentTxStatus) (MVar ()) -> -- EV.Event
    Logger ->
    Network ->
    OutPoint ->
    Int ->
    IO ((Text, Text, Int64))
getSatsValueFromOutpoint'' conn txSync lg net outPoint maxWait = do
    let qstr :: Q.QueryString Q.R (Text, Int32) (Text, Text, Int64)
        qstr = "SELECT address, script, value FROM xoken.txid_outputs WHERE txid=? AND output_index=?"
        par = getSimpleQueryParam (txHashToHex $ outPointHash outPoint, fromIntegral $ outPointIndex outPoint)
    -- parent lock begin
    -- lockp <- liftIO $ TSH.getOrCreate txSync ((outPointHash outPoint), ParentTxProcessing) (liftIO $ newMVar ())
    -- liftIO $ takeMVar lockp
    --
    res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right results ->
            -- liftIO $ putMVar lockp () -- parent lock ends/1
            do
                if (L.length results == 0)
                    then do
                        debug lg $
                            LG.msg $
                                "getSatsValueFromOutpoint txid: "
                                    ++ (show outPoint)
                                    ++ ", results: "
                                    ++ (show $ L.length results)
                        debug lg $
                            LG.msg $
                                "Tx not found: " ++ (show $ txHashToHex $ outPointHash outPoint) ++ " _waiting_ for event"
                        --
                        lockc <-
                            liftIO $
                                TSH.getOrCreate txSync ((outPointHash outPoint), ChildTxWaiting) (liftIO $ newEmptyMVar)
                        --
                        tofl <- liftIO $ race (readMVar lockc) (do threadDelay (1000000 * 10)) --(fromIntegral maxWait)))
                        case tofl of
                            Left _ -> do
                                debug lg $
                                    LG.msg $ "event received _available_: " ++ (show $ txHashToHex $ outPointHash outPoint)
                                getSatsValueFromOutpoint conn txSync lg net outPoint maxWait
                            Right _ -> do
                                debug lg $
                                    LG.msg $ "event received _timeout_: " ++ (show $ txHashToHex $ outPointHash outPoint)
                                getSatsValueFromOutpoint conn txSync lg net outPoint maxWait
                    else do
                        let (addr, scriptHash, val) = head $ results
                        return $ (addr, scriptHash, val)
        Left (e :: SomeException) ->
            --liftIO $ putMVar lockp () -- parent lock ends/2
            do
                err lg $ LG.msg $ "Error: getSatsValueFromOutpoint: " ++ show e
                throw e

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
                                                            "[ERROR] Failed to check if nUTXO nodes exist for outputs of transaction "
                                                                <> txid
                                                throw e
                                            Right res -> return res
                                    if nodesAlreadyExist
                                        then do
                                            debug lg $
                                                LG.msg $
                                                    C.pack $
                                                        "handleIfAllegoryTx: nUTXO nodes already exist for outputs of transaction "
                                                            <> txid
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
                        "[ERROR] While handling Allegory metadata for txid "
                            <> txid
                            <> " : failed to update graph ("
                            <> show e
                            <> ")"
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
        AL.index
            <$> case action al of
                OwnerAction _ oo _ -> [owner oo]
                ProducerAction _ po poo es ->
                    (producer po) :
                    (maybe mempty (\o -> [owner o]) poo)
                        ++ ( ( \e ->
                                case e of
                                    OwnerExtension oeo _ -> owner oeo
                                    ProducerExtension peo _ -> producer peo
                             )
                                <$> es
                           )

commitScriptOutputProtocol ::
    (HasXokenNodeEnv env m, MonadIO m) =>
    XCqlClientState ->
    Text ->
    (Text, Int32) ->
    Maybe (Text, Int32, Int32) ->
    Int64 ->
    Int32 ->
    m ()
commitScriptOutputProtocol conn protocol' (txid, output_index) blockInfo fees size = do
    lg <- getLogger
    tm <- liftIO getCurrentTime
    blocksSynced <- checkBlocksFullySynced conn
    nominalTxIndex <- generateNominalTxIndex blockInfo output_index
    let protocol =
            if (T.length protocol') == 0
                then "00"
                else protocol'
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
                then
                    if d2 >= d1
                        then ymdiff
                        else ymdiff - 1
                else
                    if d2 <= d1
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
        queryStr :: Q.QueryString Q.R (T.Text, Int32) (Identity Int32)
        queryStr = "SELECT output_index FROM xoken.txid_outputs WHERE txid=? AND output_index=? "
        queryPar = getSimpleQueryParam (txid, outputIndex)
    res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query queryStr queryPar)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ C.pack $ "[ERROR] Querying database while checking if output exists: " <> (show e)
            throw KeyValueDBLookupException
        Right res -> return $ not . L.null $ res

-- updateBlockInfo :: (HasXokenNodeEnv env m, MonadIO m) => (Text, Int32) -> Bool -> (Text, Int32, Int32) -> m ()
-- updateBlockInfo (txid, outputIndex) isRecv blockInfo = do
--     dbe <- getDB
--     lg <- getLogger
--     bp2pEnv <- getBitcoinP2P
--     let conn = xCqlClientState dbe
--         queryStr :: Q.QueryString Q.W ((Text, Int32, Int32), Text, Int32, Bool) ()
--         queryStr = "UPDATE xoken.txid_outputs SET block_info=? WHERE txid=? AND output_index=? "
--         queryPar = getSimpleQueryParam (blockInfo, txid, outputIndex, isRecv)
--     queryId <- liftIO $ queryPrepared conn (Q.RqPrepare (Q.Prepare queryStr))
--     res <- liftIO $ try $ write conn (Q.RqExecute (Q.Execute queryId queryPar))
--     case res of
--         Left (e :: SomeException) -> do
--             err lg $
--                 LG.msg $
--                 C.pack $
--                 "[ERROR] While updating spendInfo for confirmed Tx output " <>
--                 (T.unpack txid) <> ":" <> (show outputIndex) <> ": " <> (show e)
--             throw e
--         _ -> debug lg $ LG.msg $ "Updated BlockInfo: " ++ show blockInfo ++ " for tx:" ++ show (txid, outputIndex)
