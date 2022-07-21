{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.PeerManager (
    createSocket,
    setupSeedPeerConnection,
    runTMTDaemon,
    runBlockSync,
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.Async.Lifted as LA (async, cancel, concurrently_, race, wait, waitAnyCatch, withAsync)
import qualified Control.Concurrent.MSem as MS
import qualified Control.Concurrent.MSemN as MSN
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TBQueue
import Control.Concurrent.STM.TQueue
import Control.Concurrent.STM.TSem
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.Logger
import Control.Monad.Loops
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import Control.Monad.Trans.Control
import qualified Data.Aeson as A (decode, eitherDecode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Char
import Data.Default
import Data.Either
import Data.Function ((&))
import Data.Functor.Identity
import qualified Data.HashTable as CHT
import Data.IORef
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Pool
import Data.Serialize
import Data.String.Conversions
import qualified Data.Text as T
import qualified Data.Text.Encoding as DTE
import Data.Time.Clock.POSIX
import Data.Word
import qualified Database.Bolt as BT
import Database.XCQL.Protocol as Q
import GHC.Natural
import Network.Socket
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.ChainSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.UnconfTxSync
import Network.Xoken.Node.Service.Callbacks as CB
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import StmContainers.Map as SM
import StmContainers.Set as SS
import Streamly as S
import Streamly.Prelude (drain, each, nil, (|:))
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Xoken.NodeConfig

createSocket :: AddrInfo -> IO (Maybe Socket)
createSocket = createSocketWithOptions []

createSocketWithOptions :: [SocketOption] -> AddrInfo -> IO (Maybe Socket)
createSocketWithOptions options addr = do
    sock <- socket AF_INET Stream (addrProtocol addr)
    mapM_ (\option -> when (isSupportedSocketOption option) (setSocketOption sock option 1)) options
    res <- try $ connect sock (addrAddress addr)
    case res of
        Right () -> return $ Just sock
        Left (e :: IOException) -> do
            liftIO $ Network.Socket.close sock
            throw $ SocketConnectException (addrAddress addr)

createSocketFromSockAddr :: SockAddr -> IO (Maybe Socket)
createSocketFromSockAddr saddr = do
    ss <-
        case saddr of
            SockAddrInet pn ha -> socket AF_INET Stream defaultProtocol
            SockAddrInet6 pn6 _ ha6 _ -> socket AF_INET6 Stream defaultProtocol
    rs <- try $ connect ss saddr
    case rs of
        Right () -> return $ Just ss
        Left (e :: IOException) -> do
            liftIO $ Network.Socket.close ss
            throw $ SocketConnectException (saddr)

-- runBlockSync :: (HasXokenNodeEnv env m, MonadIO m) => m ()
-- runBlockSync = forever $ do
--   liftIO $ threadDelay (99999990000)
--   return ()

runBlockSync :: (HasXokenNodeEnv env m, MonadIO m) => m ()
runBlockSync =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        let net = bitcoinNetwork $ nodeConfig bp2pEnv
        pr <- liftIO $ atomically $ readTQueue $ peerFetchQueue bp2pEnv -- get enlisted peer
        allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        res <-
            LE.try
                ( do
                    let isPeerConnected =
                            L.filter
                                (\x -> (bpConnected (snd x)) && ((bpAddress pr) == (bpAddress $ snd x)))
                                (M.toList allPeers)
                    msg <-
                        do
                            if L.length isPeerConnected == 0
                                then return [] -- stale peer, removing implicitly
                                else do
                                    -- debug lg $ LG.msg $ "ABCD - aaa" ++ (show pr)
                                    res <- LE.try $ produceGetDataMessages pr
                                    case res of
                                        Right mm -> do
                                            if L.null mm
                                                then do
                                                    -- debug lg $ LG.msg $ "ABCD - ccc" ++ (show pr)
                                                    liftIO $ atomically $ unGetTQueue (peerFetchQueue bp2pEnv) pr
                                                    liftIO $ threadDelay (100000) -- 0.1 sec
                                                    return []
                                                else do
                                                    -- debug lg $ msg $ (val "ABCD - Got enlisted peer: ") +++ (show pr)
                                                    return mm
                                        Left (e :: SomeException) -> do
                                            liftIO $ atomically $ unGetTQueue (peerFetchQueue bp2pEnv) pr
                                            liftIO $
                                                err lg $ msg ("Error: while runBlockSync:produceGetData: " ++ show e)
                                            return []
                    mapM_ (\m -> sendRequestMessages pr m) msg
                )
        case res of
            Right (a) -> return ()
            Left (e :: SomeException) -> do
                err lg $ msg $ (val "[ERROR] FATAL error in runBlockSync ") +++ (show e)
                throw e

setupSeedPeerConnection :: (HasXokenNodeEnv env m, MonadIO m) => m ()
setupSeedPeerConnection =
    forever $ do
        bp2pEnv <- getBitcoinP2P
        lg <- getLogger
        let net = bitcoinNetwork $ nodeConfig bp2pEnv
            seeds = (staticPeerList $ nodeConfig bp2pEnv) ++ getSeeds net
            hints = defaultHints{addrSocketType = Stream}
            port = getDefaultPort net
        debug lg $ msg $ show seeds
        --let sd = map (\x -> Just (x :: HostName)) seeds
        !addrs <- liftIO $ mapConcurrently (\x -> head <$> getAddrInfo (Just hints) (Just x) (Just (show port))) seeds
        mapM_
            ( \y -> do
                debug lg $ msg ("Peer.. " ++ show (addrAddress y))
                LA.async $
                    ( do
                        blockedpr <- liftIO $ readTVarIO (blacklistedPeers bp2pEnv)
                        allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
                        -- this can be optimized
                        let connPeers =
                                L.foldl'
                                    ( \c x ->
                                        if bpConnected (snd x)
                                            then c + 1
                                            else c
                                    )
                                    0
                                    (M.toList allpr)
                        if connPeers > (maxBitcoinPeerCount $ nodeConfig bp2pEnv)
                            then liftIO $ threadDelay (10 * 1000000)
                            else do
                                let toConn =
                                        case M.lookup (addrAddress y) allpr of
                                            Just pr ->
                                                if bpConnected pr
                                                    then False
                                                    else True
                                            Nothing -> True
                                    isBlacklisted = M.member (addrAddress y) blockedpr
                                if toConn == False
                                    then do
                                        debug lg $
                                            msg ("Seed peer already connected, ignoring.. " ++ show (addrAddress y))
                                    else
                                        if isBlacklisted
                                            then do
                                                debug lg $
                                                    msg ("Seed peer blacklisted, ignoring.. " ++ show (addrAddress y))
                                            else do
                                                rl <- liftIO $ newMVar True
                                                wl <- liftIO $ newMVar ()
                                                ss <- liftIO $ newTVarIO Nothing
                                                imc <- liftIO $ newTVarIO 0
                                                rc <- liftIO $ newTVarIO Nothing
                                                st <- liftIO $ newTVarIO Nothing
                                                fw <- liftIO $ newTVarIO 0
                                                res <- LE.try $ liftIO $ createSocket y
                                                cfq <- liftIO $ newEmptyMVar
                                                nfq <- liftIO $ newEmptyMVar
                                                pdr <- liftIO $ TSH.new 1
                                                case res of
                                                    Right (sock) -> do
                                                        case sock of
                                                            Just sx -> do
                                                                fl <- doVersionHandshake net sx $ addrAddress y
                                                                let bp =
                                                                        BitcoinPeer
                                                                            (addrAddress y)
                                                                            sock
                                                                            wl
                                                                            fl
                                                                            Nothing
                                                                            99999
                                                                            pdr
                                                                liftIO $
                                                                    atomically $
                                                                        modifyTVar'
                                                                            (bitcoinPeers bp2pEnv)
                                                                            (M.insert (addrAddress y) bp)
                                                                liftIO $
                                                                    atomically $
                                                                        writeTQueue (peerFetchQueue bp2pEnv) bp
                                                                handleIncomingMessages bp -- must be the last step, as blocking
                                                            Nothing -> return ()
                                                    Left (SocketConnectException addr) ->
                                                        warn lg $ msg ("SocketConnectException: " ++ show addr)
                    )
            )
            (addrs)
        liftIO $ threadDelay (30 * 1000000)

setupPeerConnection :: (HasXokenNodeEnv env m, MonadIO m) => SockAddr -> m (Maybe BitcoinPeer)
setupPeerConnection saddr = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    blockedpr <- liftIO $ readTVarIO (blacklistedPeers bp2pEnv)
    allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    let connPeers =
            L.foldl'
                ( \c x ->
                    if bpConnected (snd x)
                        then c + 1
                        else c
                )
                0
                (M.toList allpr)
    if connPeers > (maxBitcoinPeerCount $ nodeConfig bp2pEnv)
        then return Nothing
        else do
            let toConn =
                    case M.lookup saddr allpr of
                        Just pr ->
                            if bpConnected pr
                                then False
                                else True
                        Nothing -> True
                isBlacklisted = M.member saddr blockedpr
            if toConn == False
                then do
                    debug lg $ msg ("Peer already connected, ignoring.. " ++ show saddr)
                    return Nothing
                else
                    if isBlacklisted
                        then do
                            debug lg $ msg ("Peer blacklisted, ignoring.. " ++ show saddr)
                            return Nothing
                        else do
                            res <- LE.try $ liftIO $ createSocketFromSockAddr saddr
                            case res of
                                Right (sock) -> do
                                    rl <- liftIO $ newMVar True
                                    wl <- liftIO $ newMVar ()
                                    ss <- liftIO $ newTVarIO Nothing
                                    imc <- liftIO $ newTVarIO 0
                                    rc <- liftIO $ newTVarIO Nothing
                                    st <- liftIO $ newTVarIO Nothing
                                    fw <- liftIO $ newTVarIO 0
                                    cfq <- liftIO $ newEmptyMVar
                                    nfq <- liftIO $ newEmptyMVar
                                    pdr <- liftIO $ TSH.new 1
                                    case sock of
                                        Just sx -> do
                                            debug lg $ LG.msg ("Discovered Net-Address: " ++ (show $ saddr))
                                            fl <- doVersionHandshake net sx $ saddr
                                            let bp = BitcoinPeer (saddr) sock wl fl Nothing 99999 pdr
                                            liftIO $
                                                atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.insert (saddr) bp)
                                            return $ Just bp
                                        Nothing -> return (Nothing)
                                Left (SocketConnectException addr) -> do
                                    warn lg $ msg ("SocketConnectException: " ++ show addr)
                                    return Nothing

hashPair :: Hash256 -> Hash256 -> Hash256
hashPair a b = doubleSHA256 $ encode a `B.append` encode b

pushHash :: HashCompute -> Hash256 -> Maybe Hash256 -> Maybe Hash256 -> Int8 -> Int8 -> Bool -> HashCompute
pushHash (stateMap, res) nhash left right ht ind final =
    case node prev of
        Just pv ->
            if ind < ht
                then
                    pushHash
                        ( (M.insert ind emptyMerkleNode stateMap)
                        , ( insertSpecial
                                (Just pv)
                                (left)
                                (right)
                                True
                                (insertSpecial (Just nhash) (leftChild prev) (rightChild prev) False res)
                          )
                        )
                        (hashPair pv nhash)
                        (Just pv)
                        (Just nhash)
                        ht
                        (ind + 1)
                        final
                else throw MerkleTreeInvalidException -- Fatal error, can only happen in case of invalid leaf nodes
        Nothing ->
            if ht == ind
                then (stateMap, (insertSpecial (Just nhash) left right True res))
                else
                    if final
                        then
                            pushHash
                                (updateState, (insertSpecial (Just nhash) left right True res))
                                (hashPair nhash nhash)
                                (Just nhash)
                                (Just nhash)
                                ht
                                (ind + 1)
                                final
                        else (updateState, res)
  where
    insertSpecial sib lft rht flg lst = [(MerkleNode sib lft rht flg)] ++ lst
    updateState = M.insert ind (MerkleNode (Just nhash) left right True) stateMap
    prev =
        case M.lookupIndex (fromIntegral ind) stateMap of
            Just i -> snd $ M.elemAt i stateMap
            Nothing -> emptyMerkleNode

updateMerkleSubTrees ::
    DatabaseHandles ->
    (TSH.TSHashTable Int MerkleNode) ->
    Logger ->
    HashCompute ->
    Hash256 ->
    Maybe Hash256 ->
    Maybe Hash256 ->
    Int8 ->
    Int8 ->
    Bool ->
    Bool ->
    Int32 ->
    IO (HashCompute)
updateMerkleSubTrees dbe tmtState lg hashComp newhash left right ht ind final onlySubTree pageNum = do
    eres <- try $ return $ pushHash hashComp newhash left right ht ind final
    case eres of
        Left MerkleTreeInvalidException -> do
            print (" PushHash Invalid Merkle Leaves exception")
            throw MerkleSubTreeDBInsertException
        Right (state, res) -> do
            if not $ L.null res
                then do
                    nres <-
                        if (not onlySubTree) && final
                            then do
                                let stRoot = head res
                                liftIO $ TSH.insert tmtState (fromIntegral pageNum) stRoot
                                trace lg $ LG.msg $ "tmtState, pgnum : " <> (show pageNum) ++ " , root: " ++ show stRoot
                                return $ tail res
                            else do
                                trace lg $
                                    LG.msg $
                                        "tmtState, txhash : " <> (show newhash)
                                            ++ " final: "
                                            ++ (show final)
                                            ++ " res: "
                                            ++ show res
                                return res
                    let (create, match) =
                            L.partition
                                ( \x ->
                                    case x of
                                        (MerkleNode sib lft rht _) ->
                                            if isJust sib && isJust lft && isJust rht
                                                then False
                                                else
                                                    if isJust sib
                                                        then True
                                                        else throw MerkleTreeComputeException
                                )
                                (nres)
                    let finMatch =
                            L.sortBy
                                ( \x y ->
                                    if (leftChild x == node y) || (rightChild x == node y)
                                        then GT
                                        else LT
                                )
                                match
                    if L.length create == 1 && L.null finMatch
                        then return (state, [])
                        else do
                            ores <-
                                LA.race
                                    ( liftIO $
                                        try $
                                            withResource' (pool $ graphDB dbe) (`BT.run` insertMerkleSubTree create finMatch)
                                    )
                                    (liftIO $ threadDelay (30 * 1000000))
                            case ores of
                                Right () -> do
                                    throw DBInsertTimeOutException
                                Left res -> do
                                    case res of
                                        Right rt -> return (state, [])
                                        Left (e :: SomeException) -> do
                                            if T.isInfixOf (T.pack "ConstraintValidationFailed") (T.pack $ show e)
                                                then -- could be a previously aborted block being reprocessed
                                                -- or a chain-reorg with repeat Txs, so handle TMT subtree accordingly.
                                                do
                                                    warn lg $
                                                        LG.msg $
                                                            "Warning (Ignore): likely due to reprocessing." <> (show e)
                                                    return (state, [])
                                                else do
                                                    err lg $
                                                        LG.msg $ "Error: MerkleSubTreeDBInsertException : " <> (show e)
                                                    throw MerkleSubTreeDBInsertException
                else do
                    trace lg $ LG.msg $ "tmtState, else : " <> (show pageNum)
                    return (state, res)

-- else block --

resilientRead ::
    (HasLogger m, MonadBaseControl IO m, MonadIO m) => Socket -> BlockIngestState -> Int64 -> m (([Tx], LC.ByteString), Int64)
resilientRead sock !blin maxChunkSize = do
    lg <- getLogger
    let !unspentBytesLen = LC.length $ binUnspentBytes blin
    let chunkSize = 5 * 1000 * 1000 -- 5MB
        !dlt =
            if binTxPayloadLeft blin > chunkSize
                then chunkSize - (unspentBytesLen)
                else (binTxPayloadLeft blin) - (unspentBytesLen)
    trace lg $ msg (" | Tx payload left " ++ show (binTxPayloadLeft blin))
    trace lg $ msg (" | Bytes prev unspent " ++ show unspentBytesLen)
    let !delta =
            if dlt > 0
                then dlt
                else 0
    trace lg $ msg (" | Bytes to read " ++ show delta)
    nbyt <- recvAll sock delta
    let !txbyt = (binUnspentBytes blin) `LC.append` (nbyt)
    let !txbytLen = unspentBytesLen + delta
    case runGetLazyState (getConfirmedTxBatch) txbyt of
        Left e -> do
            trace lg $ msg $ "2nd attempt|" ++ show e
            let chunkSizeFB = 20 * 1000 * 1000 -- 20 MB
                !dltNew =
                    if binTxPayloadLeft blin > chunkSizeFB
                        then chunkSizeFB - txbytLen
                        else (binTxPayloadLeft blin) - txbytLen
            let !deltaNew =
                    if dltNew > 0
                        then dltNew
                        else 0
            nbyt2 <- recvAll sock deltaNew
            let !txbyt2 = txbyt `LC.append` (nbyt2)
            let !txbytLen2 = txbytLen + deltaNew
            case runGetLazyState (getConfirmedTxBatch) txbyt2 of
                Left e -> do
                    trace lg $ msg $ "3rd attempt|" ++ show e
                    let chunkSizeXXB = maxChunkSize -- 100 * 1000 * 1000 -- 100 MB
                        dltXXNew =
                            if binTxPayloadLeft blin > chunkSizeXXB
                                then chunkSizeXXB - txbytLen2
                                else (binTxPayloadLeft blin) - txbytLen2
                        deltaXXNew =
                            if dltXXNew > 0
                                then dltXXNew
                                else 0
                    nbyt3 <- recvAll sock deltaXXNew
                    let !txbyt3 = txbyt2 `LC.append` (nbyt3)
                    let !txbytLen3 = txbytLen2 + deltaXXNew
                    case runGetLazyState (getConfirmedTxBatch) txbyt3 of
                        Left e -> do
                            err lg $ msg $ "3rd attempt|" ++ show e
                            throw ConfirmedTxParseException
                        Right res3 -> return (res3, txbytLen3)
                Right res2 -> return (res2, txbytLen2)
        Right res1 -> return (res1, txbytLen)

persistTxListByPage ::
    (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m) =>
    MerkleTxQueue ->
    BlockHash ->
    m ()
persistTxListByPage mtque blockHash = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe <- getDB
    continue <- liftIO $ newIORef True
    txPage <- liftIO $ newIORef []
    txPageNum <- liftIO $ newIORef 1
    whileM_ (liftIO $ readIORef continue) $ do
        ores <- LA.race (liftIO $ threadDelay (1000000 * 60 * 10)) (liftIO $ atomically $ readTQueue $ mTxQueue mtque)
        case ores of
            Left () ->
                -- likely the peer conn terminated, just end this thread
                -- do NOT delete queue as another peer connection could have establised meanwhile
                do
                    liftIO $ writeIORef continue False
            Right (txh, isLast) -> do
                pg <- liftIO $ readIORef txPage
                if (fromIntegral $ L.length pg) == 256 -- 100
                    then do
                        pgn <- liftIO $ readIORef txPageNum
                        commitTxPage pg blockHash pgn
                        liftIO $ writeIORef txPage [txh]
                        liftIO $ writeIORef txPageNum (pgn + 1)
                    else do
                        liftIO $ modifyIORef' txPage (\x -> x ++ [txh])
                when isLast $ do
                    pg <- liftIO $ readIORef txPage
                    if L.null pg
                        then return ()
                        else do
                            pgn <- liftIO $ readIORef txPageNum
                            commitTxPage pg blockHash pgn
                            return ()
                    liftIO $ writeIORef continue False
                    liftIO $ putMVar (mTxQueueSem mtque) ()
                    liftIO $ TSH.delete (merkleQueueMap p2pEnv) blockHash

loadTxIDPagesTMT ::
    (HasXokenNodeEnv env m, HasLogger m, MonadIO m) =>
    BlockHash ->
    Int32 ->
    Bool ->
    m ([(MerkleNode, Bool)], (Int32, Bool))
loadTxIDPagesTMT blkHash pageNum onlySubTree = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe <- getDB
    let conn = xCqlClientState $ dbe
    debug lg $ LG.msg $ "loadTxIDPagesTMT block_hash: " ++ show (blkHash) ++ ", pagenum: " ++ show pageNum
    let str = "SELECT txids from xoken.blockhash_txids where block_hash = ? and page_number = ? "
        qstr = str :: Q.QueryString Q.R (T.Text, Int32) (Identity [T.Text])
        p = getSimpleQueryParam $ (blockHashToHex blkHash, pageNum)
    res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxIDsByBlockHash: " <> show e
            throw KeyValueDBLookupException
        Right rx -> do
            if L.null rx
                then do
                    trace lg $ LG.msg $ "loadTxIDPagesTMT| waiting for page in DB: " <> show pageNum
                    liftIO $ threadDelay (100000) -- 0.1 sec
                    loadTxIDPagesTMT blkHash pageNum onlySubTree
                else do
                    let txList = runIdentity $ rx !! 0
                        txct = L.length txList
                        mklNodeList =
                            map
                                ( \(hs, indx) -> do
                                    let txhs = fromJust $ hexToTxHash hs
                                    let mn = MerkleNode (Just $ getTxHash txhs) Nothing Nothing False
                                    if indx == txct
                                        then (mn, True)
                                        else (mn, False)
                                )
                                (zip txList [1 .. txct])
                    return (mklNodeList, (pageNum, onlySubTree))

persistTMT ::
    (HasXokenNodeEnv env m, HasLogger m, MonadIO m) =>
    BlockHash ->
    Maybe Int8 ->
    (TSH.TSHashTable Int MerkleNode) ->
    ([(MerkleNode, Bool)], (Int32, Bool)) ->
    m ()
persistTMT blockHash isFixedTreeHeight tmtState (mklNodes, (pageNum, onlySubTree)) = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe <- getDB
    tv <- liftIO $ newIORef (M.empty, [])
    let treeHt =
            case isFixedTreeHeight of
                Just fth -> fth
                Nothing -> computeTreeHeight (fromIntegral $ L.length mklNodes)
    trace lg $
        LG.msg
            ( "persistTMT | blockhash "
                ++ show blockHash
                ++ " (pageNum,onlyST): "
                ++ show (pageNum, onlySubTree)
                ++ (show $ L.length mklNodes)
            )
    mapM_
        ( \(mn, isLast) -> do
            hcstate <- liftIO $ readIORef tv
            res <-
                LE.try $
                    liftIO $
                        updateMerkleSubTrees
                            dbe
                            tmtState
                            lg
                            hcstate
                            (fromJust $ node mn)
                            (leftChild mn)
                            (rightChild mn)
                            treeHt
                            0
                            isLast
                            onlySubTree
                            pageNum
            case res of
                Right (hcs) -> do
                    liftIO $ writeIORef tv hcs
                Left (ee :: SomeException) -> do
                    err lg $
                        LG.msg
                            ( "[ERROR] Quit building TMT. FATAL error!"
                                ++ (show ee)
                                ++ " | "
                                ++ show (fromJust $ node mn)
                            )
                    throw ee
        )
        (mklNodes)

processTMTSubTrees :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BlockHash -> Int32 -> m ()
processTMTSubTrees blkHash txCount = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe <- getDB
    lg <- getLogger
    lg <- getLogger
    let conn = xCqlClientState $ dbe
        dpx = txCount `divMod` 256
        pages =
            if snd dpx == 0
                then fst dpx
                else fst dpx + 1
    tmtState <- liftIO $ TSH.new 4
    debug lg $ LG.msg $ "processTMTSubTrees: " ++ (show blkHash) ++ ", txcount:" ++ (show txCount)

    xx <-
        LE.try $
            S.drain $
                aheadly $
                    (do S.fromList [1 .. pages])
                        & S.mapM
                            ( \pageNum -> do
                                if pages == 1
                                    then loadTxIDPagesTMT blkHash pageNum True
                                    else loadTxIDPagesTMT blkHash pageNum False
                            )
                        & S.mapM
                            ( \z -> do
                                let nodes = fst $ unzip $ fst z
                                liftIO $ withResource' (pool $ graphDB dbe) (`BT.run` deleteMerkleSubTree nodes)
                            )
                        & S.maxBuffer (maxTMTBuilderThreads $ nodeConfig p2pEnv) -- fixed tree height of 8 corresponding to max 256 leaf nodes
                        & S.maxThreads (maxTMTBuilderThreads $ nodeConfig p2pEnv)

    case xx of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error while deleteMerkleSubTree: " ++ show e
            throw e

    yy <-
        LE.try $
            S.drain $
                aheadly $
                    (do S.fromList [1 .. pages])
                        & S.mapM
                            ( \pageNum -> do
                                if pages == 1
                                    then loadTxIDPagesTMT blkHash pageNum True
                                    else loadTxIDPagesTMT blkHash pageNum False
                            )
                        & S.mapM (persistTMT blkHash (Just 8) tmtState)
                        & S.maxBuffer (maxTMTBuilderThreads $ nodeConfig p2pEnv) -- fixed tree height of 8 corresponding to max 256 leaf nodes
                        & S.maxThreads (maxTMTBuilderThreads $ nodeConfig p2pEnv)
    case yy of
        Right _ -> do
            if pages == 1
                then return ()
                else do
                    interimNodes <-
                        mapM
                            ( \pg -> do
                                xx <- liftIO $ TSH.lookup tmtState pg
                                case xx of
                                    Just x -> return x
                                    Nothing -> do
                                        err lg $ LG.msg $ "Error while inserting pages/tmtState: " ++ show pg ++ " block: " ++ show blkHash
                                        throw InvalidMetaDataException
                            )
                            [1 .. (fromIntegral pages)]
                    let txct = L.length interimNodes
                        finalIntermediateMerkleNodes =
                            map
                                ( \(mn, indx) -> do
                                    if indx == txct
                                        then (mn, True)
                                        else (mn, False)
                                )
                                (zip interimNodes ([1 .. txct]))
                    persistTMT blkHash Nothing tmtState (finalIntermediateMerkleNodes, (-1, True))
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error while processTMTSubTrees: " ++ show e
            throw e

--runTMTDaemon :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
--runTMTDaemon = forever $ do
--  liftIO $ threadDelay (99999990000)

runTMTDaemon :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runTMTDaemon = do
    lg <- getLogger
    dbe <- getDB
    res1 <- LE.try $ runTMTDaemon''
    case res1 of
        Right _ -> do
            return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg ("Error: runTMTDaemon: " ++ show e)
            throw e
 
runTMTDaemon'' :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runTMTDaemon'' = do
    continue <- liftIO $ newIORef True
    whileM_ (liftIO $ readIORef continue) $ do
        liftIO $ threadDelay (100000) -- 0.1 sec
        p2pEnv <- getBitcoinP2P
        lg <- getLogger
        dbe <- getDB
        lg <- getLogger
        lg <- getLogger
        let net = bitcoinNetwork $ nodeConfig p2pEnv
            conn = xCqlClientState $ dbe
        let qstr :: Q.QueryString Q.R (Identity T.Text) (Identity (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text))
            qstr = "SELECT value from xoken.misc_store where key = ?"
            p = getSimpleQueryParam "transpose_merkle_tree"
        res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
        (bhash, height) <-
            case res of
                (Right iop) -> do
                    if L.null iop
                        then do
                            let genesisHash = headerHash $ getGenesisHeader net
                            debug lg $ LG.msg $ val "Best TMT empty, starting with genesis."
                            return (genesisHash, 0)
                        else do
                            let record = runIdentity $ iop !! 0
                            debug lg $ LG.msg $ "Best TMT persisted is : " ++ show (record)
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
        let str = "SELECT block_hash, tx_count, block_header from xoken.blocks_by_height where block_height = ?  "
            qstr = str :: Q.QueryString Q.R (Identity Int32) (T.Text, Maybe Int32, T.Text)
            p = getSimpleQueryParam $ Identity (height + 1)
        res1 <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
        case res1 of
            Right iop -> do
                if L.length iop == 0
                    then return ()
                    else do
                        let (nbhs, ntxc, nbhdr) = iop !! 0
                        case ntxc of
                            Just txCount -> do
                                case (A.eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 nbhdr) of
                                    Right bh -> do
                                        debug lg $
                                            LG.msg $ "AAA comparing blk-hashes: " ++ show (prevBlock bh) ++ show bhash
                                        if (prevBlock bh) == bhash
                                            then do
                                                yy <-
                                                    LE.try $ processTMTSubTrees (fromJust $ hexToBlockHash nbhs) txCount
                                                --
                                                case yy of
                                                    Right y -> do
                                                        let q ::
                                                                Q.QueryString
                                                                    Q.W
                                                                    ( T.Text
                                                                    , ( Maybe Bool
                                                                      , Int32
                                                                      , Maybe Int64
                                                                      , T.Text
                                                                      )
                                                                    )
                                                                    ()
                                                            q =
                                                                Q.QueryString
                                                                    "insert INTO xoken.misc_store (key,value) values (?,?)"
                                                            p ::
                                                                Q.QueryParams
                                                                    ( T.Text
                                                                    , ( Maybe Bool
                                                                      , Int32
                                                                      , Maybe Int64
                                                                      , T.Text
                                                                      )
                                                                    )
                                                            p =
                                                                getSimpleQueryParam
                                                                    ( "transpose_merkle_tree"
                                                                    , (Nothing, height + 1, Nothing, nbhs)
                                                                    )
                                                        res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query q p)
                                                        case res of
                                                            Right _ -> do
                                                                if mAPICallbacksEnable $ nodeConfig p2pEnv
                                                                    then do
                                                                        triggerCallbacks -- TODO: trigger callbacks jobs
                                                                    else return ()
                                                            Left (e :: SomeException) -> do
                                                                err lg $
                                                                    LG.msg
                                                                        ( "Error: Marking [transpose_merkle_tree] failed: "
                                                                            ++ show e
                                                                        )
                                                                throw KeyValueDBInsertException
                                                        return ()
                                                    Left (e :: SomeException) -> do
                                                        err lg $ LG.msg $ "Error while persistTMT: " ++ show e
                                                        liftIO $ writeIORef continue False
                                            else err lg $ LG.msg $ "Potential Chain reorg occured, Prev-blk-hash: " ++ show (prevBlock bh) ++ ", Blk-hash: " ++ show bhash
                                    Left err -> do
                                        liftIO $ print $ "Decode failed with error: " <> show err
                            Nothing -> return ()
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ "Error invalid while querying DB: " ++ show e
                throw e

updateBlocks :: (HasLogger m, HasDatabaseHandles m, MonadIO m) => BlockHash -> BlockHeight -> Int -> Int -> Tx -> m ()
updateBlocks bhash blkht bsize txcount cbase = do
    lg <- getLogger
    dbe' <- getDB
    let conn = xCqlClientState $ dbe'
    let q1 :: Q.QueryString Q.W (T.Text, Int32, Int32, Blob) ()
        q1 =
            Q.QueryString
                "INSERT INTO xoken.blocks_by_hash (block_hash,block_size,tx_count,coinbase_tx) VALUES (?, ?, ?, ?)"
        q2 :: Q.QueryString Q.W (Int32, Int32, Int32, Blob) ()
        q2 =
            Q.QueryString
                "INSERT INTO xoken.blocks_by_height (block_height,block_size,tx_count,coinbase_tx) VALUES (?, ?, ?, ?)"
        p1 =
            getSimpleQueryParam
                ( blockHashToHex bhash
                , fromIntegral bsize
                , fromIntegral txcount
                , Blob $ runPutLazy $ putLazyByteString $ encodeLazy $ cbase
                )
        p2 =
            getSimpleQueryParam
                ( fromIntegral blkht
                , fromIntegral bsize
                , fromIntegral txcount
                , Blob $ runPutLazy $ putLazyByteString $ encodeLazy $ cbase
                )
    res1 <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query q1 p1)
    case res1 of
        Right _ -> do
            debug lg $ LG.msg $ "Updated blocks_by_hash for block_hash " ++ show bhash
            return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg ("Error: INSERT into 'blocks_by_hash' failed: " ++ show e)
            throw KeyValueDBInsertException
    res2 <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query q2 p2)
    case res2 of
        Right _ -> do
            debug lg $ LG.msg $ "Updated blocks_by_height for block_height " ++ show blkht
            return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg ("Error: INSERT into 'blocks_by_height' failed: " ++ show e)
            throw KeyValueDBInsertException

readNextMessage ::
    (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m) =>
    Network ->
    Socket ->
    Maybe IngressStreamState ->
    m ((Maybe Message, Maybe IngressStreamState))
readNextMessage net sock ingss = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe' <- getDB
    case ingss of
        Just iss -> do
            let blin = issBlockIngest iss
            ((txns, unused), txbytLen) <- resilientRead sock blin (maxChunkSize $ nodeConfig p2pEnv) 
            debug lg $ msg ("Confirmed-Tx: " ++ (show (L.length txns)) ++ " unused: " ++ show (LC.length unused))
            mqe <-
                case (issBlockInfo iss) of
                    Just bf ->
                        if (binTxIngested blin == 0) -- very first Tx
                            then do
                                liftIO $ do
                                    vala <- TSH.lookup (blockTxProcessingLeftMap p2pEnv) (biBlockHash $ bf)
                                    case vala of
                                        Just v -> return ()
                                        Nothing -> do
                                            ar <- TSH.new 4
                                            TSH.insert
                                                (blockTxProcessingLeftMap p2pEnv)
                                                (biBlockHash $ bf)
                                                (ar, (binTxTotalCount blin))
                                qq <- liftIO $ atomically $ newTQueue
                                qs <- liftIO $ newEmptyMVar
                                let mtq = MerkleTxQueue qq qs
                                -- wait for TMT threads alloc
                                liftIO $ TSH.insert (merkleQueueMap p2pEnv) (biBlockHash $ bf) mtq
                                LA.async $ persistTxListByPage mtq (biBlockHash $ bf)
                                updateBlocks
                                    (biBlockHash bf)
                                    (biBlockHeight bf)
                                    (binBlockSize blin)
                                    (binTxTotalCount blin)
                                    (txns !! 0)
                                return $ Just mtq
                            else do
                                valx <- liftIO $ TSH.lookup (merkleQueueMap p2pEnv) (biBlockHash $ bf)
                                case valx of
                                    Just q -> return $ Just q
                                    Nothing -> return Nothing -- throw MerkleQueueNotFoundException
                    Nothing -> throw MessageParsingException
            let isLastBatch = ((binTxTotalCount blin) == ((L.length txns) + binTxIngested blin))
            let txct = L.length txns
            case mqe of
                Nothing -> return ()
                Just mq -> do
                    liftIO $
                        mapM_
                            ( \(tx, ct) -> do
                                let isLast =
                                        if isLastBatch
                                            then txct == ct
                                            else False
                                atomically $ writeTQueue (mTxQueue mq) ((txHash tx), isLast)
                                if isLast
                                    then liftIO $ takeMVar (mTxQueueSem mq) --blocked until the Tx queue is processed
                                    else return ()
                            )
                            (zip txns [1 ..])
            let bio =
                    BlockIngestState
                        { binUnspentBytes = unused
                        , binTxPayloadLeft = binTxPayloadLeft blin - (txbytLen - LC.length unused)
                        , binTxTotalCount = binTxTotalCount blin
                        , binTxIngested = (L.length txns) + binTxIngested blin
                        , binBlockSize = binBlockSize blin
                        , binChecksum = binChecksum blin
                        }
            return (Just $ MConfTx txns, Just $ IngressStreamState bio (issBlockInfo iss))
        Nothing -> do
            hdr <- recvAll sock 24
            case (decodeLazy hdr) of
                Left e -> do
                    err lg $ msg ("Error decoding incoming message header: " ++ e)
                    throw MessageParsingException
                Right (MessageHeader headMagic extcmd extlen cks) -> do
                    debug lg $ msg (" message header: " ++ (show extcmd))
                    if headMagic == getNetworkMagic net
                        then do
                            if extcmd == MCBlock
                                then do
                                    byts <- recvAll sock (88) -- 80 byte Block header + VarInt (max 8 bytes) Tx count
                                    case runGetLazyState (getDeflatedBlock) (byts) of
                                        Left e -> do
                                            err lg $ msg ("Error, unexpected message header: " ++ e)
                                            throw MessageParsingException
                                        Right (blk, unused) -> do
                                            case blk of
                                                Just b -> do
                                                    trace lg $
                                                        msg
                                                            ( "DefBlock: "
                                                                ++ show blk
                                                                ++ " unused: "
                                                                ++ show (LC.length unused)
                                                            )
                                                    let bi =
                                                            BlockIngestState
                                                                { binUnspentBytes = unused
                                                                , binTxPayloadLeft =
                                                                    fromIntegral (extlen) - (88 - LC.length unused)
                                                                , binTxTotalCount = fromIntegral $ txnCount b
                                                                , binTxIngested = 0
                                                                , binBlockSize = fromIntegral $ extlen
                                                                , binChecksum = cks
                                                                }
                                                    return (Just $ MBlock b, Just $ IngressStreamState bi Nothing)
                                                Nothing -> throw DeflatedBlockParseException
                                else do
                                    byts <-
                                        if extlen == 0
                                            then return hdr
                                            else do
                                                b <- recvAll sock (fromIntegral extlen)
                                                return (hdr `BSL.append` b)
                                    case runGetLazy (getMessage net) byts of
                                        Left e -> do
                                            err lg $ msg (" type???: " ++ show e)
                                            -- throw MessageParsingException
                                            return (Just $ MOther C.empty C.empty, Nothing)
                                        Right mg -> do
                                            debug lg $ msg ("Message recv' : " ++ (show $ msgType mg))
                                            return (Just mg, Nothing)
                        else do
                            err lg $ msg ("Error, network magic mismatch!: " ++ (show headMagic))
                            throw NetworkMagicMismatchException

--
doVersionHandshake ::
    (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m) =>
    Network ->
    Socket ->
    SockAddr ->
    m (Bool)
doVersionHandshake net sock sa = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    g <- liftIO $ newStdGen
    now <- round <$> liftIO getPOSIXTime
    let ip = bitcoinNodeListenIP $ nodeConfig p2pEnv
        port = toInteger $ bitcoinNodeListenPort $ nodeConfig p2pEnv
    myaddr <- liftIO $ head <$> getAddrInfo (Just defaultHints{addrSocketType = Stream}) (Just ip) (Just $ show port)
    let nonce = fst (random g :: (Word64, StdGen))
        ad = NetworkAddress 0 $ addrAddress myaddr
        bb = 1 :: Word32 -- ### TODO: getBestBlock ###
        rmt = NetworkAddress 0 sa
        ver = buildVersion net nonce bb ad rmt now
        em = runPut . putMessage net $ (MVersion ver)
    mv <- liftIO $ (newMVar ())
    debug lg $ msg $ val "Starting handshake.. "
    liftIO $ sendEncMessage mv sock (BSL.fromStrict em)
    (hs1, _) <- readNextMessage net sock Nothing
    debug lg $ msg $ val "Got this in return - handshake.. "
    case hs1 of
        Just (MVersion __) -> do
            (hs2, _) <- readNextMessage net sock Nothing
            case hs2 of
                Just MVerAck -> do
                    let em2 = runPut . putMessage net $ (MVerAck)
                    liftIO $ sendEncMessage mv sock (BSL.fromStrict em2)
                    debug lg $ msg ("Version handshake complete: " ++ show sa)
                    return True
                __ -> do
                    err lg $ msg $ val "Error, unexpected message (2) during handshake"
                    return False
        Just (MOther _ _) -> do
            (hs2, _) <- readNextMessage net sock Nothing
            case hs2 of
                Just MVerAck -> do
                    let em2 = runPut . putMessage net $ (MVerAck)
                    liftIO $ sendEncMessage mv sock (BSL.fromStrict em2)
                    debug lg $ msg ("Version handshake complete'': " ++ show sa)
                    return True
                __ -> do
                    err lg $ msg $ val "Error, unexpected message (2) during handshake''"
                    return False
        __ -> do
            err lg $ msg $ val "Error, unexpected message (1) during handshake''"
            return False

messageHandler ::
    (HasXokenNodeEnv env m, HasLogger m, MonadIO m) =>
    BitcoinPeer ->
    (Maybe Message, Maybe IngressStreamState) ->
    m (MessageCommand)
messageHandler peer (mm, ingss) = do
    bp2pEnv <- getBitcoinP2P
    dbe <- getDB
    lg <- getLogger
    case mm of
        Just msg -> do
            case (msg) of
                MHeaders hdrs -> do
                    liftIO $ takeMVar (headersWriteLock bp2pEnv)
                    res <- LE.try $ processHeaders hdrs
                    case res of
                        Right () -> return ()
                        Left BlockHashNotFoundException -> return ()
                        Left EmptyHeadersMessageException -> return ()
                        Left InvalidBlocksException -> do
                            err lg $ LG.msg $ (val "[ERROR] Closing peer connection, Checkpoint verification failed")
                            case (bpSocket peer) of
                                Just sock -> liftIO $ Network.Socket.close sock
                                Nothing -> return ()
                            liftIO $
                                atomically $ do
                                    modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress peer))
                                    modifyTVar' (blacklistedPeers bp2pEnv) (M.insert (bpAddress peer) peer)
                            liftIO $ putMVar (headersWriteLock bp2pEnv) True
                            -- liftIO $ TSH.delete (parallelBlockProcessingMap bp2pEnv) (show $ bpAddress peer)
                            throw InvalidBlocksException
                        Left KeyValueDBInsertException -> do
                            err lg $ LG.msg $ LG.val ("[ERROR] Insert failed. KeyValueDBInsertException")
                            liftIO $ putMVar (headersWriteLock bp2pEnv) True
                            throw KeyValueDBInsertException
                        Left e -> do
                            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
                            liftIO $ putMVar (headersWriteLock bp2pEnv) True
                            throw e
                    liftIO $ putMVar (headersWriteLock bp2pEnv) True
                    return $ msgType msg
                MInv inv -> do
                    mapM_
                        ( \x ->
                            if (invType x) == InvBlock
                                then do
                                    trace lg $ LG.msg ("INV - new Block: " ++ (show $ invHash x))
                                    liftIO $ putMVar (bestBlockUpdated bp2pEnv) True -- will trigger a GetHeaders to peers
                                else
                                    if (invType x == InvTx)
                                        then do
                                            indexUnconfirmedTx <- liftIO $ readTVarIO $ indexUnconfirmedTx bp2pEnv
                                            debug lg $ LG.msg ("INV - new Tx: " ++ (show $ invHash x) ++ " , indexUnconfTx:" ++ (show indexUnconfirmedTx))
                                            if indexUnconfirmedTx == True
                                                then processTxGetData peer $ invHash x
                                                else return ()
                                        else return ()
                        )
                        (invList inv)
                    return $ msgType msg
                MAddr addrs -> do
                    mapM_
                        ( \(t, x) -> do
                            bp <- setupPeerConnection $ naAddress x
                            LA.async $
                                ( case bp of
                                    Just p -> do
                                        liftIO $ atomically $ writeTQueue (peerFetchQueue bp2pEnv) p
                                        handleIncomingMessages p -- must be the last step, as blocking loop
                                    Nothing -> return ()
                                )
                        )
                        (addrList addrs)
                    return $ msgType msg
                MConfTx txns -> do
                    case ingss of
                        Just iss -> do
                            debug lg $ LG.msg $ ("Processing Tx-batch, size :" ++ (show $ L.length txns))
                            processTxBatch txns iss
                            debug lg $ LG.msg $ ("DONE_Processing Tx-batch, size :" ++ (show $ L.length txns))
                        Nothing -> do
                            err lg $ LG.msg $ val ("[???] Unconfirmed Tx ")
                    return $ msgType msg
                MTx tx -> do
                    LA.async $
                        ( do
                            res <-
                                LE.try $
                                    processUnconfTransaction
                                        tx
                                        100 -- TODO: setting min fee for now
                            case res of
                                Right () -> return ()
                                Left (TxIDNotFoundException _) -> return ()
                                Left e -> throw e
                        )
                    return $ msgType msg
                MBlock blk -> do
                    res <- LE.try $ processBlock blk
                    case res of
                        Right () -> return ()
                        Left BlockHashNotFoundException -> return ()
                        Left EmptyHeadersMessageException -> return ()
                        Left e -> do
                            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
                            throw e
                    return $ msgType msg
                MPing ping -> do
                    bp2pEnv <- getBitcoinP2P
                    let net = bitcoinNetwork $ nodeConfig bp2pEnv
                    let em = runPut . putMessage net $ (MPong $ Pong (pingNonce ping))
                    case (bpSocket peer) of
                        Just sock -> do
                            liftIO $ sendEncMessage (bpWriteMsgLock peer) sock (BSL.fromStrict em)
                            return $ msgType msg
                        Nothing -> return $ msgType msg
                _ -> do
                    return $ msgType msg
        Nothing -> do
            err lg $ LG.msg $ val "Error, invalid message"
            throw InvalidMessageTypeException

processTxBatch :: (HasXokenNodeEnv env m, MonadIO m) => [Tx] -> IngressStreamState -> m ()
processTxBatch txns iss = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe <- getDB
    let conn = xCqlClientState $ dbe
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let bi = issBlockIngest iss
        binfo = issBlockInfo iss
    (_, chainTipHeight) <- fetchBestBlock conn net
    (_, bestSynced) <- fetchBestSyncedBlock
    let checkTxAlreadyProc =
            if chainTipHeight > (bestSynced + 4)
                then False
                else True
    debug lg $ LG.msg $ (" processTxBatch: " ++ (show $ txHash $ head txns) ++ " : " ++ (show $ L.length txns))
    case binfo of
        Just bf -> do
            valx <- liftIO $ TSH.lookup (blockTxProcessingLeftMap bp2pEnv) (biBlockHash bf)
            skip <-
                case valx of
                    Just lfa -> do
                        y <- liftIO $ TSH.lookup (fst lfa) (txHash $ head txns)
                        case y of
                            Just c -> return True -- TODO: return True
                            Nothing -> return False
                    Nothing -> return False
            if skip
                then do
                    debug lg $
                        LG.msg $
                            ( "Tx already processed, block: "
                                ++ (show $ biBlockHash bf)
                                ++ ", tx-index: "
                                ++ show (binTxIngested bi)
                            )
                else do
                    S.drain $
                        aheadly $
                            ( do
                                let start = (binTxIngested bi) - (L.length txns)
                                    end = (binTxIngested bi) - 1
                                S.fromList $ zip [start .. end] [0 ..]
                            )
                                & S.mapM
                                    ( \(cidx, idx) -> do
                                        if (idx >= (L.length txns))
                                            then debug lg $ LG.msg $ (" (error) Tx__index: " ++ show idx ++ show bf)
                                            else debug lg $ LG.msg $ ("Tx__index: " ++ show idx)
                                        return ((txns !! idx), bf, cidx)
                                    )
                                & S.mapM (processTxStream bi checkTxAlreadyProc)
                                & S.maxBuffer (maxTxProcessingBuffer $ nodeConfig bp2pEnv)
                                & S.maxThreads (maxTxProcessingThreads $ nodeConfig bp2pEnv)
                    valy <- liftIO $ TSH.lookup (blockTxProcessingLeftMap bp2pEnv) (biBlockHash bf)
                    case valy of
                        Just lefta -> do
                            debug lg $ LG.msg $ (" processTxBatch (inserting) " ++ (show $ txHash $ head txns) ++ " : " ++ (show $ L.length txns))
                            liftIO $ TSH.insert (fst lefta) (txHash $ head txns) (L.length txns)
                            return ()
                        Nothing -> return ()
                    return ()
        Nothing -> throw InvalidStreamStateException

--
--
processTxStream :: (HasXokenNodeEnv env m, MonadIO m) => BlockIngestState -> Bool -> (Tx, BlockInfo, Int) -> m ()
processTxStream bi checkTxAlreadyProc (tx, binfo, txIndex) = do
    bp2pEnv <- getBitcoinP2P
    let bhash = biBlockHash binfo
        bheight = biBlockHeight binfo
    lg <- getLogger
    res <- LE.try $ processConfTransaction bi (tx) bhash (fromIntegral bheight) txIndex checkTxAlreadyProc
    case res of
        Right () -> return ()
        Left (TxIDNotFoundException (txid, index)) -> do
            throw $ TxIDNotFoundException (txid, index)
        Left KeyValueDBInsertException -> do
            err lg $ LG.msg $ val "[ERROR] KeyValueDBInsertException"
            throw KeyValueDBInsertException
        Left e -> do
            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
            throw e

readNextMessage' ::
    (HasXokenNodeEnv env m, MonadIO m) =>
    BitcoinPeer ->
    MVar (Maybe IngressStreamState) ->
    m ((Maybe Message, Maybe IngressStreamState))
readNextMessage' peer readLock = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    case bpSocket peer of
        Just sock -> do
            prIss <- liftIO $ takeMVar $ readLock
            let prevIngressState =
                    case prIss of
                        Just pis ->
                            if (binTxTotalCount $ issBlockIngest pis) == (binTxIngested $ issBlockIngest pis)
                                then Nothing
                                else prIss
                        Nothing -> Nothing
            (msg, ingressState) <- readNextMessage net sock prevIngressState
            case ingressState of
                Just iss -> do
                    let ingst = issBlockIngest iss
                    case msg of
                        Just (MBlock blk) ->
                            -- setup state
                            do
                                let hh = headerHash $ defBlockHeader blk
                                mht <- liftIO $ TSH.lookup (blockSyncStatusMap bp2pEnv) (hh)
                                case (mht) of
                                    Just x -> return ()
                                    Nothing -> do
                                        err lg $ LG.msg $ ("InvalidBlockSyncStatusMapException - " ++ show hh)
                                        throw InvalidBlockSyncStatusMapException
                                let iz = Just (IngressStreamState ingst (Just $ BlockInfo hh (snd $ fromJust mht)))
                                liftIO $ putMVar readLock iz
                        Just (MConfTx ctx) -> do
                            case issBlockInfo iss of
                                Just bi -> do
                                    tm <- liftIO $ getCurrentTime
                                    liftIO $ TSH.insert (bpPendingRequests peer) (biBlockHash bi) (tm) -- update the time
                                    liftIO $
                                        TSH.insert
                                            (blockSyncStatusMap bp2pEnv)
                                            (biBlockHash bi)
                                            ( RecentTxReceiveTime tm
                                            , biBlockHeight bi -- track receive progress
                                            )
                                    if binTxTotalCount ingst == binTxIngested ingst
                                        then do
                                            debug lg $ LG.msg $ ("Ingested block fully - " ++ (show $ bpAddress peer))
                                            liftIO $ TSH.delete (bpPendingRequests peer) (biBlockHash bi)
                                            liftIO $ atomically $ unGetTQueue (peerFetchQueue bp2pEnv) peer -- queueing at the front, for recency!
                                            liftIO $ putMVar readLock Nothing
                                        else do
                                            debug lg $ LG.msg $ ("Ingested  ConfTx chunk - " ++ (show $ bpAddress peer))
                                            liftIO $ putMVar readLock ingressState
                                Nothing -> throw InvalidBlockInfoException
                        otherwise -> throw $ UnexpectedDuringBlockProcException "_1_"
                Nothing -> do
                    liftIO $ putMVar readLock ingressState
                    return ()
            return (msg, ingressState)
        Nothing -> throw PeerSocketNotConnectedException

handleIncomingMessages :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> m ()
handleIncomingMessages pr = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    debug lg $ msg $ "reading from: " ++ show (bpAddress pr)
    rlk <- liftIO $ newMVar Nothing
    res <-
        LE.try $
            S.drain $
                S.serially $
                    S.repeatM (readNextMessage' pr rlk)
                        & S.mapM (messageHandler pr) -- read next msgs
                        & S.mapM (logMessage pr) -- handle read msgs
                        & S.maxBuffer 5 -- log msgs & collect stats
                        & S.maxThreads 5
    case res of
        Right (a) -> return ()
        Left (e :: SomeException) ->
            -- liftIO $ atomicModifyIORef' (ptBlockFetchWindow tracker) (\x -> (0, ())) -- safety border case
            do
                err lg $ msg $ (val "[ERROR] Closing peer connection ") +++ (show e)
                case (bpSocket pr) of
                    Just sock -> liftIO $ Network.Socket.close sock
                    Nothing -> return ()
                liftIO $ atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress pr))
                -- liftIO $ TSH.delete (parallelBlockProcessingMap bp2pEnv) (show $ bpAddress pr)
                return ()

logMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> MessageCommand -> m (Bool)
logMessage peer mg = do
    lg <- getLogger
    -- liftIO $ atomically $ modifyTVar' (bpIngressMsgCount peer) (\z -> z + 1)
    debug lg $ LG.msg $ "DONE! processed: " ++ show mg
    return (True)
