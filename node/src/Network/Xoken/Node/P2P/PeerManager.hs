{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.PeerManager
    ( createSocket
    , setupSeedPeerConnection
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
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Char
import Data.Default
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
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import StmContainers.Map as SM
import StmContainers.Set as SS
import Streamly as S
import Streamly.Prelude ((|:), drain, each, nil)
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

setupSeedPeerConnection :: (HasXokenNodeEnv env m, MonadIO m) => m ()
setupSeedPeerConnection =
    forever $ do
        bp2pEnv <- getBitcoinP2P
        lg <- getLogger
        let net = bitcoinNetwork $ nodeConfig bp2pEnv
            seeds = getSeeds net
            hints = defaultHints {addrSocketType = Stream}
            port = getDefaultPort net
        debug lg $ msg $ show seeds
        --let sd = map (\x -> Just (x :: HostName)) seeds
        !addrs <- liftIO $ mapConcurrently (\x -> head <$> getAddrInfo (Just hints) (Just x) (Just (show port))) seeds
        mapM_
            (\y -> do
                 debug lg $ msg ("Peer.. " ++ show (addrAddress y))
                 LA.async $
                     (do blockedpr <- liftIO $ readTVarIO (blacklistedPeers bp2pEnv)
                         allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
                             -- this can be optimized
                         let connPeers =
                                 L.foldl'
                                     (\c x ->
                                          if bpConnected (snd x)
                                              then c + 1
                                              else c)
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
                                     else if isBlacklisted
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
                                                  trk <- liftIO $ getNewTracker
                                                  bfq <- liftIO $ newEmptyMVar
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
                                                                              trk
                                                                              bfq
                                                                  liftIO $
                                                                      atomically $
                                                                      modifyTVar'
                                                                          (bitcoinPeers bp2pEnv)
                                                                          (M.insert (addrAddress y) bp)
                                                                  handleIncomingMessages bp
                                                              Nothing -> return ()
                                                      Left (SocketConnectException addr) ->
                                                          warn lg $ msg ("SocketConnectException: " ++ show addr)))
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
                (\c x ->
                     if bpConnected (snd x)
                         then c + 1
                         else c)
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
                else if isBlacklisted
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
                                     trk <- liftIO $ getNewTracker
                                     bfq <- liftIO $ newEmptyMVar
                                     case sock of
                                         Just sx -> do
                                             debug lg $ LG.msg ("Discovered Net-Address: " ++ (show $ saddr))
                                             fl <- doVersionHandshake net sx $ saddr
                                             let bp = BitcoinPeer (saddr) sock wl fl Nothing 99999 trk bfq
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
                then pushHash
                         ( (M.insert ind emptyMerkleNode stateMap)
                         , (insertSpecial
                                (Just pv)
                                (left)
                                (right)
                                True
                                (insertSpecial (Just nhash) (leftChild prev) (rightChild prev) False res)))
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
                else if final
                         then pushHash
                                  (updateState, (insertSpecial (Just nhash) left right True res))
                                  (hashPair nhash nhash)
                                  (Just nhash)
                                  (Just nhash)
                                  ht
                                  (ind + 1)
                                  final
                         else (updateState, res)
  where
    insertSpecial sib lft rht flg lst = L.insert (MerkleNode sib lft rht flg) lst
    updateState = M.insert ind (MerkleNode (Just nhash) left right True) stateMap
    prev =
        case M.lookupIndex (fromIntegral ind) stateMap of
            Just i -> snd $ M.elemAt i stateMap
            Nothing -> emptyMerkleNode

updateMerkleSubTrees ::
       DatabaseHandles
    -> HashCompute
    -> Hash256
    -> Maybe Hash256
    -> Maybe Hash256
    -> Int8
    -> Int8
    -> Bool
    -> IO (HashCompute)
updateMerkleSubTrees dbe hashComp newhash left right ht ind final = do
    eres <- try $ return $ pushHash hashComp newhash left right ht ind final
    case eres of
        Left MerkleTreeInvalidException -> do
            print (" PushHash Invalid Merkle Leaves exception")
            throw MerkleSubTreeDBInsertException
        Right (state, res) -- = pushHash hashComp newhash left right ht ind final
         -> do
            if not $ L.null res
                then do
                    let (create, match) =
                            L.partition
                                (\x ->
                                     case x of
                                         (MerkleNode sib lft rht _) ->
                                             if isJust sib && isJust lft && isJust rht
                                                 then False
                                                 else if isJust sib
                                                          then True
                                                          else throw MerkleTreeComputeException)
                                (res)
                    let finMatch =
                            L.sortBy
                                (\x y ->
                                     if (leftChild x == node y) || (rightChild x == node y)
                                         then GT
                                         else LT)
                                match
                    if L.length create == 1 && L.null finMatch
                        then return (state, [])
                        else do
                            ores <-
                                LA.race
                                    (liftIO $
                                     try $
                                     tryWithResource (pool $ graphDB dbe) (`BT.run` insertMerkleSubTree create finMatch))
                                    (liftIO $ threadDelay (30 * 1000000))
                            case ores of
                                Right () -> do
                                    throw DBInsertTimeOutException
                                Left res -> do
                                    case res of
                                        Right rt -> do
                                            case rt of
                                                Just r -> do
                                                    return (state, [])
                                                Nothing -> do
                                                    liftIO $ threadDelay (1000000 * 5) -- time to recover
                                                    throw ResourcePoolFetchException
                                        Left (e :: SomeException) -> do
                                            if T.isInfixOf (T.pack "ConstraintValidationFailed") (T.pack $ show e)
                                            -- could be a previously aborted block being reprocessed
                                            -- or a chain-reorg with repeat Txs, so handle TMT subtree accordingly.
                                                then do
                                                    pres <-
                                                        liftIO $
                                                        try $
                                                        tryWithResource
                                                            (pool $ graphDB dbe)
                                                            (`BT.run` deleteMerkleSubTree (create ++ finMatch))
                                                    case pres of
                                                        Right rt -> throw MerkleSubTreeAlreadyExistsException -- attempt new insert
                                                        Left (e :: SomeException) ->
                                                            throw MerkleSubTreeDBInsertException
                                                else do
                                                    liftIO $ threadDelay (1000000 * 5) -- time to recover
                                                    throw MerkleSubTreeDBInsertException
                else return (state, res)
                    -- else block --

resilientRead ::
       (HasLogger m, MonadBaseControl IO m, MonadIO m) => Socket -> BlockIngestState -> m (([Tx], LC.ByteString), Int64)
resilientRead sock !blin = do
    lg <- getLogger
    let chunkSize = 200 * 1000 -- 200 KB
        !delta =
            if binTxPayloadLeft blin > chunkSize
                then chunkSize - ((LC.length $ binUnspentBytes blin))
                else (binTxPayloadLeft blin) - (LC.length $ binUnspentBytes blin)
    trace lg $ msg (" | Tx payload left " ++ show (binTxPayloadLeft blin))
    trace lg $ msg (" | Bytes prev unspent " ++ show (LC.length $ binUnspentBytes blin))
    trace lg $ msg (" | Bytes to read " ++ show delta)
    nbyt <- recvAll sock delta
    let !txbyt = (binUnspentBytes blin) `LC.append` (nbyt)
    case runGetLazyState (getConfirmedTxBatch) txbyt of
        Left e -> do
            trace lg $ msg $ "1st attempt|" ++ show e
            let chunkSizeFB = 200 * 1000 * 1000 -- 200 MB
                !deltaNew =
                    if binTxPayloadLeft blin > chunkSizeFB
                        then chunkSizeFB - ((LC.length $ binUnspentBytes blin) + delta)
                        else (binTxPayloadLeft blin) - ((LC.length $ binUnspentBytes blin) + delta)
            nbyt2 <- recvAll sock deltaNew
            let !txbyt2 = txbyt `LC.append` (nbyt2)
            case runGetLazyState (getConfirmedTxBatch) txbyt2 of
                Left e -> do
                    err lg $ msg $ "2nd attempt|" ++ show e
                    throw ConfirmedTxParseException
                Right res -> do
                    return (res, LC.length txbyt2)
        Right res -> return (res, LC.length txbyt)

merkleTreeBuilder ::
       (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m)
    => TQueue (TxHash, Bool)
    -> BlockHash
    -> Int8
    -> m ()
merkleTreeBuilder tque blockHash treeHt = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe <- getDB
    continue <- liftIO $ newIORef True
    txPage <- liftIO $ newIORef []
    txPageNum <- liftIO $ newIORef 1
    tv <- liftIO $ newIORef (M.empty, [])
    whileM_ (liftIO $ readIORef continue) $ do
        hcstate <- liftIO $ readIORef tv
        ores <- LA.race (liftIO $ threadDelay (1000000 * 60)) (liftIO $ atomically $ readTQueue tque)
        case ores of
            Left ()
                -- likely the peer conn terminated, just end this thread
                -- do NOT delete queue as another peer connection could have establised meanwhile
             -> do
                liftIO $ writeIORef continue False
                liftIO $ MS.signal (maxTMTBuilderThreadLock p2pEnv)
            Right (txh, isLast) -> do
                pg <- liftIO $ readIORef txPage
                if (fromIntegral $ L.length pg) == 100
                    then do
                        pgn <- liftIO $ readIORef txPageNum
                        LA.async $ commitTxPage pg blockHash pgn
                        liftIO $ writeIORef txPage [txh]
                        liftIO $ writeIORef txPageNum (pgn + 1)
                    else do
                        liftIO $ modifyIORef' txPage (\x -> x ++ [txh])
                res <-
                    LE.try $
                    liftIO $
                    EX.retry 3 $ updateMerkleSubTrees dbe hcstate (getTxHash txh) Nothing Nothing treeHt 0 isLast
                case res of
                    Right (hcs) -> do
                        liftIO $ writeIORef tv hcs
                    Left MerkleSubTreeAlreadyExistsException
                        -- second attempt, after deleting stale TMT nodes
                     -> do
                        pres <-
                            LE.try $
                            liftIO $
                            EX.retry 3 $
                            updateMerkleSubTrees dbe hcstate (getTxHash txh) Nothing Nothing treeHt 0 isLast
                        case pres of
                            Left (SomeException e) -> do
                                err lg $
                                    LG.msg
                                        ("[ERROR] Quit building TMT. FATAL Bug! (2)" ++
                                         show e ++ " | " ++ show (getTxHash txh))
                                liftIO $ writeIORef continue False
                                liftIO $ MS.signal (maxTMTBuilderThreadLock p2pEnv)
                                -- do NOT delete queue here, merely end this thread
                                throw e
                            Right (hcs) -> do
                                liftIO $ writeIORef tv hcs
                    Left ee -> do
                        err lg $
                            LG.msg
                                ("[ERROR] Quit building TMT. FATAL Bug! (1) " ++
                                 show ee ++ " | " ++ show (getTxHash txh))
                        liftIO $ writeIORef continue False
                        liftIO $ MS.signal (maxTMTBuilderThreadLock p2pEnv)
                        -- do NOT delete queue here, merely end this thread
                        throw ee
                when isLast $ do
                    pg <- liftIO $ readIORef txPage
                    if L.null pg
                        then return ()
                        else do
                            pgn <- liftIO $ readIORef txPageNum
                            LA.async $ commitTxPage pg blockHash pgn
                            return ()
                    liftIO $ writeIORef continue False
                    liftIO $ CHT.delete (merkleQueueMap p2pEnv) blockHash
                    liftIO $ MS.signal (maxTMTBuilderThreadLock p2pEnv)

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
                , Blob $ runPutLazy $ putLazyByteString $ encodeLazy $ cbase)
        p2 =
            getSimpleQueryParam
                ( fromIntegral blkht
                , fromIntegral bsize
                , fromIntegral txcount
                , Blob $ runPutLazy $ putLazyByteString $ encodeLazy $ cbase)
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
       (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m)
    => Network
    -> Socket
    -> Maybe IngressStreamState
    -> m ((Maybe Message, Maybe IngressStreamState))
readNextMessage net sock ingss = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe' <- getDB
    case ingss of
        Just iss -> do
            let blin = issBlockIngest iss
            ((txns, unused), txbytLen) <- resilientRead sock blin
            debug lg $ msg ("Confirmed-Tx: " ++ (show (L.length txns)) ++ " unused: " ++ show (LC.length unused))
            qe <-
                case (issBlockInfo iss) of
                    Just bf ->
                        if (binTxIngested blin == 0) -- very first Tx
                            then do
                                liftIO $ do
                                    vala <- CHT.lookup (blockTxProcessingLeftMap p2pEnv) (biBlockHash $ bf)
                                    case vala of
                                        Just v -> return ()
                                        Nothing -> do
                                            ar <- CHT.newWithDefaults 4
                                            CHT.insert
                                                (blockTxProcessingLeftMap p2pEnv)
                                                (biBlockHash $ bf)
                                                (ar, (binTxTotalCount blin))
                                            return ()
                                qq <- liftIO $ atomically $ newTQueue
                                        -- wait for TMT threads alloc
                                liftIO $ MS.wait (maxTMTBuilderThreadLock p2pEnv)
                                liftIO $ CHT.insert (merkleQueueMap p2pEnv) (biBlockHash $ bf) qq
                                LA.async $
                                    merkleTreeBuilder qq (biBlockHash $ bf) (computeTreeHeight $ binTxTotalCount blin)
                                updateBlocks
                                    (biBlockHash bf)
                                    (biBlockHeight bf)
                                    (binBlockSize blin)
                                    (binTxTotalCount blin)
                                    (txns !! 0)
                                return qq
                            else do
                                valx <- liftIO $ CHT.lookup (merkleQueueMap p2pEnv) (biBlockHash $ bf)
                                case valx of
                                    Just q -> return q
                                    Nothing -> throw MerkleQueueNotFoundException
                    Nothing -> throw MessageParsingException
            let isLastBatch = ((binTxTotalCount blin) == ((L.length txns) + binTxIngested blin))
            let txct = L.length txns
            liftIO $
                atomically $
                mapM_
                    (\(tx, ct) -> do
                         let isLast =
                                 if isLastBatch
                                     then txct == ct
                                     else False
                         writeTQueue qe ((txHash tx), isLast))
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
                Right (MessageHeader headMagic cmd len cks) -> do
                    if headMagic == getNetworkMagic net
                        then do
                            if cmd == MCBlock
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
                                                            ("DefBlock: " ++
                                                             show blk ++ " unused: " ++ show (LC.length unused))
                                                    let bi =
                                                            BlockIngestState
                                                                { binUnspentBytes = unused
                                                                , binTxPayloadLeft =
                                                                      fromIntegral (len) - (88 - LC.length unused)
                                                                , binTxTotalCount = fromIntegral $ txnCount b
                                                                , binTxIngested = 0
                                                                , binBlockSize = fromIntegral $ len
                                                                , binChecksum = cks
                                                                }
                                                    return (Just $ MBlock b, Just $ IngressStreamState bi Nothing)
                                                Nothing -> throw DeflatedBlockParseException
                                else do
                                    byts <-
                                        if len == 0
                                            then return hdr
                                            else do
                                                b <- recvAll sock (fromIntegral len)
                                                return (hdr `BSL.append` b)
                                    case runGetLazy (getMessage net) byts of
                                        Left e -> throw MessageParsingException
                                        Right mg -> do
                                            debug lg $ msg ("Message recv' : " ++ (show $ msgType mg))
                                            return (Just mg, Nothing)
                        else do
                            err lg $ msg ("Error, network magic mismatch!: " ++ (show headMagic))
                            throw NetworkMagicMismatchException

--
doVersionHandshake ::
       (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m)
    => Network
    -> Socket
    -> SockAddr
    -> m (Bool)
doVersionHandshake net sock sa = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    g <- liftIO $ newStdGen
    now <- round <$> liftIO getPOSIXTime
    let ip = bitcoinNodeListenIP $ nodeConfig p2pEnv
        port = toInteger $ bitcoinNodeListenPort $ nodeConfig p2pEnv
    myaddr <- liftIO $ head <$> getAddrInfo (Just defaultHints {addrSocketType = Stream}) (Just ip) (Just $ show port)
    let nonce = fst (random g :: (Word64, StdGen))
        ad = NetworkAddress 0 $ addrAddress myaddr
        bb = 1 :: Word32 -- ### TODO: getBestBlock ###
        rmt = NetworkAddress 0 sa
        ver = buildVersion net nonce bb ad rmt now
        em = runPut . putMessage net $ (MVersion ver)
    mv <- liftIO $ (newMVar ())
    liftIO $ sendEncMessage mv sock (BSL.fromStrict em)
    (hs1, _) <- readNextMessage net sock Nothing
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
        __ -> do
            err lg $ msg $ val "Error, unexpected message (1) during handshake"
            return False

messageHandler ::
       (HasXokenNodeEnv env m, HasLogger m, MonadIO m)
    => BitcoinPeer
    -> (Maybe Message, Maybe IngressStreamState)
    -> m (MessageCommand)
messageHandler peer (mm, ingss) = do
    bp2pEnv <- getBitcoinP2P
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
                        (\x ->
                             if (invType x) == InvBlock
                                 then do
                                     trace lg $ LG.msg ("INV - new Block: " ++ (show $ invHash x))
                                     liftIO $ putMVar (bestBlockUpdated bp2pEnv) True -- will trigger a GetHeaders to peers
                                 else if (invType x == InvTx)
                                          then do
                                              indexUnconfirmedTx <- liftIO $ readTVarIO $ indexUnconfirmedTx bp2pEnv
                                              trace lg $ LG.msg ("INV - new Tx: " ++ (show $ invHash x))
                                              if indexUnconfirmedTx == True
                                                  then processTxGetData peer $ invHash x
                                                  else return ()
                                          else return ())
                        (invList inv)
                    return $ msgType msg
                MAddr addrs -> do
                    mapM_
                        (\(t, x) -> do
                             bp <- setupPeerConnection $ naAddress x
                             LA.async $
                                 (case bp of
                                      Just p -> handleIncomingMessages p
                                      Nothing -> return ()))
                        (addrList addrs)
                    return $ msgType msg
                MConfTx txns -> do
                    case ingss of
                        Just iss -> do
                            debug lg $ LG.msg $ ("Processing Tx-batch, size :" ++ (show $ L.length txns))
                            processTxBatch txns iss
                        Nothing -> do
                            err lg $ LG.msg $ val ("[???] Unconfirmed Tx ")
                    return $ msgType msg
                MTx tx -> do
                    processUnconfTransaction tx
                    return $ msgType msg
                MBlock blk
                    -- debug lg $ LG.msg $ LG.val ("DEBUG receiving block ")
                 -> do
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
    let bi = issBlockIngest iss
    let binfo = issBlockInfo iss
    case binfo of
        Just bf -> do
            valx <- liftIO $ CHT.lookup (blockTxProcessingLeftMap bp2pEnv) (biBlockHash bf)
            skip <-
                case valx of
                    Just lfa -> do
                        y <- liftIO $ CHT.lookup (fst lfa) (txHash $ head txns)
                        case y of
                            Just c -> return True
                            Nothing -> return False
                    Nothing -> return False
            if skip
                then do
                    debug lg $
                        LG.msg $
                        ("Tx already processed, block: " ++
                         (show $ biBlockHash bf) ++ ", tx-index: " ++ show (binTxIngested bi))
                else do
                    S.drain $
                        aheadly $
                        (do let start = (binTxIngested bi) - (L.length txns)
                                end = (binTxIngested bi) - 1
                            S.fromList $ zip [start .. end] [0 ..]) &
                        S.mapM
                            (\(cidx, idx) -> do
                                 if (idx >= (L.length txns))
                                     then debug lg $ LG.msg $ (" (error) Tx__index: " ++ show idx ++ show bf)
                                     else debug lg $ LG.msg $ ("Tx__index: " ++ show idx)
                                 return ((txns !! idx), bf, cidx)) &
                        S.mapM (processTxStream) &
                        S.maxBuffer (maxTxProcessingBuffer $ nodeConfig bp2pEnv) &
                        S.maxThreads (maxTxProcessingThreads $ nodeConfig bp2pEnv)
                    valy <- liftIO $ CHT.lookup (blockTxProcessingLeftMap bp2pEnv) (biBlockHash bf)
                    case valy of
                        Just lefta -> do
                            liftIO $ CHT.insert (fst lefta) (txHash $ head txns) (L.length txns)
                            return ()
                        Nothing -> return ()
                    return ()
        Nothing -> throw InvalidStreamStateException

--
-- 
processTxStream :: (HasXokenNodeEnv env m, MonadIO m) => (Tx, BlockInfo, Int) -> m ()
processTxStream (tx, binfo, txIndex) = do
    let bhash = biBlockHash binfo
        bheight = biBlockHeight binfo
    lg <- getLogger
    res <- LE.try $ processConfTransaction (tx) bhash (fromIntegral bheight) txIndex
    case res of
        Right () -> return ()
        Left TxIDNotFoundException -> do
            throw TxIDNotFoundException
        Left KeyValueDBInsertException -> do
            err lg $ LG.msg $ val "[ERROR] KeyValueDBInsertException"
            throw KeyValueDBInsertException
        Left e -> do
            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
            throw e

readNextMessage' ::
       (HasXokenNodeEnv env m, MonadIO m)
    => BitcoinPeer
    -> MVar (Maybe IngressStreamState)
    -> m ((Maybe Message, Maybe IngressStreamState))
readNextMessage' peer readLock = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        tracker = statsTracker peer
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
                        Just (MBlock blk) -- setup state
                         -> do
                            let hh = headerHash $ defBlockHeader blk
                            mht <- liftIO $ CHT.lookup (blockSyncStatusMap bp2pEnv) (hh)
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
                                    liftIO $ writeIORef (ptLastTxRecvTime tracker) $ Just tm
                                    if binTxTotalCount ingst == binTxIngested ingst
                                        then do
                                            liftIO $ modifyIORef' (ptBlockFetchWindow tracker) (\z -> z - 1)
                                            liftIO $
                                                CHT.insert
                                                    (blockSyncStatusMap bp2pEnv)
                                                    (biBlockHash bi)
                                                    (BlockReceiveComplete tm, biBlockHeight bi)
                                            debug lg $
                                                LG.msg $ ("putMVar readLock Nothing - " ++ (show $ bpAddress peer))
                                            liftIO $ putMVar readLock Nothing
                                        else do
                                            liftIO $
                                                CHT.insert
                                                    (blockSyncStatusMap bp2pEnv)
                                                    (biBlockHash bi)
                                                    ( RecentTxReceiveTime (tm, binTxIngested ingst)
                                                    , biBlockHeight bi -- track receive progress
                                                     )
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
        LA.concurrently_
            (peerBlockSync pr) -- issue GetData msgs
            (S.drain $
             S.aheadly $
             S.repeatM (readNextMessage' pr rlk) & -- read next msgs
             S.mapM (messageHandler pr) & -- handle read msgs
             S.mapM (logMessage pr) & -- log msgs & collect stats
             S.maxBuffer 2 &
             S.maxThreads 2)
    case res of
        Right (a) -> return ()
        Left (e :: SomeException) -> do
            err lg $ msg $ (val "[ERROR] Closing peer connection ") +++ (show e)
            case (bpSocket pr) of
                Just sock -> liftIO $ Network.Socket.close sock
                Nothing -> return ()
            liftIO $ atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress pr))
            return ()

logMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> MessageCommand -> m (Bool)
logMessage peer mg = do
    lg <- getLogger
    -- liftIO $ atomically $ modifyTVar' (bpIngressMsgCount peer) (\z -> z + 1)
    debug lg $ LG.msg $ "DONE! processed: " ++ show mg
    return (True)
