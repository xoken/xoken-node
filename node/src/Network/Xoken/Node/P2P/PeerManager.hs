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
    , terminateStalePeers
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.Async.Lifted as LA (async, cancel, race, wait, waitAnyCatch, withAsync)
import qualified Control.Concurrent.MSem as MS
import qualified Control.Concurrent.MSemN as MSN
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TBQueue
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
import qualified Database.CQL.IO as Q
import GHC.Natural
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
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.ChainSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.UnconfTxSync
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import Streamly
import Streamly.Prelude ((|:), drain, nil)
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
    (host, srv) <- getNameInfo [NI_NUMERICHOST, NI_NUMERICSERV] True True $ saddr
    sock <-
        case L.findIndex (\c -> c == '.') (fromJust host) of
            Nothing -> socket AF_INET6 Stream defaultProtocol
            Just ind -> socket AF_INET Stream defaultProtocol
    res <- try $ connect sock saddr
    case res of
        Right () -> return $ Just sock
        Left (e :: IOException) -> do
            liftIO $ Network.Socket.close sock
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
                                          if bpConnected (snd x) && not (M.member (fst x) blockedpr)
                                              then c + 1
                                              else c)
                                     0
                                     (M.toList allpr)
                         if connPeers > 16
                             then liftIO $ threadDelay (10 * 1000000)
                             else do
                                 let toConn =
                                         case M.lookup (addrAddress y) allpr of
                                             Just pr ->
                                                 if bpConnected pr
                                                     then False
                                                     else True
                                             Nothing -> True
                                     isBlacklisted =
                                         case M.lookup (addrAddress y) blockedpr of
                                             Just _ -> True
                                             Nothing -> False
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
                                                  wl <- liftIO $ newMVar True
                                                  ss <- liftIO $ newTVarIO Nothing
                                                  imc <- liftIO $ newTVarIO 0
                                                  rc <- liftIO $ newTVarIO Nothing
                                                  st <- liftIO $ newTVarIO Nothing
                                                  fw <- liftIO $ newTVarIO 0
                                                  res <- LE.try $ liftIO $ createSocket y
                                                  ms <- liftIO $ MSN.new $ maxTxProcThreads $ nodeConfig bp2pEnv
                                                  case res of
                                                      Right (sock) -> do
                                                          case sock of
                                                              Just sx -> do
                                                                  fl <- doVersionHandshake net sx $ addrAddress y
                                                                  let bp =
                                                                          BitcoinPeer
                                                                              (addrAddress y)
                                                                              sock
                                                                              rl
                                                                              wl
                                                                              fl
                                                                              Nothing
                                                                              99999
                                                                              Nothing
                                                                              ss
                                                                              imc
                                                                              rc
                                                                              st
                                                                              fw
                                                                              ms
                                                                  liftIO $
                                                                      atomically $
                                                                      modifyTVar'
                                                                          (bitcoinPeers bp2pEnv)
                                                                          (M.insert (addrAddress y) bp)
                                                                  handleIncomingMessages bp
                                                              Nothing -> return ()
                                                      Left (SocketConnectException addr) ->
                                                          err lg $ msg ("SocketConnectException: " ++ show addr)))
            (addrs)
        liftIO $ threadDelay (30 * 1000000)

--
--
terminateStalePeers :: (HasXokenNodeEnv env m, MonadIO m) => m ()
terminateStalePeers =
    forever $ do
        liftIO $ threadDelay (300 * 1000000)
        bp2pEnv <- getBitcoinP2P
        lg <- getLogger
        allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        mapM_
            (\(_, pr) -> do
                 msgCt <- liftIO $ readTVarIO $ bpIngressMsgCount pr
                 if msgCt < 10
                     then do
                         debug lg $ msg ("Removing stale (connected) peer. " ++ show pr)
                         case bpSocket pr of
                             Just sock -> liftIO $ Network.Socket.close $ sock
                             Nothing -> return ()
                         liftIO $ atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress pr))
                     else do
                         debug lg $ msg ("Peer is active, remain connected. " ++ show pr))
            (M.toList allpr)

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
                     if bpConnected (snd x) && not (M.member (fst x) blockedpr)
                         then c + 1
                         else c)
                0
                (M.toList allpr)
    if connPeers > 16
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
                                     wl <- liftIO $ newMVar True
                                     ss <- liftIO $ newTVarIO Nothing
                                     imc <- liftIO $ newTVarIO 0
                                     rc <- liftIO $ newTVarIO Nothing
                                     st <- liftIO $ newTVarIO Nothing
                                     fw <- liftIO $ newTVarIO 0
                                     ms <- liftIO $ MSN.new $ maxTxProcThreads $ nodeConfig bp2pEnv
                                     case sock of
                                         Just sx -> do
                                             debug lg $ LG.msg ("Discovered Net-Address: " ++ (show $ saddr))
                                             fl <- doVersionHandshake net sx $ saddr
                                             let bp =
                                                     BitcoinPeer
                                                         (saddr)
                                                         sock
                                                         rl
                                                         wl
                                                         fl
                                                         Nothing
                                                         99999
                                                         Nothing
                                                         ss
                                                         imc
                                                         rc
                                                         st
                                                         fw
                                                         ms
                                             liftIO $
                                                 atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.insert (saddr) bp)
                                             return $ Just bp
                                         Nothing -> return (Nothing)
                                 Left (SocketConnectException addr) -> do
                                     err lg $ msg ("SocketConnectException: " ++ show addr)
                                     return Nothing

-- Helper Functions
recvAll :: (MonadIO m) => Socket -> Int -> m B.ByteString
recvAll sock len = do
    if len > 0
        then do
            res <- liftIO $ try $ SB.recv sock len
            case res of
                Left (e :: IOException) -> throw SocketReadException
                Right mesg ->
                    if B.length mesg == len
                        then return mesg
                        else if B.length mesg == 0
                                 then throw ZeroLengthSocketReadException
                                 else B.append mesg <$> recvAll sock (len - B.length mesg)
        else return (B.empty)

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
                then (updateState, (insertSpecial (Just nhash) left right True res))
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
                                                    liftIO $ threadDelay (1000000 * 2) -- time to recover
                                                    throw ResourcePoolFetchException
                                        Left (e :: SomeException) -> do
                                            if T.isInfixOf (T.pack "ConstraintValidationFailed") (T.pack $ show e)
                                                        -- not considered an error, could be a previously aborted block being reprocessed.
                                                then do
                                                    return (state, [])
                                                else do
                                                    liftIO $ threadDelay (1000000 * 2) -- time to recover
                                                    throw MerkleSubTreeDBInsertException
                else return (state, res)
                    -- else block --

resilientRead ::
       (HasLogger m, MonadBaseControl IO m, MonadIO m)
    => Socket
    -> BlockIngestState
    -> m ((Maybe Tx, C.ByteString), Int)
resilientRead sock blin = do
    lg <- getLogger
    let chunkSize = 400 * 1024
        delta =
            if binTxPayloadLeft blin > chunkSize
                then chunkSize - (B.length $ binUnspentBytes blin)
                else (binTxPayloadLeft blin) - (B.length $ binUnspentBytes blin)
    -- debug lg $ msg (" | Tx payload left " ++ show (binTxPayloadLeft blin))
    -- debug lg $ msg (" | Bytes prev unspent " ++ show (B.length $ binUnspentBytes blin))
    -- debug lg $ msg (" | Bytes to read " ++ show delta)
    nbyt <- recvAll sock delta
    let txbyt = (binUnspentBytes blin) `B.append` nbyt
    case runGetState (getConfirmedTx) txbyt 0 of
        Left e -> do
            err lg $ msg $ "1st attempt|" ++ show e
            let chunkSizeFB = (1024 * 1024 * 1024)
                deltaNew =
                    if binTxPayloadLeft blin > chunkSizeFB
                        then chunkSizeFB - ((B.length $ binUnspentBytes blin) + delta)
                        else (binTxPayloadLeft blin) - ((B.length $ binUnspentBytes blin) + delta)
            nbyt2 <- recvAll sock deltaNew
            let txbyt2 = txbyt `B.append` nbyt2
            case runGetState (getConfirmedTx) txbyt2 0 of
                Left e -> do
                    err lg $ msg $ "2nd attempt|" ++ show e
                    throw ConfirmedTxParseException
                Right res -> do
                    return (res, B.length txbyt2)
        Right res -> return (res, B.length txbyt)

merkleTreeBuilder ::
       (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m)
    => TBQueue (TxHash, Bool)
    -> BlockHash
    -> Int8
    -> m ()
merkleTreeBuilder tque blockHash treeHt = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe <- getDB
    continue <- liftIO $ newIORef True
    tv <- liftIO $ atomically $ newTVar (M.empty, [])
    whileM_ (liftIO $ readIORef continue) $ do
        hcstate <- liftIO $ readTVarIO tv
        ores <- LA.race (liftIO $ threadDelay (1000000 * 60)) (liftIO $ atomically $ readTBQueue tque)
        case ores of
            Left ()
                    -- likely the peer conn terminated, just end this thread
                    -- do NOT delete queue as another peer connection could have establised meanwhile
             -> do
                liftIO $ writeIORef continue False
                liftIO $ MS.signal (maxTMTBuilderThreadLock p2pEnv)
            Right (txh, isLast) -> do
                res <-
                    liftIO $
                    try $
                    liftIO $
                    EX.retry 5 $ updateMerkleSubTrees dbe hcstate (getTxHash txh) Nothing Nothing treeHt 0 isLast
                case res of
                    Left (e :: SomeException) -> do
                        err lg $
                            LG.msg
                                ("[ERROR] Quitting Transpose Merkle Tree building for block. FATAL Bug! " ++
                                 show e ++ show (getTxHash txh) ++ " last:" ++ show isLast)
                        liftIO $ writeIORef continue False
                        liftIO $ MS.signal (maxTMTBuilderThreadLock p2pEnv)
                        -- do NOT delete queue here, merely end this thread
                        throw e
                    Right (hcs) -> do
                        liftIO $ atomically $ writeTVar tv hcs
                        --debug lg $ msg ("updateMerkleSubTrees returned " ++ (show $ txh) ++ show)
                when isLast $ do
                    liftIO $ writeIORef continue False
                    liftIO $ atomically $ modifyTVar' (merkleQueueMap p2pEnv) (M.delete blockHash)
                    liftIO $ MS.signal (maxTMTBuilderThreadLock p2pEnv)

readNextMessage ::
       (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m)
    => Network
    -> Socket
    -> Maybe IngressStreamState
    -> m ((Maybe Message, Maybe IngressStreamState))
readNextMessage net sock ingss = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    case ingss of
        Just iss -> do
            let blin = issBlockIngest iss
            ((tx, unused), txbytLen) <- resilientRead sock blin
            case tx of
                Just t -> do
                    debug lg $ msg ("Confirmed-Tx: " ++ (show $ txHash t) ++ " unused: " ++ show (B.length unused))
                    mqm <- liftIO $ readTVarIO $ merkleQueueMap p2pEnv
                    qe <-
                        case (issBlockInfo iss) of
                            Just bf ->
                                if (binTxProcessed blin == 0) -- very first Tx
                                    then do
                                        qq <-
                                            liftIO $
                                            atomically $ newTBQueue $ intToNatural (maxTMTQueueSize $ nodeConfig p2pEnv)
                                        -- wait for TMT threads alloc
                                        liftIO $ MS.wait (maxTMTBuilderThreadLock p2pEnv)
                                        liftIO $
                                            atomically $
                                            modifyTVar' (merkleQueueMap p2pEnv) (M.insert (biBlockHash $ bf) qq)
                                        LA.async $
                                            merkleTreeBuilder
                                                qq
                                                (biBlockHash $ bf)
                                                (computeTreeHeight $ binTxTotalCount blin)
                                        return qq
                                    else case M.lookup (biBlockHash $ bf) mqm of
                                             Just q -> return q
                                             Nothing -> throw MerkleQueueNotFoundException
                            Nothing -> throw MessageParsingException
                    let isLast = ((binTxTotalCount blin) == (1 + binTxProcessed blin))
                    liftIO $ atomically $ writeTBQueue qe ((txHash t), isLast)
                    let bio =
                            BlockIngestState
                                { binUnspentBytes = unused
                                , binTxPayloadLeft = binTxPayloadLeft blin - (txbytLen - B.length unused)
                                , binTxTotalCount = binTxTotalCount blin
                                , binTxProcessed = 1 + binTxProcessed blin
                                , binChecksum = binChecksum blin
                                }
                    return
                        ( Just $ MConfTx t
                        , Just $ IngressStreamState bio (issBlockInfo iss) -- (merkleTreeHeight iss) 0 nst)
                         )
                Nothing -> do
                    throw ConfirmedTxParseException
        Nothing -> do
            hdr <- recvAll sock 24
            case (decode hdr) of
                Left e -> do
                    err lg $ msg ("Error decoding incoming message header: " ++ e)
                    throw MessageParsingException
                Right (MessageHeader _ cmd len cks) -> do
                    if cmd == MCBlock
                        then do
                            byts <- recvAll sock (88) -- 80 byte Block header + VarInt (max 8 bytes) Tx count
                            case runGetState (getDeflatedBlock) byts 0 of
                                Left e -> do
                                    err lg $ msg ("Error, unexpected message header: " ++ e)
                                    throw MessageParsingException
                                Right (blk, unused) -> do
                                    case blk of
                                        Just b
                                            -- debug lg $
                                            --     msg ("DefBlock: " ++ show blk ++ " unused: " ++ show (B.length unused))
                                         -> do
                                            let bi =
                                                    BlockIngestState
                                                        { binUnspentBytes = unused
                                                        , binTxPayloadLeft = fromIntegral (len) - (88 - B.length unused)
                                                        , binTxTotalCount = fromIntegral $ txnCount b
                                                        , binTxProcessed = 0
                                                        , binChecksum = cks
                                                        }
                                            return (Just $ MBlock b, Just $ IngressStreamState bi Nothing)
                                                      -- (computeTreeHeight $ binTxTotalCount bi)
                                                      -- 0
                                                      -- (M.empty, []))
                                        Nothing -> throw DeflatedBlockParseException
                        else do
                            byts <-
                                if len == 0
                                    then return hdr
                                    else do
                                        b <- recvAll sock (fromIntegral len)
                                        return (hdr `B.append` b)
                            case runGet (getMessage net) byts of
                                Left e -> throw MessageParsingException
                                Right msg -> do
                                    return (Just msg, Nothing)

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
    g <- liftIO $ getStdGen
    now <- round <$> liftIO getPOSIXTime
    myaddr <-
        liftIO $
        head <$>
        getAddrInfo
            (Just defaultHints {addrSocketType = Stream})
            (Just "51.89.40.95") -- "192.168.0.106")
            (Just "3000")
    let nonce = fst (random g :: (Word64, StdGen))
        ad = NetworkAddress 0 $ addrAddress myaddr -- (SockAddrInet 0 0)
        bb = 1 :: Word32 -- ### TODO: getBestBlock ###
        rmt = NetworkAddress 0 sa
        ver = buildVersion net nonce bb ad rmt now
        em = runPut . putMessage net $ (MVersion ver)
    debug lg $ msg ("ADD: " ++ show ad)
    mv <- liftIO $ (newMVar True)
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
    -- -> IORef Bool
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
                            -- liftIO $ writeIORef continue False
                            throw InvalidBlocksException
                        Left KeyValueDBInsertException -> do
                            err lg $ LG.msg $ LG.val ("[ERROR] Insert failed. KeyValueDBInsertException")
                        Left e -> do
                            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
                            throw e
                    liftIO $ putMVar (headersWriteLock bp2pEnv) True
                    return $ msgType msg
                MInv inv -> do
                    mapM_
                        (\x ->
                             if (invType x) == InvBlock
                                 then do
                                     debug lg $ LG.msg ("INV - new Block: " ++ (show $ invHash x))
                                     liftIO $ putMVar (bestBlockUpdated bp2pEnv) True -- will trigger a GetHeaders to peers
                                 else if (invType x == InvTx)
                                          then do
                                              debug lg $ LG.msg ("INV - new Tx: " ++ (show $ invHash x))
                                              if (indexUnconfirmedTx $ nodeConfig bp2pEnv) == True
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
                MConfTx tx -> do
                    case ingss of
                        Just iss
                            -- debug lg $ LG.msg $ LG.val ("DEBUG receiving confirmed Tx ")
                         -> do
                            let bi = issBlockIngest iss
                            let binfo = issBlockInfo iss
                            case binfo of
                                Just bf -> do
                                    res <-
                                        LE.try $
                                        processConfTransaction
                                            tx
                                            (biBlockHash bf)
                                            (binTxProcessed bi)
                                            (fromIntegral $ biBlockHeight bf)
                                    case res of
                                        Right () -> return ()
                                        Left BlockHashNotFoundException -> return ()
                                        Left EmptyHeadersMessageException -> return ()
                                        Left KeyValueDBInsertException -> do
                                            err lg $ LG.msg $ val "[ERROR] KeyValueDBInsertException"
                                            throw KeyValueDBInsertException
                                        Left e -> do
                                            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
                                            throw e
                                    return $ msgType msg
                                Nothing -> throw InvalidStreamStateException
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

readNextMessage' :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> m ((Maybe Message, Maybe IngressStreamState))
readNextMessage' peer = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    case bpSocket peer of
        Just sock -> do
            liftIO $ takeMVar $ bpReadMsgLock peer
            prevIngressState <- liftIO $ readTVarIO $ bpIngressState peer
            (msg, ingressState) <- readNextMessage net sock prevIngressState
            case ingressState of
                Just iss -> do
                    let ingst = issBlockIngest iss
                    case msg of
                        Just (MBlock blk) -- setup state
                         -> do
                            mp <- liftIO $ readTVarIO $ blockSyncStatusMap bp2pEnv
                            let hh = headerHash $ defBlockHeader blk
                            let mht = M.lookup hh mp
                            case (mht) of
                                Just x -> return ()
                                Nothing -> do
                                    debug lg $ LG.msg $ ("InvalidBlockSyncStatusMapException - " ++ show hh)
                                    throw InvalidBlockSyncStatusMapException
                            let iz = Just (IngressStreamState ingst (Just $ BlockInfo hh (snd $ fromJust mht)))
                                             -- (computeTreeHeight $ binTxTotalCount ingst)
                                             -- 0
                                             -- (M.empty, []))
                            liftIO $ atomically $ writeTVar (bpIngressState peer) $ iz
                        Just (MConfTx ctx) -> do
                            case issBlockInfo iss of
                                Just bi -> do
                                    tm <- liftIO $ getCurrentTime
                                    liftIO $ atomically $ writeTVar (bpLastTxRecvTime peer) $ Just tm
                                    let trig =
                                            if (binTxTotalCount ingst - 4) <= 0
                                                then binTxTotalCount ingst
                                                else (binTxTotalCount ingst - 4)
                                    if binTxProcessed ingst == trig
                                        then liftIO $ atomically $ modifyTVar' (bpBlockFetchWindow peer) (\z -> z - 1)
                                        else return ()
                                    if binTxTotalCount ingst == binTxProcessed ingst
                                            -- debug lg $ LG.msg $ ("DEBUG Block receive complete - " ++ show " ")
                                        then do
                                            liftIO $
                                                atomically $ do
                                                    writeTVar (bpIngressState peer) $ Nothing -- reset state
                                                    modifyTVar'
                                                        (blockSyncStatusMap bp2pEnv)
                                                        (M.insert (biBlockHash bi) $
                                                         (BlockReceiveComplete, biBlockHeight bi) -- mark block received
                                                         )
                                        else do
                                            liftIO $
                                                atomically $ do
                                                    writeTVar (bpIngressState peer) $ ingressState -- retain state
                                                    modifyTVar'
                                                        (blockSyncStatusMap bp2pEnv)
                                                        (M.insert (biBlockHash bi) $
                                                         ( RecentTxReceiveTime (tm, binTxProcessed ingst)
                                                         , biBlockHeight bi -- track receive progress
                                                          ))
                                Nothing -> throw InvalidBlockInfoException
                        otherwise -> throw UnexpectedDuringBlockProcException
                Nothing -> return ()
            liftIO $ putMVar (bpReadMsgLock peer) True
            return (msg, ingressState)
        Nothing -> throw PeerSocketNotConnectedException

procTxStream :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> m ()
procTxStream pr = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    res <- LE.try $ (readNextMessage' pr)
    case res of
        Right (msg, iss) -> do
            messageHandler pr (msg, iss)
            allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
            blockedPeers <- liftIO $ readTVarIO (blacklistedPeers bp2pEnv)
            let connPeers =
                    L.filter (\x -> bpConnected (snd x) && not (M.member (fst x) blockedPeers)) (M.toList allPeers)
            liftIO $ MSN.signal (bpTxSem pr) (L.length connPeers)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ (val "[ERROR] Closing peer connection ") +++ (show e)
            liftIO $ atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress pr))
            case (bpSocket pr) of
                Just sock -> liftIO $ Network.Socket.close sock
                Nothing -> return ()

handleIncomingMessages :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> m ()
handleIncomingMessages pr = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    debug lg $ msg $ "reading from: " ++ show (bpAddress pr)
    continue <- liftIO $ newIORef True
    whileM_ (liftIO $ readIORef continue) $ do
        allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        blockedPeers <- liftIO $ readTVarIO (blacklistedPeers bp2pEnv)
        let connPeers = L.filter (\x -> bpConnected (snd x) && not (M.member (fst x) blockedPeers)) (M.toList allPeers)
        debug lg $
            msg $
            (val "adjusted Tx threads per block ") +++
            (show $ divide (maxTxProcThreads $ nodeConfig bp2pEnv) (L.length connPeers))
        -- the number of active tx threads per peer is proportionally reduced based on peer count
        liftIO $ MSN.wait (bpTxSem pr) (L.length connPeers)
        LA.async $ procTxStream pr
        bps <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        case (M.lookup (bpAddress pr) bps) of
            Just _ -> return ()
            Nothing -> liftIO $ writeIORef continue False
