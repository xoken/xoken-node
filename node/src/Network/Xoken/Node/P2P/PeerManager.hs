{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.P2P.PeerManager
    ( createSocket
    , setupPeerConnection
    , initPeerListeners
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
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.ChainSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Util
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random

createSocket :: AddrInfo -> IO (Maybe Socket)
createSocket = createSocketWithOptions []

createSocketWithOptions :: [SocketOption] -> AddrInfo -> IO (Maybe Socket)
createSocketWithOptions options addr = do
    sock <- socket AF_INET Stream (addrProtocol addr)
    mapM_ (\option -> when (isSupportedSocketOption option) (setSocketOption sock option 1)) options
    res <- try $ connect sock (addrAddress addr)
    case res of
        Right () -> return $ Just sock
        Left (e :: IOException) -> throw $ SocketConnectException (addrAddress addr)

setupPeerConnection :: (HasService env m) => m ()
setupPeerConnection = do
    bp2pEnv <- getBitcoinP2PEnv
    let lg = logger bp2pEnv
        net = bncNet $ bitcoinNodeConfig bp2pEnv
        seeds = getSeeds net
        hints = defaultHints {addrSocketType = Stream}
        port = getDefaultPort net
    debug lg $ msg $ show seeds
    let sd = map (\x -> Just (x :: HostName)) seeds
    addrs <- liftIO $ mapConcurrently (\x -> head <$> getAddrInfo (Just hints) (x) (Just (show port))) sd
    res <-
        LE.try $
        mapM_
            (\y ->
                 LA.async $ do
                     sock <- liftIO $ createSocket y
                     rl <- liftIO $ newMVar True
                     wl <- liftIO $ newMVar True
                     ss <- liftIO $ newTVarIO Nothing
                     case sock of
                         Just sx -> do
                             fl <- doVersionHandshake net sx $ addrAddress y
                             let bp = BitcoinPeer (addrAddress y) sock rl wl fl Nothing 99999 Nothing ss
                             liftIO $ atomically $ modifyTVar (bitcoinPeers bp2pEnv) (M.insert (addrAddress y) bp)
                         Nothing -> do
                             let bp = BitcoinPeer (addrAddress y) sock rl wl False Nothing 99999 Nothing ss
                             liftIO $ atomically $ modifyTVar (bitcoinPeers bp2pEnv) (M.insert (addrAddress y) bp))
            addrs
    case res of
        Right () -> return ()
        Left (SocketConnectException addr) -> err lg $ msg ("SocketConnectException: " ++ show addr)
    return ()

-- Helper Functions
recvAll :: Socket -> Int -> IO B.ByteString
recvAll sock len = do
    if len > 0
        then do
            res <- try $ SB.recv sock len
            case res of
                Left (e :: IOException) -> throw SocketReadException
                Right msg ->
                    if B.length msg == len
                        then return msg
                        else B.append msg <$> recvAll sock (len - B.length msg)
        else return (B.empty)

readNextMessage ::
       (HasBitcoinP2PEnv m, MonadIO m)
    => Network
    -> Socket
    -> Maybe IngressStreamState
    -> m ((Maybe Message, Maybe IngressStreamState))
readNextMessage net sock ingss = do
    p2pEnv <- getBitcoinP2PEnv
    let lg = logger p2pEnv
    case ingss of
        Just iss -> do
            debug lg $ msg ("IngressStreamState parsing tx: " ++ show iss)
            let blin = issBlockIngest iss
                maxChunk = (1024 * 100) - (B.length $ binUnspentBytes blin)
                len =
                    if binTxPayloadLeft blin < maxChunk
                        then (binTxPayloadLeft blin) - (B.length $ binUnspentBytes blin)
                        else maxChunk
            debug lg $ msg (" | Tx payload left " ++ show (binTxPayloadLeft blin))
            debug lg $ msg (" | Bytes prev unspent " ++ show (B.length $ binUnspentBytes blin))
            debug lg $ msg (" | Bytes to read " ++ show len)
            nbyt <- liftIO $ recvAll sock len
            let txbyt = (binUnspentBytes blin) `B.append` nbyt
            case runGetState (getConfirmedTx) txbyt 0 of
                Left e -> do
                    debug lg $ msg $ ("(error) IngressStreamState: " ++ show iss)
                    debug lg $ msg $ (encodeHex txbyt)
                    throw ConfirmedTxParseException
                Right (tx, unused) -> do
                    case tx of
                        Just t -> do
                            debug lg $ msg ("Conf.Tx: " ++ show tx ++ " unused: " ++ show (B.length unused))
                            let !bio =
                                    BlockIngestState
                                        { binUnspentBytes = unused
                                        , binTxPayloadLeft = binTxPayloadLeft blin - (B.length txbyt - B.length unused)
                                        , binTxTotalCount = binTxTotalCount blin
                                        , binTxProcessed = 1 + binTxProcessed blin
                                        , binChecksum = binChecksum blin
                                        }
                            return (Just $ MTx t, Just $ IngressStreamState bio $ issBlockInfo iss)
                        Nothing -> do
                            debug lg $ msg (txbyt)
                            throw ConfirmedTxParseException
        Nothing -> do
            hdr <- liftIO $ recvAll sock 24
            case (decode hdr) of
                Left e -> do
                    err lg $ msg ("Error decoding incoming message header: " ++ e)
                    throw MessageParsingException
                Right (MessageHeader _ cmd len cks) -> do
                    if cmd == MCBlock
                        then do
                            byts <- liftIO $ recvAll sock (88) -- 80 byte Block header + VarInt (max 8 bytes) Tx count
                            case runGetState (getDeflatedBlock) byts 0 of
                                Left e -> do
                                    err lg $ msg ("Error, unexpected message header: " ++ e)
                                    throw MessageParsingException
                                Right (blk, unused) -> do
                                    case blk of
                                        Just b -> do
                                            debug lg $
                                                msg ("DefBlock: " ++ show blk ++ " unused: " ++ show (B.length unused))
                                            let !bi =
                                                    BlockIngestState
                                                        { binUnspentBytes = unused
                                                        , binTxPayloadLeft = fromIntegral (len) - (88 - B.length unused)
                                                        , binTxTotalCount = fromIntegral $ txnCount b
                                                        , binTxProcessed = 0
                                                        , binChecksum = cks
                                                        }
                                            return (Just $ MBlock b, Just $ IngressStreamState bi Nothing)
                                        Nothing -> throw DeflatedBlockParseException
                        else do
                            byts <-
                                if len == 0
                                    then return hdr
                                    else do
                                        b <- liftIO $ recvAll sock (fromIntegral len)
                                        return (hdr `B.append` b)
                            case runGet (getMessage net) byts of
                                Left e -> throw MessageParsingException
                                Right msg -> do
                                    return (Just msg, Nothing)

doVersionHandshake :: (HasBitcoinP2PEnv m, MonadIO m) => Network -> Socket -> SockAddr -> m (Bool)
doVersionHandshake net sock sa = do
    p2pEnv <- getBitcoinP2PEnv
    let lg = logger p2pEnv
    g <- liftIO $ getStdGen
    now <- round <$> liftIO getPOSIXTime
    myaddr <-
        liftIO $ head <$> getAddrInfo (Just defaultHints {addrSocketType = Stream}) (Just "192.168.0.106") (Just "3000")
    let nonce = fst (random g :: (Word64, StdGen))
        ad = NetworkAddress 0 $ addrAddress myaddr -- (SockAddrInet 0 0)
        bb = 1 :: Word32 -- ### TODO: getBestBlock ###
        rmt = NetworkAddress 0 sa
        ver = buildVersion net nonce bb ad rmt now
        em = runPut . putMessage net $ (MVersion ver)
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
                    debug lg $ msg $ val "Error, unexpected message (2) during handshake"
                    return False
        __ -> do
            debug lg $ msg $ val "Error, unexpected message (1) during handshake"
            return False

messageHandler :: (HasService env m) => BitcoinPeer -> (Maybe Message, Maybe IngressStreamState) -> m (MessageCommand)
messageHandler peer (mm, ingss) = do
    bp2pEnv <- getBitcoinP2PEnv
    let lg = logger bp2pEnv
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
                        Left e -> debug lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e) >> throw e
                    liftIO $ putMVar (headersWriteLock bp2pEnv) True
                    return $ msgType msg
                MInv inv -> do
                    let lst = invList inv
                    mapM_
                        (\x ->
                             if (invType x) == InvBlock
                                 then do
                                     debug lg $ LG.msg ("Unsolicited INV, a new Block: " ++ (show $ invHash x))
                                     liftIO $ putMVar (bestBlockUpdated bp2pEnv) True -- will trigger a GetHeaders to peers
                                 else return ())
                        lst
                    return $ msgType msg
                MTx tx -> do
                    case ingss of
                        Just iss -> do
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
                                        Left e ->
                                            debug lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e) >> throw e
                                    return $ msgType msg
                                Nothing -> throw InvalidStreamStateException
                        Nothing -> do
                            debug lg $ LG.msg $ val ("[???] Unconfirmed Tx ")
                            return $ msgType msg
                MBlock blk -> do
                    res <- LE.try $ processBlock blk
                    case res of
                        Right () -> return ()
                        Left BlockHashNotFoundException -> return ()
                        Left EmptyHeadersMessageException -> return ()
                        Left e -> debug lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e) >> throw e
                    return $ msgType msg
                _ -> do
                    return $ msgType msg
        Nothing -> do
            debug lg $ LG.msg $ val "Error, invalid message"
            throw InvalidMessageTypeException

readNextMessage' :: (HasService env m) => BitcoinPeer -> m ((Maybe Message, Maybe IngressStreamState))
readNextMessage' peer = do
    bp2pEnv <- getBitcoinP2PEnv
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    case bpSocket peer of
        Just sock -> do
            liftIO $ takeMVar $ bpReadMsgLock peer
            !prevIngressState <- liftIO $ readTVarIO $ bpIngressState peer
            (msg, ingressState) <- readNextMessage net sock prevIngressState
            case ingressState of
                Just iss -> do
                    mp <- liftIO $ readTVarIO $ blockSyncStatusMap bp2pEnv
                    let ingst = issBlockIngest iss
                    case msg of
                        Just (MBlock blk) -- setup state
                         -> do
                            let hh = headerHash $ defBlockHeader blk
                                ht =
                                    case (M.lookup hh mp) of
                                        Just (_, h) -> h
                                        Nothing -> throw InvalidBlockSyncStatusMapException
                                !iz = Just (IngressStreamState ingst $ Just $ BlockInfo hh ht)
                            liftIO $ atomically $ writeTVar (bpIngressState peer) $ iz
                        Just (MTx ctx) -> do
                            if binTxTotalCount ingst == binTxProcessed ingst
                                then do
                                    case issBlockInfo iss of
                                        Just bi ->
                                            liftIO $
                                            atomically $
                                            modifyTVar'
                                                (blockSyncStatusMap bp2pEnv)
                                                (M.insert (biBlockHash bi) $ (BlockReceived, biBlockHeight bi)) -- mark block received
                                        Nothing -> throw InvalidBlockInfoException
                                    liftIO $ atomically $ writeTVar (bpIngressState peer) $ Nothing -- reset state
                                else liftIO $ atomically $ writeTVar (bpIngressState peer) $ ingressState -- retain state
                        otherwise -> throw UnexpectedDuringBlockProcException
                Nothing -> return ()
            liftIO $ putMVar (bpReadMsgLock peer) True
            return (msg, ingressState)
        Nothing -> throw PeerSocketNotConnectedException

initPeerListeners :: (HasService env m) => m ()
initPeerListeners = do
    bp2pEnv <- getBitcoinP2PEnv
    allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    let conpr = L.filter (\x -> bpConnected (snd x)) (M.toList allpr)
    mapM_ (\pr -> LA.async $ handleIncomingMessages $ snd pr) conpr
    return ()

handleIncomingMessages :: (HasService env m) => BitcoinPeer -> m ()
handleIncomingMessages pr = do
    bp2pEnv <- getBitcoinP2PEnv
    let lg = logger bp2pEnv
    debug lg $ msg $ (val "reading from: ") +++ show (bpAddress pr)
    res <-
        LE.try $
        runStream $ asyncly $ S.repeatM (readNextMessage' pr) & S.mapM (messageHandler pr) & S.mapM (logMessage)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> do
            debug lg $ msg $ (val "[ERROR] handleIncomingMessages ") +++ (show e)
            return ()
