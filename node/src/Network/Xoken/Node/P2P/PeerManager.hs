{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
import Network.Xoken.Network.Common -- (GetData(..), MessageCommand(..), NetworkAddress(..))
import Network.Xoken.Network.Message
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.ChainSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
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
        Left (e :: IOException) -> do
            print ("TCP socket connect fail: " ++ show (addrAddress addr))
            return Nothing

setupPeerConnection :: (HasService env m) => m ()
setupPeerConnection = do
    bp2pEnv <- asks getBitcoinP2PEnv
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
        seeds = getSeeds net
        hints = defaultHints {addrSocketType = Stream}
        port = getDefaultPort net
    liftIO $ print (show seeds)
    let sd = map (\x -> Just (x :: HostName)) seeds
    addrs <- liftIO $ mapConcurrently (\x -> head <$> getAddrInfo (Just hints) (x) (Just (show port))) sd
    ss <- liftIO $ newTVarIO Nothing
    res <-
        liftIO $
        mapConcurrently
            (\y -> do
                 sock <- createSocket y
                 case sock of
                     Just sx -> do
                         fl <- doVersionHandshake net sx $ addrAddress y
                         mv <- newMVar True
                         let bp = BitcoinPeer (addrAddress y) sock mv fl Nothing 99999 Nothing ss
                         atomically $ modifyTVar (bitcoinPeers bp2pEnv) (M.insert (addrAddress y) bp)
                     Nothing -> do
                         mv <- newMVar True
                         let bp = BitcoinPeer (addrAddress y) sock mv False Nothing 99999 Nothing ss
                         atomically $ modifyTVar (bitcoinPeers bp2pEnv) (M.insert (addrAddress y) bp))
            addrs
    return ()

-- Helper Functions
recvAll :: Socket -> Int -> IO B.ByteString
recvAll sock len = do
    if len > 0
        then do
            res <- try $ SB.recv sock len
            case res of
                Left (e :: IOException) -> liftIO $ print ("socket read fail: " ++ show e) >>= throw SocketReadException
                Right msg ->
                    if B.length msg == len
                        then return msg
                        else B.append msg <$> recvAll sock (len - B.length msg)
        else return (B.empty)

readNextMessage :: Network -> Socket -> Maybe IngressStreamState -> IO ((Maybe Message, Maybe IngressStreamState))
readNextMessage net sock ingss = do
    case ingss of
        Just iss -> do
            let blin = blockIngest iss
            liftIO $ print ("BlockIngestState parsing tx: " ++ show blin)
            let maxChunk = (1024 * 100) - (B.length $ unspentBytes blin)
                len =
                    if txPayloadLeft blin < maxChunk
                        then (txPayloadLeft blin) - (B.length $ unspentBytes blin)
                        else maxChunk
            liftIO $ print (" | Tx payload left " ++ show (txPayloadLeft blin))
            liftIO $ print (" | Bytes prev unspent " ++ show (B.length $ unspentBytes blin))
            liftIO $ print (" | Bytes to read " ++ show len)
            nbyt <- recvAll sock len
            let txbyt = (unspentBytes blin) `B.append` nbyt
            case runGetState (getConfirmedTx) txbyt 0 of
                Left e -> liftIO $ print (txbyt) >>= throw ConfirmedTxParseException
                Right (tx, unused) -> do
                    case tx of
                        Just t -> do
                            liftIO $ print ("Conf.Tx: " ++ show tx ++ " unused: " ++ show (B.length unused))
                            let bio =
                                    BlockIngestState
                                        { unspentBytes = unused
                                        , txPayloadLeft = txPayloadLeft blin - (B.length txbyt - B.length unused)
                                        , txTotalCount = txTotalCount blin
                                        , txProcessed = 1 + txProcessed blin
                                        , checksum = checksum blin
                                        }
                            return (Just $ MTx t, Just $ IngressStreamState bio $ blockInfo iss)
                        Nothing -> liftIO $ print (txbyt) >>= throw ConfirmedTxParseException
        Nothing -> do
            hdr <- recvAll sock 24
            case (decode hdr) of
                Left e -> do
                    liftIO $ print ("Error decoding incoming message header: " ++ e)
                    throw MessageParsingException
                Right (MessageHeader _ cmd len cks) -> do
                    if cmd == MCBlock
                        then do
                            byts <- recvAll sock (88) -- 80 byte Block header + VarInt (max 8 bytes) Tx count
                            case runGetState (getDeflatedBlock) byts 0 of
                                Left e -> do
                                    liftIO $ print ("Error, unexpected message header: " ++ e)
                                    throw MessageParsingException
                                Right (blk, unused) -> do
                                    case blk of
                                        Just b -> do
                                            liftIO $
                                                print
                                                    ("DefBlock: " ++ show blk ++ " unused: " ++ show (B.length unused))
                                            let bi =
                                                    BlockIngestState
                                                        { unspentBytes = unused
                                                        , txPayloadLeft = fromIntegral (len) - (88 - B.length unused)
                                                        , txTotalCount = fromIntegral $ txnCount b
                                                        , txProcessed = 0
                                                        , checksum = cks
                                                        }
                                            return (Just $ MBlock b, Just $ IngressStreamState bi Nothing)
                                        Nothing -> throw DeflatedBlockParseException
                        else do
                            byts <-
                                if len == 0
                                    then return hdr
                                    else do
                                        b <- recvAll sock (fromIntegral len)
                                        return (hdr `B.append` b)
                            case runGet (getMessage net) byts of
                                Left e -> do
                                    liftIO $ do
                                        print "Error, unexpected message hdr: "
                                        throw MessageParsingException
                                Right msg -> do
                                    return (Just msg, Nothing)

doVersionHandshake :: Network -> Socket -> SockAddr -> IO (Bool)
doVersionHandshake net sock sa = do
    g <- liftIO $ getStdGen
    now <- round <$> liftIO getPOSIXTime
    myaddr <- head <$> getAddrInfo (Just defaultHints {addrSocketType = Stream}) (Just "192.168.0.106") (Just "3000")
    let nonce = fst (random g :: (Word64, StdGen))
        ad = NetworkAddress 0 $ addrAddress myaddr -- (SockAddrInet 0 0)
        bb = 1 :: Word32 -- ### TODO: getBestBlock ###
        rmt = NetworkAddress 0 sa
        ver = buildVersion net nonce bb ad rmt now
        em = runPut . putMessage net $ (MVersion ver)
    mv <- (newMVar True)
    sendEncMessage mv sock (BSL.fromStrict em)
    (hs1, _) <- readNextMessage net sock Nothing
    case hs1 of
        Just (MVersion __) -> do
            (hs2, _) <- readNextMessage net sock Nothing
            case hs2 of
                Just MVerAck -> do
                    let em2 = runPut . putMessage net $ (MVerAck)
                    sendEncMessage mv sock (BSL.fromStrict em2)
                    print ("Version handshake complete: " ++ show sa)
                    return True
                __ -> do
                    print "Error, unexpected message (2) during handshake"
                    return False
        __ -> do
            print "Error, unexpected message (1) during handshake"
            return False

messageHandler :: (HasService env m) => BitcoinPeer -> (Maybe Message, Maybe IngressStreamState) -> m (MessageCommand)
messageHandler peer (mm, ingss) = do
    bp2pEnv <- asks getBitcoinP2PEnv
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
                        Left e -> liftIO $ print ("[ERROR] Unhandled exception!" ++ show e) >> throw e
                    liftIO $ putMVar (headersWriteLock bp2pEnv) True
                    return $ msgType msg
                MInv inv -> do
                    let lst = invList inv
                    mapM_
                        (\x ->
                             if (invType x) == InvBlock
                                 then do
                                     liftIO $ print ("Unsolicited INV, a new Block: " ++ (show $ invHash x))
                                     liftIO $ putMVar (bestBlockUpdated bp2pEnv) True -- will trigger a GetHeaders to peers
                                 else return ())
                        lst
                    return $ msgType msg
                MTx tx -> do
                    case ingss of
                        Just iss -> do
                            let bi = blockIngest iss
                            let binfo = blockInfo iss -- liftIO $ readTVarIO (blockInfo $ ingressStreamState peer)
                            case binfo of
                                Just bf -> do
                                    res <-
                                        LE.try $
                                        processConfTransaction
                                            tx
                                            (blockHash bf)
                                            (txProcessed bi)
                                            (fromIntegral $ blockHeight bf)
                                    case res of
                                        Right () -> return ()
                                        Left BlockHashNotFoundException -> return ()
                                        Left EmptyHeadersMessageException -> return ()
                                        Left e -> liftIO $ print ("[ERROR] Unhandled exception!" ++ show e) >> throw e
                                    return $ msgType msg
                                Nothing -> throw InvalidStreamStateException
                        Nothing -> do
                            liftIO $ print ("[???] Unconfirmed Tx ")
                            return $ msgType msg
                MBlock blk
                    -- ss <- liftIO $ readTVarIO $ ingressStreamState peer
                    -- headerHash $ defBlockHeader blk
                 -> do
                    res <- LE.try $ processBlock blk
                    case res of
                        Right () -> return ()
                        Left BlockHashNotFoundException -> return ()
                        Left EmptyHeadersMessageException -> return ()
                        Left e -> liftIO $ print ("[ERROR] Unhandled exception!" ++ show e) >> throw e
                    return $ msgType msg
                _ -> do
                    return $ msgType msg
        Nothing -> do
            liftIO $ print "Error, invalid message"
            throw InvalidMessageTypeException

readNextMessage' :: (HasService env m) => BitcoinPeer -> m ((Maybe Message, Maybe IngressStreamState))
readNextMessage' peer = do
    bp2pEnv <- asks getBitcoinP2PEnv
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    case bpSocket peer of
        Just sock -> do
            prevIngresState <- liftIO $ readTVarIO $ ingressStreamState peer
            (msg, ingressState) <- liftIO $ readNextMessage net sock prevIngresState
            case ingressState of
                Just iss -> do
                    mp <- liftIO $ readTVarIO $ blockSyncStatusMap bp2pEnv
                    let ingst = blockIngest iss
                    case msg of
                        Just (MBlock blk) -- setup state
                         -> do
                            let hh = headerHash $ defBlockHeader blk
                                ht =
                                    case (M.lookup hh mp) of
                                        Just (_, h) -> h
                                        Nothing -> throw InvalidBlockSyncStatusMapException
                            liftIO $
                                atomically $
                                writeTVar (ingressStreamState peer) $
                                Just (IngressStreamState ingst $ Just $ BlockInfo hh ht)
                        Just (MTx ctx) -- setup state
                         -> do
                            if txTotalCount ingst == txProcessed ingst
                                then do
                                    case blockInfo iss of
                                        Just bi ->
                                            liftIO $
                                            atomically $
                                            modifyTVar
                                                (blockSyncStatusMap bp2pEnv)
                                                (M.insert (blockHash bi) $ (BlockReceived, blockHeight bi)) -- mark block received
                                        Nothing -> throw InvalidBlockInfoException
                                    liftIO $ atomically $ writeTVar (ingressStreamState peer) $ Nothing -- reset state
                                else liftIO $ atomically $ writeTVar (ingressStreamState peer) $ ingressState -- retain state
                        Nothing -> throw InvalidDeflatedBlockException
                Nothing -> return ()
            return (msg, ingressState)
        Nothing -> throw PeerSocketNotConnectedException

initPeerListeners :: (HasService env m) => m ()
initPeerListeners = do
    bp2pEnv <- asks getBitcoinP2PEnv
    allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    let conpr = L.filter (\x -> bpConnected (snd x)) (M.toList allpr)
    mapM_ (\pr -> LA.async $ handleIncomingMessages $ snd pr) conpr
    return ()

handleIncomingMessages :: (HasService env m) => BitcoinPeer -> m ()
handleIncomingMessages pr = do
    bp2pEnv <- asks getBitcoinP2PEnv
    liftIO $ print ("reading from:  " ++ show (bpAddress pr))
    res <- LE.try $ runStream $ S.repeatM (readNextMessage' pr) & S.mapM (messageHandler pr) & S.mapM (logMessage)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> liftIO $ print ("[ERROR] handleIncomingMessages " ++ show e) >> return ()
