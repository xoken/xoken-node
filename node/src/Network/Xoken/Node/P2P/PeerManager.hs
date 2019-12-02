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
    res <-
        liftIO $
        mapConcurrently
            (\y -> do
                 sock <- createSocket y
                 case sock of
                     Just sx -> do
                         fl <- doVersionHandshake net sx $ addrAddress y
                         mv <- newMVar True
                         bi <- newTVarIO Nothing
                         let bp = BitcoinPeer (addrAddress y) sock mv fl Nothing 99999 Nothing bi
                         atomically $ modifyTVar (bitcoinPeers bp2pEnv) (M.insert (addrAddress y) bp)
                     Nothing -> do
                         mv <- newMVar True
                         bi <- newTVarIO Nothing
                         let bp = BitcoinPeer (addrAddress y) sock mv False Nothing 99999 Nothing bi
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
                Left (e :: IOException) -> liftIO $ print ("socket read fail: " ++ show e) >>= throw SocketReadFailure
                Right msg ->
                    if B.length msg == len
                        then return msg
                        else B.append msg <$> recvAll sock (len - B.length msg)
        else return (B.empty)

readNextMessage :: Network -> Socket -> Maybe BlockIngestState -> IO ((Maybe Message, Maybe BlockIngestState))
readNextMessage net sock blkIng = do
    case blkIng of
        Just blin -> do
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
                Left e -> throw ConfirmedTxParseException
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
                                        , blockHash = blockHash blin
                                        , checksum = checksum blin
                                        }
                            return (Just $ MTx t, Just bio)
                        Nothing -> throw ConfirmedTxParseException
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
                                                        , blockHash = headerHash $ defBlockHeader b
                                                        , checksum = cks
                                                        }
                                            return (Just $ MBlock b, Just bi)
                                        Nothing -> throw DeflatedBlockParseError
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

messageHandler :: (HasService env m) => (Maybe Message, Maybe BlockIngestState) -> m (MessageCommand)
messageHandler (mm, blkIng) = do
    bp2pEnv <- asks getBitcoinP2PEnv
    case mm of
        Just msg -> do
            case (msg) of
                MHeaders hdrs -> do
                    liftIO $ takeMVar (headersWriteLock bp2pEnv)
                    res <- LE.try $ processHeaders hdrs
                    case res of
                        Right () -> return ()
                        Left BlockHashNotFoundInDB -> return ()
                        Left EmptyHeadersMessage -> return ()
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
                    case blkIng of
                        Just bi -> do
                            res <- LE.try $ processConfTransaction tx (blockHash bi) (txProcessed bi)
                            case res of
                                Right () -> return ()
                                Left BlockHashNotFoundInDB -> return ()
                                Left EmptyHeadersMessage -> return ()
                                Left e -> liftIO $ print ("[ERROR] Unhandled exception!" ++ show e) >> throw e
                            return $ msgType msg
                        Nothing -> do
                            liftIO $ print ("[???] Unconfirmed Tx ")
                            return $ msgType msg
                MBlock blk -> do
                    res <- LE.try $ processBlock blk
                    case res of
                        Right () -> return ()
                        Left BlockHashNotFoundInDB -> return ()
                        Left EmptyHeadersMessage -> return ()
                        Left e -> liftIO $ print ("[ERROR] Unhandled exception!" ++ show e) >> throw e
                    return $ msgType msg
                _ -> do
                    return $ msgType msg
        Nothing -> do
            liftIO $ print "Error, invalid message"
            throw InvalidMessageType

readNextMessage' :: (HasService env m) => Network -> BitcoinPeer -> m ((Maybe Message, Maybe BlockIngestState))
readNextMessage' net peer = do
    case bpSocket peer of
        Just sock -> do
            ingSt <- liftIO $ readTVarIO (bpBlockIngest peer)
            (msg, blkIng) <- liftIO $ readNextMessage net sock ingSt
            case blkIng of
                Just b -> do
                    let up =
                            if txTotalCount b == txProcessed b
                                then Nothing
                                else blkIng
                    liftIO $ atomically $ writeTVar (bpBlockIngest peer) up
                    liftIO $ print ("writing tvar " ++ show up)
                Nothing -> return ()
            return (msg, blkIng)
        Nothing -> throw PeerSocketNotConnected

initPeerListeners :: (HasService env m) => m ()
initPeerListeners = do
    bp2pEnv <- asks getBitcoinP2PEnv
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    let conpr = L.filter (\x -> bpConnected (snd x)) (M.toList allpr)
    mapM_ (\pr -> LA.async $ handleIncomingMessages $ snd pr) conpr
    return ()

handleIncomingMessages :: (HasService env m) => BitcoinPeer -> m ()
handleIncomingMessages pr = do
    bp2pEnv <- asks getBitcoinP2PEnv
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    liftIO $ print ("reading from:  " ++ show (bpAddress pr))
    res <- LE.try $ runStream $ S.repeatM (readNextMessage' net pr) & S.mapM (messageHandler) & S.mapM (logMessage)
    case res of
        Right () -> return ()
        Left (e :: SomeException) -> liftIO $ print ("[ERROR] handleIncomingMessages " ++ show e) >> return ()
