{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.P2P.BlockSync
    ( createSocket
    , setupPeerConnection
    , handleIncomingMessages
    , postRequestMessages
    ) where

import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import Data.ByteString.Short
import Data.Function ((&))
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Serialize
import Data.String.Conversions
import Data.Text (Text)
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
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
import Network.Xoken.Node.P2P.Types
import Network.Xoken.P2P.Common
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Random

--
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

sendEncMessage :: MVar Bool -> Socket -> BSL.ByteString -> IO ()
sendEncMessage writeLock sock msg = do
    a <- takeMVar writeLock
    (LB.sendAll sock msg) `catch` (\(e :: IOException) -> putStrLn ("caught: " ++ show e))
    putMVar writeLock a

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
                         let bp = BitcoinPeer (addrAddress y) sock mv fl Nothing 99999 Nothing
                         atomically $ modifyTVar (bitcoinPeers bp2pEnv) (M.insert (addrAddress y) bp)
                     Nothing -> do
                         mv <- newMVar True
                         let bp = BitcoinPeer (addrAddress y) sock mv False Nothing 99999 Nothing
                         atomically $ modifyTVar (bitcoinPeers bp2pEnv) (M.insert (addrAddress y) bp))
            addrs
    return ()

-- Helper Functions
recvAll :: Socket -> Int -> IO B.ByteString
recvAll sock len = do
    msg <- SB.recv sock len
    if B.length msg == len
        then return msg
        else B.append msg <$> recvAll sock (len - B.length msg)

readNextMessage :: Network -> Socket -> IO (Maybe Message)
readNextMessage net sock = do
    res <- liftIO $ try $ (SB.recv sock 24)
    case res of
        Right hdr -> do
            case (decode hdr) of
                Left e -> do
                    liftIO $ print ("Error decoding incoming message header: " ++ e)
                    return Nothing
                Right (MessageHeader _ _ len _) -> do
                    byts <-
                        if len == 0
                            then return hdr
                            else do
                                rs <- liftIO $ try $ (recvAll sock (fromIntegral len))
                                case rs of
                                    Left (e :: IOException) -> do
                                        liftIO $ print ("Error, reading: " ++ show e)
                                        throw e
                                    Right y -> do
                                        liftIO $ print (B.length y)
                                        return $ hdr `B.append` y
                    case runGet (getMessage net) $ byts of
                        Left e -> do
                            liftIO $ print ("Error, unexpected message header: " ++ e)
                            return Nothing
                        Right msg -> return $ Just msg
        Left (e :: IOException) -> do
            liftIO $ print ("socket read fail")
            return Nothing

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
    hs1 <- readNextMessage net sock
    case hs1 of
        Just (MVersion __) -> do
            hs2 <- readNextMessage net sock
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

postRequestMessages :: (HasService env m) => m ()
postRequestMessages = do
    bp2pEnv <- asks getBitcoinP2PEnv
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    let bl =
            GetHeaders
                { getHeadersVersion = myVersion
                , getHeadersBL = [headerHash $ getGenesisHeader net]
                , getHeadersHashStop = "0000000000000000000000000000000000000000000000000000000000000000"
                }
    liftIO $ print (headerHash $ getGenesisHeader net)
    liftIO $ print (myVersion)
    -- GetHeaders (shortBlockHash (headerHash (getGenesisHeader net)))
    allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    let conpr = L.filter (\x -> bpConnected (snd x)) (M.toList allpr)
    let pr = snd $ conpr !! 0
    case (bpSocket pr) of
        Just q -> do
            let em = runPut . putMessage net $ (MGetHeaders bl)
            liftIO $ sendEncMessage (bpSockLock pr) q (BSL.fromStrict em)
            liftIO $ print ("sending out get data: " ++ show (bpAddress pr))
        Nothing -> liftIO $ print ("Error sending, no connections available")
    return ()

messageHandler :: (HasService env m) => (Maybe Message) -> m (Maybe Message)
messageHandler mm = do
    case mm of
        Just msg -> do
            liftIO $ print (msgType msg)
            case (msgType msg) of
                MCGetHeaders -> do
                    return mm
                MCInv -> do
                    return mm
                MCTx -> do
                    return mm
                _ -> do
                    liftIO $ print "unknown type"
                    return Nothing
        Nothing -> do
            liftIO $ print "invalid message"
            return Nothing

readNextMessage' :: (HasService env m) => Network -> Socket -> m (Maybe Message)
readNextMessage' net sock = liftIO $ readNextMessage net sock

logMessage :: (HasService env m) => Message -> m ()
logMessage mg = do
    liftIO $ print (mg)
    return ()

handleIncomingMessages :: (HasService env m) => m ()
handleIncomingMessages = do
    bp2pEnv <- asks getBitcoinP2PEnv
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    let conpr = L.filter (\x -> bpConnected (snd x)) (M.toList allpr)
    let pr = snd $ conpr !! 0
    case (bpSocket pr) of
        Just s -> do
            liftIO $ print ("reading from:  " ++ show (bpAddress pr))
            runStream $ S.repeatM (readNextMessage' net s) & S.mapMaybeM (messageHandler) & S.mapM (logMessage)
                -- & S.mapM undefined
            undefined
        Nothing -> undefined
    return ()
