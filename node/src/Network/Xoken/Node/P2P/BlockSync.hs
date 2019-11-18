{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.BlockSync
    ( createSocket
    , setupPeerConnection
    ) where

import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BSL
import Data.Int
import Data.Maybe
import Data.Serialize
import Data.String.Conversions
import Data.Text (Text)
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import Network.Socket
import qualified Network.Socket.ByteString as S (recv)
import qualified Network.Socket.ByteString.Lazy as N (recv, sendAll)
import Network.Xoken.Constants

import Control.Concurrent.Async
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Types
import Network.Xoken.P2P.Common
import System.Random

--
-- | Eg: createSocket "127.0.0.1" 3000 TCP
createSocket :: AddrInfo -> IO Socket
createSocket = createSocketWithOptions []

createSocketWithOptions :: [SocketOption] -> AddrInfo -> IO Socket
createSocketWithOptions options addr = do
    liftIO $ print ("createSocketWithOptions: ")
    sock <- socket AF_INET Stream (addrProtocol addr)
    mapM_ (\option -> when (isSupportedSocketOption option) (setSocketOption sock option 1)) options
    connect sock $ addrAddress addr
    return sock

sendEncMessage :: MVar Bool -> Socket -> BSL.ByteString -> IO ()
sendEncMessage writeLock sock msg = do
    a <- takeMVar writeLock
    N.sendAll sock msg
    putMVar writeLock a

setupPeerConnection :: (HasService env m) => m ()
setupPeerConnection = do
    liftIO $ print ("setupPeerConnection: ")
    bp2pEnv <- asks getBitcoinP2PEnv
    let bnConfig = bitcoinNodeConfig bp2pEnv
        net = bncNet bnConfig
        seeds = getSeeds net
        hints = defaultHints {addrSocketType = Stream}
        port = getDefaultPort net
    liftIO $ print (show seeds)
    let sd = map (\x -> Just (x :: HostName)) seeds
    addrs <- liftIO $ mapM (\x -> head <$> getAddrInfo (Just hints) (x) (Just (show port))) sd
    liftIO $ print (show addrs)
    liftIO $
        mapM_
            (\y ->
                 async
                     (do sock <- createSocket y
                         doVersionHandshake bnConfig sock $ addrAddress y))
            addrs

readNextMessage :: Socket -> Network -> IO (Maybe MessageCommand)
readNextMessage sock net = do
    hdr <- S.recv sock 24
    print ("recieved (header): " ++ show hdr)
    case (decode hdr) of
        Left _ -> do
            print "Error decoding incoming message header"
            return Nothing
        Right (MessageHeader _ _ len _) -> do
            print ("length: " ++ show len)
            byts <-
                if len == 0
                    then return hdr
                    else do
                        y <- S.recv sock (fromIntegral len)
                        print ("recieved ....... " ++ show y)
                        return $ hdr `B.append` y
            case runGet (getMessage net) $ byts of
                Left e -> do
                    print "Error, unexpected message header"
                    return Nothing
                Right msg -> return $ Just (msgType msg)

doVersionHandshake :: BitcoinNodeConfig -> Socket -> SockAddr -> IO (Bool)
doVersionHandshake bnConfig sock sa = do
    liftIO $ print ("doVersionHandshake: ")
    g <- liftIO $ getStdGen
    now <- round <$> liftIO getPOSIXTime
    myaddr <- head <$> getAddrInfo (Just defaultHints {addrSocketType = Stream}) (Just "192.168.0.106") (Just "3000")
    let nonce = fst (random g :: (Word64, StdGen))
        ad = NetworkAddress 0 $ addrAddress myaddr -- (SockAddrInet 0 0)
        bb = 1 :: Word32 -- ### TODO: getBestBlock ###
        rmt = NetworkAddress 0 sa
        net = bncNet bnConfig
        ver = buildVersion net nonce bb ad rmt now
        em = runPut . putMessage net $ (MVersion ver)
    mv <- (newMVar True)
    print (show em)
    sendEncMessage mv sock (BSL.fromStrict em)
    hs1 <- readNextMessage sock net
    case hs1 of
        Just MCVersion -> do
            print "Got [Version]"
            hs2 <- readNextMessage sock net
            case hs2 of
                Just MCVerAck -> do
                    print "Got [VerAck]"
                    let em2 = runPut . putMessage net $ (MVerAck)
                    sendEncMessage mv sock (BSL.fromStrict em2)
                    return True
                __ -> do
                    print "Error, unexpected message (2) during handshake"
                    return False
        __ -> do
            print "Error, unexpected message (1) during handshake"
            return False
        -- Right (MessageHeader _ cmd len _) -> do
        --     case cmd of
        --         MCVersion -> do
        --             print "Got [Version]"
        --         MCVerAck -> do
        --             print "Got [VerAck]"
        --             return True
        --         __ -> do
        --             print "Error, unexpected message header"
        --             return False
