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
    ) where

import Control.Concurrent.Async (mapConcurrently)
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

-- import GHC.Exception.Type hiding (Exception)
import Control.Exception
import Network.Socket

import qualified Network.Socket.ByteString as S (recv)
import qualified Network.Socket.ByteString.Lazy as N (recv, sendAll)
import Network.Xoken.Constants
import Network.Xoken.Network.Common (MessageCommand(..), NetworkAddress(..))
import Network.Xoken.Network.Message
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Types
import Network.Xoken.P2P.Common
import System.Random

-- import qualified UnliftIO.Exception as UE (bracket, throwIO, try)
--
-- | Eg: createSocket "127.0.0.1" 3000 TCP
createSocket :: AddrInfo -> IO (Maybe Socket)
createSocket = createSocketWithOptions []

-- createSocketWithOptions :: [SocketOption] -> AddrInfo -> IO (Maybe Socket)
-- createSocketWithOptions options addr = do
--     sock <- socket AF_INET Stream (addrProtocol addr)
--     mapM_ (\option -> when (isSupportedSocketOption option) (setSocketOption sock option 1)) options
--     connect sock (addrAddress addr) `catch` (\(err :: IOException) -> putStrLn ("caught: " ++ show err))
--     return $ Just sock
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
    (N.sendAll sock msg) `catch` (\(e :: IOException) -> putStrLn ("caught: " ++ show e))
    putMVar writeLock a

setupPeerConnection :: (HasService env m) => m ()
setupPeerConnection = do
    bp2pEnv <- asks getBitcoinP2PEnv
    let bnConfig = bitcoinNodeConfig bp2pEnv
        net = bncNet bnConfig
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
                     Just sx -> doVersionHandshake bnConfig sx $ addrAddress y
                     Nothing -> return False)
            addrs
    return ()

readNextMessage :: Socket -> Network -> IO (Maybe MessageCommand)
readNextMessage sock net = do
    res <- try $ (S.recv sock 24)
    case res of
        Right hdr -> do
            case (decode hdr) of
                Left _ -> do
                    print "Error decoding incoming message header"
                    return Nothing
                Right (MessageHeader _ _ len _) -> do
                    byts <-
                        if len == 0
                            then return hdr
                            else do
                                rs <- try $ (S.recv sock (fromIntegral len))
                                case rs of
                                    Left (e :: IOException) -> throw e
                                    Right y -> return $ hdr `B.append` y
                    case runGet (getMessage net) $ byts of
                        Left e -> do
                            print "Error, unexpected message header"
                            return Nothing
                        Right msg -> return $ Just (msgType msg)
        Left (e :: IOException) -> do
            print ("socket read fail")
            return Nothing

doVersionHandshake :: BitcoinNodeConfig -> Socket -> SockAddr -> IO (Bool)
doVersionHandshake bnConfig sock sa = do
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
    sendEncMessage mv sock (BSL.fromStrict em)
    hs1 <- readNextMessage sock net
    case hs1 of
        Just MCVersion -> do
            hs2 <- readNextMessage sock net
            case hs2 of
                Just MCVerAck -> do
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
