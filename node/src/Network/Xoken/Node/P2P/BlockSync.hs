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
import qualified Network.Socket.ByteString.Lazy as N (sendAll)
import Network.Xoken.Constants
import Network.Xoken.Network.Common
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
    sock <- socket AF_INET Stream (addrProtocol addr)
    mapM_ (\option -> when (isSupportedSocketOption option) (setSocketOption sock option 1)) options
    connect sock (addrAddress addr)
    return sock

sendEncMessage :: MVar Bool -> Socket -> BSL.ByteString -> IO ()
sendEncMessage writeLock sock msg = do
    a <- takeMVar writeLock
    N.sendAll sock msg
    putMVar writeLock a

setupPeerConnection :: (HasService env m) => m ()
setupPeerConnection = do
    bp2pEnv <- asks getBitcoinP2PEnv
    let bnConfig = bitcoinNodeConfig bp2pEnv
        net = bncNet bnConfig
        seeds = getSeeds net
        hints = defaultHints {addrSocketType = Stream}
        port = getDefaultPort net
    let sd = map (\x -> Just (x :: HostName)) seeds
    addrs <- liftIO $ mapM (\x -> head <$> getAddrInfo (Just hints) (x) (Just (show port))) sd
    liftIO $
        mapM_
            (\y -> do
                 z <- (createSocket y)
                 doVersionHandshake bnConfig z (addrAddress y))
            addrs

doVersionHandshake :: BitcoinNodeConfig -> Socket -> SockAddr -> IO ()
doVersionHandshake bnConfig sock sa = do
    liftIO $ print ("setupPeerConnection: ")
    g <- liftIO $ getStdGen
    now <- round <$> liftIO getPOSIXTime
    let nonce = fst (random g :: (Word64, StdGen))
        ad = NetworkAddress 0 (SockAddrInet 0 0)
        bb = 0 :: Word32 -- ### TODO: getBestBlock ###
        rmt = NetworkAddress 0 sa
        ver = buildVersion (bncNet bnConfig) nonce bb ad rmt now
        em = encode ver
    mv <- (newMVar True)
    sendEncMessage mv sock (BSL.fromStrict em)
    return ()
