{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.BlockSync
    ( createSocket
    ) where

import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import qualified Data.ByteString.Lazy as BSL
import Data.Int
import Data.Serialize
import Data.String.Conversions
import Data.Text (Text)
import Data.Word
import Network.Socket
import qualified Network.Socket.ByteString.Lazy as N (sendAll)
import Network.Xoken.Constants
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Types
import Network.Xoken.P2P.Common

-- import Data.ByteString (ByteString)
-- import qualified Data.ByteString as B
--
-- | Eg: createSocket "127.0.0.1" 3000 TCP
createSocket :: String -> Int -> IO Socket
createSocket = createSocketWithOptions []

createSocketWithOptions :: [SocketOption] -> String -> Int -> IO Socket
createSocketWithOptions options ip port =
    withSocketsDo $ do
        let portNo = Just (show port)
        let transport_type = Stream
        let hints = defaultHints {addrSocketType = transport_type}
        addr:_ <- getAddrInfo (Just hints) (Just ip) portNo
        sock <- socket AF_INET transport_type (addrProtocol addr)
        mapM_ (\option -> when (isSupportedSocketOption option) (setSocketOption sock option 1)) options
        connect sock (addrAddress addr)
        return sock

sendMessage :: MVar Bool -> Socket -> BSL.ByteString -> IO ()
sendMessage writeLock sock msg = do
    a <- takeMVar writeLock
    N.sendAll sock msg
    putMVar writeLock a

setupConnectionPeer :: (HasService env m) => SockAddr -> m ()
setupConnectionPeer sa = do
    bp2pEnv <- asks getBitcoinP2PEnv
    let bnConfig = bitcoinNodeConfig bp2pEnv
    liftIO $ print ("setupConnectionPeer: ")
    let x = bncNet bnConfig
    -- ManagerConfig {mgrConfNetAddr = ad, mgrConfNetwork = net} <- asks myConfig
    -- let ad = NetworkAddress 0 (SockAddrInet 0 0)
    -- nonce <- liftIO randomIO
    -- bb <- 0 :: Word32 -- getBestBlock
    -- now <- round <$> liftIO getPOSIXTime
    -- let rmt = NetworkAddress (srv net) sa
    --     ver = buildVersion (bncNet bnConfig) nonce bb ad rmt now
    -- b <- asks onlinePeers
    -- _ <- atomically $ newOnlinePeer b sa nonce p a
    return ()
