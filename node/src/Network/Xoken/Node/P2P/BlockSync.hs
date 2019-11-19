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
import qualified Network.Socket.ByteString.Lazy as NB (recv, sendAll)
import Network.Xoken.Block.Common
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
    (NB.sendAll sock msg) `catch` (\(e :: IOException) -> putStrLn ("caught: " ++ show e))
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

readNextMessage :: Network -> Socket -> IO (Maybe Message)
readNextMessage net sock = do
    res <- try $ (SB.recv sock 24)
    case res of
        Right hdr -> do
            case (decode hdr) of
                Left _ -> do
                    print "Error decoding incoming message header"
                    print (show hdr)
                    return Nothing
                Right (MessageHeader _ _ len _) -> do
                    byts <-
                        if len == 0
                            then return hdr
                            else do
                                rs <- try $ (SB.recv sock (fromIntegral len))
                                case rs of
                                    Left (e :: IOException) -> throw e
                                    Right y -> return $ hdr `B.append` y
                    case runGet (getMessage net) $ byts of
                        Left e -> do
                            print "Error, unexpected message header"
                            return Nothing
                        Right msg -> return $ Just msg
        Left (e :: IOException) -> do
            print ("socket read fail")
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
    let hs =
            case net of
                bsv ->
                    [ InvVector
                          InvBlock
                          (getBlockHash "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048")
                    ]
                bsvTest ->
                    [ InvVector
                          InvBlock
                          (getBlockHash "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048")
                    ]
                bsvSTN ->
                    [ InvVector
                          InvBlock
                          (getBlockHash "00000000afa9bda45866dcd7627190ccde60327d0a30b28efd57facf684167cd")
                    ]
    allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    let conpr = L.filter (\x -> bpConnected (snd x)) (M.toList allpr)
    let pr = snd $ conpr !! 0
    case (bpSocket pr) of
        Just q -> do
            let em = runPut . putMessage net $ (MGetData $ GetData hs)
            liftIO $ sendEncMessage (bpSockLock pr) q (BSL.fromStrict em)
            liftIO $ print ("sending out get data: " ++ show (bpAddress pr))
        Nothing -> liftIO $ print ("Error sending, no connections available")
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
            liftIO $ print ("reading from: " ++ show (bpAddress pr))
            liftIO $ runStream $ S.repeatM (readNextMessage net s) & S.mapM (print)
                -- & S.mapM undefined
        Nothing -> undefined
    return ()
