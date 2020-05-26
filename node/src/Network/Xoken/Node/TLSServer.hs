{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.TLSServer
    ( module Network.Xoken.Node.TLSServer
    ) where

import Arivi.P2P.P2PEnv
import Arivi.P2P.RPC.Fetch
import Arivi.P2P.Types
import Codec.Serialise
import Control.Concurrent.Async.Lifted (async)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Loops
import Data.Aeson as A
import Data.Binary as DB
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LBS
import Data.IORef
import Data.Int
import qualified Data.Map.Strict as M
import Data.Serialize
import Data.Text as T
import Data.X509.CertificateStore
import GHC.Generics
import qualified Network.Simple.TCP.TLS as TLS
import Network.Socket
import qualified Network.TLS as NTLS
import Network.Xoken.Node.Data
import Network.Xoken.Node.Env as NEnv
import Network.Xoken.Node.XokenService
import Text.Printf
import Xoken.NodeConfig

data TLSEndpointServiceHandler =
    TLSEndpointServiceHandler
        { connQueue :: TQueue EndPointConnection
        }

data EndPointConnection =
    EndPointConnection
        { requestQueue :: TQueue XDataReq
        , respWriteLock :: MVar TLS.Context
        }

newTLSEndpointServiceHandler :: IO TLSEndpointServiceHandler
newTLSEndpointServiceHandler = do
    conQ <- atomically $ newTQueue
    return $ TLSEndpointServiceHandler conQ

newEndPointConnection :: IO EndPointConnection
newEndPointConnection = do
    reqQueue <- atomically $ newTQueue
    resLock <- newEmptyMVar
    return $ EndPointConnection reqQueue resLock

handleRPCReqResp :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => MVar TLS.Context -> Int -> RPCMessage -> m ()
handleRPCReqResp sockMVar mid encReq = do
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    liftIO $ printf "handleRPCReqResp(%d, %s)\n" mid (show encReq)
    rpcResp <- goGetResource encReq net
    let body = serialise $ XDataRPCResp (mid) (rsStatusCode rpcResp) (rsStatusMessage rpcResp) (rsBody rpcResp)
    connSock <- liftIO $ takeMVar sockMVar
    NTLS.sendData connSock $ DB.encode (Prelude.fromIntegral (LBS.length body) :: Int16)
    NTLS.sendData connSock body
    liftIO $ putMVar sockMVar connSock

handleNewConnectionRequest :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => TLSEndpointServiceHandler -> m ()
handleNewConnectionRequest handler = do
    continue <- liftIO $ newIORef True
    whileM_ (liftIO $ readIORef continue) $ do
        liftIO $ printf "handleNewConnectionRequest\n"
        epConn <- liftIO $ atomically $ readTQueue $ connQueue handler
        async $ handleRequest epConn
        return ()

handleRequest :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => EndPointConnection -> m ()
handleRequest handler = do
    continue <- liftIO $ newIORef True
    whileM_ (liftIO $ readIORef continue) $ do
        liftIO $ printf "handleRequest\n"
        xdReq <- liftIO $ atomically $ readTQueue $ requestQueue handler
        case xdReq of
            XDataRPCReq mid met par -> do
                liftIO $ printf "Decoded (%s)\n" (show met)
                let req = RPCRequest met par
                async (handleRPCReqResp (respWriteLock handler) mid req)
                return ()
            XCloseConnection -> do
                liftIO $ writeIORef continue False

enqueueRequest :: EndPointConnection -> LBS.ByteString -> IO ()
enqueueRequest epConn req = do
    let xdReq = deserialise req :: XDataReq
    atomically $ writeTQueue (requestQueue epConn) xdReq

handleConnection :: EndPointConnection -> TLS.Context -> IO ()
handleConnection epConn context = do
    continue <- liftIO $ newIORef True
    whileM_ (liftIO $ readIORef continue) $ do
        res <- try $ TLS.recv context
        case res of
            Right r ->
                case r of
                    Just l -> enqueueRequest epConn (LBS.fromStrict l)
                    Nothing -> putStrLn "Payload read error"
            Left (e :: IOException) -> do
                putStrLn "Connection closed."
                atomically $ writeTQueue (requestQueue epConn) XCloseConnection
                writeIORef continue False

startTLSEndpoint :: TLSEndpointServiceHandler -> String -> PortNumber -> [FilePath] -> IO ()
startTLSEndpoint handler listenIP listenPort [certFilePath, keyFilePath, certStoreFilePath] = do
    putStrLn $ "Starting TLS Endpoint"
    credentials <- NTLS.credentialLoadX509 certFilePath keyFilePath
    case credentials of
        Right cred
            -- cstore <- readCertificateStore mStoreFilePath
         -> do
            let settings = TLS.makeServerSettings cred Nothing
            TLS.serve settings (TLS.Host listenIP) (show listenPort) $ \(context, sockAddr) -> do
                putStrLn $ "client connection established : " ++ show sockAddr
                epConn <- newEndPointConnection
                liftIO $ atomically $ writeTQueue (connQueue handler) epConn
                handleConnection epConn context
        Left err -> do
            putStrLn $ "Unable to read credentials from file"
            error "BadCredentialFile"
