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
import Data.Functor (($>))
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

data EncodingFormat
    = CBOR
    | JSON
    | DEFAULT

data TLSEndpointServiceHandler =
    TLSEndpointServiceHandler
        { connQueue :: TQueue EndPointConnection
        }

data EndPointConnection =
    EndPointConnection
        { requestQueue :: TQueue XDataReq
        , respWriteLock :: MVar TLS.Context
        , encodingFormat :: IORef EncodingFormat
        }

newTLSEndpointServiceHandler :: IO TLSEndpointServiceHandler
newTLSEndpointServiceHandler = do
    conQ <- atomically $ newTQueue
    return $ TLSEndpointServiceHandler conQ

newEndPointConnection :: TLS.Context -> IO EndPointConnection
newEndPointConnection context = do
    reqQueue <- atomically $ newTQueue
    resLock <- newMVar context
    formatRef <- newIORef DEFAULT
    return $ EndPointConnection reqQueue resLock formatRef

handleRPCReqResp ::
       (HasXokenNodeEnv env m, HasLogger m, MonadIO m)
    => MVar TLS.Context
    -> EncodingFormat
    -> Int
    -> RPCMessage
    -> m ()
handleRPCReqResp sockMVar format mid encReq = do
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    liftIO $ printf "handleRPCReqResp (%d, %s)\n" mid (show encReq)
    rpcResp <- goGetResource encReq net
    let resp = XDataRPCResp (mid) (rsStatusCode rpcResp) (rsStatusMessage rpcResp) (rsBody rpcResp)
    let body =
            case format of
                CBOR -> serialise resp
                JSON -> A.encode resp
    connSock <- liftIO $ takeMVar sockMVar
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
handleRequest epConn = do
    continue <- liftIO $ newIORef True
    whileM_ (liftIO $ readIORef continue) $ do
        liftIO $ printf "handleRequest\n"
        xdReq <- liftIO $ atomically $ readTQueue $ requestQueue epConn
        case xdReq of
            XDataRPCReq mid met par -> do
                liftIO $ printf "Decoded (%s)\n" (show met)
                let req = RPCRequest met par
                format <- liftIO $ readIORef (encodingFormat epConn)
                async (handleRPCReqResp (respWriteLock epConn) format mid req)
                return ()
            XCloseConnection -> do
                liftIO $ writeIORef continue False

enqueueRequest :: EndPointConnection -> LBS.ByteString -> IORef Bool -> IO ()
enqueueRequest epConn req continue = do
    format <- readIORef (encodingFormat epConn)
    xdReq <-
        case format of
            DEFAULT -> do
                case eitherDecode req of
                    Right x -> writeIORef (encodingFormat epConn) JSON $> x
                    Left err -> do
                        print $ "decode failed" <> show err
                        writeIORef (encodingFormat epConn) CBOR $> deserialise req
            JSON -> do
                writeIORef (encodingFormat epConn) JSON
                case eitherDecode req of
                    Right x -> pure x
                    Left err -> do
                        print $ "[Error] Decode failed: " <> show err
                        print $ "Closing Connection"
                        writeIORef continue False
                        return XCloseConnection
            CBOR -> writeIORef (encodingFormat epConn) CBOR $> deserialise req
    atomically $ writeTQueue (requestQueue epConn) xdReq

handleConnection :: EndPointConnection -> TLS.Context -> IO ()
handleConnection epConn context = do
    continue <- liftIO $ newIORef True
    whileM_ (liftIO $ readIORef continue) $ do
        res <- try $ TLS.recv context
        case res of
            Right r ->
                case r of
                    Just l -> enqueueRequest epConn (LBS.fromStrict l) continue
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
                epConn <- newEndPointConnection context
                liftIO $ atomically $ writeTQueue (connQueue handler) epConn
                handleConnection epConn context
        Left err -> do
            putStrLn $ "Unable to read credentials from file"
            error "BadCredentialFile"
