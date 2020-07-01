{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}

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
import qualified Control.Exception.Lifted as LE (try)
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
import Data.Maybe
import Data.Serialize
import qualified Data.Serialize as S
import Data.Text as T
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.X509.CertificateStore
import qualified Database.CQL.IO as Q
import GHC.Generics
import qualified Network.Simple.TCP.TLS as TLS
import Network.Socket
import qualified Network.TLS as NTLS
import Network.Xoken.Node.Data
import Network.Xoken.Node.Env as NEnv
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.XokenService
import Prelude as P
import System.Logger as LG
import Text.Printf
import Xoken.NodeConfig

data TLSEndpointServiceHandler =
    TLSEndpointServiceHandler
        { connQueue :: TQueue EndPointConnection
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
    => EndPointConnection
    -> EncodingFormat
    -> Int
    -> Maybe String
    -> RPCMessage
    -> m ()
handleRPCReqResp epConn format mid version encReq = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    liftIO $ printf "handleRPCReqResp (%d, %s)\n" mid (show encReq)
    res <- LE.try $ delegateRequest encReq epConn net
    case res of
        Right rpcResp -> do
            let body =
                    case format of
                        CBOR ->
                            case rsResp rpcResp of
                                Left (RPCError err rsData) ->
                                    serialise $
                                    CBORRPCResponse
                                        (mid)
                                        (rsStatusCode rpcResp)
                                        (Just $ show err)
                                        Nothing
                                Right rsBody ->
                                    serialise $
                                    CBORRPCResponse
                                        (mid)
                                        (rsStatusCode rpcResp)
                                        Nothing
                                        (rsBody)
                        JSON ->
                            case rsResp rpcResp of
                                Left (RPCError err rsData) ->
                                    A.encode
                                        (JSONRPCErrorResponse
                                             mid
                                             (ErrorResponse (getJsonRPCErrorCode err) (show err) rsData)
                                             (fromJust version))
                                Right rsBody -> A.encode (JSONRPCSuccessResponse (fromJust version) (rsBody) mid)
            connSock <- liftIO $ takeMVar (context epConn)
            let prefixbody = LBS.append (DB.encode (fromIntegral (LBS.length body) :: Int32)) body
            NTLS.sendData connSock prefixbody
            liftIO $ putMVar (context epConn) connSock
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: goGetResource / authenticate " ++ show e

handleNewConnectionRequest :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => TLSEndpointServiceHandler -> m ()
handleNewConnectionRequest handler = do
    continue <- liftIO $ newIORef True
    whileM_ (liftIO $ readIORef continue) $ do
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
            XDataRPCReq mid met par version -> do
                liftIO $ printf "Decoded (%s)\n" (show met)
                let req = RPCRequest met par
                format <- liftIO $ readIORef (encodingFormat epConn)
                async (handleRPCReqResp epConn format mid version req)
                return ()
            XDataRPCBadRequest -> do
                format <- liftIO $ readIORef (encodingFormat epConn)
                let body =
                        case format of
                            CBOR -> serialise $ CBORRPCResponse (-1) 400 (Just "Invalid request") Nothing
                            _ ->
                                A.encode $
                                JSONRPCErrorResponse
                                    (-1)
                                    (ErrorResponse (getJsonRPCErrorCode INVALID_REQUEST) (show INVALID_REQUEST) Nothing)
                                    "2.0"
                connSock <- liftIO $ takeMVar (context epConn)
                let prefixbody = LBS.append (DB.encode (fromIntegral (LBS.length body) :: Int32)) body
                NTLS.sendData connSock prefixbody
                liftIO $ putMVar (context epConn) connSock
            XCloseConnection -> do
                liftIO $ writeIORef continue False

enqueueRequest :: EndPointConnection -> LBS.ByteString -> IO ()
enqueueRequest epConn req = do
    format <- readIORef (encodingFormat epConn)
    xdReq <-
        case format of
            DEFAULT -> do
                case eitherDecode req of
                    Right JSONRPCRequest {..} ->
                        writeIORef (encodingFormat epConn) JSON $> XDataRPCReq id method params (Just jsonrpc)
                    Left err -> do
                        print $ "[Error] Decode failed: " <> show err
                        case deserialiseOrFail req of
                            Right CBORRPCRequest {..} ->
                                writeIORef (encodingFormat epConn) CBOR $> XDataRPCReq reqId method params Nothing
                            Left err -> do
                                print $ "[Error] Deserialize failed: " <> show err
                                return XDataRPCBadRequest
            JSON -> do
                case eitherDecode req of
                    Right JSONRPCRequest {..} -> return $ XDataRPCReq id method params (Just jsonrpc)
                    Left err -> do
                        print $ "[Error] Decode failed: " <> show err
                        return XDataRPCBadRequest
            CBOR -> do
                case deserialiseOrFail req of
                    Right CBORRPCRequest {..} -> return $ XDataRPCReq reqId method params Nothing
                    Left err -> do
                        print $ "[Error] Deserialize failed: " <> show err
                        return XDataRPCBadRequest
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
                epConn <- newEndPointConnection context
                liftIO $ atomically $ writeTQueue (connQueue handler) epConn
                handleConnection epConn context
        Left err -> do
            putStrLn $ "Unable to read credentials from file"
            P.error "BadCredentialFile"
