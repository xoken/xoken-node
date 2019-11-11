-- {-# LANGUAGE MonoLocalBinds #-}
-- {-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}

module Network.Xoken.Node.AriviService
    ( module Network.Xoken.Node.AriviService
    ) where

import Arivi.P2P.MessageHandler.HandlerTypes (HasNetworkConfig, networkConfig)
import Arivi.P2P.P2PEnv
import Arivi.P2P.PubSub.Class
import Arivi.P2P.PubSub.Env
import Arivi.P2P.PubSub.Publish as Pub
import Arivi.P2P.PubSub.Types
import Arivi.P2P.RPC.Env
import Arivi.P2P.RPC.Fetch
import Arivi.P2P.Types hiding (msgType)

--import AriviNetworkServiceHandler
import Codec.Serialise
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted (async)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad.Reader
import Data.Aeson as A
import Data.Binary as DB
import qualified Data.ByteString.Char8 ()
import qualified Data.ByteString.Lazy as LBS
import Data.Hashable
import Data.Int
import Data.Map.Strict as M
import Data.Serialize
import Data.Set as Set
import Data.Text as DT
import GHC.Generics
import Network.Simple.TCP as T

import System.Random
import Text.Printf

data ServiceResource =
    AriviService
        {
        }
    deriving (Eq, Ord, Show, Generic)

type ServiceTopic = String

instance Serialise ServiceResource

instance Hashable ServiceResource

data ServiceEnv m r t rmsg pmsg =
    ServiceEnv
        { p2pEnv :: P2PEnv m r t rmsg pmsg
        }

type HasService env m = (HasP2PEnv env m ServiceResource ServiceTopic String String, MonadReader env m)

globalHandlerRpc :: (HasService env m) => String -> m (Maybe String)
globalHandlerRpc msg = do
    liftIO $ printf "Decoded resp: %s\n" (msg)
    return $ Just "<<_dummy_response_>>"

--do
--     tcpE <- asks getTCPEnv
--     let que = reqQueue tcpE
--     mid <- liftIO $ randomRIO (1, 268435456)
--     let req = IPCMessage mid "RPC_REQ" (M.singleton "encReq" msg)
--     mv <- liftIO $ newEmptyMVar
--     liftIO $ atomically $ writeTChan que (req, mv)
--     resp <- liftIO $ takeMVar mv
--     liftIO $ print (resp)
--     return (Just (resp))
--
globalHandlerPubSub :: (HasService env m) => String -> String -> m Status
globalHandlerPubSub tpc msg = undefined

-- do
--     liftIO $ print ("globalHandlerPubSub")
--     tcpE <- asks getTCPEnv
--     let que = reqQueue tcpE
--     mid <- liftIO $ randomRIO (1, 268435456)
--     let hm = M.union (M.singleton "subject" tpc) (M.singleton "body" msg)
--     let req = IPCMessage mid "PUB_REQ" hm
--     mv <- liftIO $ newEmptyMVar
--     liftIO $ atomically $ writeTChan que (req, mv)
--     resp <- liftIO $ takeMVar mv
--   -- parse this response and either send Ok or Error, Arivi.P2P.PubSub.Types.Error
--     liftIO $ print (resp)
--     return (Ok)
--
-- processIPCRequests :: (HasService env m) => m ()
-- processIPCRequests =
--     forever $ do
--         tcpE <- asks getTCPEnv
--         let connSock = fst (tcpConn tcpE)
--         let que = reqQueue tcpE
--         let mm = msgMatch tcpE
--         req <- liftIO $ atomically $ readTChan que
--         mp <- liftIO $ readTVarIO mm
--         let nmp = M.insert (msgId (fst req)) (snd req) mp
--         liftIO $ atomically $ writeTVar mm nmp
--         let body = A.encode (fst req)
--         liftIO $ print (body)
--         let ma = LBS.length body
--         let xa = Prelude.fromIntegral (ma) :: Int16
--         T.sendLazy connSock (DB.encode (xa :: Int16))
--         T.sendLazy connSock (body)
--         return ()
-- decodeIPCResponse :: M.Map Int (MVar String) -> LBS.ByteString -> IO ()
-- decodeIPCResponse mp resp = do
--     let ipcReq = A.decode resp :: Maybe IPCMessage
--     case ipcReq of
--         Just x -> do
--             printf "Decoded resp: %s\n" (show x)
--             let mid = msgId x
--             case (msgType x) of
--                 "RPC_RESP" -> do
--                     case (M.lookup "encResp" (payload x)) of
--                         Just rsp -> do
--                             printf "msgId: %d\n" (mid)
--                             case (M.lookup mid mp) of
--                                 Just k -> do
--                                     liftIO $ putMVar k rsp
--                                     return ()
--                                 Nothing -> liftIO $ print ("Lookup failed.")
--                             return ()
--                         Nothing -> printf "Invalid RPC payload.\n"
--                 "PUB_RESP" -> do
--                     case (M.lookup "status" (payload x)) of
--                         Just "ACK" -> do
--                             printf "Publish resp status: Ok!\n"
--                             case (M.lookup mid mp) of
--                                 Just k -> do
--                                     liftIO $ putMVar k "ACK"
--                                 Nothing -> liftIO $ print ("Lookup failed.")
--                         Just "ERR" -> do
--                             printf "Publish resp status: Error!\n"
--                             case (M.lookup mid mp) of
--                                 Just k -> do
--                                     liftIO $ putMVar k "ERR"
--                                 Nothing -> liftIO $ print ("Lookup failed.")
--                         ___ -> printf "Invalid Publish resp status.\n"
--                 ___ -> printf "Invalid message type.\n"
--         Nothing -> printf "Decode 'IPCMessage' failed.\n" (show ipcReq)
--
-- handleResponse :: Socket -> TVar (M.Map Int (MVar String)) -> IO ()
-- handleResponse connSock mm =
--     forever $ do
--         lenBytes <- T.recv connSock 2
--         mp <- liftIO $ readTVarIO mm
--         case lenBytes of
--             Just l -> do
--                 let lenPrefix = runGet getWord16be l -- Char8.readInt l
--                 case lenPrefix of
--                     Right a -> do
--                         pl <- T.recv connSock (fromIntegral (toInteger a))
--                         case pl of
--                             Just y -> decodeIPCResponse mp $ LBS.fromStrict y
--                             Nothing -> printf "Payload read error\n"
--                     Left _b -> printf "Length prefix corrupted.\n"
--             Nothing -> do
--                 printf "Connection closed.\n"
--                 liftIO $ threadDelay 15000000
--         return ()
--
-- processIPCResponses :: (HasService env m) => m ()
-- processIPCResponses = do
--     tcpE <- asks getTCPEnv
--     let connSock = fst (tcpConn tcpE)
--     let mm = msgMatch tcpE
--     liftIO $ handleResponse connSock mm
--     return ()
-- registerAriviSecureRPC :: (HasP2PEnv env m ServiceResource String String String) => m ()
-- registerAriviSecureRPC =
--     registerResource AriviSecureRPC handler Archived >>
--     liftIO (threadDelay 5000000) >>
--     updatePeerInResourceMap AriviSecureRPC
-- goGetResource :: (HasP2PEnv env m ServiceResource ServiceTopic String String) => RPCCall -> m ()
-- goGetResource rpcCall = do
--     let req = (request rpcCall)
--     let ind = rPCReq_key req
--     let msg = DT.unpack (rPCReq_request req)
--     liftIO $ print ("fetchResource")
--     resource <- fetchResource (RpcPayload AriviSecureRPC msg)
--     case resource of
--         Left _ -> do
--             liftIO $ print "Exception: No peers available to issue RPC"
--             let errMsg = DT.pack "__EXCEPTION__NO_PEERS"
--             liftIO $ (putMVar (response rpcCall) (RPCResp ind errMsg))
--             return ()
--         Right (RpcError _) -> liftIO $ print "Exception: RPC error"
--         Right (RpcPayload _ str) -> do
--             liftIO $ print (str)
--             let respMsg = DT.pack str
--             liftIO $ (putMVar (response rpcCall) (RPCResp ind respMsg))
--             return ()
-- loopRPC :: (HasP2PEnv env m ServiceResource ServiceTopic String String) => (TChan RPCCall) -> m ()
-- loopRPC queue =
--     forever $ do
--         item <- liftIO $ atomically $ (readTChan queue)
--         __ <- async (goGetResource item)
--         return ()
-- pubSubMsgType :: PubSubMsg -> [Char]
-- pubSubMsgType (Subscribe1 _t) = "SUBSCRIBE"
-- pubSubMsgType (Publish1 _t _m) = "PUBLISH"
-- pubSubMsgType (Notify1 _t _m) = "NOTIFY"
-- loopPubSub :: (HasP2PEnv env m ServiceResource ServiceTopic String String) => (TChan PubSubMsg) -> m ()
-- loopPubSub queue =
--     forever $ do
--         item <- liftIO $ atomically $ (readTChan queue)
--         liftIO $ print ("read something..")
--         case (pubSubMsgType item) of
--             "SUBSCRIBE" -> do
--                 topicVar <- asks topics
--                 liftIO $ atomically $ modifyTVar' topicVar (Set.insert (topic item))
--             "PUBLISH" -> do
--                 liftIO $ print ("PUBLISH")
--                 Pub.publish (PubSubPayload ((topic item), (message item)))
--             "NOTIFY" -> undefined
--             __ -> undefined
--       --loopPubSub queue
instance HasNetworkConfig (ServiceEnv m r t rmsg pmsg) NetworkConfig where
    networkConfig f se =
        fmap
            (\nc ->
                 se
                     { p2pEnv =
                           (p2pEnv se)
                               {nodeEndpointEnv = (nodeEndpointEnv (p2pEnv se)) {Arivi.P2P.P2PEnv._networkConfig = nc}}
                     })
            (f ((Arivi.P2P.P2PEnv._networkConfig . nodeEndpointEnv . p2pEnv) se))

instance HasTopics (ServiceEnv m r t rmsg pmsg) t where
    topics = pubSubTopics . psEnv . p2pEnv

instance HasSubscribers (ServiceEnv m r t rmsg pmsg) t where
    subscribers = pubSubSubscribers . psEnv . p2pEnv

instance HasNotifiers (ServiceEnv m r t rmsg pmsg) t where
    notifiers = pubSubNotifiers . psEnv . p2pEnv

instance HasPubSubEnv (ServiceEnv m r t rmsg pmsg) t where
    pubSubEnv = psEnv . p2pEnv

instance HasRpcEnv (ServiceEnv m r t rmsg pmsg) r rmsg where
    rpcEnv = rEnv . p2pEnv

instance HasPSGlobalHandler (ServiceEnv m r t rmsg pmsg) m r t rmsg pmsg where
    psGlobalHandler = psHandler . p2pEnv

instance HasRpcGlobalHandler (ServiceEnv m r t rmsg pmsg) m r t rmsg pmsg where
    rpcGlobalHandler = rHandler . p2pEnv
