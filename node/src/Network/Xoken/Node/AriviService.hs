{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RecordWildCards #-}

--
module Network.Xoken.Node.AriviService where

import Arivi.P2P.MessageHandler.HandlerTypes (HasNetworkConfig, networkConfig)
import Arivi.P2P.P2PEnv
import Arivi.P2P.PubSub.Class
import Arivi.P2P.PubSub.Env
import Arivi.P2P.PubSub.Publish as Pub
import Arivi.P2P.PubSub.Types
import Arivi.P2P.RPC.Env
import Arivi.P2P.RPC.Fetch
import Arivi.P2P.Types hiding (msgType)
import Codec.Compression.GZip as GZ
import Codec.Serialise
import Conduit hiding (runResourceT)
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted (async)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Logger
import Control.Monad.Loops
import Control.Monad.Reader
import Data.Aeson as A
import Data.Aeson.Encoding (encodingToLazyByteString, fromEncoding)
import Data.Binary as DB
import Data.Bits
import Data.ByteString.Base64 as B64
import Data.ByteString.Base64.Lazy as B64L
import qualified Data.ByteString.Char8 ()
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as C
import qualified Data.ByteString.UTF8 as BSU (toString)
import Data.Hashable
import Data.Int
import Data.List
import Data.Map.Strict as M
import Data.Maybe
import Data.Serialize
import Data.Set as Set
import qualified Data.Text as DT
import qualified Database.CQL.IO as Q
import qualified GHC.Exts as Exts
import GHC.Generics
import Network.Simple.TCP as T
import Network.Xoken.Node.Data

import Network.Xoken.Node.Env

import Network.Xoken.Node.P2P.Types

import System.Random
import Text.Printf
import UnliftIO
import UnliftIO.Resource
import Xoken

-- import Xoken.P2P
jsonSerialiseAny :: (JsonSerial a) => Network -> a -> L.ByteString
jsonSerialiseAny net = encodingToLazyByteString . jsonSerial net
    -- __ <- jsonSerialiseAny net i

gzipCompressBase64Encode :: C.ByteString -> String
gzipCompressBase64Encode val = BSU.toString $ B64.encode $ L.toStrict $ GZ.compress (val)

getMissingParamError :: Int -> C.ByteString
getMissingParamError msgid = A.encode $ getJsonRpcErrorObj msgid (-32602) "Invalid method parameter(s)."

getJsonRpcErrorObj :: Int -> Int -> Value -> Value
getJsonRpcErrorObj messageId errCode errMsg =
    Object $
    Exts.fromList
        [ ("id", (Number $ fromIntegral messageId))
        , ("error", Object $ Exts.fromList [("code", (Number $ fromIntegral errCode)), ("message", errMsg)])
        ]

xGetBlockHash :: (MonadUnliftIO m) => Network -> BlockHash -> m (L.ByteString)
xGetBlockHash net hash = do
    res <- undefined -- liftIO $ getBlockDB hash
    case res of
        Just b -> return undefined -- $ jsonSerialiseAny net (b)
        Nothing -> return $ C.pack "{}"

xGetBlocksHashes :: (MonadUnliftIO m) => Network -> [BlockHash] -> m (L.ByteString)
xGetBlocksHashes net hashes = do
    res <- undefined -- mapM getBlockDB hashes
    let ar = catMaybes res
    return undefined -- $ jsonSerialiseAny net (ar)

xGetBlockHeight :: (MonadUnliftIO m) => Network -> Word32 -> m (L.ByteString)
xGetBlockHeight net height = do
    hs <- undefined -- getBlocksAtHeightDB height
    if undefined -- length hs == undefined -- 0
        then return $ C.pack "{}"
        else do
            res <- undefined -- getBlockDB (hs !! 0)
            case res of
                Just b -> return undefined -- $ jsonSerialiseAny net (b)
                Nothing -> return $ C.pack "{}"

xGetBlocksHeights :: (MonadUnliftIO m) => Network -> [Word32] -> m (L.ByteString)
xGetBlocksHeights net heights = do
    hs <- undefined -- concat <$> mapM getBlocksAtHeightDB (nub heights)
    res <- undefined -- mapM getBlockDB hs
    let ar = catMaybes res
    return undefined -- $ jsonSerialiseAny net (ar)

goGetResource1 :: Q.ClientState -> String -> Network -> IO (String)
goGetResource1 ldb msg net = do
    let bs = C.pack $ msg
    let jsonStr = GZ.decompress $ B64L.decodeLenient bs
    liftIO $ print (jsonStr)
    let rpcReq = A.decode jsonStr :: Maybe RPCRequest
    case rpcReq of
        Just x -> do
            liftIO $ printf "RPC: (%s)\n" (method x)
            runner <- askRunInIO
            val <-
                liftIO $ do
                    case (method x) of
                        "get_block_hash" -> do
                            let z = A.decode (A.encode (params x)) :: Maybe GetBlockHash
                            case z of
                                Just a -> xGetBlockHash net (blockhash a)
                                Nothing -> return $ getMissingParamError (msgid x)
                        "get_blocks_hashes" -> do
                            let z = A.decode (A.encode (params x)) :: Maybe GetBlocksHashes
                            case z of
                                Just a -> xGetBlocksHashes net (blockhashes a)
                                Nothing -> return $ getMissingParamError (msgid x)
                        "get_block_height" -> do
                            let z = A.decode (A.encode (params x)) :: Maybe GetBlockHeight
                            case z of
                                Just a -> xGetBlockHeight net (height a)
                                Nothing -> return $ getMissingParamError (msgid x)
                        "get_blocks_heights" -> do
                            let z = A.decode (A.encode (params x)) :: Maybe GetBlocksHeights
                            case z of
                                Just a -> xGetBlocksHeights net (heights a)
                                Nothing -> return $ getMissingParamError (msgid x)
                        _____ -> do
                            return $ A.encode (getJsonRpcErrorObj (msgid x) (-32601) "The method does not exist.")
            let x = gzipCompressBase64Encode val
            return x
        Nothing -> do
            liftIO $ printf "Decode failed.\n"
            let v = getJsonRpcErrorObj 0 (-32600) "Invalid Request"
            return $ gzipCompressBase64Encode (A.encode (v))

globalHandlerRpc :: (HasService env m) => String -> m (Maybe String)
globalHandlerRpc msg = do
    liftIO $ printf "Decoded resp: %s\n" (msg)
    dbe <- getDBEnv
    let ldb = keyValDB (dbHandles dbe)
    st <- liftIO $ goGetResource1 ldb msg "bsv"
    return (Just $ st)

globalHandlerPubSub :: (HasService env m) => String -> String -> m Status
globalHandlerPubSub tpc msg = undefined

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
