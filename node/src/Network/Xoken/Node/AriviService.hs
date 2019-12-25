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

-- import Data.Aeson as A
-- import Data.Aeson.Encoding (encodingToLazyByteString, fromEncoding)
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
import Database.CQL.Protocol
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

xGetBlockHash :: (HasXokenNodeEnv env m, MonadIO m) => Network -> String -> m (Maybe BlockRecord)
xGetBlockHash net hash = do
    dbe <- getDB
    let conn = keyValDB (dbe)
        str = "SELECT block_hash, block_height, block_header from xoken.blocks_by_hash where block_hash = ?"
        qstr = str :: Q.QueryString Q.R (Identity DT.Text) (DT.Text, Int32, DT.Text)
        p = Q.defQueryParams Q.One $ Identity $ DT.pack hash
    iop <- Q.runClient conn (Q.query qstr p)
    if length iop == 0
        then return Nothing
        else do
            let (hs, ht, hdr) = iop !! 0
            return $ Just $ BlockRecord (fromIntegral ht) (DT.unpack hs) (DT.unpack hdr)

xGetBlocksHashes :: (HasXokenNodeEnv env m, MonadIO m) => Network -> [String] -> m ([BlockRecord])
xGetBlocksHashes net hashes = do
    dbe <- getDB
    let conn = keyValDB (dbe)
        str = "SELECT block_hash, block_height, block_header from xoken.blocks_by_hash where block_hash in ?"
        qstr = str :: Q.QueryString Q.R (Identity [DT.Text]) (DT.Text, Int32, DT.Text)
        p = Q.defQueryParams Q.One $ Identity $ Data.List.map (DT.pack) hashes
    iop <- Q.runClient conn (Q.query qstr p)
    if length iop == 0
        then return []
        else do
            return $
                Data.List.map (\(hs, ht, hdr) -> BlockRecord (fromIntegral ht) (DT.unpack hs) (DT.unpack hdr)) (iop)

xGetBlockHeight :: (HasXokenNodeEnv env m, MonadIO m) => Network -> Int32 -> m (Maybe BlockRecord)
xGetBlockHeight net height = do
    dbe <- getDB
    let conn = keyValDB (dbe)
        str = "SELECT block_hash, block_height, block_header from xoken.blocks_by_height where block_height = ?"
        qstr = str :: Q.QueryString Q.R (Identity Int32) (DT.Text, Int32, DT.Text)
        p = Q.defQueryParams Q.One $ Identity height
    iop <- Q.runClient conn (Q.query qstr p)
    if length iop == 0
        then return Nothing
        else do
            let (hs, ht, hdr) = iop !! 0
            return $ Just $ BlockRecord (fromIntegral ht) (DT.unpack hs) (DT.unpack hdr)

xGetBlocksHeights :: (HasXokenNodeEnv env m, MonadIO m) => Network -> [Int32] -> m ([BlockRecord])
xGetBlocksHeights net heights = do
    dbe <- getDB
    let conn = keyValDB (dbe)
        str = "SELECT block_hash, block_height, block_header from xoken.blocks_by_height where block_height in ?"
        qstr = str :: Q.QueryString Q.R (Identity [Int32]) (DT.Text, Int32, DT.Text)
        p = Q.defQueryParams Q.One $ Identity heights
    iop <- Q.runClient conn (Q.query qstr p)
    liftIO $ print ("Query output :" ++ show iop)
    if length iop == 0
        then return []
        else do
            return $
                Data.List.map (\(hs, ht, hdr) -> BlockRecord (fromIntegral ht) (DT.unpack hs) (DT.unpack hdr)) (iop)

xGetTxHash :: (HasXokenNodeEnv env m, MonadIO m) => Network -> String -> m (Maybe TxRecord)
xGetTxHash net hash = do
    dbe <- getDB
    let conn = keyValDB (dbe)
        str = "SELECT tx_id, block_info, tx_serialized from xoken.transactions where tx_id = ?"
        qstr = str :: Q.QueryString Q.R (Identity DT.Text) (DT.Text, ((DT.Text, Int32), Int32), Blob)
        p = Q.defQueryParams Q.One $ Identity $ DT.pack hash
    iop <- Q.runClient conn (Q.query qstr p)
    if length iop == 0
        then return Nothing
        else do
            let (txid, ((bhash, txind), blkht), sz) = iop !! 0
            return $
                Just $
                TxRecord
                    (DT.unpack txid)
                    (BlockInfo' (DT.unpack bhash) (fromIntegral txind) (fromIntegral blkht))
                    (fromBlob sz)

xGetTxHashes :: (HasXokenNodeEnv env m, MonadIO m) => Network -> [String] -> m ([TxRecord])
xGetTxHashes net hashes = do
    dbe <- getDB
    let conn = keyValDB (dbe)
        str = "SELECT tx_id, block_info, tx_serialized from xoken.transactions where tx_id in ?"
        qstr = str :: Q.QueryString Q.R (Identity [DT.Text]) (DT.Text, ((DT.Text, Int32), Int32), Blob)
        p = Q.defQueryParams Q.One $ Identity $ Data.List.map (DT.pack) hashes
    iop <- Q.runClient conn (Q.query qstr p)
    if length iop == 0
        then return []
        else do
            return $
                Data.List.map
                    (\(txid, ((bhash, txind), blkht), sz) ->
                         TxRecord
                             (DT.unpack txid)
                             (BlockInfo' (DT.unpack bhash) (fromIntegral txind) (fromIntegral blkht))
                             (fromBlob sz))
                    iop

xGetOutputsAddress :: (HasXokenNodeEnv env m, MonadIO m) => Network -> String -> m ([AddressOutputs])
xGetOutputsAddress net address = do
    dbe <- getDB
    let conn = keyValDB (dbe)
        str =
            "SELECT address,output,block_info,is_block_confirmed,is_output_spent,is_type_receive,other_address,prev_outpoint,value from xoken.address_outputs where address = ?"
        qstr =
            str :: Q.QueryString Q.R (Identity DT.Text) ( DT.Text
                                                        , (DT.Text, Int32)
                                                        , ((DT.Text, Int32), Int32)
                                                        , Bool
                                                        , Bool
                                                        , Bool
                                                        , DT.Text
                                                        , (DT.Text, Int32)
                                                        , Int64)
        p = Q.defQueryParams Q.One $ Identity $ DT.pack address
    iop <- Q.runClient conn (Q.query qstr p)
    if length iop == 0
        then return []
        else do
            return $
                Data.List.map
                    (\(addr, (txhs, ind), ((bhash, txind), blkht), fconf, fospent, freceive, oaddr, (ptxhs, pind), val) ->
                         AddressOutputs
                             (DT.unpack addr)
                             (OutPoint' (DT.unpack txhs) (fromIntegral ind))
                             (BlockInfo' (DT.unpack bhash) (fromIntegral txind) (fromIntegral blkht))
                             fconf
                             fospent
                             freceive
                             (DT.unpack oaddr)
                             (OutPoint' (DT.unpack ptxhs) (fromIntegral pind))
                             val)
                    iop

xGetOutputsAddresses :: (HasXokenNodeEnv env m, MonadIO m) => Network -> [String] -> m ([AddressOutputs])
xGetOutputsAddresses net addresses = do
    dbe <- getDB
    let conn = keyValDB (dbe)
        str =
            "SELECT address,output,block_info,is_block_confirmed,is_output_spent,is_type_receive,other_address,prev_outpoint,value from xoken.address_outputs where address in ?"
        qstr =
            str :: Q.QueryString Q.R (Identity [DT.Text]) ( DT.Text
                                                          , (DT.Text, Int32)
                                                          , ((DT.Text, Int32), Int32)
                                                          , Bool
                                                          , Bool
                                                          , Bool
                                                          , DT.Text
                                                          , (DT.Text, Int32)
                                                          , Int64)
        p = Q.defQueryParams Q.One $ Identity $ Data.List.map (DT.pack) addresses
    iop <- Q.runClient conn (Q.query qstr p)
    if length iop == 0
        then return []
        else do
            return $
                Data.List.map
                    (\(addr, (txhs, ind), ((bhash, txind), blkht), fconf, fospent, freceive, oaddr, (ptxhs, pind), val) ->
                         AddressOutputs
                             (DT.unpack addr)
                             (OutPoint' (DT.unpack txhs) (fromIntegral ind))
                             (BlockInfo' (DT.unpack bhash) (fromIntegral txind) (fromIntegral blkht))
                             fconf
                             fospent
                             freceive
                             (DT.unpack oaddr)
                             (OutPoint' (DT.unpack ptxhs) (fromIntegral pind))
                             val)
                    iop

goGetResource :: (HasXokenNodeEnv env m, MonadIO m) => RPCMessage -> Network -> m (RPCMessage)
goGetResource msg net = do
    dbe <- getDB
    let kvdb = keyValDB (dbe)
    case rqMethod msg of
        "HASH->BLOCK" -> do
            case rqParams msg of
                Just (GetBlockByHash hs) -> do
                    blk <- xGetBlockHash net (hs)
                    case blk of
                        Just b -> return $ RPCResponse 200 Nothing $ Just $ RespBlockByHash b
                        Nothing -> return $ RPCResponse 404 (Just "Record Not found") Nothing
                Nothing -> return $ RPCResponse 400 (Just "Error: Invalid Params") Nothing
        "[HASH]->[BLOCK]" -> do
            case rqParams msg of
                Just (GetBlocksByHashes hashes) -> do
                    blks <- xGetBlocksHashes net hashes
                    return $ RPCResponse 200 Nothing $ Just $ RespBlocksByHashes blks
                Nothing -> return $ RPCResponse 400 (Just "Error: Invalid Params") Nothing
        "HEIGHT->BLOCK" -> do
            case rqParams msg of
                Just (GetBlockByHeight ht) -> do
                    blk <- xGetBlockHeight net (fromIntegral ht)
                    case blk of
                        Just b -> return $ RPCResponse 200 Nothing $ Just $ RespBlockByHash b
                        Nothing -> return $ RPCResponse 404 (Just "Record Not found") Nothing
                Nothing -> return $ RPCResponse 400 (Just "Error: Invalid Params") Nothing
        "[HEIGHT]->[BLOCK]" -> do
            case rqParams msg of
                Just (GetBlocksByHeight hts) -> do
                    blks <- xGetBlocksHeights net $ Data.List.map (fromIntegral) hts
                    return $ RPCResponse 200 Nothing $ Just $ RespBlocksByHashes blks
                Nothing -> return $ RPCResponse 400 (Just "Error: Invalid Params") Nothing
        "TXID->TX" -> do
            case rqParams msg of
                Just (GetTransactionByTxID hs) -> do
                    tx <- xGetTxHash net (hs)
                    case tx of
                        Just t -> return $ RPCResponse 200 Nothing $ Just $ RespTransactionByTxID t
                        Nothing -> return $ RPCResponse 404 (Just "Record Not found") Nothing
                Nothing -> return $ RPCResponse 400 (Just "Error: Invalid Params") Nothing
        "[TXID]->[TX]" -> do
            case rqParams msg of
                Just (GetTransactionsByTxIDs hashes) -> do
                    txs <- xGetTxHashes net hashes
                    return $ RPCResponse 200 Nothing $ Just $ RespTransactionsByTxIDs txs
                Nothing -> return $ RPCResponse 400 (Just "Error: Invalid Params") Nothing
        "ADDR->[OUTPUT]" -> do
            case rqParams msg of
                Just (GetOutputsByAddress addr) -> do
                    ops <- xGetOutputsAddress net (addr)
                    return $ RPCResponse 200 Nothing $ Just $ RespOutputsByAddress ops
                Nothing -> return $ RPCResponse 400 (Just "Error: Invalid Params") Nothing
        "[ADDR]->[OUTPUT]" -> do
            case rqParams msg of
                Just (GetOutputsByAddresses addrs) -> do
                    ops <- xGetOutputsAddresses net addrs
                    return $ RPCResponse 200 Nothing $ Just $ RespOutputsByAddresses ops
                Nothing -> return $ RPCResponse 400 (Just "Error: Invalid Params") Nothing
        _____ -> do
            return $ RPCResponse 400 (Just "Error: Invalid Method") Nothing

globalHandlerRpc :: (HasService env m) => RPCMessage -> m (Maybe RPCMessage)
globalHandlerRpc msg = do
    bp2pEnv <- getBitcoinP2P
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    liftIO $ printf "Decoded resp: %s\n" (show msg)
    st <- goGetResource msg net
    return (Just $ st)

globalHandlerPubSub :: (HasService env m) => ServiceTopic -> PubNotifyMessage -> m Status
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
