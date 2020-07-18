{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.Service.Chain where

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
import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (AsyncCancelled, mapConcurrently, mapConcurrently_, race_)
import qualified Control.Concurrent.Async.Lifted as LA (async, concurrently, mapConcurrently, wait)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Concurrent.STM.TVar
import qualified Control.Error.Util as Extra
import Control.Exception
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Logger
import Control.Monad.Loops
import Control.Monad.Reader
import Control.Monad.Trans.Control
import Data.Aeson as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16 (decode, encode)
import Data.ByteString.Base64 as B64
import Data.ByteString.Base64.Lazy as B64L
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as C
import qualified Data.ByteString.Short as BSS
import qualified Data.ByteString.UTF8 as BSU (toString)
import Data.Char
import Data.Default
import qualified Data.HashTable.IO as H
import Data.Hashable
import Data.IORef
import Data.Int
import Data.List
import qualified Data.List as L
import Data.Map.Strict as M
import Data.Maybe
import Data.Pool
import qualified Data.Serialize as S
import Data.Serialize
import qualified Data.Serialize as DS (decode, encode)
import qualified Data.Set as S
import Data.String (IsString, fromString)
import qualified Data.Text as DT
import qualified Data.Text.Encoding as DTE
import qualified Data.Text.Encoding as E
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import Data.Yaml
import qualified Database.Bolt as BT
import qualified Database.CQL.IO as Q
import Database.CQL.Protocol as DCP
import qualified Network.Simple.TCP.TLS as TLS
import Network.Xoken.Address.Base58
import Network.Xoken.Block.Common
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.Allegory
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Util (bsToInteger, integerToBS)
import Numeric (showHex)
import System.Logger as LG
import System.Logger.Message
import System.Random
import Text.Read
import Xoken
import qualified Xoken.NodeConfig as NC

xGetChainInfo :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m (Maybe ChainInfo)
xGetChainInfo = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB $ dbe
        str = "SELECT key,value from xoken.misc_store"
        qstr = str :: Q.QueryString Q.R () (DT.Text, (Maybe Bool, Int32, Maybe Int64, DT.Text))
        p = Q.defQueryParams Q.One ()
    res <- LE.try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            if L.length iop < 3
                then do
                    return Nothing
                else do
                    let (_, blocks, _, bestSyncedHash) = snd . head $ (L.filter (\x -> fst x == "best-synced") iop)
                        (_, headers, _, bestBlockHash) = snd . head $ (L.filter (\x -> fst x == "best_chain_tip") iop)
                        (_, lagHeight, _, chainwork) = snd . head $ (L.filter (\x -> fst x == "chain-work") iop)
                    lagCW <- calculateChainWork [(lagHeight + 1) .. (headers)] conn
                    return $
                        Just $
                        ChainInfo
                            "main"
                            (showHex (lagCW + (read . DT.unpack $ chainwork)) "")
                            (headers)
                            (blocks)
                            (DT.unpack bestBlockHash)
                            (DT.unpack bestSyncedHash)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetChainInfo: " ++ show e
            throw KeyValueDBLookupException

xGetChainHeaders :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Int32 -> Int -> m [ChainHeader]
xGetChainHeaders sblk pgsize = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB (dbe)
        str = "SELECT block_hash,block_height,tx_count,block_header from xoken.blocks_by_height where block_height in ?"
        qstr = str :: Q.QueryString Q.R (Identity [Int32]) (DT.Text, Int32, Maybe Int32, DT.Text)
        p = Q.defQueryParams Q.One $ Identity (L.take pgsize [sblk ..])
    res <- LE.try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return []
                else do
                    case traverse
                             (\(hash, ht, txc, hdr) ->
                                  case (eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 hdr) of
                                      (Right bh) ->
                                          Right $ ChainHeader ht (DT.unpack hash) bh (maybe (-1) fromIntegral txc)
                                      Left e -> Left e)
                             (iop) of
                        Right x -> return x
                        Left e -> do
                            err lg $ LG.msg $ "Error: xGetChainHeaders: decode failed for blockrecord: " <> show e
                            return []
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetChainHeaders: " ++ show e
            throw KeyValueDBLookupException
