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

module Network.Xoken.Node.Service.Block where

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
import Database.XCQL.Protocol as Q
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

xGetBlockHash :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> m (Maybe BlockRecord)
xGetBlockHash hash = do
    dbe <- getDB
    lg <- getLogger
    let conn = connection (dbe)
        str =
            "SELECT block_hash,block_height,block_header,next_block_hash,block_size,tx_count,coinbase_tx from xoken.blocks_by_hash where block_hash = ?"
        qstr =
            str :: Q.QueryString Q.R (Identity DT.Text) ( DT.Text
                                                        , Int32
                                                        , DT.Text
                                                        , Maybe DT.Text
                                                        , Maybe Int32
                                                        , Maybe Int32
                                                        , Maybe Blob)
        p = getSimpleQueryParam $ Identity hash
    res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return Nothing
                else do
                    let (hs, ht, hdr, nbhs, size, txc, cbase) = iop !! 0
                    case eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 hdr of
                        Right bh ->
                            return $
                            Just $
                            BlockRecord
                                (fromIntegral ht)
                                (DT.unpack hs)
                                bh
                                (maybe "" DT.unpack nbhs)
                                (maybe (-1) fromIntegral size)
                                (maybe (-1) fromIntegral txc)
                                ("")
                                (maybe "" (coinbaseTxToMessage . fromBlob) cbase)
                                (maybe "" fromBlob cbase)
                        Left err -> do
                            liftIO $ print $ "Decode failed with error: " <> show err
                            return Nothing
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHash: " ++ show e
            throw KeyValueDBLookupException

xGetBlocksHashes :: (HasXokenNodeEnv env m, MonadIO m) => [DT.Text] -> m ([BlockRecord])
xGetBlocksHashes hashes = do
    dbe <- getDB
    lg <- getLogger
    let conn = connection (dbe)
        str =
            "SELECT block_hash,block_height,block_header,next_block_hash,block_size,tx_count,coinbase_tx from xoken.blocks_by_hash where block_hash in ?"
        qstr =
            str :: Q.QueryString Q.R (Identity [DT.Text]) ( DT.Text
                                                          , Int32
                                                          , DT.Text
                                                          , Maybe DT.Text
                                                          , Maybe Int32
                                                          , Maybe Int32
                                                          , Maybe Blob)
        p = getSimpleQueryParam $ Identity $ hashes
    res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return []
                else do
                    case traverse
                             (\(hs, ht, hdr, nbhs, size, txc, cbase) ->
                                  case (eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 hdr) of
                                      (Right bh) ->
                                          Right $
                                          BlockRecord
                                              (fromIntegral ht)
                                              (DT.unpack hs)
                                              bh
                                              (maybe "" DT.unpack nbhs)
                                              (maybe (-1) fromIntegral size)
                                              (maybe (-1) fromIntegral txc)
                                              ("")
                                              (maybe "" (coinbaseTxToMessage . fromBlob) cbase)
                                              (maybe "" fromBlob cbase)
                                      Left err -> Left err)
                             (iop) of
                        Right x -> return x
                        Left err -> do
                            liftIO $ print $ "decode failed for blockrecord: " <> show err
                            return []
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHashes: " ++ show e
            throw KeyValueDBLookupException

xGetBlockHeight :: (HasXokenNodeEnv env m, MonadIO m) => Int32 -> m (Maybe BlockRecord)
xGetBlockHeight height = do
    dbe <- getDB
    lg <- getLogger
    let conn = connection (dbe)
        str =
            "SELECT block_hash,block_height,block_header,next_block_hash,block_size,tx_count,coinbase_tx from xoken.blocks_by_height where block_height = ?"
        qstr =
            str :: Q.QueryString Q.R (Identity Int32) ( DT.Text
                                                      , Int32
                                                      , DT.Text
                                                      , Maybe DT.Text
                                                      , Maybe Int32
                                                      , Maybe Int32
                                                      , Maybe Blob)
        p = getSimpleQueryParam $ Identity height
    res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return Nothing
                else do
                    let (hs, ht, hdr, nbhs, size, txc, cbase) = iop !! 0
                    case eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 hdr of
                        Right bh -> do
                            return $
                                Just $
                                BlockRecord
                                    (fromIntegral ht)
                                    (DT.unpack hs)
                                    bh
                                    (maybe "" DT.unpack nbhs)
                                    (maybe (-1) fromIntegral size)
                                    (maybe (-1) fromIntegral txc)
                                    ("")
                                    (maybe "" (coinbaseTxToMessage . fromBlob) cbase)
                                    (maybe "" fromBlob cbase)
                        Left err -> do
                            liftIO $ print $ "Decode failed with error: " <> show err
                            return Nothing
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlockHeight: " <> show e
            throw KeyValueDBLookupException

xGetBlocksHeights :: (HasXokenNodeEnv env m, MonadIO m) => [Int32] -> m ([BlockRecord])
xGetBlocksHeights heights = do
    dbe <- getDB
    lg <- getLogger
    let conn = connection (dbe)
        str =
            "SELECT block_hash,block_height,block_header,next_block_hash,block_size,tx_count,coinbase_tx from xoken.blocks_by_height where block_height in ?"
        qstr =
            str :: Q.QueryString Q.R (Identity [Int32]) ( DT.Text
                                                        , Int32
                                                        , DT.Text
                                                        , Maybe DT.Text
                                                        , Maybe Int32
                                                        , Maybe Int32
                                                        , Maybe Blob)
        p = getSimpleQueryParam $ Identity $ heights
    res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return []
                else do
                    case traverse
                             (\(hs, ht, hdr, nbhs, size, txc, cbase) ->
                                  case (eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 hdr) of
                                      (Right bh) ->
                                          Right $
                                          BlockRecord
                                              (fromIntegral ht)
                                              (DT.unpack hs)
                                              bh
                                              (maybe "" DT.unpack nbhs)
                                              (maybe (-1) fromIntegral size)
                                              (maybe (-1) fromIntegral txc)
                                              ("")
                                              (maybe "" (coinbaseTxToMessage . fromBlob) cbase)
                                              (maybe "" fromBlob cbase)
                                      Left err -> Left err)
                             (iop) of
                        Right x -> return x
                        Left err -> do
                            liftIO $ print $ "decode failed for blockrecord: " <> show err
                            return []
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlockHeights: " ++ show e
            throw KeyValueDBLookupException
