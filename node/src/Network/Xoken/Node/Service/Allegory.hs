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

module Network.Xoken.Node.Service.Allegory where

import Arivi.P2P.MessageHandler.HandlerTypes (HasNetworkConfig, networkConfig)
import Arivi.P2P.P2PEnv
import Arivi.P2P.PubSub.Class
import Arivi.P2P.PubSub.Env
import Arivi.P2P.PubSub.Publish as Pub
import Arivi.P2P.PubSub.Types
import Arivi.P2P.RPC.Env
import Arivi.P2P.RPC.Fetch
import Arivi.P2P.Types hiding (msgType)
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
import Network.Xoken.Node.Service.Address
import Network.Xoken.Node.Service.Transaction
import Network.Xoken.Script
import Network.Xoken.Util (bsToInteger, integerToBS)
import Numeric (showHex)
import StmContainers.Map as SM
import System.Logger as LG
import System.Logger.Message
import System.Random
import Text.Read
import Xoken
import qualified Xoken.NodeConfig as NC

xGetAllegoryNameBranch :: (HasXokenNodeEnv env m, MonadIO m) => String -> Bool -> m ([(OutPoint', [MerkleBranchNode'])])
xGetAllegoryNameBranch name isProducer = do
    dbe <- getDB
    lg <- getLogger
    res <- liftIO $ try $ withResource (pool $ graphDB dbe) (`BT.run` queryAllegoryNameBranch (DT.pack name) isProducer)
    case res of
        Right nb -> do
            liftIO $
                mapConcurrently
                    (\x -> do
                         let sp = DT.split (== ':') x
                         let txid = DT.unpack $ sp !! 0
                         let index = readMaybe (DT.unpack $ sp !! 1) :: Maybe Int32
                         case index of
                             Just i -> do
                                 rs <-
                                     liftIO $
                                     try $ withResource (pool $ graphDB dbe) (`BT.run` queryMerkleBranch (DT.pack txid))
                                 case rs of
                                     Right mb -> do
                                         let mnodes =
                                                 Data.List.map
                                                     (\y -> MerkleBranchNode' (DT.unpack $ _nodeValue y) (_isLeftNode y))
                                                     mb
                                         return $ (OutPoint' txid i, mnodes)
                                     Left (e :: SomeException) -> do
                                         err lg $ LG.msg $ "Error: xGetMerkleBranch: " ++ show e
                                         throw KeyValueDBLookupException
                             Nothing -> throw KeyValueDBLookupException)
                    (nb)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetAllegoryNameBranch: " ++ show e
            throw KeyValueDBLookupException

getProducerRoot :: (HasXokenNodeEnv env m, MonadIO m) => [Int] -> m ([Int], OutPoint', DT.Text, Bool, Bool)
getProducerRoot nameArr = do
    dbe <- getDB
    lg <- getLogger
    let name = DT.pack $ L.map (\x -> chr x) (nameArr)
    res <- liftIO $ try $ withResource (pool $ graphDB dbe) (`BT.run` queryAllegoryNameScriptOp name True)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error fetching Allegory name input: " <> show e
            throw e
        Right [] -> do
            if nameArr == []
                then do
                    err lg $ LG.msg $ show "Allegory root not initialised!"
                    throw KeyValueDBLookupException
                else getProducerRoot $ init nameArr
        Right nb -> do
            let sp = DT.split (== ':') $ fst3 $ head nb
                txid = DT.unpack $ sp !! 0
                index = readMaybe (DT.unpack $ sp !! 1) :: Maybe Int
                scr = snd3 $ head nb
                conf = thd3 $ head nb
            case index of
                Just i -> return (nameArr, OutPoint' txid (fromIntegral i), scr, conf, True)
                Nothing -> throw KeyValueDBLookupException

getOwnerRoot :: (HasXokenNodeEnv env m, MonadIO m) => [Int] -> m ([Int], OutPoint', DT.Text, Bool, Bool)
getOwnerRoot nameArr = do
    dbe <- getDB
    lg <- getLogger
    let name = DT.pack $ L.map (\x -> chr x) (nameArr)
    res <- liftIO $ try $ withResource (pool $ graphDB dbe) (`BT.run` queryAllegoryNameScriptOp name False)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: While fetching Allegory name input: " <> show e
            throw e
        Right [] -> do
            if nameArr == []
                then do
                    err lg $ LG.msg $ show "Error: Allegory root not initialised!"
                    throw KeyValueDBLookupException
                else getProducerRoot nameArr
        Right nb -> do
            let sp = DT.split (== ':') $ fst3 (head nb)
                txid = DT.unpack $ sp !! 0
                index = readMaybe (DT.unpack $ sp !! 1) :: Maybe Int
                scr = snd3 $ head nb
                conf = thd3 $ head nb
            case index of
                Just i -> return (nameArr, OutPoint' txid (fromIntegral i), scr, conf, False)
                Nothing -> throw KeyValueDBLookupException

xGetOutpointByName :: (HasXokenNodeEnv env m, MonadIO m) => [Int] -> Bool -> m ([Int], OutPoint', DT.Text, Bool, Bool)
xGetOutpointByName nameArr isProducer = do
    op' <-
        if isProducer
            then LE.try $ getProducerRoot nameArr
            else LE.try $ getOwnerRoot nameArr
    case op' of
        Left (e :: SomeException) -> throw e
        Right op -> return op

xFindNameReseller :: (HasXokenNodeEnv env m, MonadIO m) => [Int] -> Bool -> m ([Int], String, String, Bool, Bool)
xFindNameReseller nameArr isProducer = do
    lg <- getLogger
    op' <- LE.try $ xGetOutpointByName nameArr isProducer
    case op' of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: Failed to fetch outpoint for Allegory name: " <> (show e)
            throw e
        Right op@(forName, OutPoint' txid index, scr, conf, isProducer) -> do
            tx' <- xGetTxHash (DT.pack txid)
            case tx' of
                Nothing -> do
                    err lg $
                        LG.msg $ "Error: Name outpoint not found; transaction " <> (show txid) <> " missing in database"
                    throw KeyValueDBLookupException
                Just tx -> do
                    case txOutputs tx of
                        Nothing -> do
                            err lg $ LG.msg $ "Error: Outputs for transaction " <> (show txid) <> " missing"
                            throw KeyValueDBLookupException
                        Just ops -> do
                            case decodeOutputScript $ lockingScript $ head ops of
                                Left e -> do
                                    err lg $
                                        LG.msg $
                                        "Error: Could not read Allegory metadata, failed to read output script ('" <> e <>
                                        "')"
                                    throw KeyValueDBLookupException
                                Right os -> do
                                    let allegoryHeader = scriptOps os !! 2
                                        allegoryData = scriptOps os !! 3
                                    case (allegoryHeader, allegoryData) of
                                        (OP_PUSHDATA "Allegory/AllPay" OPCODE, OP_PUSHDATA allegory _) -> do
                                            let alg' =
                                                    deserialiseOrFail $ C.fromStrict allegory :: Either DeserialiseFailure Allegory
                                            case alg' of
                                                Left df@(DeserialiseFailure b s) -> do
                                                    err lg $
                                                        LG.msg $
                                                        "Error: Failed to deserialise Allegory metadata, at byte offset " <>
                                                        (show b) <>
                                                        ", cause: " <>
                                                        (show s)
                                                    throw df
                                                Right alg -> do
                                                    let eps (ProducerAction _ (ProducerOutput (Index pri) mbe) _ _) key
                                                            | pri == key = mbe
                                                        eps (ProducerAction _ _ (Just (OwnerOutput (Index owi) mbe)) _) key
                                                            | owi == key = mbe
                                                        eps (OwnerAction _ (OwnerOutput (Index owi) mbe) _) key
                                                            | owi == key = mbe
                                                        eps (ProducerAction _ _ _ ext) key =
                                                            (\l ->
                                                                 if length l == 0
                                                                     then Nothing
                                                                     else head l) $
                                                            L.filter (Nothing /=) $ exs key <$> ext
                                                        exs key (OwnerExtension (OwnerOutput (Index oIndex) mbe) _)
                                                            | oIndex == key = mbe
                                                        exs key (ProducerExtension (ProducerOutput (Index pIndex) mbe) _)
                                                            | pIndex == key = mbe
                                                        exs _ _ = Nothing
                                                        endPoint = eps (action alg) (fromIntegral index)
                                                    case endPoint of
                                                        Nothing -> do
                                                            err lg $
                                                                LG.msg $
                                                                show
                                                                    "Error: No endpoint information in Allegory metadata"
                                                            throw KeyValueDBLookupException
                                                        Just ep ->
                                                            return (forName, protocol ep, uri ep, conf, isProducer)
                                        _ -> do
                                            err lg $ LG.msg $ show "Error: Not a valid Allegory/AllPay OP_RETURN output"
                                            throw KeyValueDBLookupException

