{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Network.Xoken.Node.Service.Callbacks where

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
import Data.Serialize
import qualified Data.Serialize as DS (decode, encode)
import qualified Data.Serialize as S
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

xGetCallbackByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> DT.Text -> m (Maybe MapiCallback)
xGetCallbackByUsername name cbName = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        context = DT.append (DT.pack "USER/") name
        key = DT.append cbName context
    cachedCallback <- liftIO $ H.lookup (callbacksDataCache bp2pEnv) (context)
    case cachedCallback of
        Just cp -> return cachedCallback
        Nothing -> do
            let str = "SELECT context, callback_name, callback_url, auth_type, auth_key, events, created_time from xoken.callbacks where context = ? AND callback_group = ? AND callback_name = ? "
            let qstr =
                    str ::
                        Q.QueryString
                            Q.R
                            (DT.Text, DT.Text, DT.Text)
                            ( DT.Text
                            , DT.Text
                            , DT.Text
                            , DT.Text
                            , DT.Text
                            , [DT.Text]
                            , UTCTime
                            )
            let p = getSimpleQueryParam $ (context, DT.pack "mAPI", cbName)
            res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query qstr p)
            case res of
                Right iop -> do
                    if length iop == 0
                        then return Nothing
                        else do
                            let (context, callbackName, callbackUrl, authType, authKey, events, createdTime) = iop !! 0
                            let mp = Just $ MapiCallback context (DT.pack "mAPI") callbackName callbackUrl authType authKey createdTime
                            return mp
                Left (e :: SomeException) -> do
                    err lg $ LG.msg $ "Error: xGetCallbackByUsername: " ++ show e
                    throw KeyValueDBLookupException

xDeleteCallbackByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> DT.Text -> m ()
xDeleteCallbackByUsername name cbName = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        context = DT.append (DT.pack "USER/") name
        key = DT.append cbName context
        str = "DELETE FROM xoken.callbacks WHERE = ? and policy_name = ? "
        qstr = str :: Q.QueryString Q.W (DT.Text, DT.Text) ()
        par = getSimpleQueryParam (context, DT.pack "mAPI")
    res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right _ -> do
            liftIO $ (H.delete $ userPolicies $ policyDataCache bp2pEnv) key
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xDeletePolicyByUsername: " ++ show e
            throw e

xUpdateCallbackByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> DT.Text -> DT.Text -> DT.Text -> DT.Text -> [DT.Text] -> m (Maybe MapiCallback)
xUpdateCallbackByUsername name cbName cbUrl authType authToken events = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    tm <- liftIO $ getCurrentTime
    mapM_
        ( \ev -> do
            let evs = DT.unpack ev
            case evs of
                "merkleProofSingle" -> return ()
                "merkleProofComposite" -> return ()
                "doubleSpendingCheck" -> return ()
                "ancestorsDiscovered" -> return ()
                _ -> throw InvalidMessageTypeException
        )
        events

    let str = "INSERT INTO xoken.callbacks ( context, callback_group, callback_name, callback_url, auth_type, auth_key, events, created_time) VALUES (?,?,?,?,?,?,?,?)"
        context = DT.append (DT.pack "USER/") name
        key = DT.append cbName context
        qstr = str :: Q.QueryString Q.W (DT.Text, DT.Text, DT.Text, DT.Text, DT.Text, DT.Text, [DT.Text], UTCTime) ()
        skTime = (addUTCTime (nominalDay * 30) tm)
        par = getSimpleQueryParam (context, DT.pack "mAPI", cbName, cbUrl, authType, authToken, events, skTime)
    let conn = xCqlClientState dbe
    res' <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr par)
    case res' of
        Right _ -> do
            liftIO $
                H.insert
                    (callbacksDataCache bp2pEnv)
                    key
                    (MapiCallback context (DT.pack "mAPI") cbName cbUrl authType authToken skTime)
            cbRec <- xGetCallbackByUsername name cbName
            return cbRec
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xUpdatePolicyByUsername (updating data): " ++ show e
            throw e
