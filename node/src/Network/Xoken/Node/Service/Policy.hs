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

module Network.Xoken.Node.Service.Policy where

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
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
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

applyPolicyPatch ::  (HasXokenNodeEnv env m, MonadIO m) => (Maybe MapiPolicyPatch) -> Maybe MapiPolicy -> m(Maybe MapiPolicy)
applyPolicyPatch patch defaultPolicy = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P

    case defaultPolicy of
        Nothing -> do
          debug lg $ LG.msg $ "here XXX: " ++ show defaultPolicy
          return Nothing
        Just dp -> do
            case patch of
                Just pp -> do
                    debug lg $ LG.msg $ "here AAA dp : " ++ show defaultPolicy
                    debug lg $ LG.msg $ "here AAA patch: " ++ show patch
                    let txsz = case ppMaxTxSize pp of
                            Just x -> x
                            Nothing -> mpMaxTxSize dp
                    let scrsz = case ppMaxUnitScriptSize pp of
                            Just x -> x
                            Nothing -> mpMaxUnitScriptSize dp
                    let opcodecount = case ppMaxCumulativeOpCodeCount pp of
                            Just x -> x
                            Nothing -> mpMaxCumulativeOpCodeCount dp
                    let datasz = case ppMaxCumulativeDataSize pp of
                            Just x -> x
                            Nothing -> mpMaxCumulativeDataSize dp
                    let anclim = case ppTxAncestorLimit pp of
                            Just x -> x
                            Nothing -> mpTxAncestorLimit dp
                    let inpsats = case ppMinUnitTxInputSatoshis pp of
                            Just x -> x
                            Nothing -> mpMinUnitTxInputSatoshis dp
                    let opsats = case ppMinUnitTxOutputSatoshis pp of
                            Just x -> x
                            Nothing -> mpMinUnitTxOutputSatoshis dp
                    let consoltxns = case ppGraceConsolidationTxnsQuota pp of
                            Just x -> x
                            Nothing -> mpGraceConsolidationTxnsQuota dp
                    let feesexceed = case ppMaxTxFeesExceedLimit pp of
                            Just x -> x
                            Nothing -> mpMaxTxFeesExceedLimit dp
                    let fees = case ppFees pp of
                            Just x -> x
                            Nothing -> mpFees dp
                    return $ Just $ MapiPolicy txsz scrsz opcodecount datasz anclim inpsats opsats consoltxns feesexceed fees
                Nothing -> do
                  debug lg $ LG.msg $ "here : " ++ show defaultPolicy
                  return defaultPolicy

xGetPolicyByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> m (Maybe MapiPolicy)
xGetPolicyByUsername name = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    mpp <- xGetPolicyPatchByUsername name
    dp <- liftIO $ readIORef $ defaultPolicy $ policyDataCache bp2pEnv
    case dp of
        Just defpol -> do
            patchedPolicy <- applyPolicyPatch mpp dp
            debug lg $ LG.msg $ "here patched 111: " ++ show patchedPolicy
            return patchedPolicy
        Nothing -> do
            policy <- xGetPolicyDefault
            patchedPolicy <- applyPolicyPatch mpp policy
            debug lg $ LG.msg $ "here patched 222: " ++ show patchedPolicy
            return patchedPolicy

xGetPolicyDefault :: (HasXokenNodeEnv env m, MonadIO m) => m (Maybe MapiPolicy)
xGetPolicyDefault = do
    defaultPolicyValue <- xGetPolicyPatch DT.empty
    case defaultPolicyValue of
        Just defpol -> do
            let mpy = A.decode (C.pack $ DT.unpack defpol) :: Maybe MapiPolicy
            return mpy
        Nothing -> return Nothing

xGetPolicyPatchByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> m (Maybe MapiPolicyPatch)
xGetPolicyPatchByUsername name = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let context = DT.append (DT.pack "USER/") name
    cachedPolicy <- liftIO $ TSH.lookup (userPolicies $ policyDataCache bp2pEnv) (context)
    case cachedPolicy of
        Just cp -> return cachedPolicy
        Nothing -> do
            policyValue <- xGetPolicyPatch name
            case policyValue of
                Just polval -> do
                    let mpy = A.decode (C.pack $ DT.unpack polval) :: Maybe MapiPolicyPatch
                    return mpy
                Nothing -> return Nothing

xGetPolicyPatch :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> m (Maybe DT.Text)
xGetPolicyPatch name = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        context =
            if DT.null name
                then DT.pack "default"
                else DT.append (DT.pack "USER/") name

    let str = "SELECT context, policy_name, value, created_time, policy_expiry_time from xoken.policies where context = ? AND policy_name = ? "
        qstr =
            str ::
                Q.QueryString
                    Q.R
                    (DT.Text, DT.Text)
                    ( DT.Text
                    , DT.Text
                    , DT.Text
                    , UTCTime
                    , UTCTime
                    )
        p = getSimpleQueryParam $ (context, DT.pack "mAPI")
    res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return Nothing
                else do
                    let (context, policyName, policyValue, createdTime, expiryTime) = iop !! 0
                    return $ Just policyValue
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetPolicyByUsername: " ++ show e
            throw KeyValueDBLookupException

xDeletePolicyByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> m ()
xDeletePolicyByUsername name = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        context = DT.append (DT.pack "USER/") name
        str = "DELETE FROM xoken.policies WHERE context= ? and policy_name = ? "
        qstr = str :: Q.QueryString Q.W (DT.Text, DT.Text) ()
        par = getSimpleQueryParam (context, DT.pack "mAPI")
    res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right _ -> do
            liftIO $ (TSH.delete $ userPolicies $ policyDataCache bp2pEnv) context
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xDeletePolicyByUsername: " ++ show e
            throw e

xUpdatePolicyByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> MapiPolicyPatch -> m Bool
xUpdatePolicyByUsername name mapiPolicy = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    res <- LE.try $ xGetPolicyByUsername name
    case res of
        Right _ -> do
            tm <- liftIO $ getCurrentTime
            let mapy = DT.pack $ C.unpack $ A.encode $ mapiPolicy
            let str = "INSERT INTO xoken.policies ( context, policy_name,value,created_time,policy_expiry_time) VALUES (?,?,?,?,?)"
                context =
                    if DT.null name
                        then DT.pack "default"
                        else DT.append (DT.pack "USER/") name
                qstr = str :: Q.QueryString Q.W (DT.Text, DT.Text, DT.Text, UTCTime, UTCTime) ()
                skTime = (addUTCTime (nominalDay * 30) tm)
                par = getSimpleQueryParam (context, DT.pack "mAPI", mapy, skTime, skTime)
            let conn = xCqlClientState dbe
            res' <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr par)
            case res' of
                Right _ -> do
                    liftIO $ TSH.delete (userPolicies $ policyDataCache bp2pEnv) context
                    liftIO $
                        TSH.insert
                            (userPolicies $ policyDataCache bp2pEnv)
                            context
                            mapiPolicy
                    return True
                Left (e :: SomeException) -> do
                    err lg $ LG.msg $ "Error: xUpdatePolicyByUsername (updating data): " ++ show e
                    throw e
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xUpdatePolicyByUsername: " ++ show e
            throw e
