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

module Network.Xoken.Node.Service.User where

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

xGetUserByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> m (Maybe User)
xGetUserByUsername name = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = keyValDB (dbe)
        str =
            "SELECT username,password,first_name,last_name,emailid,permissions,api_quota,api_used,api_expiry_time,session_key,session_key_expiry_time from xoken.user_permission where username = ?"
        qstr =
            str :: Q.QueryString Q.R (Identity DT.Text) ( DT.Text
                                                        , DT.Text
                                                        , DT.Text
                                                        , DT.Text
                                                        , DT.Text
                                                        , Set DT.Text
                                                        , Int32
                                                        , Int32
                                                        , UTCTime
                                                        , DT.Text
                                                        , UTCTime)
        p = Q.defQueryParams Q.One $ Identity name
    res <- LE.try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return Nothing
                else do
                    let (uname, pwd, fname, lname, email, roles, apiQ, apiU, apiE, sk, skE) = iop !! 0
                    userData <- liftIO $ H.lookup (userDataCache bp2pEnv) (sk)
                    case userData of
                        Just (_, _, used, _, _) ->
                            return $
                            Just $
                            User
                                (DT.unpack uname)
                                (DT.unpack pwd)
                                (DT.unpack fname)
                                (DT.unpack lname)
                                (DT.unpack email)
                                (DT.unpack <$> (DCP.fromSet roles))
                                (fromIntegral apiQ)
                                (fromIntegral used)
                                apiE
                                (maskAfter 10 $ DT.unpack sk)
                                skE
                        Nothing ->
                            return $
                            Just $
                            User
                                (DT.unpack uname)
                                (DT.unpack pwd)
                                (DT.unpack fname)
                                (DT.unpack lname)
                                (DT.unpack email)
                                (DT.unpack <$> (DCP.fromSet roles))
                                (fromIntegral apiQ)
                                (fromIntegral apiU)
                                apiE
                                (maskAfter 10 $ DT.unpack sk)
                                skE
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetUserByUsername: " ++ show e
            throw KeyValueDBLookupException

xDeleteUserByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> m ()
xDeleteUserByUsername name = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = keyValDB (dbe)
        str = "DELETE FROM xoken.user_permission WHERE username=?"
        qstr = str :: Q.QueryString Q.W (Identity DT.Text) ()
        par = Q.defQueryParams Q.One (Identity name)
    res <- liftIO $ try $ Q.runClient conn (Q.write qstr par)
    case res of
        Right () -> do
            cacheList <- liftIO $ (H.toList $ userDataCache bp2pEnv)
            liftIO $
                mapM_
                    (\(k, (n, _, _, _, _)) ->
                         if n == name
                             then H.delete (userDataCache bp2pEnv) k
                             else return ())
                    cacheList
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xDeleteUserByUsername: " ++ show e
            throw e

xUpdateUserByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> UpdateUserByUsername' -> m Bool
xUpdateUserByUsername name (UpdateUserByUsername' {..}) = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    res <- LE.try $ xGetUserByUsername name
    case res of
        Right (Just (User {..})) -> do
            let conn = keyValDB (dbe)
                str =
                    "UPDATE xoken.user_permission SET password=?,first_name=?,last_name=?,emailid=?,api_quota=?,permissions=?,api_expiry_time=? WHERE username=?"
                qstr =
                    str :: Q.QueryString Q.W (DT.Text, DT.Text, DT.Text, DT.Text, Int32, Set DT.Text, UTCTime, DT.Text) ()
                par =
                    Q.defQueryParams
                        Q.One
                        ( fromMaybe (DT.pack uHashedPassword) ((encodeHex . S.encode . sha256 . BC.pack) <$> uuPassword)
                        , DT.pack $ fromMaybe uFirstName uuFirstName
                        , DT.pack $ fromMaybe uLastName uuLastName
                        , DT.pack $ fromMaybe uEmail uuEmail
                        , fromMaybe (fromIntegral uApiQuota) uuApiQuota
                        , DCP.Set $ fmap DT.pack $ fromMaybe uRoles uuRoles
                        , fromMaybe uApiExpiryTime uuApiExpiryTime
                        , name)
            res' <- liftIO $ try $ Q.runClient conn (Q.write qstr par)
            case res' of
                Right () -> do
                    if isJust uuPassword
                        then do
                            tm <- liftIO $ getCurrentTime
                            newSessionKey <- liftIO $ generateSessionKey
                            let str' =
                                    "UPDATE xoken.user_permission SET session_key=?, session_key_expiry_time=? WHERE username=?"
                                qstr' = str' :: Q.QueryString Q.W (DT.Text, UTCTime, DT.Text) ()
                                skTime = (addUTCTime (nominalDay * 30) tm)
                                par' = Q.defQueryParams Q.One (newSessionKey, skTime, name)
                            res'' <- liftIO $ try $ Q.runClient conn (Q.write qstr' par')
                            case res'' of
                                Right () -> do
                                    userData <- liftIO $ H.lookup (userDataCache bp2pEnv) (DT.pack uSessionKey)
                                    case userData of
                                        Just (n, q, u, e, r) -> do
                                            liftIO $ H.delete (userDataCache bp2pEnv) (DT.pack uSessionKey)
                                            liftIO $
                                                H.insert
                                                    (userDataCache bp2pEnv)
                                                    (newSessionKey)
                                                    (n, fromMaybe (fromIntegral uApiQuota) uuApiQuota, u, skTime, r)
                                        Nothing -> return ()
                                    return True
                                Left (e :: SomeException) -> do
                                    err lg $ LG.msg $ "Error: xUpdateUserByUsername (updating sessionKey): " ++ show e
                                    throw e
                        else do
                            liftIO $
                                H.mutate
                                    (userDataCache bp2pEnv)
                                    (DT.pack uSessionKey)
                                    (\v ->
                                         ( (\(n, _, u, e, r) ->
                                                (n, fromMaybe (fromIntegral uApiQuota) uuApiQuota, u, e, r)) <$>
                                           v
                                         , True))
                Left (e :: SomeException) -> do
                    err lg $ LG.msg $ "Error: xUpdateUserByUsername (updating data): " ++ show e
                    throw e
        Right Nothing -> return False
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xUpdateUserByUsername: " ++ show e
            throw e

xGetUserBySessionKey :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> m (Maybe User)
xGetUserBySessionKey skey = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = keyValDB (dbe)
        str =
            "SELECT username,first_name,last_name,emailid,permissions,api_quota,api_used,api_expiry_time,session_key,session_key_expiry_time from xoken.user_permission where session_key = ? ALLOW FILTERING "
        qstr =
            str :: Q.QueryString Q.R (Identity DT.Text) ( DT.Text
                                                        , DT.Text
                                                        , DT.Text
                                                        , DT.Text
                                                        , Set DT.Text
                                                        , Int32
                                                        , Int32
                                                        , UTCTime
                                                        , DT.Text
                                                        , UTCTime)
        p = Q.defQueryParams Q.One $ Identity skey
    res <- LE.try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return Nothing
                else do
                    let (uname, fname, lname, email, roles, apiQ, apiU, apiE, sk, skE) = iop !! 0
                    userData <- liftIO $ H.lookup (userDataCache bp2pEnv) (sk)
                    case userData of
                        Just (_, _, used, _, _) ->
                            return $
                            Just $
                            User
                                (DT.unpack uname)
                                (DT.unpack "")
                                (DT.unpack fname)
                                (DT.unpack lname)
                                (DT.unpack email)
                                (DT.unpack <$> (DCP.fromSet roles))
                                (fromIntegral apiQ)
                                (fromIntegral used)
                                apiE
                                (maskAfter 10 $ DT.unpack sk)
                                skE
                        Nothing ->
                            return $
                            Just $
                            User
                                (DT.unpack uname)
                                (DT.unpack "")
                                (DT.unpack fname)
                                (DT.unpack lname)
                                (DT.unpack email)
                                (DT.unpack <$> (DCP.fromSet roles))
                                (fromIntegral apiQ)
                                (fromIntegral apiU)
                                apiE
                                (maskAfter 10 $ DT.unpack sk)
                                skE
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetUserBySessionKey: " ++ show e
            throw KeyValueDBLookupException

login :: (MonadIO m, HasXokenNodeEnv env m) => DT.Text -> BC.ByteString -> m AuthResp
login user pass = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = keyValDB dbe
        hashedPasswd = encodeHex ((S.encode $ sha256 pass))
        str =
            " SELECT password, api_quota, api_used, session_key, session_key_expiry_time, permissions FROM xoken.user_permission WHERE username = ? "
        qstr = str :: Q.QueryString Q.R (Identity DT.Text) (DT.Text, Int32, Int32, DT.Text, UTCTime, Set DT.Text)
        p = Q.defQueryParams Q.One $ Identity $ user
    res <- liftIO $ try $ Q.runClient conn (Q.query (Q.prepared qstr) p)
    case res of
        Left (SomeException e) -> do
            err lg $ LG.msg $ "Error: SELECT'ing from 'user_permission': " ++ show e
            throw e
        Right (op) -> do
            if length op == 0
                then return $ AuthResp Nothing 0 0
                else do
                    case (op !! 0) of
                        (pw, quota, used, sk, _, roles) -> do
                            if (pw /= hashedPasswd)
                                then return $ AuthResp Nothing 0 0
                                else do
                                    tm <- liftIO $ getCurrentTime
                                    newSessionKey <- liftIO $ generateSessionKey
                                    let str1 =
                                            "UPDATE xoken.user_permission SET session_key = ?, session_key_expiry_time = ? WHERE username = ? "
                                        qstr1 = str1 :: Q.QueryString Q.W (DT.Text, UTCTime, DT.Text) ()
                                        par1 =
                                            Q.defQueryParams
                                                Q.One
                                                (newSessionKey, (addUTCTime (nominalDay * 30) tm), user)
                                    res1 <- liftIO $ try $ Q.runClient conn (Q.write (qstr1) par1)
                                    case res1 of
                                        Right () -> do
                                            userData <- liftIO $ H.lookup (userDataCache bp2pEnv) (sk)
                                            case userData of
                                                Just (n, q, u, e, r) -> do
                                                    liftIO $ H.delete (userDataCache bp2pEnv) (sk)
                                                    liftIO $
                                                        H.insert
                                                            (userDataCache bp2pEnv)
                                                            (newSessionKey)
                                                            (n, q, u, (addUTCTime (nominalDay * 30) tm), r)
                                                    return $
                                                        AuthResp
                                                            (Just $ DT.unpack newSessionKey)
                                                            (fromIntegral u)
                                                            (fromIntegral $ q - u)
                                                Nothing -> do
                                                    liftIO $
                                                        H.insert
                                                            (userDataCache bp2pEnv)
                                                            (newSessionKey)
                                                            ( user
                                                            , quota
                                                            , used
                                                            , (addUTCTime (nominalDay * 30) tm)
                                                            , DCP.fromSet roles)
                                                    return $
                                                        AuthResp
                                                            (Just $ DT.unpack newSessionKey)
                                                            (fromIntegral used)
                                                            (fromIntegral $ quota - used)
                                        Left (SomeException e) -> do
                                            err lg $ LG.msg $ "Error: UPDATE'ing into 'user_permission': " ++ show e
                                            throw e
