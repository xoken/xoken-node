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

module Network.Xoken.Node.XokenService where

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
import Network.Xoken.Node.Service
import Network.Xoken.Util (bsToInteger, integerToBS)
import Numeric (showHex)
import System.Logger as LG
import System.Logger.Message
import System.Random
import Text.Read
import Xoken
import qualified Xoken.NodeConfig as NC

data EncodingFormat
    = CBOR
    | JSON
    | DEFAULT

data EndPointConnection =
    EndPointConnection
        { requestQueue :: TQueue XDataReq
        , context :: MVar TLS.Context
        , encodingFormat :: IORef EncodingFormat
        }

authLoginClient ::
       (HasXokenNodeEnv env m, MonadIO m) => RPCMessage -> Network -> EndPointConnection -> Bool -> m (RPCMessage)
authLoginClient msg net epConn pretty = do
    dbe <- getDB
    lg <- getLogger
    case rqMethod msg of
        "AUTHENTICATE" ->
            case rqParams msg of
                (AuthenticateReq user pass pretty) -> do
                    resp <- login (DT.pack user) (BC.pack pass)
                    return $ RPCResponse 200 pretty $ Right $ Just $ AuthenticateResp resp
                ___ -> return $ RPCResponse 404 pretty $ Left $ RPCError INVALID_REQUEST Nothing
        _____ -> return $ RPCResponse 200 pretty $ Right $ Just $ AuthenticateResp $ AuthResp Nothing 0 0

delegateRequest :: (HasXokenNodeEnv env m, MonadIO m) => RPCMessage -> EndPointConnection -> Network -> m (RPCMessage)
delegateRequest encReq epConn net = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState (dbe)
    case rqParams encReq of
        (AuthenticateReq _ _ pretty) -> authLoginClient encReq net epConn pretty
        (GeneralReq sessionKey pretty _) -> do
            userData <- liftIO $ H.lookup (userDataCache bp2pEnv) (DT.pack sessionKey)
            case userData of
                Just (name, quota, used, exp, roles) -> do
                    curtm <- liftIO $ getCurrentTime
                    if exp > curtm && quota > used
                        then do
                            if (used + 1) `mod` 100 == 0
                                then do
                                    let str = " UPDATE xoken.user_permission SET api_used = ? WHERE username = ? "
                                        qstr = str :: Q.QueryString Q.W (Int32, DT.Text) ()
                                        p = getSimpleQueryParam $ (used + 1, name)
                                    res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr p)
                                    case res of
                                        Left (SomeException e) -> do
                                            err lg $ LG.msg $ "Error: UPDATE'ing into 'user_permission': " ++ show e
                                            throw e
                                        Right _ -> return ()
                                else return ()
                            liftIO $
                                H.insert
                                    (userDataCache bp2pEnv)
                                    (DT.pack sessionKey)
                                    (name, quota, used + 1, exp, roles)
                            goGetResource encReq net roles (DT.pack sessionKey) pretty
                        else do
                            liftIO $ H.delete (userDataCache bp2pEnv) (DT.pack sessionKey)
                            return $ RPCResponse 200 pretty $ Right $ Just $ AuthenticateResp $ AuthResp Nothing 0 0
                Nothing -> do
                    let str =
                            " SELECT username, api_quota, api_used, session_key_expiry_time, permissions FROM xoken.user_permission WHERE session_key = ? ALLOW FILTERING "
                        qstr = str :: Q.QueryString Q.R (Identity DT.Text) (DT.Text, Int32, Int32, UTCTime, Set DT.Text)
                        p = getSimpleQueryParam $ Identity $ (DT.pack sessionKey)
                    res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
                    case res of
                        Left (SomeException e) -> do
                            err lg $ LG.msg $ "Error: SELECT'ing from 'user_permission': " ++ show e
                            throw e
                        Right (op) -> do
                            if length op == 0
                                then do
                                    return $
                                        RPCResponse 200 pretty $ Right $ Just $ AuthenticateResp $ AuthResp Nothing 0 0
                                else do
                                    case op !! 0 of
                                        (name, quota, used, exp, roles) -> do
                                            curtm <- liftIO $ getCurrentTime
                                            if exp > curtm && quota > used
                                                then do
                                                    liftIO $
                                                        H.insert
                                                            (userDataCache bp2pEnv)
                                                            (DT.pack sessionKey)
                                                            (name, quota, used + 1, exp, Q.fromSet roles)
                                                    goGetResource
                                                        encReq
                                                        net
                                                        (Q.fromSet roles)
                                                        (DT.pack sessionKey)
                                                        pretty
                                                else return $
                                                     RPCResponse 200 pretty $
                                                     Right $ Just $ AuthenticateResp $ AuthResp Nothing 0 0

goGetResource ::
       (HasXokenNodeEnv env m, MonadIO m) => RPCMessage -> Network -> [DT.Text] -> DT.Text -> Bool -> m (RPCMessage)
goGetResource msg net roles sessKey pretty = do
    dbe <- getDB
    lg <- getLogger
    let grdb = graphDB (dbe)
        conn = xCqlClientState (dbe)
    case rqMethod msg of
        "ADD_USER" -> do
            case methodParams $ rqParams msg of
                Just (AddUser uname apiExp apiQuota fname lname email userRoles) -> do
                    if "admin" `elem` roles
                        then do
                            if validateEmail email
                                then do
                                    usr <-
                                        liftIO $
                                        addNewUser
                                            conn
                                            (DT.pack $ uname)
                                            (DT.pack $ fname)
                                            (DT.pack $ lname)
                                            (DT.pack $ email)
                                            (userRoles)
                                            (apiQuota)
                                            (apiExp)
                                    case usr of
                                        Just u -> return $ RPCResponse 200 pretty $ Right $ Just $ RespAddUser u
                                        Nothing ->
                                            return $
                                            RPCResponse 400 pretty $
                                            Left $
                                            RPCError
                                                INVALID_PARAMS
                                                (Just $ "User with username " ++ uname ++ " already exists")
                                else return $
                                     RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS (Just "Invalid email")
                        else return $
                             RPCResponse 403 pretty $
                             Left $ RPCError INVALID_PARAMS (Just "User lacks permission to create users")
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "DELETE_USER" -> do
            case methodParams $ rqParams msg of
                Just (UserByUsername u) -> do
                    if "admin" `elem` roles
                        then do
                            xDeleteUserByUsername (DT.pack u)
                            return $ RPCResponse 200 pretty $ Right $ Nothing
                        else return $
                             RPCResponse 403 pretty $
                             Left $ RPCError INVALID_PARAMS (Just "User lacks permission to fetch users")
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "UPDATE_USER" -> do
            case methodParams $ rqParams msg of
                Just (UpdateUserByUsername u d) -> do
                    if "admin" `elem` roles
                        then do
                            upd <- xUpdateUserByUsername (DT.pack u) d
                            case upd of
                                True -> return $ RPCResponse 200 pretty $ Right $ Nothing
                                False ->
                                    return $
                                    RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS (Just "User doesn't exist")
                        else return $
                             RPCResponse 403 pretty $
                             Left $ RPCError INVALID_PARAMS (Just "User lacks permission to fetch users")
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "USER" -> do
            usr <- xGetUserBySessionKey sessKey
            return $ RPCResponse 200 pretty $ Right $ Just $ RespUser usr
        "USERNAME->USER" -> do
            case methodParams $ rqParams msg of
                Just (UserByUsername u) -> do
                    if "admin" `elem` roles
                        then do
                            usr <- xGetUserByUsername (DT.pack u)
                            return $ RPCResponse 200 pretty $ Right $ Just $ RespUser usr
                        else return $
                             RPCResponse 403 pretty $
                             Left $ RPCError INVALID_PARAMS (Just "User lacks permission to fetch users")
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "CHAIN_INFO" -> do
            cw <- xGetChainInfo
            case cw of
                Just c -> return $ RPCResponse 200 pretty $ Right $ Just $ RespChainInfo c
                Nothing -> return $ RPCResponse 404 pretty $ Left $ RPCError INVALID_REQUEST Nothing
        "CHAIN_HEADERS" -> do
            case methodParams $ rqParams msg of
                Just (GetChainHeaders ht pg) -> do
                    hdrs <- xGetChainHeaders ht pg
                    return $ RPCResponse 200 pretty $ Right $ Just $ RespChainHeaders hdrs
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "HASH->BLOCK" -> do
            case methodParams $ rqParams msg of
                Just (GetBlockByHash hs) -> do
                    blk <- xGetBlockHash (DT.pack hs)
                    case blk of
                        Just b -> return $ RPCResponse 200 pretty $ Right $ Just $ RespBlockByHash b
                        Nothing -> return $ RPCResponse 404 pretty $ Left $ RPCError INVALID_REQUEST Nothing
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "[HASH]->[BLOCK]" -> do
            case methodParams $ rqParams msg of
                Just (GetBlocksByHashes hashes) -> do
                    blks <- xGetBlocksHashes (DT.pack <$> hashes)
                    return $ RPCResponse 200 pretty $ Right $ Just $ RespBlocksByHashes blks
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "HEIGHT->BLOCK" -> do
            case methodParams $ rqParams msg of
                Just (GetBlockByHeight ht) -> do
                    blk <- xGetBlockHeight (fromIntegral ht)
                    case blk of
                        Just b -> return $ RPCResponse 200 pretty $ Right $ Just $ RespBlockByHash b
                        Nothing -> return $ RPCResponse 404 pretty $ Left $ RPCError INVALID_REQUEST Nothing
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "[HEIGHT]->[BLOCK]" -> do
            case methodParams $ rqParams msg of
                Just (GetBlocksByHeights hts) -> do
                    blks <- xGetBlocksHeights $ Data.List.map (fromIntegral) hts
                    return $ RPCResponse 200 pretty $ Right $ Just $ RespBlocksByHashes blks
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "HASH->[TXID]" -> do
            case methodParams $ rqParams msg of
                Just (GetTxIDsByBlockHash hash pgSize pgNum) -> do
                    txids <- xGetTxIDsByBlockHash hash pgSize pgNum
                    return $ RPCResponse 200 pretty $ Right $ Just $ RespTxIDsByBlockHash txids
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "TXID->RAWTX" -> do
            case methodParams $ rqParams msg of
                Just (GetRawTransactionByTxID hs) -> do
                    tx <- xGetTxHash (DT.pack hs)
                    case tx of
                        Just t -> return $ RPCResponse 200 pretty $ Right $ Just $ RespRawTransactionByTxID t
                        Nothing -> return $ RPCResponse 200 pretty $ Right $ Nothing
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "TXID->TX" -> do
            case methodParams $ rqParams msg of
                Just (GetTransactionByTxID hs) -> do
                    tx <- xGetTxHash (DT.pack hs)
                    case tx of
                        Just RawTxRecord {..} ->
                            case S.decodeLazy txSerialized of
                                Right rt ->
                                    return $
                                    RPCResponse 200 pretty $
                                    Right $
                                    Just $
                                    RespTransactionByTxID
                                        (TxRecord
                                             txId
                                             size
                                             txBlockInfo
                                             (txToTx' rt txOutputs txInputs)
                                             fees
                                             txMerkleBranch)
                                Left err -> return $ RPCResponse 400 pretty $ Left $ RPCError INTERNAL_ERROR Nothing
                        Nothing -> return $ RPCResponse 200 pretty $ Right Nothing
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "[TXID]->[RAWTX]" -> do
            case methodParams $ rqParams msg of
                Just (GetRawTransactionsByTxIDs hashes) -> do
                    txs <- xGetTxHashes (DT.pack <$> hashes)
                    return $ RPCResponse 200 pretty $ Right $ Just $ RespRawTransactionsByTxIDs txs
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "[TXID]->[TX]" -> do
            case methodParams $ rqParams msg of
                Just (GetTransactionsByTxIDs hashes) -> do
                    txs <- xGetTxHashes (DT.pack <$> hashes)
                    let rawTxs =
                            (\RawTxRecord {..} ->
                                 (TxRecord txId size txBlockInfo <$>
                                  (txToTx' <$> (Extra.hush $ S.decodeLazy txSerialized) <*> (pure txOutputs) <*>
                                   (pure txInputs)) <*>
                                  (pure fees) <*>
                                  (pure txMerkleBranch))) <$>
                            txs
                    return $ RPCResponse 200 pretty $ Right $ Just $ RespTransactionsByTxIDs $ catMaybes rawTxs
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "ADDR->[OUTPUT]" -> do
            case methodParams $ rqParams msg of
                Just (GetOutputsByAddress addr psize cursor) -> do
                    ops <- xGetOutputsAddress addr psize (decodeNTI cursor)
                    return $
                        RPCResponse 200 pretty $
                        Right $
                        Just $ RespOutputsByAddress (encodeNTI $ getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "[ADDR]->[OUTPUT]" -> do
            case methodParams $ rqParams msg of
                Just (GetOutputsByAddresses addrs pgSize cursor) -> do
                    ops <- runWithManyInputs xGetOutputsAddress addrs pgSize (decodeNTI cursor)
                    return $
                        RPCResponse 200 pretty $
                        Right $
                        Just $ RespOutputsByAddresses (encodeNTI $ getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "SCRIPTHASH->[OUTPUT]" -> do
            case methodParams $ rqParams msg of
                Just (GetOutputsByScriptHash sh pgSize cursor) -> do
                    ops <- xGetOutputsScriptHash sh pgSize (decodeNTI cursor)
                    return $
                        RPCResponse 200 pretty $
                        Right $
                        Just $ RespOutputsByScriptHash (encodeNTI $ getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "[SCRIPTHASH]->[OUTPUT]" -> do
            case methodParams $ rqParams msg of
                Just (GetOutputsByScriptHashes shs pgSize cursor) -> do
                    ops <- runWithManyInputs xGetOutputsScriptHash shs pgSize (decodeNTI cursor)
                    return $
                        RPCResponse 200 pretty $
                        Right $
                        Just $ RespOutputsByScriptHashes (encodeNTI $ getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "ADDR->[UTXO]" -> do
            case methodParams $ rqParams msg of
                Just (GetUTXOsByAddress addr psize cursor) -> do
                    ops <- xGetUTXOsAddress addr psize (decodeOP cursor)
                    return $
                        RPCResponse 200 pretty $
                        Right $ Just $ RespUTXOsByAddress (encodeOP $ getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "[ADDR]->[UTXO]" -> do
            case methodParams $ rqParams msg of
                Just (GetUTXOsByAddresses addrs pgSize cursor) -> do
                    ops <- runWithManyInputs xGetUTXOsAddress addrs pgSize (decodeOP cursor)
                    return $
                        RPCResponse 200 pretty $
                        Right $
                        Just $ RespUTXOsByAddresses (encodeOP $ getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "SCRIPTHASH->[UTXO]" -> do
            case methodParams $ rqParams msg of
                Just (GetUTXOsByScriptHash sh pgSize cursor) -> do
                    ops <- xGetUTXOsScriptHash sh pgSize (decodeOP cursor)
                    return $
                        RPCResponse 200 pretty $
                        Right $
                        Just $ RespUTXOsByScriptHash (encodeOP $ getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "[SCRIPTHASH]->[UTXO]" -> do
            case methodParams $ rqParams msg of
                Just (GetUTXOsByScriptHashes shs pgSize cursor) -> do
                    ops <- runWithManyInputs xGetUTXOsScriptHash shs pgSize (decodeOP cursor)
                    return $
                        RPCResponse 200 pretty $
                        Right $
                        Just $ RespUTXOsByScriptHashes (encodeOP $ getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "TXID->[MNODE]" -> do
            case methodParams $ rqParams msg of
                Just (GetMerkleBranchByTxID txid) -> do
                    ops <- xGetMerkleBranch txid
                    return $ RPCResponse 200 pretty $ Right $ Just $ RespMerkleBranchByTxID ops
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "NAME->[OUTPOINT]" -> do
            case methodParams $ rqParams msg of
                Just (GetAllegoryNameBranch name isProducer) -> do
                    ops <- xGetAllegoryNameBranch name isProducer
                    return $ RPCResponse 200 pretty $ Right $ Just $ RespAllegoryNameBranch ops
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "RELAY_TX" -> do
            case methodParams $ rqParams msg of
                Just (RelayTx tx) -> do
                    ops <- xRelayTx tx
                    return $ RPCResponse 200 pretty $ Right $ Just $ RespRelayTx ops
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "RELAY_MULTIPLE_TX" -> do
            case methodParams $ rqParams msg of
                Just (RelayMultipleTx txns) -> do
                    ops <- xRelayMultipleTx txns
                    return $ RPCResponse 200 pretty $ Right $ Just $ RespRelayMultipleTx ops
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "GET_PRODUCER" -> do
            case methodParams $ rqParams msg of
                Just (GetProducer name) -> do
                    res <- LE.try $ xGetProducer name
                    case res of
                        Left (e :: SomeException) -> do
                            debug lg $ LG.msg $ "Allegory error: xGetProducer: " ++ show e
                            return $ RPCResponse 400 pretty $ Left $ RPCError INTERNAL_ERROR Nothing
                        Right (name, op, scr) ->
                            return $ RPCResponse 200 pretty $ Right $ Just $ RespGetProducer name op (DT.unpack scr)
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        "OUTPOINT->SPEND_STATUS" -> do
            case methodParams $ rqParams msg of
                Just (GetTxOutputSpendStatus txid index) -> do
                    txss <- xGetTxOutputSpendStatus txid index
                    return $ RPCResponse 200 pretty $ Right $ Just $ RespTxOutputSpendStatus txss
                _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_PARAMS Nothing
        _____ -> return $ RPCResponse 400 pretty $ Left $ RPCError INVALID_METHOD Nothing
