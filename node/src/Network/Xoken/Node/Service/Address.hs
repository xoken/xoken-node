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

module Network.Xoken.Node.Service.Address where

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
import Network.Xoken.Util (bsToInteger, integerToBS)
import Numeric (showHex)
import System.Logger as LG
import System.Logger.Message
import System.Random
import Text.Read
import Xoken
import qualified Xoken.NodeConfig as NC

maxNTI :: Int64
maxNTI = 9000000 * 1000000000

getTxOutputsData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => (DT.Text, Int32) -> m TxOutputData
getTxOutputsData (txid, index) = do
    dbe <- getDB
    lg <- getLogger
    let conn = xCqlClientState dbe
        toStr = "SELECT block_info,is_recv,other,value,address FROM xoken.txid_outputs WHERE txid=? AND output_index=?"
        toQStr =
            toStr :: Q.QueryString Q.R (DT.Text, Int32) ( (DT.Text, Int32, Int32)
                                                        , Bool
                                                        , Set ((DT.Text, Int32), Int32, (DT.Text, Int64))
                                                        , Int64
                                                        , DT.Text)
        top = getSimpleQueryParam (txid, index)
    toRes <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query toQStr top)
    case toRes of
        Right es -> do
            if L.null es
                then do
                    err lg $
                        LG.msg $
                        "Error: getTxOutputsData: No entry in txid_outputs for (txid,index): " ++ show (txid, index)
                    throw KeyValueDBLookupException
                else do
                    let txg = L.sortBy (\(_, x, _, _, _) (_, y, _, _, _) -> compare x y) es
                    debug lg $ LG.msg $ BC.pack $ "getTxOutputsData: Tx output data pair: got: " <> (show txg)
                    let outputData =
                            case txg of
                                [x] -> genTxOutputData (txid, index, x, Nothing)
                                [x, y] -> genTxOutputData (txid, index, y, Just x)
                    debug lg $ LG.msg $ BC.pack $ "getTxOutputsData: Tx output data: generated: " <> (show txg)
                    return outputData
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getTxOutputsData: " ++ show e
            throw KeyValueDBLookupException

getUnConfTxOutputsData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => (DT.Text, Int32) -> m TxOutputData
getUnConfTxOutputsData (txid, index) = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    ep <- liftIO $ readTVarIO (epochType bp2pEnv)
    let conn = xCqlClientState dbe
        toStr = "SELECT is_recv,other,value,address FROM xoken.txid_outputs WHERE txid=? AND output_index=?"
        toQStr =
            toStr :: Q.QueryString Q.R (DT.Text, Int32) ( Bool
                                                        , Set ((DT.Text, Int32), Int32, (DT.Text, Int64))
                                                        , Int64
                                                        , DT.Text)
        top = getSimpleQueryParam (txid, index)
    toRes <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query toQStr top)
    case toRes of
        Right es -> do
            if L.null es
                then do
                    err lg $
                        LG.msg $
                        "Error: getUnConfTxOutputsData: No entry in ep_txid_outputs for (txid,index): " ++
                        show (txid, index)
                    throw KeyValueDBLookupException
                else do
                    let txg = L.sortBy (\(x, _, _, _) (y, _, _, _) -> compare x y) es
                    return $
                        case txg of
                            [x] -> genUnConfTxOutputData (txid, index, x, Nothing)
                            [x, y] -> genUnConfTxOutputData (txid, index, y, Just x)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getUnConfTxOutputsData: " ++ show e
            throw KeyValueDBLookupException

deleteDuplicateUnconfs ::
       (ResultWithCursor r a -> ResultWithCursor r a -> Bool)
    -> [ResultWithCursor r a]
    -> [ResultWithCursor r a]
    -> [ResultWithCursor r a]
deleteDuplicateUnconfs compareBy unconf conf =
    let possibleDups = L.take (L.length unconf) conf
     in runDeleteBy possibleDups unconf
  where
    runDeleteBy [] uc = uc
    runDeleteBy (c:cs) uc = runDeleteBy cs (L.deleteBy compareBy c uc)

compareRWCAddressOutputs :: ResultWithCursor AddressOutputs a -> ResultWithCursor AddressOutputs b -> Bool
compareRWCAddressOutputs rwc1 rwc2 = (aoOutput $ res rwc1) == (aoOutput $ res rwc2)

compareRWCScriptOutputs :: ResultWithCursor ScriptOutputs a -> ResultWithCursor ScriptOutputs b -> Bool
compareRWCScriptOutputs rwc1 rwc2 = (scOutput $ res rwc1) == (scOutput $ res rwc2)

xGetOutputsAddress ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> m ([ResultWithCursor AddressOutputs Int64])
xGetOutputsAddress address pgSize mbNominalTxIndex = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO (epochType bp2pEnv)
    res <-
        LE.try $
        LA.concurrently
            (getUnconfirmedOutputsByAddress epoch address pgSize mbNominalTxIndex)
            (getConfirmedOutputsByAddress address pgSize mbNominalTxIndex)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ BC.pack $ "[ERROR] xGetOutputsAddress: Fetching confirmed/unconfirmed outputs: " <> show e
            throw KeyValueDBLookupException
        Right (unconf, conf) ->
            let (unconfRwc, confRwc) =
                    ( addressOutputToResultWithCursor address <$> unconf
                    , addressOutputToResultWithCursor address <$> conf)
             in return $
                L.take (fromMaybe maxBound (fromIntegral <$> pgSize)) $
                (deleteDuplicateUnconfs compareRWCAddressOutputs unconfRwc confRwc) ++ confRwc

xGetUtxosAddress ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> m ([ResultWithCursor AddressOutputs Int64])
xGetUtxosAddress address pgSize mbNominalTxIndex = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO (epochType bp2pEnv)
    res <-
        LE.try $
        LA.concurrently
            (getUnconfirmedUtxosByAddress epoch address pgSize mbNominalTxIndex)
            (getConfirmedUtxosByAddress address pgSize mbNominalTxIndex)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ BC.pack $ "[ERROR] xGetUtxosAddress: Fetching confirmed/unconfirmed outputs: " <> show e
            throw KeyValueDBLookupException
        Right (unconf, conf) -> do
            debug lg $ LG.msg $ BC.pack $ "xGetUtxosAddress: Got confirmed utxos: " <> (show conf)
            debug lg $ LG.msg $ BC.pack $ "xGetUtxosAddress: Got unconfirmed utxos: " <> (show unconf)
            let (unconfRwc, confRwc) =
                    ( addressOutputToResultWithCursor address <$> unconf
                    , addressOutputToResultWithCursor address <$> conf)
             in return $
                L.take (fromMaybe maxBound (fromIntegral <$> pgSize)) $
                (deleteDuplicateUnconfs compareRWCAddressOutputs unconfRwc confRwc) ++ confRwc

getConfirmedOutputsByAddress ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> m [((Int64, (DT.Text, Int32)), TxOutputData)]
getConfirmedOutputsByAddress address pgSize mbNominalTxIndex = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        nominalTxIndex =
            case mbNominalTxIndex of
                Just n -> n
                Nothing -> maxNTI
        confOutputsByAddressQuery =
            "SELECT nominal_tx_index, output FROM xoken.script_hash_outputs WHERE script_hash=? AND nominal_tx_index<?"
        queryString = confOutputsByAddressQuery :: Q.QueryString Q.R (DT.Text, Int64) (Int64, (DT.Text, Int32))
        scriptHash = convertToScriptHash net address
        addressQueryParams = getSimpleQueryParam (DT.pack address, nominalTxIndex)
        scriptHashQueryParams = getSimpleQueryParam (maybe "" DT.pack scriptHash, nominalTxIndex)
    res <-
        if isNothing pgSize || pgSize > Just 0
            then LE.try $
                 LA.concurrently
                     (case scriptHash of
                          Nothing -> return []
                          Just sh ->
                              liftIO $
                              query conn (Q.RqQuery $ Q.Query queryString (scriptHashQueryParams {pageSize = pgSize})))
                     (case address of
                          ('3':_) -> return []
                          _ ->
                              liftIO $
                              query conn (Q.RqQuery $ Q.Query queryString (addressQueryParams {pageSize = pgSize})))
            else return $ Right mempty
    (outpoints, outputData) <-
        case res of
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ BC.pack $ "[ERROR] getConfirmedOutputsByAddress: While running query: " <> show e
                throw KeyValueDBLookupException
            Right (scriptHashResults, addressResults) -> do
                let allOutpoints =
                        fmap head $
                        L.groupBy (\(_, op1) (_, op2) -> op1 == op2) $
                        L.sortBy
                            (\(nti1, _) (nti2, _) ->
                                 if nti1 < nti2
                                     then GT
                                     else LT)
                            (scriptHashResults ++ addressResults)
                    outpoints =
                        case pgSize of
                            Nothing -> allOutpoints
                            Just pg -> L.take (fromIntegral pg) allOutpoints
                (outpoints, ) <$> (sequence $ (\(_, (txid, index)) -> getTxOutputsData (txid, index)) <$> outpoints)
    return $ zip outpoints outputData

getUnconfirmedOutputsByAddress ::
       (HasXokenNodeEnv env m, MonadIO m)
    => Bool
    -> String
    -> Maybe Int32
    -> Maybe Int64
    -> m [((Int64, (DT.Text, Int32)), TxOutputData)]
getUnconfirmedOutputsByAddress epoch address pgSize mbNominalTxIndex = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        nominalTxIndex =
            case mbNominalTxIndex of
                Just n -> n
                Nothing -> maxNTI
        unconfOutputsByAddressQuery =
            "SELECT nominal_tx_index, output FROM xoken.script_hash_outputs WHERE script_hash=? AND nominal_tx_index<?"
        queryString = unconfOutputsByAddressQuery :: Q.QueryString Q.R (DT.Text, Int64) (Int64, (DT.Text, Int32))
        scriptHash = convertToScriptHash net address
        addressQueryParams = getSimpleQueryParam (DT.pack address, nominalTxIndex)
        scriptHashQueryParams = getSimpleQueryParam (maybe "" DT.pack scriptHash, nominalTxIndex)
    res <-
        if isNothing pgSize || pgSize > Just 0
            then LE.try $
                 LA.concurrently
                     (case scriptHash of
                          Nothing -> return []
                          Just sh ->
                              liftIO $
                              query conn (Q.RqQuery $ Q.Query queryString (scriptHashQueryParams {pageSize = pgSize})))
                     (case address of
                          ('3':_) -> return []
                          _ ->
                              liftIO $
                              query conn (Q.RqQuery $ Q.Query queryString (addressQueryParams {pageSize = pgSize})))
            else return $ Right mempty
    (outpoints, outputData) <-
        case res of
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ BC.pack $ "[ERROR] getUnconfirmedOutputsByAddress: While running query: " <> show e
                throw KeyValueDBLookupException
            Right (scriptHashResults, addressResults) -> do
                let allOutpoints =
                        fmap head $
                        L.groupBy (\(_, op1) (_, op2) -> op1 == op2) $
                        L.sortBy
                            (\(nti1, _) (nti2, _) ->
                                 if nti1 < nti2
                                     then GT
                                     else LT)
                            (scriptHashResults ++ addressResults)
                    outpoints =
                        case pgSize of
                            Nothing -> allOutpoints
                            Just pg -> L.take (fromIntegral pg) allOutpoints
                (outpoints, ) <$>
                    (sequence $ (\(_, (txid, index)) -> getUnConfTxOutputsData (txid, index)) <$> outpoints)
    return $ zip outpoints outputData

getConfirmedUtxosByAddress ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> m [((Int64, (DT.Text, Int32)), TxOutputData)]
getConfirmedUtxosByAddress address pgSize nominalTxIndex = do
    lg <- getLogger
    res <- LE.try $ getConfirmedOutputsByAddress address pgSize nominalTxIndex
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ BC.pack $ "[ERROR] getConfirmedUtxosByAddress: Fetching outputs: " <> show e
            throw KeyValueDBLookupException
        Right outputs ->
            let utxos = L.filter (\(_, outputData) -> isNothing $ spendInfo outputData) outputs
             in if (L.length utxos < fromMaybe (L.length utxos) (fromIntegral <$> pgSize)) && (not . L.null $ outputs)
                    then do
                        let nextPgSize = (fromJust pgSize) - (fromIntegral $ L.length utxos)
                            nextCursor = fst $ fst $ last outputs
                        nextPage <- getConfirmedUtxosByAddress address (Just nextPgSize) (Just nextCursor)
                        return $ utxos ++ nextPage
                    else return utxos

getUnconfirmedUtxosByAddress ::
       (HasXokenNodeEnv env m, MonadIO m)
    => Bool
    -> String
    -> Maybe Int32
    -> Maybe Int64
    -> m [((Int64, (DT.Text, Int32)), TxOutputData)]
getUnconfirmedUtxosByAddress epoch address pgSize nominalTxIndex = do
    lg <- getLogger
    res <- LE.try $ getUnconfirmedOutputsByAddress epoch address pgSize nominalTxIndex
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ BC.pack $ "[ERROR] getUnconfirmedUtxosByAddress: Fetching outputs: " <> show e
            throw KeyValueDBLookupException
        Right outputs ->
            let utxos = L.filter (\(_, outputData) -> isNothing $ spendInfo outputData) outputs
             in if (L.length utxos < fromMaybe (L.length utxos) (fromIntegral <$> pgSize)) && (not . L.null $ outputs)
                    then do
                        let nextPgSize = (fromJust pgSize) - (fromIntegral $ L.length utxos)
                            nextCursor = fst $ fst $ last outputs
                        nextPage <- getUnconfirmedUtxosByAddress epoch address (Just nextPgSize) (Just nextCursor)
                        return $ utxos ++ nextPage
                    else return utxos

xGetOutputsScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> m ([ResultWithCursor ScriptOutputs Int64])
xGetOutputsScriptHash scriptHash pgSize mbNominalTxIndex = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO (epochType bp2pEnv)
    res <-
        LE.try $
        LA.concurrently
            (getUnconfirmedOutputsByScriptHash epoch scriptHash pgSize mbNominalTxIndex)
            (getConfirmedOutputsByScriptHash scriptHash pgSize mbNominalTxIndex)
    case res of
        Left (e :: SomeException) -> do
            err lg $
                LG.msg $ BC.pack $ "[ERROR] xGetOutputsScriptHash: Fetching confirmed/unconfirmed outputs: " <> show e
            throw KeyValueDBLookupException
        Right (unconf, conf) ->
            let (unconfRwc, confRwc) =
                    (scriptOutputToResultWithCursor <$> unconf, scriptOutputToResultWithCursor <$> conf)
             in return $
                L.take (fromMaybe maxBound (fromIntegral <$> pgSize)) $
                (deleteDuplicateUnconfs compareRWCScriptOutputs unconfRwc confRwc) ++ confRwc

xGetUtxosScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> m ([ResultWithCursor ScriptOutputs Int64])
xGetUtxosScriptHash scriptHash pgSize mbNominalTxIndex = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO (epochType bp2pEnv)
    res <-
        LE.try $
        LA.concurrently
            (getUnconfirmedUtxosByScriptHash epoch scriptHash pgSize mbNominalTxIndex)
            (getConfirmedUtxosByScriptHash scriptHash pgSize mbNominalTxIndex)
    case res of
        Left (e :: SomeException) -> do
            err lg $
                LG.msg $ BC.pack $ "[ERROR] xGetUtxosScriptHash: Fetching confirmed/unconfirmed outputs: " <> show e
            throw KeyValueDBLookupException
        Right (unconf, conf) ->
            let (unconfRwc, confRwc) =
                    (scriptOutputToResultWithCursor <$> unconf, scriptOutputToResultWithCursor <$> conf)
             in return $
                L.take (fromMaybe maxBound (fromIntegral <$> pgSize)) $
                (deleteDuplicateUnconfs compareRWCScriptOutputs unconfRwc confRwc) ++ confRwc

getConfirmedOutputsByScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> m [((DT.Text, Int64, (DT.Text, Int32)), TxOutputData)]
getConfirmedOutputsByScriptHash scriptHash pgSize mbNominalTxIndex = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        nominalTxIndex =
            case mbNominalTxIndex of
                Just n -> n
                Nothing -> maxNTI
        confOutputsByScriptHashQuery =
            "SELECT script_hash, nominal_tx_index, output FROM xoken.script_hash_outputs WHERE script_hash=? AND nominal_tx_index<?"
        queryString =
            confOutputsByScriptHashQuery :: Q.QueryString Q.R (DT.Text, Int64) (DT.Text, Int64, (DT.Text, Int32))
        params = getSimpleQueryParam (DT.pack scriptHash, nominalTxIndex)
    res <-
        if isNothing pgSize || pgSize > Just 0
            then LE.try $ liftIO $ query conn (Q.RqQuery $ Q.Query queryString (params {pageSize = pgSize}))
            else return $ Right mempty
    (outpoints, outputData) <-
        case res of
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ BC.pack $ "[ERROR] getConfirmedOutputsByScriptHash: While running query: " <> show e
                throw KeyValueDBLookupException
            Right results ->
                (results, ) <$> (sequence $ (\(_, _, (txid, index)) -> getTxOutputsData (txid, index)) <$> results)
    return $ zip outpoints outputData

getUnconfirmedOutputsByScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => Bool
    -> String
    -> Maybe Int32
    -> Maybe Int64
    -> m [((DT.Text, Int64, (DT.Text, Int32)), TxOutputData)]
getUnconfirmedOutputsByScriptHash epoch scriptHash pgSize mbNominalTxIndex = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        nominalTxIndex =
            case mbNominalTxIndex of
                Just n -> n
                Nothing -> maxNTI
        unconfOutputsByScriptHashQuery =
            "SELECT script_hash, nominal_tx_index, output FROM xoken.script_hash_outputs WHERE script_hash=? AND nominal_tx_index<?"
        queryString =
            unconfOutputsByScriptHashQuery :: Q.QueryString Q.R (DT.Text, Int64) (DT.Text, Int64, (DT.Text, Int32))
        params = getSimpleQueryParam (DT.pack scriptHash, nominalTxIndex)
    res <-
        if isNothing pgSize || pgSize > Just 0
            then LE.try $ liftIO $ query conn (Q.RqQuery $ Q.Query queryString (params {pageSize = pgSize}))
            else return $ Right mempty
    (outpoints, outputData) <-
        case res of
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ BC.pack $ "[ERROR] getUnconfirmedOutputsByScriptHash: While running query: " <> show e
                throw KeyValueDBLookupException
            Right results ->
                (results, ) <$>
                (sequence $ (\(_, _, (txid, index)) -> getUnConfTxOutputsData (txid, index)) <$> results)
    return $ zip outpoints outputData

getConfirmedUtxosByScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> m [((DT.Text, Int64, (DT.Text, Int32)), TxOutputData)]
getConfirmedUtxosByScriptHash scriptHash pgSize nominalTxIndex = do
    lg <- getLogger
    res <- LE.try $ getConfirmedOutputsByScriptHash scriptHash pgSize nominalTxIndex
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ BC.pack $ "[ERROR] getConfirmedUtxosByScriptHash: Fetching outputs: " <> show e
            throw KeyValueDBLookupException
        Right outputs ->
            let utxos = L.filter (\(_, outputData) -> isNothing $ spendInfo outputData) outputs
             in if (L.length utxos < fromMaybe (L.length utxos) (fromIntegral <$> pgSize)) && (not . L.null $ outputs)
                    then do
                        let nextPgSize = (fromJust pgSize) - (fromIntegral $ L.length utxos)
                            nextCursor = snd3 $ fst $ last outputs
                        nextPage <- getConfirmedUtxosByScriptHash scriptHash (Just nextPgSize) (Just nextCursor)
                        return $ utxos ++ nextPage
                    else return utxos

getUnconfirmedUtxosByScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => Bool
    -> String
    -> Maybe Int32
    -> Maybe Int64
    -> m [((DT.Text, Int64, (DT.Text, Int32)), TxOutputData)]
getUnconfirmedUtxosByScriptHash epoch scriptHash pgSize nominalTxIndex = do
    lg <- getLogger
    res <- LE.try $ getUnconfirmedOutputsByScriptHash epoch scriptHash pgSize nominalTxIndex
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ BC.pack $ "[ERROR] getUnconfirmedUtxosByScriptHash: Fetching outputs: " <> show e
            throw KeyValueDBLookupException
        Right outputs ->
            let utxos = L.filter (\(_, outputData) -> isNothing $ spendInfo outputData) outputs
             in if (L.length utxos < fromMaybe (L.length utxos) (fromIntegral <$> pgSize)) && (not . L.null $ outputs)
                    then do
                        let nextPgSize = (fromJust pgSize) - (fromIntegral $ L.length utxos)
                            nextCursor = snd3 $ fst $ last outputs
                        nextPage <- getUnconfirmedUtxosByScriptHash epoch scriptHash (Just nextPgSize) (Just nextCursor)
                        return $ utxos ++ nextPage
                    else return utxos

runWithManyInputs ::
       (HasXokenNodeEnv env m, MonadIO m, Ord c, Eq r, Integral p, Bounded p)
    => (i -> Maybe p -> Maybe c -> m ([ResultWithCursor r c]))
    -> [i]
    -> Maybe p
    -> Maybe c
    -> m ([ResultWithCursor r c])
runWithManyInputs fx inputs mbPgSize cursor = do
    let pgSize =
            fromIntegral $
            case mbPgSize of
                Just ps -> ps
                Nothing -> maxBound
    li <- LA.mapConcurrently (\input -> fx input mbPgSize cursor) inputs
    return $ (L.take pgSize . sort . concat $ li)

convertToScriptHash :: Network -> String -> Maybe String
convertToScriptHash net s = do
    let addr = stringToAddr net (DT.pack s)
    (DT.unpack . txHashToHex . TxHash . sha256 . addressToScriptBS) <$> addr

getNextCursor :: [ResultWithCursor r c] -> Maybe c
getNextCursor [] = Nothing
getNextCursor aos =
    let nextCursor = cur $ last aos
     in Just nextCursor

-- encode/decode NominalTxIndex and Output cursor types
encodeNTI :: Maybe Int64 -> Maybe String
encodeNTI mbNTI = show <$> mbNTI

decodeNTI :: Maybe String -> Maybe Int64
decodeNTI Nothing = Nothing
decodeNTI (Just nti) = readMaybe nti :: Maybe Int64

encodeOP :: Maybe (DT.Text, Int32) -> Maybe String
encodeOP Nothing = Nothing
encodeOP (Just op) = Just $ (DT.unpack $ fst op) ++ (show $ snd op)

decodeOP :: Maybe String -> Maybe (DT.Text, Int32)
decodeOP Nothing = Nothing
decodeOP (Just c)
    | length c < 65 = Nothing
    | otherwise =
        case readMaybe mbIndex :: Maybe Int32 of
            Nothing -> Nothing
            Just index -> Just (DT.pack txid, index)
  where
    (txid, mbIndex) = L.splitAt 64 c
