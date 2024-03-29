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
maxNTI = maxBound

getTxOutputsData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => (DT.Text, Int32) -> m TxOutput
getTxOutputsData (txid, index) = do
    dbe <- getDB
    lg <- getLogger
    let conn = xCqlClientState dbe
        toStr =
            "SELECT output_index, value, address, script, spend_info FROM xoken.txid_outputs WHERE txid=? AND output_index=?"
        toQStr = toStr :: Q.QueryString Q.R (DT.Text, Int32) (Int32, Int64, DT.Text, DT.Text, Maybe (DT.Text, Int32))
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
                    case es of
                        [(idx, val, addr, script, si)] -> do
                            case si of
                                Nothing -> do
                                    return $ TxOutput idx (DT.unpack addr) Nothing val (BC.pack $ DT.unpack script)
                                Just situple -> do
                                    let conn1 = xCqlClientState dbe
                                        toStr1 = "SELECT block_info FROM xoken.transactions WHERE tx_id=?"
                                        toQStr1 =
                                            toStr1 :: Q.QueryString Q.R (Identity DT.Text) (Identity ( DT.Text
                                                                                                     , Int32
                                                                                                     , Int32))
                                        par1 = getSimpleQueryParam (Identity $ fst situple)
                                    res1 <- liftIO $ LE.try $ query conn1 (Q.RqQuery $ Q.Query toQStr1 par1)
                                    case res1 of
                                        Right rx -> do
                                            let (bhash, bht, txindx) = runIdentity $ rx !! 0
                                            return $
                                                TxOutput
                                                    idx
                                                    (DT.unpack addr)
                                                    (Just $
                                                     SpendInfo
                                                         (DT.unpack $ fst situple)
                                                         (snd situple)
                                                         (BlockInfo' (DT.unpack bhash) bht txindx)
                                                         [])
                                                    val
                                                    (BC.pack $ DT.unpack script)
                                        Left (e :: SomeException) -> do
                                            err lg $ LG.msg $ "Error: getTxOutputsData: " ++ show e
                                            throw KeyValueDBLookupException
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getTxOutputsData: " ++ show e
            throw KeyValueDBLookupException

compareRWCAddressOutputs :: ResultWithCursor AddressOutputs a -> ResultWithCursor AddressOutputs b -> Bool
compareRWCAddressOutputs rwc1 rwc2 = (aoOutput $ res rwc1) == (aoOutput $ res rwc2)

compareRWCScriptOutputs :: ResultWithCursor ScriptOutputs a -> ResultWithCursor ScriptOutputs b -> Bool
compareRWCScriptOutputs rwc1 rwc2 = (scOutput $ res rwc1) == (scOutput $ res rwc2)

xGetOutputsAddress ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> Bool
    -> m ([ResultWithCursor AddressOutputs Int64])
xGetOutputsAddress address pgSize mbNominalTxIndex isAsc = do
    lg <- getLogger
    dbe <- getDB
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO (epochType bp2pEnv)
    res <- LE.try $ getConfirmedOutputsByAddress address pgSize mbNominalTxIndex isAsc
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ BC.pack $ "[ERROR] xGetOutputsAddress: Fetching confirmed/unconfirmed outputs: " <> show e
            throw KeyValueDBLookupException
        Right conf -> do
            xx <-
                LA.mapConcurrently
                    (\ado -> do
                         case ado of
                             ((_, (opTxId, _)), _) -> do
                                 let conn1 = xCqlClientState dbe
                                     toStr1 = "SELECT block_info, inputs FROM xoken.transactions WHERE tx_id=?"
                                     toQStr1 =
                                         toStr1 :: Q.QueryString Q.R (Identity DT.Text) ( Maybe (DT.Text, Int32, Int32)
                                                                                        , Set ( (DT.Text, Int32)
                                                                                              , Int32
                                                                                              , (DT.Text, Int64)))
                                     par1 = getSimpleQueryParam (Identity opTxId)
                                 res1 <- liftIO $ LE.try $ query conn1 (Q.RqQuery $ Q.Query toQStr1 par1)
                                 case res1 of
                                     Right [(bi, inputs)] -> do
                                         case bi of
                                             Nothing -> do
                                                 return $
                                                     addressOutputToResultWithCursor
                                                         address
                                                         ado
                                                         (Q.fromSet inputs)
                                                         Nothing
                                             Just blkinf -> do
                                                 let (bhash, bht, txindx) = blkinf
                                                 return $
                                                     addressOutputToResultWithCursor
                                                         address
                                                         ado
                                                         (Q.fromSet inputs)
                                                         (Just $ BlockInfo' (DT.unpack bhash) bht txindx)
                                     Left (e :: SomeException) -> do
                                         err lg $ LG.msg $ "Error: xGetOutputsAddress: " ++ show e
                                         throw KeyValueDBLookupException)
                    conf
            return $ L.take (fromMaybe maxBound (fromIntegral <$> pgSize)) $ xx

xGetUtxosAddress ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> Bool
    -> m ([ResultWithCursor AddressOutputs Int64])
xGetUtxosAddress address pgSize mbNominalTxIndex isAsc = do
    lg <- getLogger
    dbe <- getDB
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO (epochType bp2pEnv)
    res <- LE.try $ getConfirmedUtxosByAddress address pgSize mbNominalTxIndex isAsc
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ BC.pack $ "[ERROR] xGetUtxosAddress: Fetching confirmed/unconfirmed outputs: " <> show e
            throw KeyValueDBLookupException
        Right conf -> do
            debug lg $ LG.msg $ BC.pack $ "xGetUtxosAddress: Got confirmed utxos: " <> (show conf)
            xx <-
                LA.mapConcurrently
                    (\ado -> do
                         case ado of
                             ((_, (opTxId, _)), _) -> do
                                 let conn1 = xCqlClientState dbe
                                     toStr1 = "SELECT block_info, inputs FROM xoken.transactions WHERE tx_id=?"
                                     toQStr1 =
                                         toStr1 :: Q.QueryString Q.R (Identity DT.Text) ( Maybe (DT.Text, Int32, Int32)
                                                                                        , Set ( (DT.Text, Int32)
                                                                                              , Int32
                                                                                              , (DT.Text, Int64)))
                                     par1 = getSimpleQueryParam (Identity opTxId)
                                 res1 <- liftIO $ LE.try $ query conn1 (Q.RqQuery $ Q.Query toQStr1 par1)
                                 case res1 of
                                     Right [(bi, inputs)] -> do
                                         case bi of
                                             Nothing -> do
                                                 return $
                                                     addressOutputToResultWithCursor
                                                         address
                                                         ado
                                                         (Q.fromSet inputs)
                                                         Nothing
                                             Just blkinf -> do
                                                 let (bhash, bht, txindx) = blkinf
                                                 return $
                                                     addressOutputToResultWithCursor
                                                         address
                                                         ado
                                                         (Q.fromSet inputs)
                                                         (Just $ BlockInfo' (DT.unpack bhash) bht txindx)
                                     Left (e :: SomeException) -> do
                                         err lg $ LG.msg $ "Error: xGetUtxosAddress: " ++ show e
                                         throw KeyValueDBLookupException)
                    conf
            return $ L.take (fromMaybe maxBound (fromIntegral <$> pgSize)) $ xx

getConfirmedOutputsByAddress ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> Bool
    -> m [((Int64, (DT.Text, Int32)), TxOutput)]
getConfirmedOutputsByAddress address pgSize mbNominalTxIndex isAsc = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        nominalTxIndex =
            case mbNominalTxIndex of
                Just n -> n
                Nothing ->
                    if isAsc
                        then 0
                        else maxNTI
        confOutputsByAddressQuery =
            "SELECT nominal_tx_index, output FROM xoken.script_hash_outputs WHERE script_hash=?" <>
            (if isAsc
                 then " AND nominal_tx_index>? ORDER BY nominal_tx_index ASC"
                 else " AND nominal_tx_index<?")
        queryString =
            fromString confOutputsByAddressQuery :: Q.QueryString Q.R (DT.Text, Int64) (Int64, (DT.Text, Int32))
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
                debug lg $ LG.msg $ "AAAA: scriptHashResults: " ++ show (scriptHashResults)
                debug lg $ LG.msg $ "AAAA: addressResults: " ++ show (addressResults)
                let allOutpoints =
                        fmap head $
                        L.groupBy (\(_, op1) (_, op2) -> op1 == op2) $
                        L.sortBy
                            (\(nti1, _) (nti2, _) ->
                                 if nti1 < nti2
                                     then (if isAsc
                                               then LT
                                               else GT)
                                     else (if isAsc
                                               then GT
                                               else LT))
                            (scriptHashResults ++ addressResults)
                    outpoints =
                        case pgSize of
                            Nothing -> allOutpoints
                            Just pg -> L.take (fromIntegral pg) allOutpoints
                (outpoints, ) <$> (sequence $ (\(_, (txid, index)) -> getTxOutputsData (txid, index)) <$> outpoints)
    return $ zip outpoints outputData

getConfirmedUtxosByAddress ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> Bool
    -> m [((Int64, (DT.Text, Int32)), TxOutput)]
getConfirmedUtxosByAddress address pgSize nominalTxIndex isAsc = do
    lg <- getLogger
    res <- LE.try $ getConfirmedOutputsByAddress address pgSize nominalTxIndex isAsc
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ BC.pack $ "[ERROR] getConfirmedUtxosByAddress: Fetching outputs: " <> show e
            throw KeyValueDBLookupException
        Right outputs -> do
            debug lg $
                LG.msg $
                BC.pack $
                "gcuba: Call to gcoba pgSize: " <>
                (show pgSize) <> " nti: " <> (show nominalTxIndex) <> ", outs: " <> (show outputs)
            let utxos = L.filter (\(_, outputData) -> isNothing $ txSpendInfo outputData) outputs
            debug lg $
                LG.msg $
                BC.pack $
                "gcuba: Call to gcoba pgSize: " <>
                (show pgSize) <> " nti: " <> (show nominalTxIndex) <> ", filtered: " <> (show utxos)
            if (L.length utxos < fromMaybe (L.length utxos) (fromIntegral <$> pgSize)) &&
               (not . L.null $ outputs) && (not . isNothing $ pgSize)
                then do
                    let nextPgSize = 2 * ((fromJust pgSize) - (fromIntegral $ L.length utxos))
                        nextCursor = fst $ fst $ last outputs
                    nextPage <- getConfirmedUtxosByAddress address (Just nextPgSize) (Just nextCursor) isAsc
                    return $ utxos ++ nextPage
                else return utxos

xGetOutputsScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> Bool
    -> m ([ResultWithCursor ScriptOutputs Int64])
xGetOutputsScriptHash scriptHash pgSize mbNominalTxIndex isAsc = do
    lg <- getLogger
    dbe <- getDB
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO (epochType bp2pEnv)
    res <- LE.try $ getConfirmedOutputsByScriptHash scriptHash pgSize mbNominalTxIndex isAsc
    case res of
        Left (e :: SomeException) -> do
            err lg $
                LG.msg $ BC.pack $ "[ERROR] xGetOutputsScriptHash: Fetching confirmed/unconfirmed outputs: " <> show e
            throw KeyValueDBLookupException
        Right conf -> do
            debug lg $ LG.msg $ BC.pack $ "xGetUtxosAddress: Got confirmed utxos: " <> (show conf)
            xx <-
                LA.mapConcurrently
                    (\ado -> do
                         case ado of
                             ((_, (opTxId, _)), _) -> do
                                 let conn1 = xCqlClientState dbe
                                     toStr1 = "SELECT block_info, inputs FROM xoken.transactions WHERE tx_id=?"
                                     toQStr1 =
                                         toStr1 :: Q.QueryString Q.R (Identity DT.Text) ( Maybe (DT.Text, Int32, Int32)
                                                                                        , Set ( (DT.Text, Int32)
                                                                                              , Int32
                                                                                              , (DT.Text, Int64)))
                                     par1 = getSimpleQueryParam (Identity opTxId)
                                 res1 <- liftIO $ LE.try $ query conn1 (Q.RqQuery $ Q.Query toQStr1 par1)
                                 case res1 of
                                     Right [(bi, inputs)] -> do
                                         case bi of
                                             Nothing -> do
                                                 return $
                                                     scriptOutputToResultWithCursor
                                                         scriptHash
                                                         ado
                                                         (Q.fromSet inputs)
                                                         Nothing
                                             Just blkinf -> do
                                                 let (bhash, bht, txindx) = blkinf
                                                 return $
                                                     scriptOutputToResultWithCursor
                                                         scriptHash
                                                         ado
                                                         (Q.fromSet inputs)
                                                         (Just $ BlockInfo' (DT.unpack bhash) bht txindx)
                                     Left (e :: SomeException) -> do
                                         err lg $ LG.msg $ "Error: xGetOutputsScriptHash: " ++ show e
                                         throw KeyValueDBLookupException)
                    conf
            return $ L.take (fromMaybe maxBound (fromIntegral <$> pgSize)) $ xx

xGetUtxosScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> Bool
    -> m ([ResultWithCursor ScriptOutputs Int64])
xGetUtxosScriptHash scriptHash pgSize mbNominalTxIndex isAsc = do
    lg <- getLogger
    dbe <- getDB
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO (epochType bp2pEnv)
    res <- LE.try $ getConfirmedUtxosByScriptHash scriptHash pgSize mbNominalTxIndex isAsc
    case res of
        Left (e :: SomeException) -> do
            err lg $
                LG.msg $ BC.pack $ "[ERROR] xGetUtxosScriptHash: Fetching confirmed/unconfirmed outputs: " <> show e
            throw KeyValueDBLookupException
        Right conf -> do
            debug lg $ LG.msg $ BC.pack $ "xGetUtxosAddress: Got confirmed utxos: " <> (show conf)
            xx <-
                LA.mapConcurrently
                    (\ado -> do
                         case ado of
                             ((_, (opTxId, _)), _) -> do
                                 let conn1 = xCqlClientState dbe
                                     toStr1 = "SELECT block_info, inputs FROM xoken.transactions WHERE tx_id=?"
                                     toQStr1 =
                                         toStr1 :: Q.QueryString Q.R (Identity DT.Text) ( Maybe (DT.Text, Int32, Int32)
                                                                                        , Set ( (DT.Text, Int32)
                                                                                              , Int32
                                                                                              , (DT.Text, Int64)))
                                     par1 = getSimpleQueryParam (Identity opTxId)
                                 res1 <- liftIO $ LE.try $ query conn1 (Q.RqQuery $ Q.Query toQStr1 par1)
                                 case res1 of
                                     Right [(bi, inputs)] -> do
                                         case bi of
                                             Nothing -> do
                                                 return $
                                                     scriptOutputToResultWithCursor
                                                         scriptHash
                                                         ado
                                                         (Q.fromSet inputs)
                                                         Nothing
                                             Just blkinf -> do
                                                 let (bhash, bht, txindx) = blkinf
                                                 return $
                                                     scriptOutputToResultWithCursor
                                                         scriptHash
                                                         ado
                                                         (Q.fromSet inputs)
                                                         (Just $ BlockInfo' (DT.unpack bhash) bht txindx)
                                     Left (e :: SomeException) -> do
                                         err lg $ LG.msg $ "Error: xGetUtxosScriptHash: " ++ show e
                                         throw KeyValueDBLookupException)
                    conf
            return $ L.take (fromMaybe maxBound (fromIntegral <$> pgSize)) $ xx

getConfirmedOutputsByScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> Bool
    -> m [((Int64, (DT.Text, Int32)), TxOutput)]
getConfirmedOutputsByScriptHash scriptHash pgSize mbNominalTxIndex isAsc = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        nominalTxIndex =
            case mbNominalTxIndex of
                Just n -> n
                Nothing ->
                    if isAsc
                        then 0
                        else maxNTI
        confOutputsByScriptHashQuery =
            "SELECT nominal_tx_index, output FROM xoken.script_hash_outputs WHERE script_hash=?" ++
            (if isAsc
                 then " AND nominal_tx_index>? ORDER BY nominal_tx_index ASC"
                 else " AND nominal_tx_index<?")
        queryString =
            fromString confOutputsByScriptHashQuery :: Q.QueryString Q.R (DT.Text, Int64) (Int64, (DT.Text, Int32))
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
                (results, ) <$> (sequence $ (\(_, (txid, index)) -> getTxOutputsData (txid, index)) <$> results)
    return $ zip outpoints outputData

getConfirmedUtxosByScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> Bool
    -> m [((Int64, (DT.Text, Int32)), TxOutput)]
getConfirmedUtxosByScriptHash scriptHash pgSize nominalTxIndex isAsc = do
    lg <- getLogger
    res <- LE.try $ getConfirmedOutputsByScriptHash scriptHash pgSize nominalTxIndex isAsc
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ BC.pack $ "[ERROR] getConfirmedUtxosByScriptHash: Fetching outputs: " <> show e
            throw KeyValueDBLookupException
        Right outputs ->
            let utxos = L.filter (\(_, outputData) -> isNothing $ txSpendInfo outputData) outputs
             in if (L.length utxos < fromMaybe (L.length utxos) (fromIntegral <$> pgSize)) && (not . L.null $ outputs)
                    then do
                        let nextPgSize = (fromJust pgSize) - (fromIntegral $ L.length utxos)
                            nextCursor = fst $ fst $ last outputs
                        nextPage <- getConfirmedUtxosByScriptHash scriptHash (Just nextPgSize) (Just nextCursor) isAsc
                        return $ utxos ++ nextPage
                    else return utxos

runWithManyInputs ::
       (HasXokenNodeEnv env m, MonadIO m, Ord c, Eq r, Integral p, Bounded p)
    => (i -> Maybe p -> Maybe c -> Bool -> m ([ResultWithCursor r c]))
    -> [i]
    -> Maybe p
    -> Maybe c
    -> Bool
    -> m ([ResultWithCursor r c])
runWithManyInputs fx inputs mbPgSize cursor isAsc = do
    let pgSize =
            fromIntegral $
            case mbPgSize of
                Just ps -> ps
                Nothing -> maxBound
    li <- LA.mapConcurrently (\input -> fx input mbPgSize cursor isAsc) inputs
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
