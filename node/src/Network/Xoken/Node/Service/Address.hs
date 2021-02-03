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
                    return $
                        case txg of
                            [x] -> genTxOutputData (txid, index, x, Nothing)
                            [x, y] -> genTxOutputData (txid, index, y, Just x)
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
        toStr =
            "SELECT is_recv,other,value,address FROM xoken.ep_txid_outputs WHERE epoch = ? AND txid=? AND output_index=?"
        toQStr =
            toStr :: Q.QueryString Q.R (Bool, DT.Text, Int32) ( Bool
                                                              , Set ((DT.Text, Int32), Int32, (DT.Text, Int64))
                                                              , Int64
                                                              , DT.Text)
        top = getSimpleQueryParam (ep, txid, index)
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
xGetOutputsAddress address pgSize mbNomTxInd = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    ep <- liftIO $ readTVarIO (epochType bp2pEnv)
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        nominalTxIndex =
            case mbNomTxInd of
                (Just n) -> n
                Nothing -> maxBound
        sh = convertToScriptHash net address
        str = "SELECT nominal_tx_index,output FROM xoken.script_hash_outputs WHERE script_hash=? AND nominal_tx_index<?"
        qstr = str :: Q.QueryString Q.R (DT.Text, Int64) (Int64, (DT.Text, Int32))
        ustr =
            "SELECT nominal_tx_index,output FROM xoken.ep_script_hash_outputs WHERE epoch = ? AND script_hash=? AND nominal_tx_index<?"
        uqstr = ustr :: Q.QueryString Q.R (Bool, DT.Text, Int64) (Int64, (DT.Text, Int32))
        aop = getSimpleQueryParam (DT.pack address, nominalTxIndex)
        shp = getSimpleQueryParam (maybe "" DT.pack sh, nominalTxIndex)
        uaop = getSimpleQueryParam (ep, DT.pack address, nominalTxIndex)
        ushp = getSimpleQueryParam (ep, maybe "" DT.pack sh, nominalTxIndex)
    ures <-
        if nominalTxIndex <= maxBound
            then LE.try $
                 LA.concurrently
                     (case sh of
                          Nothing -> return []
                          Just s -> liftIO $ query conn (Q.RqQuery $ Q.Query uqstr (ushp {pageSize = pgSize})))
                     (case address of
                          ('3':_) -> return []
                          _ -> liftIO $ query conn (Q.RqQuery $ Q.Query uqstr (uaop {pageSize = pgSize})))
            else return $ Right mempty
    (ui, ur) <-
        case ures of
            Right (sr, ar) -> do
                let iops =
                        fmap head $
                        L.groupBy (\(_, x) (_, y) -> x == y) $
                        L.sortBy
                            (\(x, _) (y, _) ->
                                 if x < y
                                     then GT
                                     else LT)
                            (sr ++ ar)
                    iop =
                        case pgSize of
                            Nothing -> iops
                            (Just pg) -> L.take (fromIntegral pg) iops
                (iop, ) <$> (sequence $ (\(_, (txid, index)) -> getUnConfTxOutputsData (txid, index)) <$> iop)
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ "Error: xGetOutputsAddress':" ++ show e
                throw KeyValueDBLookupException
    let pgSize' = maybe Nothing (\p -> Just $ p - (fromIntegral $ length ui)) pgSize
    res <-
        if isNothing pgSize' || pgSize' > Just 0
            then LE.try $
                 LA.concurrently
                     (case sh of
                          Nothing -> return []
                          Just s -> liftIO $ query conn (Q.RqQuery $ Q.Query qstr (shp {pageSize = pgSize'})))
                     (case address of
                          ('3':_) -> return []
                          _ -> liftIO $ query conn (Q.RqQuery $ Q.Query qstr (aop {pageSize = pgSize'})))
            else return $ Right mempty
    (i, r) <-
        case res of
            Right (sr, ar) -> do
                let iops =
                        fmap head $
                        L.groupBy (\(_, x) (_, y) -> x == y) $
                        L.sortBy
                            (\(x, _) (y, _) ->
                                 if x < y
                                     then GT
                                     else LT)
                            (sr ++ ar)
                    iop =
                        case pgSize' of
                            Nothing -> iops
                            (Just pg) -> L.take (fromIntegral pg) iops
                (iop, ) <$> (sequence $ (\(_, (txid, index)) -> getTxOutputsData (txid, index)) <$> iop)
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ "Error: xGetOutputsAddress':" ++ show e
                throw KeyValueDBLookupException
    let z = zip (ui ++ i) (ur ++ r)
        unconfLength = L.length ui
        rwc =
            ((\((nti, (op_txid, op_txidx)), TxOutputData _ _ _ val bi ips si) ->
                  ResultWithCursor
                      (AddressOutputs
                           (address)
                           (OutPoint' (DT.unpack op_txid) (fromIntegral op_txidx))
                           bi
                           si
                           ((\((oph, opi), ii, (_, ov)) ->
                                 (OutPoint' (DT.unpack oph) (fromIntegral opi), fromIntegral ii, fromIntegral ov)) <$>
                            ips)
                           val)
                      nti) <$>)
                (maybe z (flip L.take z . fromIntegral) pgSize')
        (unconf, conf) = (L.take unconfLength rwc, L.drop unconfLength rwc)
    return $ (deleteDuplicateUnconfs compareRWCAddressOutputs unconf conf) ++ conf

xGetUTXOsAddress ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe (DT.Text, Int32)
    -> m ([ResultWithCursor AddressOutputs (DT.Text, Int32)])
xGetUTXOsAddress address pgSize mbFromOutput = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    ep <- liftIO $ readTVarIO $ epochType bp2pEnv
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        sh = convertToScriptHash net address
        fromOutput =
            case mbFromOutput of
                (Just n) -> n
                Nothing -> maxBoundOutput
        str = "SELECT output FROM xoken.script_hash_unspent_outputs WHERE script_hash=? AND output<?"
        qstr = str :: Q.QueryString Q.R (DT.Text, (DT.Text, Int32)) (Identity (DT.Text, Int32))
        ustr = "SELECT output FROM xoken.ep_script_hash_unspent_outputs WHERE epoch = ?  AND script_hash=? AND output<?"
        uqstr = ustr :: Q.QueryString Q.R (Bool, DT.Text, (DT.Text, Int32)) (Identity (DT.Text, Int32))
        aop = getSimpleQueryParam (DT.pack address, fromOutput)
        shp = getSimpleQueryParam (maybe "" DT.pack sh, fromOutput)
        uaop = getSimpleQueryParam (ep, DT.pack address, fromOutput)
        ushp = getSimpleQueryParam (ep, maybe "" DT.pack sh, fromOutput)
    ures <-
        LE.try $
        LA.concurrently
            (case sh of
                 Nothing -> return []
                 Just s -> liftIO $ query conn (Q.RqQuery $ Q.Query uqstr (ushp {pageSize = pgSize})))
            (case address of
                 ('3':_) -> return []
                 _ -> liftIO $ query conn (Q.RqQuery $ Q.Query uqstr (uaop {pageSize = pgSize})))
    (uoi, uop) <-
        case ures of
            Right (sr, ar) -> do
                let iops =
                        fmap head $
                        L.groupBy (\(Identity x) (Identity y) -> x == y) $
                        L.sortBy
                            (\(Identity x) (Identity y) ->
                                 if x < y
                                     then GT
                                     else LT)
                            (sr ++ ar)
                    iop =
                        case pgSize of
                            Nothing -> iops
                            (Just pg) -> L.take (fromIntegral pg) iops
                (iop, ) <$> (sequence $ (\(Identity (txid, index)) -> getUnConfTxOutputsData (txid, index)) <$> iop)
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ "Error: xGetUTXOsAddress:" ++ show e
                throw KeyValueDBLookupException
    let pgSize' = maybe Nothing (\p -> Just (p - (fromIntegral $ length uoi))) pgSize
    res <-
        if isNothing pgSize' || pgSize' > Just 0
            then LE.try $
                 LA.concurrently
                     (case sh of
                          Nothing -> return []
                          Just s -> liftIO $ query conn (Q.RqQuery $ Q.Query qstr (shp {pageSize = pgSize'})))
                     (case address of
                          ('3':_) -> return []
                          _ -> liftIO $ query conn (Q.RqQuery $ Q.Query qstr (aop {pageSize = pgSize'})))
            else return $ Right mempty
    (oi, op) <-
        case res of
            Right (sr, ar) -> do
                let iops =
                        fmap head $
                        L.groupBy (\(Identity x) (Identity y) -> x == y) $
                        L.sortBy
                            (\(Identity x) (Identity y) ->
                                 if x < y
                                     then GT
                                     else LT)
                            (sr ++ ar)
                    iop =
                        case pgSize' of
                            Nothing -> iops
                            (Just pg) -> L.take (fromIntegral pg) iops
                (iop, ) <$> (sequence $ (\(Identity (txid, index)) -> getTxOutputsData (txid, index)) <$> iop)
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ "Error: xGetUTXOsAddress:" ++ show e
                throw KeyValueDBLookupException
    let z = zip (uoi ++ oi) (uop ++ op)
        unconfLength = L.length uoi
        rwc =
            ((\((Identity (op_txid, op_txidx)), TxOutputData _ _ _ val bi ips si) ->
                  ResultWithCursor
                      (AddressOutputs
                           (address)
                           (OutPoint' (DT.unpack op_txid) (fromIntegral op_txidx))
                           bi
                           si
                           ((\((oph, opi), ii, (_, ov)) ->
                                 (OutPoint' (DT.unpack oph) (fromIntegral opi), fromIntegral ii, fromIntegral ov)) <$>
                            ips)
                           val)
                      (op_txid, op_txidx)) <$>)
                z
        (unconf, conf) = (L.take unconfLength rwc, L.drop unconfLength rwc)
    return $ (deleteDuplicateUnconfs compareRWCAddressOutputs unconf conf) ++ conf

xGetOutputsScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> m ([ResultWithCursor ScriptOutputs Int64])
xGetOutputsScriptHash scriptHash pgSize mbNomTxInd = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    ep <- liftIO $ readTVarIO $ epochType bp2pEnv
    let conn = xCqlClientState dbe
        nominalTxIndex =
            case mbNomTxInd of
                (Just n) -> n
                Nothing -> maxBound
        str =
            "SELECT script_hash,nominal_tx_index,output FROM xoken.script_hash_outputs WHERE script_hash=? AND nominal_tx_index<?"
        qstr = str :: Q.QueryString Q.R (DT.Text, Int64) (DT.Text, Int64, (DT.Text, Int32))
        par = getSimpleQueryParam (DT.pack scriptHash, nominalTxIndex)
        ustr =
            "SELECT script_hash,nominal_tx_index,output FROM xoken.ep_script_hash_outputs WHERE epoch = ? AND script_hash=? AND nominal_tx_index<?"
        uqstr = ustr :: Q.QueryString Q.R (Bool, DT.Text, Int64) (DT.Text, Int64, (DT.Text, Int32))
        upar = getSimpleQueryParam (ep, DT.pack scriptHash, nominalTxIndex)
    let uf =
            if nominalTxIndex <= maxBound
                then query conn (Q.RqQuery $ Q.Query uqstr (upar {pageSize = pgSize}))
                else return []
    fres <- LE.try $ liftIO uf
    case fres of
        Right uiop -> do
            let pgSize' = maybe Nothing (\p -> Just $ p - (fromIntegral $ length uiop)) pgSize
            iop <-
                if isNothing pgSize' || pgSize' > Just 0
                    then do
                        res <- LE.try $ liftIO $ query conn (Q.RqQuery $ Q.Query qstr (par {pageSize = pgSize'}))
                        case res of
                            Right i -> return i
                            Left (e :: SomeException) -> do
                                err lg $ LG.msg $ "Error: xGetOutputsScriptHash':" ++ show e
                                return []
                    else return []
            ures <- sequence $ (\(_, _, (txid, index)) -> getUnConfTxOutputsData (txid, index)) <$> uiop
            res <- sequence $ (\(_, _, (txid, index)) -> getTxOutputsData (txid, index)) <$> iop
            let z = zip (uiop ++ iop) (ures ++ res)
                unconfLength = L.length uiop
                rwc =
                    ((\((addr, nti, (op_txid, op_txidx)), TxOutputData _ _ _ val bi ips si) ->
                          ResultWithCursor
                              (ScriptOutputs
                                   (DT.unpack addr)
                                   (OutPoint' (DT.unpack op_txid) (fromIntegral op_txidx))
                                   bi
                                   si
                                   ((\((oph, opi), ii, (_, ov)) ->
                                         ( OutPoint' (DT.unpack oph) (fromIntegral opi)
                                         , fromIntegral ii
                                         , fromIntegral ov)) <$>
                                    ips)
                                   val)
                              nti) <$>)
                        z
                (unconf, conf) = (L.take unconfLength rwc, L.drop unconfLength rwc)
            return $ (deleteDuplicateUnconfs compareRWCScriptOutputs unconf conf) ++ conf
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetOutputsScriptHash':" ++ show e
            throw KeyValueDBLookupException

xGetUTXOsScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe (DT.Text, Int32)
    -> m ([ResultWithCursor ScriptOutputs (DT.Text, Int32)])
xGetUTXOsScriptHash scriptHash pgSize mbFromOutput = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    ep <- liftIO $ readTVarIO $ epochType bp2pEnv
    let conn = xCqlClientState dbe
        fromOutput =
            case mbFromOutput of
                (Just n) -> n
                Nothing -> maxBoundOutput
        str = "SELECT script_hash,output FROM xoken.script_hash_unspent_outputs WHERE script_hash=? AND output<?"
        qstr = str :: Q.QueryString Q.R (DT.Text, (DT.Text, Int32)) (DT.Text, (DT.Text, Int32))
        par = getSimpleQueryParam (DT.pack scriptHash, fromOutput)
        ustr =
            "SELECT script_hash,output FROM xoken.ep_script_hash_unspent_outputs WHERE epoch = ? AND script_hash=? AND output<?"
        uqstr = ustr :: Q.QueryString Q.R (Bool, DT.Text, (DT.Text, Int32)) (DT.Text, (DT.Text, Int32))
        upar = getSimpleQueryParam (ep, DT.pack scriptHash, fromOutput)
    ures <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query uqstr (upar {pageSize = pgSize}))
    (uio, uou) <-
        case ures of
            Right uiop ->
                (uiop, ) <$> (sequence $ (\(_, (txid, index)) -> getUnConfTxOutputsData (txid, index)) <$> uiop)
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ "Error: xGetUTXOsScriptHash':" ++ show e
                throw KeyValueDBLookupException
    let pgSize' = maybe Nothing (\p -> Just $ p - (fromIntegral $ length uio)) pgSize
    res <-
        if isNothing pgSize' || pgSize' > Just 0
            then liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query qstr (par {pageSize = pgSize'}))
            else return $ Right mempty
    (io, ou) <-
        case res of
            Right iop -> do
                (iop, ) <$> (sequence $ (\(_, (txid, index)) -> getTxOutputsData (txid, index)) <$> iop)
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ "Error: xGetUTXOsScriptHash':" ++ show e
                throw KeyValueDBLookupException
    let z = zip (uio ++ io) (uou ++ ou)
        unconfLength = L.length uio
        rwc =
            ((\((addr, (op_txid, op_txidx)), TxOutputData _ _ _ val bi ips si) ->
                  ResultWithCursor
                      (ScriptOutputs
                           (DT.unpack addr)
                           (OutPoint' (DT.unpack op_txid) (fromIntegral op_txidx))
                           bi
                           si
                           ((\((oph, opi), ii, (_, ov)) ->
                                 (OutPoint' (DT.unpack oph) (fromIntegral opi), fromIntegral ii, fromIntegral ov)) <$>
                            ips)
                           val)
                      (op_txid, op_txidx)) <$>)
                z
        (unconf, conf) = (L.take unconfLength rwc, L.drop unconfLength rwc)
    return $ (deleteDuplicateUnconfs compareRWCScriptOutputs unconf conf) ++ conf

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
    