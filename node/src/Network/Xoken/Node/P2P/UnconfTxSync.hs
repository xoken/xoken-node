{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}

module Network.Xoken.Node.P2P.UnconfTxSync
    ( processUnconfTransaction
    , processTxGetData
    , runEpochSwitcher
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently, race_)
import Control.Concurrent.Async.Lifted as LA (async)
import Control.Concurrent.Event as EV
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import qualified Data.HashTable.IO as H
import Data.Int
import qualified Data.IntMap as I
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Serialize
import Data.Serialize as S
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Time.LocalTime
import Data.Word
import Database.XCQL.Protocol as Q
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Address
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Script.Standard
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import StmContainers.Map as SM
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Xoken.NodeConfig

processTxGetData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
processTxGetData pr txHash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    indexUnconfirmedTx <- liftIO $ readTVarIO $ indexUnconfirmedTx bp2pEnv
    if indexUnconfirmedTx == False
        then return ()
        else do
            let net = bitcoinNetwork $ nodeConfig bp2pEnv
            debug lg $ LG.msg $ val "processTxGetData - called."
            bp2pEnv <- getBitcoinP2P
            tuple <-
                liftIO $
                TSH.lookup
                    (unconfirmedTxCache bp2pEnv)
                    (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
            case tuple of
                Just (st, fh) ->
                    if st == False
                        then do
                            liftIO $ threadDelay (1000000 * 30)
                            tuple2 <-
                                liftIO $
                                TSH.lookup
                                    (unconfirmedTxCache bp2pEnv)
                                    (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
                            case tuple2 of
                                Just (st2, fh2) ->
                                    if st2 == False
                                        then sendTxGetData pr txHash
                                        else return ()
                                Nothing -> return ()
                        else return ()
                Nothing -> sendTxGetData pr txHash

sendTxGetData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
sendTxGetData pr txHash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let gd = GetData $ [InvVector InvTx txHash]
        msg = MGetData gd
    debug lg $ LG.msg $ "sendTxGetData: " ++ show gd
    case (bpSocket pr) of
        Just s -> do
            let em = runPut . putMessage net $ msg
            res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
            case res of
                Right _ ->
                    liftIO $
                    TSH.insert
                        (unconfirmedTxCache bp2pEnv)
                        (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
                        (False, TxHash txHash)
                Left (e :: SomeException) -> debug lg $ LG.msg $ "Error, sending out data: " ++ show e
            debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
        Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"

runEpochSwitcher :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runEpochSwitcher =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        dbe' <- getDB
        tm <- liftIO $ getCurrentTime
        let conn = connection $ dbe'
            hour = todHour $ timeToTimeOfDay $ utctDayTime tm
            minute = todMin $ timeToTimeOfDay $ utctDayTime tm
            epoch =
                case hour `mod` 2 of
                    0 -> True
                    1 -> False
        liftIO $ atomically $ writeTVar (epochType bp2pEnv) epoch
        if minute == 0
            then do
                let str = "DELETE from xoken.ep_transactions where epoch = ?"
                    qstr = str :: Q.QueryString Q.W (Identity Bool) ()
                    p = getSimpleQueryParam $ Identity (not epoch)
                res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr p)
                case res of
                    Right _ -> return ()
                    Left (e :: SomeException) -> do
                        err lg $ LG.msg ("Error: deleting stale epoch Txs: " ++ show e)
                        throw e
                let str = "DELETE from xoken.ep_script_hash_outputs where epoch = ?"
                    qstr = str :: Q.QueryString Q.W (Identity Bool) ()
                    p = getSimpleQueryParam $ Identity (not epoch)
                res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr p)
                case res of
                    Right _ -> return ()
                    Left (e :: SomeException) -> do
                        err lg $ LG.msg ("Error: deleting stale epoch script_hash_outputs: " ++ show e)
                        throw e
                let str = "DELETE from xoken.ep_txid_outputs where epoch = ?"
                    qstr = str :: Q.QueryString Q.W (Identity Bool) ()
                    p = getSimpleQueryParam $ Identity (not epoch)
                res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr p)
                case res of
                    Right _ -> return ()
                    Left (e :: SomeException) -> do
                        err lg $ LG.msg ("Error: deleting stale epoch txid_outputs: " ++ show e)
                        throw e
                liftIO $ threadDelay (1000000 * 60 * 60)
            else liftIO $ threadDelay (1000000 * 60 * (60 - minute))
        return ()

commitEpochScriptHashOutputs ::
       (HasLogger m, MonadIO m)
    => CqlConnection
    -> Bool -- epoch
    -> Text -- scriptHash
    -> (Text, Int32) -- output (txid, index)
    -> m ()
commitEpochScriptHashOutputs conn epoch sh output = do
    lg <- getLogger
    let strAddrOuts = "INSERT INTO xoken.ep_script_hash_outputs (epoch, script_hash, output) VALUES (?,?,?)"
        qstrAddrOuts = strAddrOuts :: Q.QueryString Q.W (Bool, Text, (Text, Int32)) ()
        parAddrOuts = getSimpleQueryParam (epoch, sh, output)
    resAddrOuts <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query (qstrAddrOuts) parAddrOuts)
    case resAddrOuts of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into 'ep_script_hash_outputs': " ++ show e
            throw KeyValueDBInsertException

commitEpochScriptHashUnspentOutputs ::
       (HasLogger m, MonadIO m) => CqlConnection -> Bool -> Text -> (Text, Int32) -> m ()
commitEpochScriptHashUnspentOutputs conn epoch sh output = do
    lg <- getLogger
    let str = "INSERT INTO xoken.ep_script_hash_unspent_outputs (epoch, script_hash, output) VALUES (?,?,?)"
        qstr = str :: Q.QueryString Q.W (Bool, Text, (Text, Int32)) ()
        par = getSimpleQueryParam (epoch, sh, output)
    res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into 'ep_script_hash_unspent_outputs': " ++ show e
            throw KeyValueDBInsertException

deleteEpochScriptHashUnspentOutputs ::
       (HasLogger m, MonadIO m) => CqlConnection -> Bool -> Text -> (Text, Int32) -> m ()
deleteEpochScriptHashUnspentOutputs conn epoch sh output = do
    lg <- getLogger
    let str = "DELETE FROM xoken.ep_script_hash_unspent_outputs WHERE epoch=? AND script_hash=? AND output=?"
        qstr = str :: Q.QueryString Q.W (Bool, Text, (Text, Int32)) ()
        par = getSimpleQueryParam (epoch, sh, output)
    res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: DELETE'ing from 'ep_script_hash_unspent_outputs': " ++ show e
            throw e

insertEpochTxIdOutputs ::
       (HasLogger m, MonadIO m)
    => CqlConnection
    -> Bool
    -> (Text, Int32)
    -> Text
    -> Text
    -> Bool
    -> [((Text, Int32), Int32, (Text, Int64))]
    -> Int64
    -> m ()
insertEpochTxIdOutputs conn epoch (txid, outputIndex) address scriptHash isRecv other value = do
    lg <- getLogger
    let str =
            "INSERT INTO xoken.ep_txid_outputs (epoch,txid,output_index,address,script_hash,is_recv,other,value) VALUES (?,?,?,?,?,?,?,?)"
        qstr =
            str :: Q.QueryString Q.W ( Bool
                                     , Text
                                     , Int32
                                     , Text
                                     , Text
                                     , Bool
                                     , [((Text, Int32), Int32, (Text, Int64))]
                                     , Int64) ()
        par = getSimpleQueryParam (epoch, txid, outputIndex, address, scriptHash, isRecv, other, value)
    res <- liftIO $ try $ write conn $ (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into ep_txid_outputs: " ++ show e
            throw KeyValueDBInsertException

processUnconfTransaction :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> m ()
processUnconfTransaction tx = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    epoch <- liftIO $ readTVarIO $ epochType bp2pEnv
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let conn = connection $ dbe'
    debug lg $ LG.msg $ "Processing unconfirmed transaction: " ++ show (txHash tx)
    --
    let inAddrs = zip (txIn tx) [0 :: Int32 ..]
    let outAddrs =
            zip3
                (map (\y ->
                          case scriptToAddressBS $ scriptOutput y of
                              Left e -> ""
                              Right os ->
                                  case addrToString net os of
                                      Nothing -> ""
                                      Just addr -> addr)
                     (txOut tx))
                (txOut tx)
                [0 :: Int32 ..]
    inputs <-
        mapM
            (\(b, j) -> do
                 tuple <-
                     liftIO $
                     TSH.lookup
                         (txOutputValuesCache bp2pEnv)
                         (getTxShortHash (txHash tx) (txOutputValuesCacheKeyBits $ nodeConfig bp2pEnv))
                 val <-
                     case tuple of
                         Just (ftxh, indexvals) ->
                             if ftxh == (outPointHash $ prevOutput b)
                                 then do
                                     let rr =
                                             head $
                                             filter
                                                 (\x -> fst x == (fromIntegral $ outPointIndex $ prevOutput b))
                                                 indexvals
                                     return $ snd $ rr
                                 else do
                                     valFromDB <-
                                         liftIO $
                                         getSatsValueFromEpochOutpoint
                                             conn
                                             epoch
                                             (txSynchronizer bp2pEnv)
                                             lg
                                             net
                                             (prevOutput b)
                                             (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                                     return valFromDB
                         Nothing -> do
                             valFromDB <-
                                 liftIO $
                                 getSatsValueFromEpochOutpoint
                                     conn
                                     epoch
                                     (txSynchronizer bp2pEnv)
                                     lg
                                     net
                                     (prevOutput b)
                                     (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                             return valFromDB
                 return
                     ((txHashToHex $ outPointHash $ prevOutput b, fromIntegral $ outPointIndex $ prevOutput b), j, val))
            inAddrs
    let ovs =
            map
                (\(a, o, i) ->
                     ( fromIntegral $ i
                     , (a, (txHashToHex $ TxHash $ sha256 (scriptOutput o)), fromIntegral $ outValue o)))
                outAddrs
    liftIO $
        TSH.insert
            (txOutputValuesCache bp2pEnv)
            (getTxShortHash (txHash tx) (txOutputValuesCacheKeyBits $ nodeConfig bp2pEnv))
            (txHash tx, ovs)
    --
    mapM_
        (\(a, o, i) -> do
             let sh = txHashToHex $ TxHash $ sha256 (scriptOutput o)
             let output = (txHashToHex $ txHash tx, i)
             insertEpochTxIdOutputs conn epoch output a sh True (stripScriptHash <$> inputs) (fromIntegral $ outValue o)
             commitEpochScriptHashOutputs conn epoch sh output
             commitEpochScriptHashUnspentOutputs conn epoch sh output
             return ())
        outAddrs
    mapM_
        (\((o, i), (a, sh)) -> do
             let prevOutpoint = (txHashToHex $ outPointHash $ prevOutput o, fromIntegral $ outPointIndex $ prevOutput o)
             let output = (txHashToHex $ txHash tx, i)
             let spendInfo = (\ov -> ((txHashToHex $ txHash tx, fromIntegral $ fst ov), i, snd $ ov)) <$> ovs
             insertEpochTxIdOutputs conn epoch prevOutpoint a sh False (stripScriptHash <$> spendInfo) 0
             deleteEpochScriptHashUnspentOutputs conn epoch sh prevOutpoint)
        (zip inAddrs (map (\x -> (fst3 $ thd3 x, snd3 $ thd3 x)) inputs))
    --
    let ipSum = foldl (+) 0 $ (\(_, _, (_, _, val)) -> val) <$> inputs
        opSum = foldl (+) 0 $ (\(_, o, _) -> fromIntegral $ outValue o) <$> outAddrs
        fees = ipSum - opSum
    --
    let str = "INSERT INTO xoken.ep_transactions (epoch, tx_id, tx_serialized, inputs, fees) values (?, ?, ?, ?, ?)"
        qstr = str :: Q.QueryString Q.W (Bool, Text, Blob, [((Text, Int32), Int32, (Text, Int64))], Int64) ()
        par =
            getSimpleQueryParam
                ( epoch
                , txHashToHex $ txHash tx
                , Blob $ runPutLazy $ putLazyByteString $ S.encodeLazy tx
                , (stripScriptHash <$> inputs)
                , fees)
    res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            liftIO $ err lg $ LG.msg $ "Error: INSERTing into 'xoken.ep_transactions':" ++ show e
            throw KeyValueDBInsertException
    --
    vall <- liftIO $ TSH.lookup (txSynchronizer bp2pEnv) (txHash tx)
    case vall of
        Just ev -> liftIO $ EV.signal $ ev
        Nothing -> return ()

getSatsValueFromEpochOutpoint ::
       CqlConnection
    -> Bool
    -> (TSH.TSHashTable TxHash EV.Event)
    -> Logger
    -> Network
    -> OutPoint
    -> Int
    -> IO ((Text, Text, Int64))
getSatsValueFromEpochOutpoint conn epoch txSync lg net outPoint waitSecs = do
    let str =
            "SELECT address, script_hash, value FROM xoken.ep_txid_outputs WHERE epoch=? AND txid=? AND output_index=?"
        qstr = str :: Q.QueryString Q.R (Bool, Text, Int32) (Text, Text, Int64)
        par = getSimpleQueryParam $ (epoch, txHashToHex $ outPointHash outPoint, fromIntegral $ outPointIndex outPoint)
    res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right results -> do
            if L.length results == 0
                then do
                    debug lg $
                        LG.msg $
                        "[Unconfirmed] Tx not found: " ++
                        (show $ txHashToHex $ outPointHash outPoint) ++ "... waiting for event"
                    valx <- liftIO $ TSH.lookup txSync (outPointHash outPoint)
                    event <-
                        case valx of
                            Just evt -> return evt
                            Nothing -> EV.new
                    liftIO $ TSH.insert txSync (outPointHash outPoint) event
                    tofl <- waitTimeout event (1000000 * (fromIntegral waitSecs))
                    if tofl == False
                        then do
                            liftIO $ TSH.delete txSync (outPointHash outPoint)
                            debug lg $
                                LG.msg $
                                "[Unconfirmed] TxIDNotFoundException: " ++ (show $ txHashToHex $ outPointHash outPoint)
                            throw TxIDNotFoundException
                        else getSatsValueFromEpochOutpoint conn epoch txSync lg net outPoint waitSecs
                else do
                    let (addr, scriptHash, val) = head $ results
                    return $ (addr, scriptHash, val)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getSatsValueFromEpochOutpoint: " ++ show e
            throw e

-- sourceSatValuesFromOutpoint ::
--        CqlConnection -> (SM.Map TxHash EV.Event) -> Logger -> Network -> OutPoint -> Int -> IO (Maybe Text)
-- sourceSatValuesFromOutpoint conn txSync lg net outPoint waitSecs = do
--     res <- liftIO $ try $ getSatsValueFromOutpoint conn txSync lg net outPoint waitSecs
--     case res of
--         Right (addr) -> do
--             case addr of
--                 Nothing -> getSatsValueFromEpochOutpoint conn txSync lg net outPoint waitSecs
--                 Just a -> return addr
--         Left TxIDNotFoundException -> do
--             getSatsValueFromEpochOutpoint conn txSync lg net outPoint waitSecs
--
--
-- sourceScriptHashFromOutpoint ::
--        CqlConnection -> (SM.Map TxHash EV.Event) -> Logger -> Network -> OutPoint -> Int -> IO (Maybe Text)
-- sourceScriptHashFromOutpoint conn txSync lg net outPoint waitSecs = do
--     res <- liftIO $ try $ getScriptHashFromOutpoint conn txSync lg net outPoint waitSecs
--     case res of
--         Right (addr) -> do
--             case addr of
--                 Nothing -> getEpochScriptHashFromOutpoint conn txSync lg net outPoint waitSecs
--                 Just a -> return addr
--         Left TxIDNotFoundException -> do
--             getEpochScriptHashFromOutpoint conn txSync lg net outPoint waitSecs
--
--
-- getEpochScriptHashFromOutpoint ::
--        CqlConnection -> (SM.Map TxHash EV.Event) -> Logger -> Network -> OutPoint -> Int -> IO (Maybe Text)
-- getEpochScriptHashFromOutpoint conn txSync lg net outPoint waitSecs = do
--     let str = "SELECT tx_serialized from xoken.ep_transactions where tx_id = ?"
--         qstr = str :: Q.QueryString Q.R (Identity Text) (Identity Blob)
--         p = getSimpleQueryParam $ Identity $ txHashToHex $ outPointHash outPoint
--     res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
--     case res of
--         Left (e :: SomeException) -> do
--             err lg $ LG.msg ("Error: getEpochScriptHashFromOutpoint: " ++ show e)
--             throw e
--         Right (iop) -> do
--             if L.length iop == 0
--                 then do
--                     debug lg $
--                         LG.msg ("TxID not found: (waiting for event) " ++ (show $ txHashToHex $ outPointHash outPoint))
--                     -- tmap <- liftIO $ takeMVar (txSync)
--                     valx <- liftIO $ atomically $ SM.lookup (outPointHash outPoint) txSync
--                     event <-
--                         case valx of
--                             Just evt -> return evt
--                             Nothing -> EV.new
--                     -- liftIO $ putMVar (txSync) (M.insert (outPointHash outPoint) event tmap)
--                     liftIO $ atomically $ SM.insert event (outPointHash outPoint) txSync
--                     tofl <- waitTimeout event (1000000 * (fromIntegral waitSecs))
--                     if tofl == False
--                             -- liftIO $ putMVar (txSync) (M.delete (outPointHash outPoint) tmap)
--                         then do
--                             liftIO $ atomically $ SM.delete (outPointHash outPoint) txSync
--                             debug lg $ LG.msg ("TxIDNotFoundException" ++ (show $ txHashToHex $ outPointHash outPoint))
--                             throw TxIDNotFoundException
--                         else getEpochScriptHashFromOutpoint conn txSync lg net outPoint waitSecs -- if signalled, try querying DB again so it succeeds
--                 else do
--                     let txbyt = runIdentity $ iop !! 0
--                     case runGetLazy (getConfirmedTx) (fromBlob txbyt) of
--                         Left e -> do
--                             debug lg $ LG.msg (encodeHex $ BSL.toStrict $ fromBlob txbyt)
--                             throw DBTxParseException
--                         Right (txd) -> do
--                             case txd of
--                                 Just tx ->
--                                     if (fromIntegral $ outPointIndex outPoint) > (L.length $ txOut tx)
--                                         then throw InvalidOutpointException
--                                         else do
--                                             let output = (txOut tx) !! (fromIntegral $ outPointIndex outPoint)
--                                             return $ Just $ txHashToHex $ TxHash $ sha256 (scriptOutput output)
--                                 Nothing -> return Nothing
convertToScriptHash :: Network -> String -> Maybe String
convertToScriptHash net s = do
    let addr = stringToAddr net (T.pack s)
    (T.unpack . txHashToHex . TxHash . sha256 . addressToScriptBS) <$> addr
