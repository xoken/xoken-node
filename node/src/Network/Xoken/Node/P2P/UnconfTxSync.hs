{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.UnconfTxSync (
    processUnconfTransaction,
    processTxGetData,
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently, race, race_)
import Control.Concurrent.Async.Lifted (concurrently_)
import Control.Concurrent.Async.Lifted as LA (async, race)
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
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import qualified Data.HashTable as CHT
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
import qualified Data.Text.Encoding as DTE
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import Data.Time.LocalTime
import Data.Word
import Database.XCQL.Protocol as Q
import qualified GHC.Base as GB (id)
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
import Network.Xoken.Script
import Network.Xoken.Script.Standard
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import StmContainers.Map as SM
import Streamly
import Streamly.Prelude (nil, (|:))
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
                Right _ -> do
                    liftIO $
                        TSH.insert
                            (unconfirmedTxCache bp2pEnv)
                            (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
                            (False, TxHash txHash)
                    return ()
                Left (e :: SomeException) -> do
                    debug lg $ LG.msg $ "Error, sending out data: " ++ show e
                    throw e
            debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
        Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"

processUnconfTransaction :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> Int64 -> m ()
processUnconfTransaction tx feeReq = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    epoch <- liftIO $ readTVarIO $ epochType bp2pEnv
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        conn = xCqlClientState $ dbe'
        txhs = txHash tx
    debug lg $ LG.msg $ "Processing unconfirmed transaction <begin> :" ++ show txhs
    --
    let inAddrs = zip (txIn tx) [0 :: Int32 ..]
    let outAddrs =
            zip3
                ( map
                    ( \y ->
                        case scriptToAddressBS $ scriptOutput y of
                            Left e -> ""
                            Right os ->
                                case addrToString net os of
                                    Nothing -> ""
                                    Just addr -> addr
                    )
                    (txOut tx)
                )
                (txOut tx)
                [0 :: Int32 ..]

    !inputs <-
        mapM
            ( \(b, j) -> do
                tuple <- return Nothing
                --  liftIO $
                --  TSH.lookup
                --      (txOutputValuesCache bp2pEnv)
                --      (getTxShortHash (txHash tx) (txOutputValuesCacheKeyBits $ nodeConfig bp2pEnv))
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
                                            getSatsValueFromOutpoint
                                                conn
                                                (txSynchronizer bp2pEnv)
                                                lg
                                                net
                                                (prevOutput b)
                                                (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                                    return valFromDB
                        Nothing -> do
                            valFromDB <-
                                liftIO $
                                    getSatsValueFromOutpoint
                                        conn
                                        (txSynchronizer bp2pEnv)
                                        lg
                                        net
                                        (prevOutput b)
                                        (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                            return valFromDB
                return
                    ((txHashToHex $ outPointHash $ prevOutput b, fromIntegral $ outPointIndex $ prevOutput b), j, val)
            )
            inAddrs
    let ovs =
            map
                ( \(a, o, i) ->
                    ( fromIntegral $ i
                    , (a, (txHashToHex $ TxHash $ sha256 (scriptOutput o)), fromIntegral $ outValue o)
                    )
                )
                outAddrs
    --
    let ipSum = foldl (+) 0 $ (\(_, _, (_, _, val)) -> val) <$> inputs
        opSum = foldl (+) 0 $ (\(_, o, _) -> fromIntegral $ outValue o) <$> outAddrs
        fees = ipSum - opSum
        serbs = runPutLazy $ putLazyByteString $ S.encodeLazy tx
        count = BSL.length serbs
    debug lg $ LG.msg $ "Processing unconfirmed transaction <fetched inputs> :" ++ show txhs

    if fees < feeReq
        then throw FeePolicyNotMetException
        else return ()
    --
    --

    mapM_
        ( \(a, o, i) -> do
            let sh = txHashToHex $ TxHash $ sha256 (scriptOutput o)
            let output = (txHashToHex $ txHash tx, i)
            let bsh = B16.encode $ scriptOutput o
            let (op, rem) = B.splitAt 2 bsh
            let (op_false, op_return, remD) =
                    if op == "6a"
                        then ("00", op, rem)
                        else (\(a, b) -> (op, a, b)) $ B.splitAt 2 rem
            -- outputsExist <- checkOutputDataExists output
            -- unless outputsExist $ do
            commitScriptHashOutputs conn sh output Nothing
            case decodeOutputBS $ scriptOutput o of
                Right so ->
                    if isPayPK so
                        then commitScriptHashOutputs conn a output Nothing
                        else return ()
                Left e -> return ()
            when (op_false == "00" && op_return == "6a") $ do
                props <-
                    case runGet (getPropsG 3) (fst $ B16.decode remD) of
                        Right p -> return p
                        Left str -> do
                            liftIO $ err lg $ LG.msg ("Error: Getting protocol name " ++ show str)
                            return []
                when (isJust (headMaybe props)) $ do
                    let protocol = snd <$> props
                        prot = tail $ L.inits protocol
                    mapM_
                        ( \p ->
                            commitScriptOutputProtocol
                                conn
                                (T.intercalate "." p)
                                output
                                Nothing
                                fees
                                (fromIntegral count)
                        )
                        prot
            insertTxIdOutputs output a sh (fromIntegral $ outValue o)
        )
        outAddrs

    debug lg $ LG.msg $ "Processing unconfirmed transaction <committed outputs> :" ++ show txhs

    mapM_
        ( \((o, i), (a, sh)) -> do
            let prevOutpoint = (txHashToHex $ outPointHash $ prevOutput o, fromIntegral $ outPointIndex $ prevOutput o)
            let output = (txHashToHex $ txHash tx, i)
            let spendInfo = (\ov -> ((txHashToHex $ txHash tx, fromIntegral $ fst ov), i, snd $ ov)) <$> ovs
            -- this will fail to update for coinbase txns, its cool
            updateSpendInfoOutputs (fst prevOutpoint) (snd prevOutpoint) (txHashToHex txhs, i)
        )
        (zip inAddrs (map (\x -> (fst3 $ thd3 x, snd3 $ thd3 x)) inputs))
    debug lg $ LG.msg $ "Processing unconfirmed transaction <updated inputs> :" ++ show txhs
    let str = "UPDATE xoken.transactions SET tx_serialized=?, inputs=?, fees=? WHERE tx_id=?"
        qstr = str :: Q.QueryString Q.W (Blob, [((Text, Int32), Int32, (Text, Int64))], Int64, Text) ()
        serbs = runPutLazy $ putLazyByteString $ S.encodeLazy tx
        count = BSL.length serbs
        smb a = a * 16 * 1000 * 1000
        segments =
            let (d, m) = divMod count (smb 1)
             in d
                    + ( if m == 0
                            then 0
                            else 1
                      )
        fst =
            if segments > 1
                then (LC.replicate 32 'f') <> (BSL.fromStrict $ DTE.encodeUtf8 $ T.pack $ show $ segments)
                else serbs
    let par = getSimpleQueryParam (Blob fst, (stripScriptHash <$> inputs), fees, txHashToHex $ txHash tx)
    queryI <- liftIO $ queryPrepared conn (Q.RqPrepare $ Q.Prepare qstr)
    res <- liftIO $ try $ write conn (Q.RqExecute $ Q.Execute queryI par)
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            liftIO $ err lg $ LG.msg ("Error: INSERTing into 'xoken.transactions': " ++ show e)
            throw KeyValueDBInsertException
    when (segments > 1) $ do
        let segmentsData = chunksOf (smb 1) serbs
        mapM_
            ( \(seg, i) -> do
                let par = getSimpleQueryParam (Blob seg, [], fees, (txHashToHex $ txHash tx) <> (T.pack $ show i))
                queryI <- liftIO $ queryPrepared conn (Q.RqPrepare $ Q.Prepare qstr)
                res <- liftIO $ try $ write conn (Q.RqExecute $ Q.Execute queryI par)
                case res of
                    Right _ -> return ()
                    Left (e :: SomeException) -> do
                        liftIO $ err lg $ LG.msg ("Error: INSERTing into 'xoken.transactions': " ++ show e)
                        throw KeyValueDBInsertException
            )
            (zip segmentsData [1 ..])
    --
    debug lg $ LG.msg $ "Processing unconfirmed transaction <end> :" ++ show txhs

    vall <- liftIO $ TSH.lookup (txSynchronizer bp2pEnv) (txHash tx, ChildTxWaiting)
    case vall of
        Just ev -> do
            liftIO $ tryPutMVar ev () --EV.set ev
            return ()
        Nothing -> return ()

--

convertToScriptHash :: Network -> String -> Maybe String
convertToScriptHash net s = do
    let addr = stringToAddr net (T.pack s)
    (T.unpack . txHashToHex . TxHash . sha256 . addressToScriptBS) <$> addr
