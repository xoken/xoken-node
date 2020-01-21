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
import qualified Database.CQL.IO as Q
import Database.CQL.Protocol
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
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Script.Standard
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random

processTxGetData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
processTxGetData pr txHash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    if indexUnconfirmedTx bp2pEnv == False
        then return ()
        else do
            let net = bncNet $ bitcoinNodeConfig bp2pEnv
            debug lg $ LG.msg $ val "processTxGetData - called."
            bp2pEnv <- getBitcoinP2P
            tuple <- liftIO $ H.lookup (unconfirmedTxCache bp2pEnv) (getTxShortHash $ TxHash txHash)
            case tuple of
                Just (st, fh) ->
                    if st == False
                        then do
                            liftIO $ threadDelay (1000000 * 30)
                            tuple2 <- liftIO $ H.lookup (unconfirmedTxCache bp2pEnv) (getTxShortHash $ TxHash txHash)
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
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    let gd = GetData $ [InvVector InvTx txHash]
        msg = MGetData gd
    debug lg $ LG.msg $ "sendTxGetData: " ++ show gd
    case (bpSocket pr) of
        Just s -> do
            let em = runPut . putMessage net $ msg
            res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
            case res of
                Right () ->
                    liftIO $
                    H.insert (unconfirmedTxCache bp2pEnv) (getTxShortHash $ TxHash txHash) (False, TxHash txHash)
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
        let conn = keyValDB $ dbe'
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
                    p = Q.defQueryParams Q.One $ Identity (not epoch)
                res <- liftIO $ try $ Q.runClient conn (Q.write qstr p)
                case res of
                    Right () -> return ()
                    Left (e :: SomeException) -> do
                        err lg $ LG.msg ("Error: deleting stale epoch Txs: " ++ show e)
                        throw e
                let str = "DELETE from xoken.ep_address_outputs where epoch = ?"
                    qstr = str :: Q.QueryString Q.W (Identity Bool) ()
                    p = Q.defQueryParams Q.One $ Identity (not epoch)
                res <- liftIO $ try $ Q.runClient conn (Q.write qstr p)
                case res of
                    Right () -> return ()
                    Left (e :: SomeException) -> do
                        err lg $ LG.msg ("Error: deleting stale epoch Addr-outputs: " ++ show e)
                        throw e
                liftIO $ threadDelay (1000000 * 60 * 60)
            else liftIO $ threadDelay (1000000 * 60 * (60 - minute))
        return ()

commitEpochAddressOutputs ::
       (HasLogger m, MonadIO m)
    => Q.ClientState
    -> Bool
    -> Text
    -> Bool
    -> Maybe Text
    -> (Text, Int32)
    -> (Text, Int32)
    -> Int64
    -> m ()
commitEpochAddressOutputs conn epoch addr typeRecv otherAddr output prevOutpoint value = do
    lg <- getLogger
    let str =
            "insert INTO xoken.ep_address_outputs ( epoch , address, is_type_receive,other_address, output,  prev_outpoint, value, is_output_spent ) values (?, ?, ?, ?, ?, ? ,? ,? )"
        qstr = str :: Q.QueryString Q.W (Bool, Text, Bool, Maybe Text, (Text, Int32), (Text, Int32), Int64, Bool) ()
        par = Q.defQueryParams Q.One (epoch, addr, typeRecv, otherAddr, output, prevOutpoint, value, False)
    res1 <- liftIO $ try $ Q.runClient conn (Q.write (qstr) par)
    case res1 of
        Right () -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into 'ep_address_outputs': " ++ show e
            throw KeyValueDBInsertException

processUnconfTransaction :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> m ()
processUnconfTransaction tx = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO $ epochType bp2pEnv
    lg <- getLogger
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    let conn = keyValDB $ dbe'
        str = "insert INTO xoken.ep_transactions ( epoch, tx_id, tx_serialized ) values (?, ?, ?)"
        qstr = str :: Q.QueryString Q.W (Bool, Text, Blob) ()
        par =
            Q.defQueryParams
                Q.One
                (epoch, txHashToHex $ txHash tx, Blob $ runPutLazy $ putLazyByteString $ S.encodeLazy tx)
    debug lg $ LG.msg ("processing Unconfirmed Transaction: " ++ show (txHash tx))
    res <- liftIO $ try $ Q.runClient conn (Q.write (qstr) par)
    case res of
        Right () -> liftIO $ H.insert (unconfirmedTxCache bp2pEnv) (getTxShortHash $ txHash tx) (False, txHash tx)
        Left (e :: SomeException) -> do
            liftIO $ err lg $ LG.msg ("Error: INSERTing into 'xoken.ep_transactions': " ++ show e)
            throw KeyValueDBInsertException
    --
    let inAddrs =
            zip3
                (map (\x -> do
                          case decodeInputBS net $ scriptInput x of
                              Left e -> Nothing
                              Right is ->
                                  case inputAddress is of
                                      Just s -> addrToString net s
                                      Nothing -> Nothing)
                     (txIn tx))
                (txIn tx)
                [0 :: Int32 ..]
    let outAddrs =
            zip3
                (catMaybes $
                 map
                     (\y ->
                          case scriptToAddressBS $ scriptOutput y of
                              Left e -> Nothing
                              Right os -> addrToString net os)
                     (txOut tx))
                (txOut tx)
                [0 :: Int32 ..]
    lookupInAddrs <-
        mapM
            (\(a, b, c) ->
                 case a of
                     Just a -> return $ Just (a, b, c)
                     Nothing -> do
                         if (outPointHash nullOutPoint) == (outPointHash $ prevOutput b)
                             then return Nothing
                             else do
                                 res <-
                                     liftIO $
                                     try $
                                     liftIO $
                                     EX.retryBool
                                         (\e ->
                                              case e of
                                                  TxIDNotFoundRetryException -> True
                                                  otherwise -> False)
                                         15
                                         (sourceAddressFromOutpoint conn lg net $ prevOutput b)
                                 case res of
                                     Right (ma) -> do
                                         case (ma) of
                                             Just x ->
                                                 case addrToString net x of
                                                     Just as -> return $ Just (as, b, c)
                                                     Nothing -> throw InvalidAddressException
                                             Nothing -> throw OutpointAddressNotFoundException
                                     Left TxIDNotFoundRetryException -- ignore if ample time elapsed
                                      -> do
                                         return Nothing)
            inAddrs
    mapM_
        (\(x, a, i) ->
             mapM_
                 (\(y, b, j) ->
                      commitEpochAddressOutputs
                          conn
                          epoch
                          x
                          True
                          y
                          (txHashToHex $ txHash tx, i)
                          (txHashToHex $ outPointHash $ prevOutput b, fromIntegral $ outPointIndex $ prevOutput b)
                          (fromIntegral $ outValue a))
                 inAddrs)
        outAddrs
    mapM_
        (\(x, a, i) ->
             mapM_
                 (\(y, b, j) ->
                      commitEpochAddressOutputs
                          conn
                          epoch
                          x
                          False
                          (Just y)
                          (txHashToHex $ txHash tx, i)
                          (txHashToHex $ outPointHash $ prevOutput a, fromIntegral $ outPointIndex $ prevOutput a)
                          (fromIntegral $ outValue b))
                 outAddrs)
        (catMaybes lookupInAddrs)
    return ()

--
--
sourceAddressFromOutpoint :: Q.ClientState -> Logger -> Network -> OutPoint -> IO (Maybe Address)
sourceAddressFromOutpoint conn lg net outPoint = do
    res <- liftIO $ try $ getAddressFromOutpoint conn lg net outPoint
    case res of
        Right (addr) -> do
            case addr of
                Nothing -> getEpochAddressFromOutpoint conn lg net outPoint
                Just a -> return addr
        Left TxIDNotFoundRetryException -> do
            getEpochAddressFromOutpoint conn lg net outPoint

--
--
getEpochAddressFromOutpoint :: Q.ClientState -> Logger -> Network -> OutPoint -> IO (Maybe Address)
getEpochAddressFromOutpoint conn lg net outPoint = do
    let str = "SELECT tx_serialized from xoken.ep_transactions where tx_id = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Identity Blob)
        p = Q.defQueryParams Q.One $ Identity $ txHashToHex $ outPointHash outPoint
    res <- liftIO $ try $ Q.runClient conn (Q.query qstr p)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg ("Error: getEpochAddressFromOutpoint: " ++ show e)
            throw e
        Right (iop) -> do
            if L.length iop == 0
                        -- debug lg $ LG.msg ("(retry) TxID not found: " ++ (show $ txHashToHex $ outPointHash outPoint))
                then do
                    liftIO $ threadDelay (1000000 * 1)
                    throw TxIDNotFoundRetryException
                else do
                    let txbyt = runIdentity $ iop !! 0
                    case runGetLazy (getConfirmedTx) (fromBlob txbyt) of
                        Left e
                                -- debug lg $ LG.msg (encodeHex $ BSL.toStrict $ fromBlob txbyt)
                         -> do
                            throw DBTxParseException
                        Right (txd) -> do
                            case txd of
                                Just tx ->
                                    if (fromIntegral $ outPointIndex outPoint) > (L.length $ txOut tx)
                                        then throw InvalidOutpointException
                                        else do
                                            let output = (txOut tx) !! (fromIntegral $ outPointIndex outPoint)
                                            case scriptToAddressBS $ scriptOutput output of
                                                Left e -> return Nothing
                                                Right os -> return $ Just os
                                Nothing -> undefined
