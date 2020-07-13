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
import Codec.Compression.GZip as GZ
import Codec.Serialise
import Conduit hiding (runResourceT)
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

xGetChainInfo :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m (Maybe ChainInfo)
xGetChainInfo = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB $ dbe
        str = "SELECT key,value from xoken.misc_store"
        qstr = str :: Q.QueryString Q.R () (DT.Text, (Maybe Bool, Int32, Maybe Int64, DT.Text))
        p = Q.defQueryParams Q.One ()
    res <- LE.try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            if L.length iop < 3
                then do
                    return Nothing
                else do
                    let (_, blocks, _, _) = snd . head $ (L.filter (\x -> fst x == "best-synced") iop)
                        (_, headers, _, bestBlockHash) = snd . head $ (L.filter (\x -> fst x == "best_chain_tip") iop)
                        (_, lagHeight, _, chainwork) = snd . head $ (L.filter (\x -> fst x == "chain-work") iop)
                    blk <- xGetBlockHeight headers
                    lagCW <- calculateChainWork [(lagHeight + 1) .. (headers)] conn
                    case blk of
                        Nothing -> return Nothing
                        Just b -> do
                            return $
                                Just $
                                ChainInfo
                                    "main"
                                    (showHex (lagCW + (read . DT.unpack $ chainwork)) "")
                                    (convertBitsToDifficulty . blockBits . rbHeader $ b)
                                    (headers)
                                    (blocks)
                                    (rbHash b)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetChainInfo: " ++ show e
            throw KeyValueDBLookupException

xGetBlockHash :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> m (Maybe BlockRecord)
xGetBlockHash hash = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB (dbe)
        str =
            "SELECT block_hash,block_height,block_header,next_block_hash,block_size,tx_count,coinbase_tx from xoken.blocks_by_hash where block_hash = ?"
        qstr =
            str :: Q.QueryString Q.R (Identity DT.Text) ( DT.Text
                                                        , Int32
                                                        , DT.Text
                                                        , Maybe DT.Text
                                                        , Maybe Int32
                                                        , Maybe Int32
                                                        , Maybe Blob)
        p = Q.defQueryParams Q.One $ Identity hash
    res <- LE.try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return Nothing
                else do
                    let (hs, ht, hdr, nbhs, size, txc, cbase) = iop !! 0
                    case eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 hdr of
                        Right bh ->
                            return $
                            Just $
                            BlockRecord
                                (fromIntegral ht)
                                (DT.unpack hs)
                                bh
                                (maybe "" DT.unpack nbhs)
                                (maybe (-1) fromIntegral size)
                                (maybe (-1) fromIntegral txc)
                                ("")
                                (maybe "" (coinbaseTxToMessage . fromBlob) cbase)
                                (maybe "" fromBlob cbase)
                        Left err -> do
                            liftIO $ print $ "Decode failed with error: " <> show err
                            return Nothing
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHash: " ++ show e
            throw KeyValueDBLookupException

xGetBlocksHashes :: (HasXokenNodeEnv env m, MonadIO m) => [DT.Text] -> m ([BlockRecord])
xGetBlocksHashes hashes = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB (dbe)
        str =
            "SELECT block_hash,block_height,block_header,next_block_hash,block_size,tx_count,coinbase_tx from xoken.blocks_by_hash where block_hash in ?"
        qstr =
            str :: Q.QueryString Q.R (Identity [DT.Text]) ( DT.Text
                                                          , Int32
                                                          , DT.Text
                                                          , Maybe DT.Text
                                                          , Maybe Int32
                                                          , Maybe Int32
                                                          , Maybe Blob)
        p = Q.defQueryParams Q.One $ Identity $ hashes
    res <- LE.try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return []
                else do
                    case traverse
                             (\(hs, ht, hdr, nbhs, size, txc, cbase) ->
                                  case (eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 hdr) of
                                      (Right bh) ->
                                          Right $
                                          BlockRecord
                                              (fromIntegral ht)
                                              (DT.unpack hs)
                                              bh
                                              (maybe "" DT.unpack nbhs)
                                              (maybe (-1) fromIntegral size)
                                              (maybe (-1) fromIntegral txc)
                                              ("")
                                              (maybe "" (coinbaseTxToMessage . fromBlob) cbase)
                                              (maybe "" fromBlob cbase)
                                      Left err -> Left err)
                             (iop) of
                        Right x -> return x
                        Left err -> do
                            liftIO $ print $ "decode failed for blockrecord: " <> show err
                            return []
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHashes: " ++ show e
            throw KeyValueDBLookupException

xGetBlockHeight :: (HasXokenNodeEnv env m, MonadIO m) => Int32 -> m (Maybe BlockRecord)
xGetBlockHeight height = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB (dbe)
        str =
            "SELECT block_hash,block_height,block_header,next_block_hash,block_size,tx_count,coinbase_tx from xoken.blocks_by_height where block_height = ?"
        qstr =
            str :: Q.QueryString Q.R (Identity Int32) ( DT.Text
                                                      , Int32
                                                      , DT.Text
                                                      , Maybe DT.Text
                                                      , Maybe Int32
                                                      , Maybe Int32
                                                      , Maybe Blob)
        p = Q.defQueryParams Q.One $ Identity height
    res <- LE.try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return Nothing
                else do
                    let (hs, ht, hdr, nbhs, size, txc, cbase) = iop !! 0
                    case eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 hdr of
                        Right bh -> do
                            return $
                                Just $
                                BlockRecord
                                    (fromIntegral ht)
                                    (DT.unpack hs)
                                    bh
                                    (maybe "" DT.unpack nbhs)
                                    (maybe (-1) fromIntegral size)
                                    (maybe (-1) fromIntegral txc)
                                    ("")
                                    (maybe "" (coinbaseTxToMessage . fromBlob) cbase)
                                    (maybe "" fromBlob cbase)
                        Left err -> do
                            liftIO $ print $ "Decode failed with error: " <> show err
                            return Nothing
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlockHeight: " <> show e
            throw KeyValueDBLookupException

xGetTxOutputSpendStatus :: (HasXokenNodeEnv env m, MonadIO m) => String -> Int32 -> m (Maybe TxOutputSpendStatus)
xGetTxOutputSpendStatus txId outputIndex = do
    dbe <- getDB
    let conn = keyValDB (dbe)
        str = "SELECT is_recv, block_info, other FROM xoken.txid_outputs WHERE txid=? AND output_index=?"
        qstr =
            str :: Q.QueryString Q.R (DT.Text, Int32) ( Bool
                                                      , (DT.Text, Int32, Int32)
                                                      , Set ((DT.Text, Int32), Int32, (DT.Text, Int64)))
        p = Q.defQueryParams Q.One (DT.pack txId, outputIndex)
    iop <- Q.runClient conn (Q.query qstr p)
    if length iop == 0
        then return Nothing
        else do
            if L.length iop == 1
                then return $ Just $ TxOutputSpendStatus False Nothing Nothing Nothing
                else do
                    let siop = L.sortBy (\(x, _, _) (y, _, _) -> compare x y) iop
                        (_, (_, spendingTxBlkHeight, _), other) = siop !! 0
                        ((spendingTxID, _), spendingTxIndex, _) = head $ DCP.fromSet other
                    return $
                        Just $
                        TxOutputSpendStatus
                            True
                            (Just $ DT.unpack spendingTxID)
                            (Just spendingTxBlkHeight)
                            (Just spendingTxIndex)

xGetBlocksHeights :: (HasXokenNodeEnv env m, MonadIO m) => [Int32] -> m ([BlockRecord])
xGetBlocksHeights heights = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB (dbe)
        str =
            "SELECT block_hash,block_height,block_header,next_block_hash,block_size,tx_count,coinbase_tx from xoken.blocks_by_height where block_height in ?"
        qstr =
            str :: Q.QueryString Q.R (Identity [Int32]) ( DT.Text
                                                        , Int32
                                                        , DT.Text
                                                        , Maybe DT.Text
                                                        , Maybe Int32
                                                        , Maybe Int32
                                                        , Maybe Blob)
        p = Q.defQueryParams Q.One $ Identity $ heights
    res <- LE.try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then return []
                else do
                    case traverse
                             (\(hs, ht, hdr, nbhs, size, txc, cbase) ->
                                  case (eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 hdr) of
                                      (Right bh) ->
                                          Right $
                                          BlockRecord
                                              (fromIntegral ht)
                                              (DT.unpack hs)
                                              bh
                                              (maybe "" DT.unpack nbhs)
                                              (maybe (-1) fromIntegral size)
                                              (maybe (-1) fromIntegral txc)
                                              ("")
                                              (maybe "" (coinbaseTxToMessage . fromBlob) cbase)
                                              (maybe "" fromBlob cbase)
                                      Left err -> Left err)
                             (iop) of
                        Right x -> return x
                        Left err -> do
                            liftIO $ print $ "decode failed for blockrecord: " <> show err
                            return []
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlockHeights: " ++ show e
            throw KeyValueDBLookupException

xGetTxIDsByBlockHash :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => String -> Int32 -> Int32 -> m [String]
xGetTxIDsByBlockHash hash pgSize pgNum = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB $ dbe
        txsToSkip = pgSize * (pgNum - 1)
        firstPage = (+ 1) $ fromIntegral $ floor $ (fromIntegral txsToSkip) / 100
        lastPage = (+ 1) $ fromIntegral $ floor $ (fromIntegral $ txsToSkip + pgSize) / 100
        txDropFromFirst = fromIntegral $ txsToSkip `mod` 100
        str = "SELECT page_number, txids from xoken.blockhash_txids where block_hash = ? and page_number in ? "
        qstr = str :: Q.QueryString Q.R (DT.Text, [Int32]) (Int32, [DT.Text])
        p = Q.defQueryParams Q.One $ (DT.pack hash, [firstPage .. lastPage])
    res <- liftIO $ try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop ->
            return . L.take (fromIntegral pgSize) . L.drop txDropFromFirst . L.concat $ (fmap DT.unpack . snd) <$> iop
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxIDsByBlockHash: " <> show e
            throw KeyValueDBLookupException

xGetTxHash :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> m (Maybe RawTxRecord)
xGetTxHash hash = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = keyValDB (dbe)
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        str = "SELECT tx_id, block_info, tx_serialized, inputs, fees from xoken.transactions where tx_id = ?"
        qstr =
            str :: Q.QueryString Q.R (Identity DT.Text) ( DT.Text
                                                        , (DT.Text, Int32, Int32)
                                                        , Blob
                                                        , Set ((DT.Text, Int32), Int32, (DT.Text, Int64))
                                                        , Int64)
        p = Q.defQueryParams Q.One $ Identity $ hash
    res <-
        LE.try $
        LA.concurrently
            (LA.concurrently (Q.runClient conn (Q.query qstr p)) (getTxOutputsFromTxId hash))
            (xGetMerkleBranch $ DT.unpack hash)
    case res of
        Right ((iop, outs), mrkl) ->
            if length iop == 0
                then return Nothing
                else do
                    let (txid, (bhash, blkht, txind), sz, sinps, fees) = iop !! 0
                        inps = L.sortBy (\(_, x, _) (_, y, _) -> compare x y) $ DCP.fromSet sinps
                        tx = fromJust $ Extra.hush $ S.decodeLazy $ fromBlob sz
                    return $
                        Just $
                        RawTxRecord
                            (DT.unpack txid)
                            (fromIntegral $ C.length $ fromBlob sz)
                            (BlockInfo' (DT.unpack bhash) (fromIntegral blkht) (fromIntegral txind))
                            (fromBlob sz)
                            (zipWith mergeTxOutTxOutput (txOut tx) outs)
                            (zipWith mergeTxInTxInput (txIn tx) $
                             (\((outTxId, outTxIndex), inpTxIndex, (addr, value)) ->
                                  TxInput (DT.unpack outTxId) outTxIndex inpTxIndex (DT.unpack addr) value "") <$>
                             inps)
                            fees
                            mrkl
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxHash: " ++ show e
            throw KeyValueDBLookupException

xGetTxHashes :: (HasXokenNodeEnv env m, MonadIO m) => [DT.Text] -> m ([RawTxRecord])
xGetTxHashes hashes = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = keyValDB (dbe)
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        str = "SELECT tx_id, block_info, tx_serialized, inputs, fees from xoken.transactions where tx_id in ?"
        qstr =
            str :: Q.QueryString Q.R (Identity [DT.Text]) ( DT.Text
                                                          , (DT.Text, Int32, Int32)
                                                          , Blob
                                                          , Set ((DT.Text, Int32), Int32, (DT.Text, Int64))
                                                          , Int64)
        p = Q.defQueryParams Q.One $ Identity $ hashes
    res <- LE.try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            txRecs <-
                traverse
                    (\(txid, (bhash, blkht, txind), sz, sinps, fees) -> do
                         let inps = L.sortBy (\(_, x, _) (_, y, _) -> compare x y) $ DCP.fromSet sinps
                             tx = fromJust $ Extra.hush $ S.decodeLazy $ fromBlob sz
                         res' <-
                             LE.try $ LA.concurrently (getTxOutputsFromTxId txid) (xGetMerkleBranch $ DT.unpack txid)
                         case res' of
                             Right (outs, mrkl) ->
                                 return $
                                 Just $
                                 RawTxRecord
                                     (DT.unpack txid)
                                     (fromIntegral $ C.length $ fromBlob sz)
                                     (BlockInfo' (DT.unpack bhash) (fromIntegral blkht) (fromIntegral txind))
                                     (fromBlob sz)
                                     (zipWith mergeTxOutTxOutput (txOut tx) outs)
                                     (zipWith mergeTxInTxInput (txIn tx) $
                                      (\((outTxId, outTxIndex), inpTxIndex, (addr, value)) ->
                                           TxInput (DT.unpack outTxId) outTxIndex inpTxIndex (DT.unpack addr) value "") <$>
                                      inps)
                                     fees
                                     mrkl
                             Left (e :: SomeException) -> do
                                 err lg $ LG.msg $ "Error: xGetTxHashes: " ++ show e
                                 return Nothing)
                    iop
            return $ fromMaybe [] (sequence txRecs)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxHashes: " ++ show e
            throw KeyValueDBLookupException

getTxOutputsFromTxId :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => DT.Text -> m [TxOutput]
getTxOutputsFromTxId txid = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB (dbe)
        toStr = "SELECT output_index,block_info,is_recv,other,value,address FROM xoken.txid_outputs WHERE txid=?"
        toQStr =
            toStr :: Q.QueryString Q.R (Identity DT.Text) ( Int32
                                                          , (DT.Text, Int32, Int32)
                                                          , Bool
                                                          , Set ((DT.Text, Int32), Int32, (DT.Text, Int64))
                                                          , Int64
                                                          , DT.Text)
        par = Q.defQueryParams Q.One (Identity txid)
    res <- LE.try $ Q.runClient conn (Q.query toQStr par)
    case res of
        Right t -> do
            if length t == 0
                then do
                    err lg $ LG.msg $ "Error: getTxOutputsFromTxId: No entry in txid_outputs for txid: " ++ show txid
                    return []
                else do
                    let txg =
                            (L.sortBy (\(_, _, x, _, _, _) (_, _, y, _, _, _) -> compare x y)) <$>
                            (L.groupBy (\(x, _, _, _, _, _) (y, _, _, _, _, _) -> x == y) t)
                        txOutData =
                            (\inp ->
                                 case inp of
                                     [(idx, bif, recv, oth, val, addr)] ->
                                         genTxOutputData (txid, idx, (bif, recv, oth, val, addr), Nothing)
                                     [(idx1, bif1, recv1, oth1, val1, addr1), (_, bif2, recv2, oth2, val2, addr2)] ->
                                         genTxOutputData
                                             ( txid
                                             , idx1
                                             , (bif1, recv1, oth1, val1, addr1)
                                             , Just (bif2, recv2, oth2, val2, addr2))) <$>
                            txg
                    return $ txOutputDataToOutput <$> txOutData
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getTxOutputsFromTxId: " ++ show e
            throw KeyValueDBLookupException

getTxOutputsData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => (DT.Text, Int32) -> m TxOutputData
getTxOutputsData (txid, index) = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB (dbe)
        toStr = "SELECT block_info,is_recv,other,value,address FROM xoken.txid_outputs WHERE txid=? AND output_index=?"
        toQStr =
            toStr :: Q.QueryString Q.R (DT.Text, Int32) ( (DT.Text, Int32, Int32)
                                                        , Bool
                                                        , Set ((DT.Text, Int32), Int32, (DT.Text, Int64))
                                                        , Int64
                                                        , DT.Text)
        top = Q.defQueryParams Q.One (txid, index)
    toRes <- LE.try $ Q.runClient conn (Q.query toQStr top)
    case toRes of
        Right es -> do
            if length es == 0
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
                            [x, y] -> genTxOutputData (txid, index, x, Just y)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getTxOutputsData: " ++ show e
            throw KeyValueDBLookupException

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
    let conn = keyValDB (dbe)
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        nominalTxIndex =
            case mbNomTxInd of
                (Just n) -> n
                Nothing -> maxBound
        sh = convertToScriptHash net address
        str = "SELECT nominal_tx_index,output FROM xoken.script_hash_outputs WHERE script_hash=? AND nominal_tx_index<?"
        qstr = str :: Q.QueryString Q.R (DT.Text, Int64) (Int64, (DT.Text, Int32))
        aop = Q.defQueryParams Q.One (DT.pack address, nominalTxIndex)
        shp = Q.defQueryParams Q.One (maybe "" DT.pack sh, nominalTxIndex)
    res <-
        LE.try $
        LA.concurrently
            (case sh of
                 Nothing -> return []
                 Just s -> Q.runClient conn (Q.query qstr (shp {pageSize = pgSize})))
            (case address of
                 ('3':_) -> return []
                 _ -> Q.runClient conn (Q.query qstr (aop {pageSize = pgSize})))
    case res of
        Right (sr, ar) -> do
            let iops =
                    fmap head $
                    L.groupBy (\(x, _) (y, _) -> x == y) $
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
            if length iop == 0
                then return []
                else do
                    res' <- sequence $ (\(_, (txid, index)) -> getTxOutputsData (txid, index)) <$> iop
                    return $
                        ((\((nti, (op_txid, op_txidx)), TxOutputData _ _ _ val bi ips si) ->
                              ResultWithCursor
                                  (AddressOutputs
                                       (address)
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
                            (zip iop res')
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetOutputsAddress':" ++ show e
            throw KeyValueDBLookupException

xGetOutputsScriptHash ::
       (HasXokenNodeEnv env m, MonadIO m)
    => String
    -> Maybe Int32
    -> Maybe Int64
    -> m ([ResultWithCursor ScriptOutputs Int64])
xGetOutputsScriptHash scriptHash pgSize mbNomTxInd = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB (dbe)
        nominalTxIndex =
            case mbNomTxInd of
                (Just n) -> n
                Nothing -> maxBound
        str =
            "SELECT script_hash,nominal_tx_index,output FROM xoken.script_hash_outputs WHERE script_hash=? AND nominal_tx_index<?"
        qstr = str :: Q.QueryString Q.R (DT.Text, Int64) (DT.Text, Int64, (DT.Text, Int32))
        par = Q.defQueryParams Q.One (DT.pack scriptHash, nominalTxIndex)
    res <- LE.try $ Q.runClient conn (Q.query qstr (par {pageSize = pgSize}))
    case res of
        Right iop -> do
            if length iop == 0
                then return []
                else do
                    res <- sequence $ (\(_, _, (txid, index)) -> getTxOutputsData (txid, index)) <$> iop
                    return $
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
                            (zip iop res)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetOutputsScriptHash':" ++ show e
            throw KeyValueDBLookupException

runManyInputsFor ::
       (HasXokenNodeEnv env m, MonadIO m, Ord c, Eq r, Integral p, Bounded p)
    => (i -> Maybe p -> Maybe c -> m ([ResultWithCursor r c]))
    -> [i]
    -> Maybe p
    -> Maybe c
    -> m ([ResultWithCursor r c])
runManyInputsFor fx inputs pgSize cursor = do
    li <- LA.mapConcurrently (\input -> fx input pgSize cursor) inputs
    return $ (L.take (fromMaybePageSize pgSize) . sort . concat $ li)

xGetMerkleBranch :: (HasXokenNodeEnv env m, MonadIO m) => String -> m ([MerkleBranchNode'])
xGetMerkleBranch txid = do
    dbe <- getDB
    lg <- getLogger
    res <- liftIO $ try $ withResource (pool $ graphDB dbe) (`BT.run` queryMerkleBranch (DT.pack txid))
    case res of
        Right mb -> do
            return $ Data.List.map (\x -> MerkleBranchNode' (DT.unpack $ _nodeValue x) (_isLeftNode x)) mb
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetMerkleBranch: " ++ show e
            throw KeyValueDBLookupException

xGetAllegoryNameBranch :: (HasXokenNodeEnv env m, MonadIO m) => String -> Bool -> m ([(OutPoint', [MerkleBranchNode'])])
xGetAllegoryNameBranch name isProducer = do
    dbe <- getDB
    lg <- getLogger
    res <- liftIO $ try $ withResource (pool $ graphDB dbe) (`BT.run` queryAllegoryNameBranch (DT.pack name) isProducer)
    case res of
        Right nb -> do
            liftIO $
                mapConcurrently
                    (\x -> do
                         let sp = DT.split (== ':') x
                         let txid = DT.unpack $ sp !! 0
                         let index = readMaybe (DT.unpack $ sp !! 1) :: Maybe Int32
                         case index of
                             Just i -> do
                                 rs <-
                                     liftIO $
                                     try $ withResource (pool $ graphDB dbe) (`BT.run` queryMerkleBranch (DT.pack txid))
                                 case rs of
                                     Right mb -> do
                                         let mnodes =
                                                 Data.List.map
                                                     (\y -> MerkleBranchNode' (DT.unpack $ _nodeValue y) (_isLeftNode y))
                                                     mb
                                         return $ (OutPoint' txid i, mnodes)
                                     Left (e :: SomeException) -> do
                                         err lg $ LG.msg $ "Error: xGetMerkleBranch: " ++ show e
                                         throw KeyValueDBLookupException
                             Nothing -> throw KeyValueDBLookupException)
                    (nb)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetAllegoryNameBranch: " ++ show e
            throw KeyValueDBLookupException

getOrMakeProducer :: (HasXokenNodeEnv env m, MonadIO m) => [Int] -> m (((OutPoint', DT.Text), Bool))
getOrMakeProducer nameArr = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let name = DT.pack $ L.map (\x -> chr x) (nameArr)
    let anutxos = NC.allegoryNameUtxoSatoshis $ nodeConfig $ bp2pEnv
    res <- liftIO $ try $ withResource (pool $ graphDB dbe) (`BT.run` queryAllegoryNameScriptOp (name) True)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "error fetching allegory name input :" ++ show e
            throw e
        Right [] -> do
            debug lg $ LG.msg $ "allegory name not found, create recursively (1): " <> name
            createCommitImplictTx (nameArr)
            inres <- liftIO $ try $ withResource (pool $ graphDB dbe) (`BT.run` queryAllegoryNameScriptOp (name) True)
            case inres of
                Left (e :: SomeException) -> do
                    err lg $ LG.msg $ "error fetching allegory name input :" ++ show e
                    throw e
                Right [] -> do
                    err lg $ LG.msg $ "allegory name still not found, recursive create must've failed (1): " <> name
                    throw KeyValueDBLookupException
                Right nb -> do
                    liftIO $ print $ "nb2~" <> show nb
                    let sp = DT.split (== ':') $ fst (head nb)
                    let txid = DT.unpack $ sp !! 0
                    let index = readMaybe (DT.unpack $ sp !! 1) :: Maybe Int
                    case index of
                        Just i -> return $ ((OutPoint' txid (fromIntegral i), (snd $ head nb)), False)
                        Nothing -> throw KeyValueDBLookupException
        Right nb -> do
            debug lg $ LG.msg $ "allegory name found! (1): " <> name
            let sp = DT.split (== ':') $ fst (head nb)
            let txid = DT.unpack $ sp !! 0
            let index = readMaybe (DT.unpack $ sp !! 1) :: Maybe Int
            case index of
                Just i -> return $ ((OutPoint' txid (fromIntegral i), (snd $ head nb)), True)
                Nothing -> throw KeyValueDBLookupException

createCommitImplictTx :: (HasXokenNodeEnv env m, MonadIO m) => [Int] -> m ()
createCommitImplictTx nameArr = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    alg <- getAllegory
    let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
    (nameip, existed) <- getOrMakeProducer (init nameArr)
    let anutxos = NC.allegoryNameUtxoSatoshis $ nodeConfig $ bp2pEnv
    let ins =
            L.map
                (\(x, s) ->
                     TxIn (OutPoint (fromString $ opTxHash x) (fromIntegral $ opIndex x)) (fromJust $ decodeHex s) 0)
                ([nameip])
        -- construct OP_RETURN
    let al =
            Allegory
                1
                (init nameArr)
                (ProducerAction
                     (Index 0)
                     (ProducerOutput (Index 1) (Just $ Endpoint "XokenP2P" "someuri-1"))
                     Nothing
                     [ (ProducerExtension
                            (ProducerOutput (Index 2) (Just $ Endpoint "XokenP2P" "someuri-2"))
                            (last nameArr))
                     , (OwnerExtension (OwnerOutput (Index 3) (Just $ Endpoint "XokenP2P" "someuri-3")) (last nameArr))
                     ])
    let opRetScript = frameOpReturn $ C.toStrict $ serialise al
        -- derive producer's Address
    let prAddr = pubKeyAddr $ derivePubKeyI $ wrapSecKey True $ allegorySecretKey alg
    let prScript = addressToScriptBS prAddr
    let !outs = [TxOut 0 opRetScript] ++ L.map (\_ -> TxOut (fromIntegral anutxos) prScript) [1, 2, 3]
    let !sigInputs =
            L.map
                (\x -> do SigInput (addressToOutput x) (fromIntegral anutxos) (prevOutput $ head ins) sigHashAll Nothing)
                [prAddr, prAddr]
    let psatx = Tx version ins outs locktime
    case signTx net psatx sigInputs [allegorySecretKey alg] of
        Right tx -> do
            xRelayTx $ Data.Serialize.encode tx
            return ()
        Left err -> do
            liftIO $ print $ "error occurred while signing the Tx: " <> show err
            throw KeyValueDBLookupException
  where
    version = 1
    locktime = 0

xGetPartiallySignedAllegoryTx ::
       (HasXokenNodeEnv env m, MonadIO m)
    => [(OutPoint', Int)]
    -> ([Int], Bool)
    -> (String)
    -> (String)
    -> m (BC.ByteString)
xGetPartiallySignedAllegoryTx payips (nameArr, isProducer) owner change = do
    dbe <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    alg <- getAllegory
    let conn = keyValDB (dbe)
    let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
    -- check if name (of given type) exists
    let name = DT.pack $ L.map (\x -> chr x) (nameArr)
    -- read from config file
    let anutxos = NC.allegoryNameUtxoSatoshis $ nodeConfig $ bp2pEnv
    let feeSatsCreate = NC.allegoryTxFeeSatsProducerAction $ nodeConfig $ bp2pEnv
    let feeSatsTransfer = NC.allegoryTxFeeSatsOwnerAction $ nodeConfig $ bp2pEnv
    res <- liftIO $ try $ withResource (pool $ graphDB dbe) (`BT.run` queryAllegoryNameScriptOp (name) isProducer)
    (nameip, existed) <-
        case res of
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ "error fetching allegory name input :" ++ show e
                throw e
            Right [] -> do
                debug lg $ LG.msg $ "allegory name not found, get or make interim producers recursively : " <> name
                getOrMakeProducer (init nameArr)
            Right nb -> do
                debug lg $ LG.msg $ "allegory name found! : " <> name
                let sp = DT.split (== ':') $ fst (head nb)
                let txid = DT.unpack $ sp !! 0
                let index = readMaybe (DT.unpack $ sp !! 1) :: Maybe Int32
                case index of
                    Just i -> return $ ((OutPoint' txid i, (snd $ head nb)), True)
                    Nothing -> throw KeyValueDBLookupException
    inputHash <-
        liftIO $
        traverse
            (\(w, _) -> do
                 let op = OutPoint (fromString $ opTxHash w) (fromIntegral $ opIndex w)
                 sh <- getScriptHashFromOutpoint conn (txSynchronizer bp2pEnv) lg net op 0
                 return $ (w, ) <$> sh)
            payips
    let totalEffectiveInputSats = sum $ snd $ unzip payips
    let ins =
            L.map
                (\(x, s) ->
                     TxIn (OutPoint (fromString $ opTxHash x) (fromIntegral $ opIndex x)) (fromJust $ decodeHex s) 0)
                ([nameip] ++ (catMaybes inputHash))
    sigInputs <-
        mapM
            (\(x, s) -> do
                 case (decodeOutputBS ((fst . B16.decode) (E.encodeUtf8 s))) of
                     Left e -> do
                         liftIO $
                             print
                                 ("error (allegory) unable to decode scriptOutput! | " ++
                                  show name ++ " " ++ show (x, s) ++ " | " ++ show ((fst . B16.decode) (E.encodeUtf8 s)))
                         throw KeyValueDBLookupException
                     Right scr -> do
                         return $
                             SigInput
                                 scr
                                 (fromIntegral $ anutxos)
                                 (OutPoint (fromString $ opTxHash x) (fromIntegral $ opIndex x))
                                 sigHashAll
                                 Nothing)
            [nameip]
    --
    let outs =
            if existed
                then if isProducer
                         then do
                             let al =
                                     Allegory
                                         1
                                         (init nameArr)
                                         (ProducerAction
                                              (Index 0)
                                              (ProducerOutput (Index 1) (Just $ Endpoint "XokenP2P" "someuri_1"))
                                              Nothing
                                              [])
                             let opRetScript = frameOpReturn $ C.toStrict $ serialise al
                            -- derive producer's Address
                             let prAddr = pubKeyAddr $ derivePubKeyI $ wrapSecKey True $ allegorySecretKey alg
                             let prScript = addressToScriptBS prAddr
                             let payAddr = pubKeyAddr $ derivePubKeyI $ wrapSecKey True $ allegorySecretKey alg
                             let payScript = addressToScriptBS payAddr
                             let paySats = 1000000
                             let changeSats = totalEffectiveInputSats - (paySats + feeSatsCreate)
                             [TxOut 0 opRetScript] ++
                                 (L.map
                                      (\x -> do
                                           let addr =
                                                   case stringToAddr net (DT.pack $ fst x) of
                                                       Just a -> a
                                                       Nothing -> throw InvalidOutputAddressException
                                           let script = addressToScriptBS addr
                                           TxOut (fromIntegral $ snd x) script)
                                      [(owner, (fromIntegral $ anutxos)), (change, changeSats)]) ++
                                 [TxOut ((fromIntegral paySats) :: Word64) payScript] -- the charge for the name transfer
                         else do
                             let al =
                                     Allegory
                                         1
                                         (nameArr)
                                         (OwnerAction
                                              (Index 0)
                                              (OwnerOutput (Index 1) (Just $ Endpoint "XokenP2P" "someuri_1"))
                                              [ ProxyProvider
                                                    "AllPay"
                                                    "Public"
                                                    (Endpoint "XokenP2P" "someuri_2")
                                                    (Registration "addrCommit" "utxoCommit" "signature" 876543)
                                              ])
                             let opRetScript = frameOpReturn $ C.toStrict $ serialise al
                             let payAddr = pubKeyAddr $ derivePubKeyI $ wrapSecKey True $ allegorySecretKey alg
                             let payScript = addressToScriptBS payAddr
                             let paySats = 1000000
                             let changeSats = totalEffectiveInputSats - (paySats + feeSatsTransfer)
                             [TxOut 0 opRetScript] ++
                                 (L.map
                                      (\x -> do
                                           let addr =
                                                   case stringToAddr net (DT.pack $ fst x) of
                                                       Just a -> a
                                                       Nothing -> throw InvalidOutputAddressException
                                           let script = addressToScriptBS addr
                                           TxOut (fromIntegral $ snd x) script)
                                      [(owner, (fromIntegral $ anutxos)), (change, changeSats)]) ++
                                 [TxOut (fromIntegral anutxos) payScript] -- the charge for the name transfer
                else do
                    let al =
                            Allegory
                                1
                                (init nameArr)
                                (ProducerAction
                                     (Index 0)
                                     (ProducerOutput (Index 1) (Just $ Endpoint "XokenP2P" "someuri_1"))
                                     Nothing
                                     [ OwnerExtension
                                           (OwnerOutput (Index 2) (Just $ Endpoint "XokenP2P" "someuri_3"))
                                           (last nameArr)
                                     ])
                    let opRetScript = frameOpReturn $ C.toStrict $ serialise al
                    -- derive producer's Address
                    let prAddr = pubKeyAddr $ derivePubKeyI $ wrapSecKey True $ allegorySecretKey alg
                    let prScript = addressToScriptBS prAddr
                    let payAddr = pubKeyAddr $ derivePubKeyI $ wrapSecKey True $ allegorySecretKey alg
                    let payScript = addressToScriptBS payAddr
                    let paySats = 1000000
                    let changeSats = totalEffectiveInputSats - ((fromIntegral $ anutxos) + paySats + feeSatsCreate)
                    [TxOut 0 opRetScript] ++
                        [TxOut (fromIntegral anutxos) prScript] ++
                        (L.map
                             (\x -> do
                                  let addr =
                                          case stringToAddr net (DT.pack $ fst x) of
                                              Just a -> a
                                              Nothing -> throw InvalidOutputAddressException
                                  let script = addressToScriptBS addr
                                  TxOut (fromIntegral $ snd x) script)
                             [(owner, (fromIntegral $ anutxos)), (change, changeSats)]) ++
                        [TxOut ((fromIntegral paySats) :: Word64) payScript] -- the charge for the name transfer
    --
    let psatx = Tx version ins outs locktime
    case signTx net psatx sigInputs [allegorySecretKey alg] of
        Right tx -> do
            return $ BSL.toStrict $ A.encode $ tx
        Left err -> do
            liftIO $ print $ "error occurred while signing the Tx: " <> show err
            return $ BC.empty
  where
    version = 1
    locktime = 0

xRelayTx :: (HasXokenNodeEnv env m, MonadIO m) => BC.ByteString -> m (Bool)
xRelayTx rawTx = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = keyValDB (dbe)
    -- broadcast Tx
    case runGetState (getConfirmedTx) (rawTx) 0 of
        Left e -> do
            err lg $ LG.msg $ "error decoding rawTx :" ++ show e
            throw ConfirmedTxParseException
        Right res -> do
            debug lg $ LG.msg $ val $ "broadcasting tx"
            case fst res of
                Just tx -> do
                    let outpoints = L.map (\x -> prevOutput x) (txIn tx)
                    tr <-
                        mapM
                            (\x -> do
                                 let txid = txHashToHex $ outPointHash $ prevOutput x
                                     str =
                                         "SELECT tx_id, block_info, tx_serialized from xoken.transactions where tx_id = ?"
                                     qstr =
                                         str :: Q.QueryString Q.R (Identity DT.Text) ( DT.Text
                                                                                     , (DT.Text, Int32, Int32)
                                                                                     , Blob)
                                     p = Q.defQueryParams Q.One $ Identity $ (txid)
                                 iop <- Q.runClient conn (Q.query qstr p)
                                 if length iop == 0
                                     then do
                                         debug lg $ LG.msg $ "not found" ++ show txid
                                         return Nothing
                                     else do
                                         let (txid, _, sz) = iop !! 0
                                         case runGetLazy (getConfirmedTx) (fromBlob sz) of
                                             Left e -> do
                                                 debug lg $ LG.msg (encodeHex $ BSL.toStrict $ fromBlob sz)
                                                 return Nothing
                                             Right (txd) -> do
                                                 case txd of
                                                     Nothing -> return Nothing
                                                     Just txn -> do
                                                         let cout =
                                                                 (txOut txn) !!
                                                                 fromIntegral (outPointIndex $ prevOutput x)
                                                         case (decodeOutputBS $ scriptOutput cout) of
                                                             Right (so) -> do
                                                                 return $ Just (so, outValue cout, prevOutput x)
                                                             Left (e) -> do
                                                                 err lg $ LG.msg $ "error decoding rawTx :" ++ show e
                                                                 return Nothing)
                            (txIn tx)
                    -- if verifyStdTx net tx $ catMaybes tr
                    --     then do
                    allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
                    let !connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
                    debug lg $ LG.msg $ val $ "transaction verified - broadcasting tx"
                    mapM_ (\(_, peer) -> do sendRequestMessages peer (MTx (fromJust $ fst res))) connPeers
                    eres <- LE.try $ handleIfAllegoryTx tx True -- MUST be False
                    case eres of
                        Right (flg) -> return True
                        Left (e :: SomeException) -> return False
                Nothing -> do
                    err lg $ LG.msg $ val $ "error decoding rawTx (2)"
                    return $ False

authLoginClient :: (HasXokenNodeEnv env m, MonadIO m) => RPCMessage -> Network -> EndPointConnection -> m (RPCMessage)
authLoginClient msg net epConn = do
    dbe <- getDB
    lg <- getLogger
    case rqMethod msg of
        "AUTHENTICATE" ->
            case rqParams msg of
                (AuthenticateReq user pass) -> do
                    resp <- login (DT.pack user) (BC.pack pass)
                    return $ RPCResponse 200 $ Right $ Just $ AuthenticateResp resp
                ___ -> return $ RPCResponse 404 $ Left $ RPCError INVALID_REQUEST Nothing
        _____ -> return $ RPCResponse 200 $ Right $ Just $ AuthenticateResp $ AuthResp Nothing 0 0

login :: (MonadIO m, HasXokenNodeEnv env m) => DT.Text -> BC.ByteString -> m AuthResp
login user pass = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB dbe
        hashedPasswd = encodeHex ((S.encode $ sha256 pass))
        str =
            " SELECT password, api_quota, api_used, session_key_expiry_time FROM xoken.user_permission WHERE username = ? "
        qstr = str :: Q.QueryString Q.R (Identity DT.Text) (DT.Text, Int32, Int32, UTCTime)
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
                        (sk, _, _, _) -> do
                            if (sk /= hashedPasswd)
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
                                        Right () -> return ()
                                        Left (SomeException e) -> do
                                            err lg $ LG.msg $ "Error: UPDATE'ing into 'user_permission': " ++ show e
                                            throw e
                                    return $ AuthResp (Just $ DT.unpack newSessionKey) 1 100

delegateRequest :: (HasXokenNodeEnv env m, MonadIO m) => RPCMessage -> EndPointConnection -> Network -> m (RPCMessage)
delegateRequest encReq epConn net = do
    dbe <- getDB
    lg <- getLogger
    let conn = keyValDB (dbe)
    case rqParams encReq of
        (AuthenticateReq _ _) -> authLoginClient encReq net epConn
        (GeneralReq sessionKey __) -> do
            let str =
                    " SELECT api_quota, api_used, session_key_expiry_time, permissions FROM xoken.user_permission WHERE session_key = ? ALLOW FILTERING "
                qstr = str :: Q.QueryString Q.R (Q.Identity DT.Text) (Int32, Int32, UTCTime, Set DT.Text)
                p = Q.defQueryParams Q.One $ Identity $ (DT.pack sessionKey)
            res <- liftIO $ try $ Q.runClient conn (Q.query (Q.prepared qstr) p)
            case res of
                Left (SomeException e) -> do
                    err lg $ LG.msg $ "Error: SELECT'ing from 'user_permission': " ++ show e
                    throw e
                Right (op) -> do
                    if length op == 0
                        then do
                            return $ RPCResponse 200 $ Right $ Just $ AuthenticateResp $ AuthResp Nothing 0 0
                        else do
                            case op !! 0 of
                                (quota, used, exp, roles) -> do
                                    curtm <- liftIO $ getCurrentTime
                                    if exp > curtm && quota > used
                                        then goGetResource encReq net (DCP.fromSet roles)
                                        else return $
                                             RPCResponse 200 $ Right $ Just $ AuthenticateResp $ AuthResp Nothing 0 0

goGetResource :: (HasXokenNodeEnv env m, MonadIO m) => RPCMessage -> Network -> [DT.Text] -> m (RPCMessage)
goGetResource msg net roles = do
    dbe <- getDB
    lg <- getLogger
    let grdb = graphDB (dbe)
        conn = keyValDB (dbe)
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
                                        Just u -> return $ RPCResponse 200 $ Right $ Just $ RespAddUser u
                                        Nothing ->
                                            return $
                                            RPCResponse 400 $
                                            Left $
                                            RPCError
                                                INVALID_PARAMS
                                                (Just $ "User with username " ++ uname ++ " already exists")
                                else return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS (Just "Invalid email")
                        else return $
                             RPCResponse 403 $
                             Left $ RPCError INVALID_PARAMS (Just "User lacks permission to create users")
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "CHAIN_INFO" -> do
            cw <- xGetChainInfo
            case cw of
                Just c -> return $ RPCResponse 200 $ Right $ Just $ RespChainInfo c
                Nothing -> return $ RPCResponse 404 $ Left $ RPCError INVALID_REQUEST Nothing
        "HASH->BLOCK" -> do
            case methodParams $ rqParams msg of
                Just (GetBlockByHash hs) -> do
                    blk <- xGetBlockHash (DT.pack hs)
                    case blk of
                        Just b -> return $ RPCResponse 200 $ Right $ Just $ RespBlockByHash b
                        Nothing -> return $ RPCResponse 404 $ Left $ RPCError INVALID_REQUEST Nothing
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "[HASH]->[BLOCK]" -> do
            case methodParams $ rqParams msg of
                Just (GetBlocksByHashes hashes) -> do
                    blks <- xGetBlocksHashes (DT.pack <$> hashes)
                    return $ RPCResponse 200 $ Right $ Just $ RespBlocksByHashes blks
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "HEIGHT->BLOCK" -> do
            case methodParams $ rqParams msg of
                Just (GetBlockByHeight ht) -> do
                    blk <- xGetBlockHeight (fromIntegral ht)
                    case blk of
                        Just b -> return $ RPCResponse 200 $ Right $ Just $ RespBlockByHash b
                        Nothing -> return $ RPCResponse 404 $ Left $ RPCError INVALID_REQUEST Nothing
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "[HEIGHT]->[BLOCK]" -> do
            case methodParams $ rqParams msg of
                Just (GetBlocksByHeight hts) -> do
                    blks <- xGetBlocksHeights $ Data.List.map (fromIntegral) hts
                    return $ RPCResponse 200 $ Right $ Just $ RespBlocksByHashes blks
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "HASH->[TXID]" -> do
            case methodParams $ rqParams msg of
                Just (GetTxIDsByBlockHash hash pgSize pgNum) -> do
                    txids <- xGetTxIDsByBlockHash hash pgSize pgNum
                    return $ RPCResponse 200 $ Right $ Just $ RespTxIDsByBlockHash txids
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "TXID->RAWTX" -> do
            case methodParams $ rqParams msg of
                Just (GetRawTransactionByTxID hs) -> do
                    tx <- xGetTxHash (DT.pack hs)
                    case tx of
                        Just t -> return $ RPCResponse 200 $ Right $ Just $ RespRawTransactionByTxID t
                        Nothing -> return $ RPCResponse 200 $ Right $ Nothing
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "TXID->TX" -> do
            case methodParams $ rqParams msg of
                Just (GetTransactionByTxID hs) -> do
                    tx <- xGetTxHash (DT.pack hs)
                    case tx of
                        Just RawTxRecord {..} ->
                            case S.decodeLazy txSerialized of
                                Right rt ->
                                    return $
                                    RPCResponse 200 $
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
                                Left err -> return $ RPCResponse 400 $ Left $ RPCError INTERNAL_ERROR Nothing
                        Nothing -> return $ RPCResponse 200 $ Right Nothing
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "[TXID]->[RAWTX]" -> do
            case methodParams $ rqParams msg of
                Just (GetRawTransactionsByTxIDs hashes) -> do
                    txs <- xGetTxHashes (DT.pack <$> hashes)
                    return $ RPCResponse 200 $ Right $ Just $ RespRawTransactionsByTxIDs txs
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
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
                    return $ RPCResponse 200 $ Right $ Just $ RespTransactionsByTxIDs $ catMaybes rawTxs
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "ADDR->[OUTPUT]" -> do
            case methodParams $ rqParams msg of
                Just (GetOutputsByAddress addr psize cursor) -> do
                    ops <- xGetOutputsAddress addr psize cursor
                    return $
                        RPCResponse 200 $
                        Right $ Just $ RespOutputsByAddress (getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "[ADDR]->[OUTPUT]" -> do
            case methodParams $ rqParams msg of
                Just (GetOutputsByAddresses addrs pgSize cursor) -> do
--                    ops <- xGetOutputsAddresses addrs pgSize cursor
                    ops <- runManyInputsFor xGetOutputsAddress addrs pgSize cursor
                    return $
                        RPCResponse 200 $
                        Right $ Just $ RespOutputsByAddresses (getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "SCRIPTHASH->[OUTPUT]" -> do
            case methodParams $ rqParams msg of
                Just (GetOutputsByScriptHash sh pgSize nomTxInd) -> do
                    ops <- xGetOutputsScriptHash (sh) pgSize nomTxInd
                    return $
                        RPCResponse 200 $
                        Right $ Just $ RespOutputsByScriptHash (getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "[SCRIPTHASH]->[OUTPUT]" -> do
            case methodParams $ rqParams msg of
                Just (GetOutputsByScriptHashes shs pgSize nomTxInd) -> do
--                    ops <- xGetOutputsScriptHashes shs pgSize nomTxInd
                    ops <- runManyInputsFor xGetOutputsScriptHash shs pgSize nomTxInd
                    return $
                        RPCResponse 200 $
                        Right $ Just $ RespOutputsByScriptHashes (getNextCursor ops) (fromResultWithCursor <$> ops)
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "TXID->[MNODE]" -> do
            case methodParams $ rqParams msg of
                Just (GetMerkleBranchByTxID txid) -> do
                    ops <- xGetMerkleBranch txid
                    return $ RPCResponse 200 $ Right $ Just $ RespMerkleBranchByTxID ops
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "NAME->[OUTPOINT]" -> do
            case methodParams $ rqParams msg of
                Just (GetAllegoryNameBranch name isProducer) -> do
                    ops <- xGetAllegoryNameBranch name isProducer
                    return $ RPCResponse 200 $ Right $ Just $ RespAllegoryNameBranch ops
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "RELAY_TX" -> do
            case methodParams $ rqParams msg of
                Just (RelayTx tx) -> do
                    ops <- xRelayTx tx
                    return $ RPCResponse 200 $ Right $ Just $ RespRelayTx ops
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "PS_ALLEGORY_TX" -> do
            case methodParams $ rqParams msg of
                Just (GetPartiallySignedAllegoryTx payips (name, isProducer) owner change) -> do
                    opsE <- LE.try $ xGetPartiallySignedAllegoryTx payips (name, isProducer) owner change
                    case opsE of
                        Right ops -> return $ RPCResponse 200 $ Right $ Just $ RespPartiallySignedAllegoryTx ops
                        Left (e :: SomeException) -> do
                            liftIO $ print e
                            return $ RPCResponse 400 $ Left $ RPCError INTERNAL_ERROR Nothing
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        "OUTPOINT->SPEND_STATUS" -> do
            case methodParams $ rqParams msg of
                Just (GetTxOutputSpendStatus txid index) -> do
                    txss <- xGetTxOutputSpendStatus txid index
                    return $ RPCResponse 200 $ Right $ Just $ RespTxOutputSpendStatus txss
                _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_METHOD Nothing

convertToScriptHash :: Network -> String -> Maybe String
convertToScriptHash net s = do
    let addr = stringToAddr net (DT.pack s)
    (DT.unpack . txHashToHex . TxHash . sha256 . addressToScriptBS) <$> addr

getNextCursor :: [ResultWithCursor r c] -> Maybe c
getNextCursor [] = Nothing
getNextCursor aos =
    let nextCursor = cur $ last aos
     in Just nextCursor

fromMaybePageSize :: (Integral p, Bounded p) => Maybe p -> Int
fromMaybePageSize mps =
    fromIntegral $
    case mps of
        Just ps -> ps
        Nothing -> maxBound
