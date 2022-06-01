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

module Network.Xoken.Node.Service.Transaction where

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
import qualified Control.Concurrent.Async.Lifted as LA (async, concurrently, mapConcurrently, wait, race)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Concurrent.STM.TVar
import qualified Control.Error.Util as Extra
import Control.Exception
import qualified Control.Exception.Extra as EX (retry)
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
import qualified Data.Either as DE
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
import Network.Xoken.Node.Data as D
import Network.Xoken.Node.Data.Allegory
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.UnconfTxSync
import Network.Xoken.Node.Service.Policy
import Network.Xoken.Util (bsToInteger, integerToBS)
import Numeric (showHex)
import System.Logger as LG
import System.Logger.Message
import System.Random
import Text.Read
import Xoken
import qualified Xoken.NodeConfig as NC

xGetTxHash :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> m (Maybe RawTxRecord)
xGetTxHash hash = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    ep <- liftIO $ readTVarIO (epochType bp2pEnv)
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        str = "SELECT tx_id, block_info, tx_serialized, inputs, fees from xoken.transactions where tx_id = ?"
        qstr =
            str ::
                Q.QueryString
                    Q.R
                    (Identity DT.Text)
                    ( DT.Text
                    , Maybe (DT.Text, Int32, Int32)
                    , Blob
                    , Set ((DT.Text, Int32), Int32, (DT.Text, Int64))
                    , Int64
                    )
        p = getSimpleQueryParam $ Identity $ hash
    res <-
        LE.try $
            LA.concurrently
                (LA.concurrently (liftIO $ query conn (Q.RqQuery $ Q.Query qstr p)) (getTxOutputsFromTxId hash))
                (xGetMerkleBranch $ DT.unpack hash)
    case res of
        Right ((iop, outs), mrkl) ->
            if length iop == 0
                then return Nothing
                else do
                    let (txid, binfo, psz, sinps, fees) = iop !! 0
                        inps = L.sortBy (\(_, x, _) (_, y, _) -> compare x y) $ Q.fromSet sinps
                    sz <-
                        if isSegmented $ fromBlob psz
                            then liftIO $ getCompleteTx conn hash (getSegmentCount (fromBlob psz))
                            else pure $ fromBlob psz
                    let tx = fromJust $ Extra.hush $ S.decodeLazy sz
                        (bi, mrkl') =
                            case binfo of
                                Nothing -> (Nothing, Nothing)
                                Just ("", -1, -1) -> (Nothing, Nothing)
                                Just (bhash, blkht, txind) ->
                                    ( Just $ BlockInfo' (DT.unpack bhash) (fromIntegral blkht) (fromIntegral txind)
                                    , Just mrkl
                                    )
                    return $
                        Just $
                            RawTxRecord
                                (DT.unpack txid)
                                (fromIntegral $ C.length sz)
                                bi
                                (sz)
                                (Just $ zipWith mergeTxOutTxOutput (txOut tx) outs)
                                ( zipWith mergeTxInTxInput (txIn tx) $
                                    ( \((outTxId, outTxIndex), inpTxIndex, (addr, value)) ->
                                        TxInput (DT.unpack outTxId) outTxIndex inpTxIndex (DT.unpack addr) value ""
                                    )
                                        <$> inps
                                )
                                fees
                                (mrkl')
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxHash: " ++ show e
            throw KeyValueDBLookupException

xGetTxHashes :: (HasXokenNodeEnv env m, MonadIO m) => [DT.Text] -> m ([RawTxRecord])
xGetTxHashes hashes = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    ep <- liftIO $ readTVarIO (epochType bp2pEnv)
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        str = "SELECT tx_id, block_info, tx_serialized, inputs, fees from xoken.transactions where tx_id in ?"
        qstr =
            str ::
                Q.QueryString
                    Q.R
                    (Identity [DT.Text])
                    ( DT.Text
                    , Maybe (DT.Text, Int32, Int32)
                    , Blob
                    , Set ((DT.Text, Int32), Int32, (DT.Text, Int64))
                    , Int64
                    )
        p = getSimpleQueryParam $ Identity $ hashes
    res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query qstr p)
    iop <-
        case res of
            Right iop -> do
                debug lg $ LG.msg $ "xGetTxHashes got blockInfo: " ++ (show $ (\(_, bi, _, _, _) -> bi) <$> iop)
                let recs =
                        (\(txid, bi, psz, sinps, fees) -> (txid, (txid, getBlockInfo bi, psz, Q.fromSet sinps, fees)))
                            <$> iop
                debug lg $
                    LG.msg $ "xGetTxHashes got blockInfo (rec): " ++ (show $ (\(_, bi, _, _, _) -> bi) <$> snd <$> recs)
                pure $ snd <$> recordSorter hashes recs
              where
                recordSorter txids res = catMaybes $ sorter txids res
                sorter [] res = []
                sorter (txid : txids) res = (finder txid res) : (sorter txids res)
                finder txid [] = Nothing
                finder txid ((txid', rec) : res)
                    | txid == txid' = Just (txid, rec)
                    | otherwise = finder txid res
            Left (e :: SomeException) -> do
                err lg $ LG.msg $ "Error: xGetTxHashes: " ++ show e
                throw KeyValueDBLookupException
    txRecs <-
        traverse
            ( \(txid, bi, psz, sinps, fees) -> do
                debug lg $ LG.msg $ "xGetTxHashes got blockInfo (traversal): " ++ (show bi)
                let inps = L.sortBy (\(_, x, _) (_, y, _) -> compare x y) sinps
                sz <-
                    if isSegmented (fromBlob psz)
                        then liftIO $ getCompleteTx conn txid (getSegmentCount (fromBlob psz))
                        else pure $ fromBlob psz
                let tx = fromJust $ Extra.hush $ S.decodeLazy sz
                    mrklF =
                        case bi of
                            Just b -> Just <$> (xGetMerkleBranch $ DT.unpack txid)
                            Nothing -> pure Nothing
                let oF = Just <$> getTxOutputsFromTxId txid
                res' <- LE.try $ LA.concurrently oF mrklF
                case res' of
                    Right (outs, mrkl) ->
                        return $
                            Just $
                                RawTxRecord
                                    (DT.unpack txid)
                                    (fromIntegral $ C.length sz)
                                    ( ( \(bhash, blkht, txind) ->
                                            BlockInfo' (DT.unpack bhash) (fromIntegral blkht) (fromIntegral txind)
                                      )
                                        <$> bi
                                    )
                                    sz
                                    (zipWith mergeTxOutTxOutput (txOut tx) <$> outs)
                                    ( zipWith mergeTxInTxInput (txIn tx) $
                                        ( \((outTxId, outTxIndex), inpTxIndex, (addr, value)) ->
                                            TxInput (DT.unpack outTxId) outTxIndex inpTxIndex (DT.unpack addr) value ""
                                        )
                                            <$> inps
                                    )
                                    fees
                                    mrkl
                    Left (e :: SomeException) -> do
                        err lg $ LG.msg $ "Error: xGetTxHashes: " ++ show e
                        return Nothing
            )
            iop
    return $ fromMaybe [] (sequence txRecs)
  where
    runDeleteBy [] uc = uc
    runDeleteBy (c : cs) uc = runDeleteBy cs (L.deleteBy (\(txid, _, _, _, _) (txid2, _, _, _, _) -> txid == txid2) c uc)

getTxOutputsFromTxId :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => DT.Text -> m [TxOutput]
getTxOutputsFromTxId txid = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    ep <- liftIO $ readTVarIO (epochType bp2pEnv)
    let conn = xCqlClientState dbe
        toStr = "SELECT output_index, value, address, script, spend_info FROM xoken.txid_outputs WHERE txid=?"
        toQStr = toStr :: Q.QueryString Q.R (Identity DT.Text) (Int32, Int64, DT.Text, DT.Text, Maybe (DT.Text, Int32))
        par = getSimpleQueryParam (Identity txid)
    res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query toQStr par)
    case res of
        Right t -> do
            if length t == 0
                then do
                    err lg $ LG.msg $ "Error: getTxOutputsFromTxId: No entry in txid_outputs for txid: " ++ show txid
                    return []
                else do
                    LA.mapConcurrently
                        ( \(idx, val, addr, script, si) -> do
                            case si of
                                Nothing -> do
                                    return $ TxOutput idx (DT.unpack addr) Nothing val (BC.pack $ DT.unpack script)
                                Just situple -> do
                                    let conn1 = xCqlClientState dbe
                                        toStr1 = "SELECT block_info FROM xoken.transactions WHERE tx_id=?"
                                        toQStr1 =
                                            toStr1 ::
                                                Q.QueryString
                                                    Q.R
                                                    (Identity DT.Text)
                                                    ( Identity
                                                        ( DT.Text
                                                        , Int32
                                                        , Int32
                                                        )
                                                    )
                                        par1 = getSimpleQueryParam (Identity $ fst situple)
                                    res1 <- liftIO $ LE.try $ query conn1 (Q.RqQuery $ Q.Query toQStr1 par1)
                                    case res1 of
                                        Right rx -> do
                                            let (bhash, bht, txindx) = runIdentity $ rx !! 0
                                            return $
                                                TxOutput
                                                    idx
                                                    (DT.unpack addr)
                                                    ( Just $
                                                        SpendInfo
                                                            (DT.unpack $ fst situple)
                                                            (snd situple)
                                                            (BlockInfo' (DT.unpack bhash) bht txindx)
                                                            []
                                                    )
                                                    val
                                                    (BC.pack $ DT.unpack script)
                                        Left (e :: SomeException) -> do
                                            err lg $ LG.msg $ "Error: getTxOutputsFromTxId:(1) " ++ show e
                                            throw KeyValueDBLookupException
                        )
                        t
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getTxOutputsFromTxId:(2) " ++ show e
            throw KeyValueDBLookupException

xGetTxIDsByBlockHash :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => String -> Int32 -> Int32 -> m [String]
xGetTxIDsByBlockHash hash pgSize pgNum = do
    dbe <- getDB
    lg <- getLogger
    let conn = xCqlClientState $ dbe
        txsToSkip = pgSize * (pgNum - 1)
        firstPage = (+ 1) $ fromIntegral $ floor $ (fromIntegral txsToSkip) / 100
        lastPage = (+ 1) $ fromIntegral $ floor $ (fromIntegral $ txsToSkip + pgSize) / 100
        txDropFromFirst = fromIntegral $ txsToSkip `mod` 100
        str = "SELECT page_number, txids from xoken.blockhash_txids where block_hash = ? and page_number in ? "
        qstr = str :: Q.QueryString Q.R (DT.Text, [Int32]) (Int32, [DT.Text])
        p = getSimpleQueryParam $ (DT.pack hash, [firstPage .. lastPage])
    res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
    case res of
        Right iop ->
            return . L.take (fromIntegral pgSize) . L.drop txDropFromFirst . L.concat $ (fmap DT.unpack . snd) <$> iop
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxIDsByBlockHash: " <> show e
            throw KeyValueDBLookupException

xGetTxOutputSpendStatus :: (HasXokenNodeEnv env m, MonadIO m) => String -> Int32 -> m (Maybe TxOutputSpendStatus)
xGetTxOutputSpendStatus txId outputIndex = do
    dbe <- getDB
    let conn = xCqlClientState dbe
        str = "SELECT is_recv, block_info, other FROM xoken.txid_outputs WHERE txid=? AND output_index=?"
        qstr =
            str ::
                Q.QueryString
                    Q.R
                    (DT.Text, Int32)
                    ( Bool
                    , Maybe (DT.Text, Int32, Int32)
                    , Set ((DT.Text, Int32), Int32, (DT.Text, Int64))
                    )
        p = getSimpleQueryParam (DT.pack txId, outputIndex)
    iop <- liftIO $ query conn (Q.RqQuery $ Q.Query qstr p)
    if length iop == 0
        then return Nothing
        else do
            if L.length iop == 1
                then return $ Just $ TxOutputSpendStatus False Nothing Nothing Nothing
                else do
                    let siop = L.sortBy (\(x, _, _) (y, _, _) -> compare x y) iop
                        (_, binfo, other) = siop !! 0
                        (_, spendingTxBlkHeight, _) = fromMaybe ("", -1, -1) binfo
                        ((spendingTxID, _), spendingTxIndex, _) = head $ Q.fromSet other
                    return $
                        Just $
                            TxOutputSpendStatus
                                True
                                (Just $ DT.unpack spendingTxID)
                                (Just spendingTxBlkHeight)
                                (Just spendingTxIndex)

xGetMerkleBranch :: (HasXokenNodeEnv env m, MonadIO m) => String -> m ([MerkleBranchNode'])
xGetMerkleBranch txid = do
    dbe <- getDB
    lg <- getLogger
    res <- liftIO $ try $ withResource' (pool $ graphDB dbe) (`BT.run` queryMerkleBranch (DT.pack txid))
    case res of
        Right mb -> do
            return $ Data.List.map (\x -> MerkleBranchNode' (DT.unpack $ fst x) (snd x)) mb
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetMerkleBranch: " ++ show e
            throw KeyValueDBLookupException

xRelayMultipleTx :: (HasXokenNodeEnv env m, MonadIO m) => [BC.ByteString] -> m [Bool]
xRelayMultipleTx = f []
  where
    f r [] = return r
    f r (t : ts) = xRelayTx t >>= \r' -> f (r' : r) ts

checkOutpointSpendStatus :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> m (Bool, Bool)
checkOutpointSpendStatus outpoint = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let (txid, outputIndex) = (txHashToHex $ outPointHash outpoint, fromIntegral $ outPointIndex outpoint)
        conn = xCqlClientState dbe
        queryStr :: Q.QueryString Q.R (DT.Text, Int32) (Identity (Maybe (DT.Text, Int32)))
        queryStr = "SELECT spend_info FROM xoken.txid_outputs WHERE txid=? AND output_index=?"
        queryPar = getSimpleQueryParam (txid, outputIndex)
    res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query queryStr queryPar)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ BC.pack $ "[ERROR] Querying database while checking spend status: " <> (show e)
            throw KeyValueDBLookupException
        Right res ->
            if L.null res
                then return (False, False)
                else do
                    let spendinf = runIdentity $ res !! 0
                    case spendinf of
                        Just x -> return (True, True)
                        Nothing -> return (True, False)

xRelayTx :: (HasXokenNodeEnv env m, MonadIO m) => BC.ByteString -> m (Bool)
xRelayTx rawTx = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        bheight = 100000
        bhash = hexToBlockHash "0000000000000000000000000000000000000000000000000000000000000000"
    debug lg $ LG.msg $ "relayTx bhash " ++ show bhash
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
                        txId = DT.unpack . txHashToHex . txHash $ tx
                    tr <-
                        mapM
                            ( \x -> do
                                let txid = txHashToHex $ outPointHash $ prevOutput x
                                    str =
                                        "SELECT tx_id, block_info, tx_serialized from xoken.transactions where tx_id = ?"
                                    qstr =
                                        str ::
                                            Q.QueryString
                                                Q.R
                                                (Identity DT.Text)
                                                ( DT.Text
                                                , Maybe (DT.Text, Int32, Int32)
                                                , Blob
                                                )
                                    p = getSimpleQueryParam $ Identity $ (txid)
                                iop <- liftIO $ query conn (Q.RqQuery $ Q.Query qstr p)
                                if length iop == 0
                                    then do
                                        debug lg $ LG.msg $ "not found" ++ show txid
                                        return Nothing
                                    else do
                                        let (txid, _, psz) = iop !! 0
                                        sz <-
                                            if isSegmented (fromBlob psz)
                                                then liftIO $ getCompleteTx conn txid (getSegmentCount (fromBlob psz))
                                                else pure $ fromBlob psz
                                        case runGetLazy (getConfirmedTx) sz of
                                            Left e -> do
                                                debug lg $ LG.msg (encodeHex $ BSL.toStrict sz)
                                                return Nothing
                                            Right (txd) -> do
                                                case txd of
                                                    Nothing -> return Nothing
                                                    Just txn -> do
                                                        let cout =
                                                                (txOut txn)
                                                                    !! fromIntegral (outPointIndex $ prevOutput x)
                                                        case (decodeOutputScript $ scriptOutput cout) of
                                                            Right (so) -> do
                                                                return $ Just (so, outValue cout, prevOutput x)
                                                            Left (e) -> do
                                                                err lg $ LG.msg $ "error decoding rawTx :" ++ show e
                                                                return Nothing
                            )
                            (txIn tx)
                    allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
                    let !connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
                    mapM_
                        ( \outpoint -> do
                            res <- LE.try $ checkOutpointSpendStatus outpoint
                            case res of
                                Left (e :: SomeException) -> throw e
                                Right (isOp, isSpent) -> do
                                    case isOp of
                                        False -> throw InvalidOutpointException
                                        True -> case isSpent of
                                            False -> return ()
                                            True -> throw $ DoubleSpendException outpoint
                        )
                        (prevOutput <$> txIn tx)
                    -- unless (L.null spentInputs) $ throw $ DoubleSpendException spentInputs
                    debug lg $ LG.msg $ val $ "transaction verified - broadcasting tx"
                    ingestRes <- LE.try $ processUnconfTransaction tx (100) -- TODO: a dummy min fee for now
                    case ingestRes of
                        Left (e :: SomeException) -> do
                            err lg $
                                LG.msg $ "[ERROR] While processing parent(s) of unconfirmed transaction: " <> (show e)
                            throw $ ParentProcessingException (show e)
                        Right () -> do
                            broadcastResult <-
                                mapM
                                    ( \(_, peer) -> do
                                        res <- LE.try $ sendRequestMessages peer (MTx (fromJust $ fst res))
                                        case res of
                                            Left (e :: SomeException) -> return (peer, Left (show e))
                                            Right () -> return (peer, Right ())
                                    )
                                    connPeers
                            debug lg $ LG.msg $ "Broadcast result for " <> txId <> ": " <> (show broadcastResult)
                            if all (DE.isLeft) (snd <$> broadcastResult)
                                then throw RelayFailureException
                                else return ()
                            eres <- LE.try $ handleIfAllegoryTx tx False False -- MUST be False
                            case eres of
                                Right (flg) -> return True
                                Left (e :: SomeException) -> do
                                    err lg $ LG.msg $ "[ERROR] Failed to process Allegory metadata: " <> (show e)
                                    return False
                Nothing -> do
                    err lg $ LG.msg $ val $ "error decoding rawTx (2)"
                    return $ False

xSubmitTx :: (HasXokenNodeEnv env m, MonadIO m) => BC.ByteString -> DT.Text -> DT.Text -> Maybe DT.Text -> m (Maybe DT.Text)
xSubmitTx rawTx userId callbackName state = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        bheight = 100000
        bhash = hexToBlockHash "0000000000000000000000000000000000000000000000000000000000000000"
    debug lg $ LG.msg $ "submitTx bhash " ++ show bhash
    -- broadcast Tx
    case runGetState (getConfirmedTx) (rawTx) 0 of
        Left e -> do
            err lg $ LG.msg $ "error decoding rawTx :" ++ show e
            throw ConfirmedTxParseException
        Right res -> do
            debug lg $ LG.msg $ val $ "submitting tx"
            case fst res of
                Just tx -> do
                    let outpoints = L.map (\x -> prevOutput x) (txIn tx)
                        txId = txHashToHex . txHash $ tx
                    allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
                    let !connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
                    mapM_
                        ( \outpoint -> do
                            res <- LE.try $ checkOutpointSpendStatus outpoint
                            case res of
                                Left (e :: SomeException) -> throw e
                                Right (isOp, isSpent) -> do
                                    case isOp of
                                        False -> throw InvalidOutpointException
                                        True -> case isSpent of
                                            False -> return ()
                                            True -> throw $ DoubleSpendException outpoint
                        )
                        [] --  (prevOutput <$> txIn tx)  TODO ######## commented out temporarily for empty list
                        -- verify against users fee policy
                    userPolicy <- xGetPolicyByUsername userId
                    feeReq <-
                        case userPolicy of
                            Just upol -> do
                                let feemap = getOpcodeFeeMap (pfOpcodes $ mpFees upol)
                                    pushDataChunk = fdBytes $ pfData $ mpFees upol
                                    pushDataSats = fdSatoshis $ pfData $ mpFees upol
                                ife <-
                                    mapM
                                        ( \inp -> do
                                            let scrp = scriptInput inp
                                            case (decodeOutputScript scrp) of
                                                Right (so) -> do
                                                    let ff =
                                                            L.map
                                                                ( \scrop' -> do
                                                                    let (scrop, blen) = case scrop' of
                                                                            OP_PUSHDATA byt typ -> (OP_PUSHDATA BC.empty typ, BC.length byt)
                                                                            _ -> (scrop', 0)
                                                                    case M.lookup scrop feemap of
                                                                        Just v -> v
                                                                        Nothing -> pushDataSats * ((blen `div` pushDataChunk) + 1)
                                                                )
                                                                (scriptOps so)
                                                    return $ sum ff
                                                Left (e) -> do
                                                    err lg $ LG.msg $ "error decoding output:" ++ show e
                                                    return 0
                                        )
                                        (txIn tx)
                                ofe <-
                                    mapM
                                        ( \out -> do
                                            let scrp = scriptOutput out
                                            case (decodeOutputScript scrp) of
                                                Right (so) -> do
                                                    let ff =
                                                            L.map
                                                                ( \scrop' -> do
                                                                    let (scrop, blen) = case scrop' of
                                                                            OP_PUSHDATA byt typ -> (OP_PUSHDATA BC.empty typ, BC.length byt)
                                                                            _ -> (scrop', 0)

                                                                    case M.lookup scrop feemap of
                                                                        Just v -> v
                                                                        Nothing -> pushDataSats * ((blen `div` pushDataChunk) + 1)
                                                                )
                                                                (scriptOps so)
                                                    return $ sum ff
                                                Left (e) -> do
                                                    err lg $ LG.msg $ "error decoding output:" ++ show e
                                                    return 0
                                        )
                                        (txOut tx)
                                return $ (sum ife) + (sum ofe)

                    -- unless (L.null spentInputs) $ throw $ DoubleSpendException spentInputs
                    debug lg $ LG.msg $ " fee required :" ++ (show feeReq)
                    -- ingestRes <- LE.try $ processUnconfTransaction tx (fromIntegral feeReq)
                    ores <-
                        LA.race
                            (liftIO $ threadDelay (30000000)) -- 30 secs
                            (LE.try $ processUnconfTransaction tx (fromIntegral feeReq))
                    case ores of
                        Right (ingestRes) -> do
                            case ingestRes of
                                Left (e :: SomeException) -> do
                                    err lg $
                                        LG.msg $ "[ERROR] While processing parent(s) of unconfirmed transaction: " <> (show e)
                                    throw $ ParentProcessingException (show e)
                                Right () -> do
                                    let qstr :: Q.QueryString Q.W (DT.Text, DT.Text, DT.Text, Int32, Maybe DT.Text) ()
                                        qstr = "INSERT INTO xoken.callback_registrations (txid, user_id, callback_name, flags_bitmask, state) VALUES (?,?,?,?,?)"
                                        par = getSimpleQueryParam (txId, userId, callbackName, 0, state)
                                    queryId <- liftIO $ queryPrepared conn (Q.RqPrepare (Q.Prepare qstr))
                                    resReg <- liftIO $ try $ write conn (Q.RqExecute (Q.Execute queryId par))
                                    case resReg of
                                        Right _ -> return ()
                                        Left (e :: SomeException) -> do
                                            err lg $ LG.msg $ "Error: INSERTing into 'script_output_protocol: " ++ show e
                                            throw KeyValueDBInsertException

                                    broadcastResult <-
                                        mapM
                                            ( \(_, peer) -> do
                                                res <- LE.try $ sendRequestMessages peer (MTx (fromJust $ fst res))
                                                case res of
                                                    Left (e :: SomeException) -> return (peer, Left (show e))
                                                    Right () -> return (peer, Right ())
                                            )
                                            connPeers
                                    debug lg $ LG.msg $ "Broadcast result for " <> (DT.unpack txId) <> ": " <> (show broadcastResult)
                                    if all (DE.isLeft) (snd <$> broadcastResult)
                                        then return () -- throw RelayFailureException -- TODO temporarily commenting out
                                        else return ()
                                    eres <- LE.try $ handleIfAllegoryTx tx False False -- MUST be False
                                    case eres of
                                        Right (flg) -> return $ Just txId
                                        Left (e :: SomeException) -> do
                                            err lg $ LG.msg $ "[ERROR] Failed to process Allegory metadata: " <> (show e)
                                            return Nothing
                        Left () -> do
                            LG.err lg $ LG.msg $ LG.val "Error: submitTx timed-out "
                            throw $ ParentProcessingException (show "")
                Nothing -> do
                    err lg $ LG.msg $ val $ "error decoding rawTx (2)"
                    return Nothing

xSubscribeTx :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> DT.Text -> DT.Text -> Maybe DT.Text -> m (Maybe DT.Text)
xSubscribeTx txId userId callbackName state = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        bheight = 100000
        bhash = hexToBlockHash "0000000000000000000000000000000000000000000000000000000000000000"
    debug lg $ LG.msg $ "subscribeTx bhash " ++ show bhash
    -- confirm tx is present in db
    knownTx <- checkOutputDataExists (txId, 0) -- suffice to try for first output
    if knownTx
        then do
            let qstr :: Q.QueryString Q.W (DT.Text, DT.Text, DT.Text, Int32, Maybe DT.Text) ()
                qstr = "INSERT INTO xoken.callback_registrations (txid, user_id, callback_name, flags_bitmask, state) VALUES (?,?,?,?,?)"
                par = getSimpleQueryParam (txId, userId, callbackName, 0, state)
            queryId <- liftIO $ queryPrepared conn (Q.RqPrepare (Q.Prepare qstr))
            resReg <- liftIO $ try $ write conn (Q.RqExecute (Q.Execute queryId par))
            case resReg of
                Right _ -> return $ Just txId
                Left (e :: SomeException) -> do
                    err lg $ LG.msg $ "Error: INSERTing into 'script_output_protocol: " ++ show e
                    throw KeyValueDBInsertException
        else do
            err lg $ LG.msg $ val $ "error decoding rawTx (2)"
            return Nothing

getOpcodeFeeMap :: FeeOpcodes -> M.Map ScriptOp Int
getOpcodeFeeMap fo = do
    M.fromList
        [ (OP_PUSHDATA BC.empty OPCODE, fsSatoshis $ D.opCode fo)
        , (OP_PUSHDATA BC.empty OPDATA1, fsSatoshis $ opPushData1 fo)
        , (OP_PUSHDATA BC.empty OPDATA2, fsSatoshis $ opPushData2 fo)
        , (OP_PUSHDATA BC.empty OPDATA4, fsSatoshis $ opPushData4 fo)
        , (OP_0, fsSatoshis $ op0 fo)
        , (OP_1NEGATE, fsSatoshis $ op1negate fo)
        , (OP_RESERVED, fsSatoshis $ opReserved fo)
        , (OP_1, fsSatoshis $ op1 fo)
        , (OP_2, fsSatoshis $ op2 fo)
        , (OP_3, fsSatoshis $ op3 fo)
        , (OP_4, fsSatoshis $ op4 fo)
        , (OP_5, fsSatoshis $ op5 fo)
        , (OP_6, fsSatoshis $ op6 fo)
        , (OP_7, fsSatoshis $ op7 fo)
        , (OP_8, fsSatoshis $ op8 fo)
        , (OP_9, fsSatoshis $ op9 fo)
        , (OP_10, fsSatoshis $ op10 fo)
        , (OP_11, fsSatoshis $ op11 fo)
        , (OP_12, fsSatoshis $ op12 fo)
        , (OP_13, fsSatoshis $ op13 fo)
        , (OP_14, fsSatoshis $ op14 fo)
        , (OP_15, fsSatoshis $ op15 fo)
        , (OP_16, fsSatoshis $ op16 fo)
        , -- Flow control
          (OP_NOP, fsSatoshis $ opNop fo)
        , (OP_VER, fsSatoshis $ opVer fo) -- reserved
        , (OP_IF, fsSatoshis $ opIf fo)
        , (OP_NOTIF, fsSatoshis $ opNotif fo)
        , (OP_VERIF, fsSatoshis $ opVerif fo) -- resreved
        , (OP_VERNOTIF, fsSatoshis $ opVerNotif fo) -- reserved
        , (OP_ELSE, fsSatoshis $ opElse fo)
        , (OP_ENDIF, fsSatoshis $ opEndif fo)
        , (OP_VERIFY, fsSatoshis $ opVerify fo)
        , (OP_RETURN, fsSatoshis $ opReturn fo)
        , -- Stack operations
          (OP_TOALTSTACK, fsSatoshis $ opToAltStack fo)
        , (OP_FROMALTSTACK, fsSatoshis $ opFromAltStack fo)
        , (OP_IFDUP, fsSatoshis $ opIfDup fo)
        , (OP_DEPTH, fsSatoshis $ opDepth fo)
        , (OP_DROP, fsSatoshis $ opDrop fo)
        , (OP_DUP, fsSatoshis $ opDup fo)
        , (OP_NIP, fsSatoshis $ opNip fo)
        , (OP_OVER, fsSatoshis $ opOver fo)
        , (OP_PICK, fsSatoshis $ opPick fo)
        , (OP_ROLL, fsSatoshis $ opRoll fo)
        , (OP_ROT, fsSatoshis $ opRot fo)
        , (OP_SWAP, fsSatoshis $ opSwap fo)
        , (OP_TUCK, fsSatoshis $ opTuck fo)
        , (OP_2DROP, fsSatoshis $ op2Drop fo)
        , (OP_2DUP, fsSatoshis $ op2Dup fo)
        , (OP_3DUP, fsSatoshis $ op3Dup fo)
        , (OP_2OVER, fsSatoshis $ op2Over fo)
        , (OP_2ROT, fsSatoshis $ op2Rot fo)
        , (OP_2SWAP, fsSatoshis $ op2Swap fo)
        , -- Splice
          (OP_CAT, fsSatoshis $ opCat fo)
        , (OP_SUBSTR, fsSatoshis $ opSubstr fo)
        , (OP_LEFT, fsSatoshis $ opLeft fo)
        , (OP_RIGHT, fsSatoshis $ opRight fo)
        , (OP_SIZE, fsSatoshis $ opSize fo)
        , -- Bitwise logic
          (OP_INVERT, fsSatoshis $ opInvert fo)
        , (OP_AND, fsSatoshis $ opAnd fo)
        , (OP_OR, fsSatoshis $ opOr fo)
        , (OP_XOR, fsSatoshis $ opXor fo)
        , (OP_EQUAL, fsSatoshis $ opEqual fo)
        , (OP_EQUALVERIFY, fsSatoshis $ opEqualVerify fo)
        , (OP_RESERVED1, fsSatoshis $ opReserved1 fo)
        , (OP_RESERVED2, fsSatoshis $ opReserved2 fo)
        , -- Arithmetic
          (OP_1ADD, fsSatoshis $ op1Add fo)
        , (OP_1SUB, fsSatoshis $ op1Sub fo)
        , (OP_2MUL, fsSatoshis $ op2Mul fo)
        , (OP_2DIV, fsSatoshis $ op2Div fo)
        , (OP_NEGATE, fsSatoshis $ opNegate fo)
        , (OP_ABS, fsSatoshis $ opAbs fo)
        , (OP_NOT, fsSatoshis $ opNot fo)
        , (OP_0NOTEQUAL, fsSatoshis $ op0NotEqual fo)
        , (OP_ADD, fsSatoshis $ opAdd fo)
        , (OP_SUB, fsSatoshis $ opSub fo)
        , (OP_MUL, fsSatoshis $ opMul fo)
        , (OP_DIV, fsSatoshis $ opDiv fo)
        , (OP_MOD, fsSatoshis $ opMod fo)
        , (OP_LSHIFT, fsSatoshis $ opLShift fo)
        , (OP_RSHIFT, fsSatoshis $ opRShift fo)
        , (OP_BOOLAND, fsSatoshis $ opBoolAnd fo)
        , (OP_BOOLOR, fsSatoshis $ opBoolOr fo)
        , (OP_NUMEQUAL, fsSatoshis $ opNumEqual fo)
        , (OP_NUMEQUALVERIFY, fsSatoshis $ opNumEqualVefify fo)
        , (OP_NUMNOTEQUAL, fsSatoshis $ opNumNotEqual fo)
        , (OP_LESSTHAN, fsSatoshis $ opLessThan fo)
        , (OP_GREATERTHAN, fsSatoshis $ opGreaterThan fo)
        , (OP_LESSTHANOREQUAL, fsSatoshis $ opLessThanOrEqual fo)
        , (OP_GREATERTHANOREQUAL, fsSatoshis $ opGreaterThanOrEqual fo)
        , (OP_MIN, fsSatoshis $ opMin fo)
        , (OP_MAX, fsSatoshis $ opMax fo)
        , (OP_WITHIN, fsSatoshis $ opWithin fo)
        , -- Crypto
          (OP_RIPEMD160, fsSatoshis $ opRipemd160 fo)
        , (OP_SHA1, fsSatoshis $ opSha1 fo)
        , (OP_SHA256, fsSatoshis $ opSha256 fo)
        , (OP_HASH160, fsSatoshis $ opHash160 fo)
        , (OP_HASH256, fsSatoshis $ opHash256 fo)
        , (OP_CODESEPARATOR, fsSatoshis $ opCodeSeperator fo)
        , (OP_CHECKSIG, fsSatoshis $ opCheckSig fo)
        , (OP_CHECKSIGVERIFY, fsSatoshis $ opCheckSigVefify fo)
        , (OP_CHECKMULTISIG, fsSatoshis $ opCheckMultiSig fo)
        , (OP_CHECKMULTISIGVERIFY, fsSatoshis $ opCheckMultiSigVerify fo)
        , -- Expansion
          -- Other
          (OP_PUBKEYHASH, fsSatoshis $ opPubKeyHash fo)
        , (OP_PUBKEY, fsSatoshis $ opPubKey fo)
        ]

deleteDuplicates ::
    (ResultWithCursor r a -> ResultWithCursor r a -> Bool) ->
    [ResultWithCursor r a] ->
    [ResultWithCursor r a] ->
    [ResultWithCursor r a]
deleteDuplicates compareBy unconf conf = runDeleteBy conf unconf
  where
    runDeleteBy [] uc = uc
    runDeleteBy (c : cs) uc = runDeleteBy cs (L.deleteBy compareBy c uc)

compareRWCText :: ResultWithCursor DT.Text a -> ResultWithCursor DT.Text b -> Bool
compareRWCText rwc1 rwc2 = res rwc1 == res rwc2

xGetTxIDByProtocol ::
    (HasXokenNodeEnv env m, MonadIO m) =>
    DT.Text ->
    [DT.Text] ->
    Maybe Int32 ->
    Maybe Int64 ->
    m [ResultWithCursor DT.Text Int64]
xGetTxIDByProtocol prop1 props pgSize mbNomTxInd = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        nominalTxIndex =
            case mbNomTxInd of
                Just n -> n
                Nothing -> maxBound
        str =
            "SELECT txid, nominal_tx_index FROM xoken.script_output_protocol WHERE proto_str=? AND nominal_tx_index<? ORDER BY nominal_tx_index DESC"
        protocol = DT.intercalate "." $ prop1 : props
        qstr = str :: Q.QueryString Q.R (DT.Text, Int64) (DT.Text, Int64)
        uqstr = getSimpleQueryParam $ (protocol, nominalTxIndex)
    eResp <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query qstr (uqstr{pageSize = maybe (Just 100) Just pgSize}))
    case eResp of
        Right mb -> return $ (\(x, y) -> ResultWithCursor x y) <$> mb
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getTxIDByProtocol: " ++ show e
            throw KeyValueDBLookupException
