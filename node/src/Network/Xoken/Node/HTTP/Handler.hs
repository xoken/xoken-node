{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE TupleSections #-}

module Network.Xoken.Node.HTTP.Handler where

import Arivi.P2P.Config (decodeHex, encodeHex)
import Control.Applicative ((<|>))
import qualified Control.Error.Util as Extra
import Control.Exception (SomeException(..), throw, try)
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Identity
import Control.Monad.State.Class
import qualified Data.Aeson as Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as C
import qualified Data.Either as Either
import qualified Data.HashTable.IO as H
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.Serialize as S
import qualified Data.Text as DT
import qualified Data.Text.Encoding as DTE
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Database.XCQL.Protocol as Q
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.Data
    ( AddUserResp(..)
    , BlockRecord(..)
    , RPCReqParams(..)
    , RPCReqParams'(..)
    , RPCResponseBody(..)
    , RawTxRecord(..)
    , TxRecord(..)
    , UpdateUserByUsername'(..)
    , coinbaseTxToMessage
    , encodeResp
    , fromResultWithCursor
    , txToTx'
    )
import Network.Xoken.Node.Env
import Network.Xoken.Node.HTTP.Types
import Network.Xoken.Node.P2P.Common
    ( BlockSyncException(..)
    , addNewUser
    , generateSessionKey
    , getSimpleQueryParam
    , indexMaybe
    , query
    , write
    )
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Service
import Snap
import System.Logger as LG
import Text.Read (readMaybe)
import qualified Xoken.NodeConfig as NC

authClient :: RPCReqParams -> Handler App App ()
authClient AuthenticateReq {..} = do
    pretty <- (maybe True (read . DT.unpack . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    resp <- LE.try $ login (DT.pack username) (BC.pack password)
    case resp of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ar -> writeBS $ BSL.toStrict $ encodeResp pretty $ AuthenticateResp ar
authClient _ = throwBadRequest

addUser :: RPCReqParams' -> Handler App App ()
addUser AddUser {..} = do
    dbe <- getDB
    pretty <- (maybe True (read . DT.unpack . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    let conn = xCqlClientState dbe
    resp <-
        LE.try $
        return $
        addNewUser
            conn
            (DT.pack auUsername)
            (DT.pack auFirstName)
            (DT.pack auLastName)
            (DT.pack auEmail)
            auRoles
            auApiQuota
            auApiExpiryTime
    case resp of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ar -> do
            ar' <- liftIO $ ar
            case ar' of
                Nothing -> throwBadRequest
                Just aur -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespAddUser aur
addUser _ = throwBadRequest

getChainInfo :: Handler App App ()
getChainInfo = do
    lg <- getLogger
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetChainInfo
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetChainInfo: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (Just ci) -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespChainInfo ci
        Right Nothing -> throwNotFound

getChainHeaders :: Handler App App ()
getChainHeaders = do
    lg <- getLogger
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    ht <- (maybe 1 (read . DT.unpack . DTE.decodeUtf8)) <$> (getQueryParam "startBlockHeight")
    pgsize <- (maybe 2000 (read . DT.unpack . DTE.decodeUtf8)) <$> (getQueryParam "pagesize")
    res <- LE.try $ xGetChainHeaders ht pgsize
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetChainHeaders: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ch -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespChainHeaders ch

getBlockByHash :: Handler App App ()
getBlockByHash = do
    hash <- getParam "hash"
    lg <- getLogger
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetBlockHash (DTE.decodeUtf8 $ fromJust hash)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (Just rec) -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespBlockByHash rec
        Right Nothing -> throwNotFound

getBlocksByHash :: Handler App App ()
getBlocksByHash = do
    allMap <- getQueryParams
    lg <- getLogger
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetBlocksHashes (DTE.decodeUtf8 <$> (fromJust $ Map.lookup "hash" allMap))
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right rec -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespBlocksByHashes rec

getBlockByHeight :: Handler App App ()
getBlockByHeight = do
    height <- getParam "height"
    lg <- getLogger
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetBlockHeight (read $ DT.unpack $ DTE.decodeUtf8 $ fromJust height)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHeight: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (Just rec) -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespBlockByHeight rec
        Right Nothing -> throwNotFound

getBlocksByHeight :: Handler App App ()
getBlocksByHeight = do
    allMap <- getQueryParams
    lg <- getLogger
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetBlocksHeights (read . DT.unpack . DTE.decodeUtf8 <$> (fromJust $ Map.lookup "height" allMap))
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHeight: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right rec -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespBlocksByHeight rec

getRawTxById :: Handler App App ()
getRawTxById = do
    txId <- getParam "id"
    lg <- getLogger
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetTxHash (DTE.decodeUtf8 $ fromJust txId)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (Just rec) -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespRawTransactionByTxID rec
        Right Nothing -> throwNotFound

getRawTxByIds :: Handler App App ()
getRawTxByIds = do
    allMap <- getQueryParams
    lg <- getLogger
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetTxHashes (DTE.decodeUtf8 <$> (fromJust $ Map.lookup "id" allMap))
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right rec -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespRawTransactionsByTxIDs rec

getTxById :: Handler App App ()
getTxById = do
    txId <- getParam "id"
    lg <- getLogger
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetTxHash (DTE.decodeUtf8 $ fromJust txId)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (Just RawTxRecord {..}) -> do
            case S.decodeLazy txSerialized of
                Right rt ->
                    writeBS $
                    BSL.toStrict $
                    encodeResp pretty $
                    RespTransactionByTxID
                        (TxRecord
                             txId
                             size
                             txBlockInfo
                             (txToTx' rt (fromMaybe [] txOutputs) txInputs)
                             fees
                             txMerkleBranch)
                Left err -> do
                    modifyResponse $ setResponseStatus 400 "Bad Request"
                    writeBS "400 error"
        Right Nothing -> throwNotFound

getTxByIds :: Handler App App ()
getTxByIds = do
    allMap <- getQueryParams
    lg <- getLogger
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetTxHashes (DTE.decodeUtf8 <$> (fromJust $ Map.lookup "id" allMap))
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right txs -> do
            let rawTxs =
                    (\RawTxRecord {..} ->
                         (TxRecord txId size txBlockInfo <$>
                          (txToTx' <$> (Extra.hush $ S.decodeLazy txSerialized) <*> (pure $ fromMaybe [] txOutputs) <*>
                           (pure txInputs)) <*>
                          (pure fees) <*>
                          (pure txMerkleBranch))) <$>
                    txs
            writeBS $ BSL.toStrict $ encodeResp pretty $ RespTransactionsByTxIDs $ catMaybes rawTxs

getTxIDsByBlockHash :: Handler App App ()
getTxIDsByBlockHash = do
    lg <- getLogger
    hash <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getParam "hash")
    pgNumber <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagenumber")
    pgSize <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagesize")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetTxIDsByBlockHash (fromJust hash) (fromMaybe 100 pgSize) (fromMaybe 1 pgNumber)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxIDsByBlockHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right txids -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespTxIDsByBlockHash txids

getTxOutputSpendStatus :: Handler App App ()
getTxOutputSpendStatus = do
    txid <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getParam "txid")
    index <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getParam "index")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    lg <- getLogger
    res <- LE.try $ xGetTxOutputSpendStatus (fromJust txid) (fromJust index)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxOutputSpendStatus: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right txss -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespTxOutputSpendStatus txss

getOutputsByAddr :: Handler App App ()
getOutputsByAddr = do
    addr <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getParam "address")
    pgSize <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagesize")
    cursor <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "cursor")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    bp2pEnv <- getBitcoinP2P
    let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
    lg <- getLogger
    res <- LE.try $ xGetOutputsAddress (fromJust addr) pgSize (decodeNTI cursor)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetOutputsAddress: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> do
            writeBS $
                BSL.toStrict $
                encodeResp pretty $ RespOutputsByAddress (encodeNTI $ getNextCursor ops) (fromResultWithCursor <$> ops)

getOutputsByAddrs :: Handler App App ()
getOutputsByAddrs = do
    addresses <- (fmap $ words . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "address")
    case addresses of
        Just (addrs :: [String]) -> do
            pgSize <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagesize")
            cursor <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "cursor")
            pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
            bp2pEnv <- getBitcoinP2P
            let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
            lg <- getLogger
            res <- LE.try $ runWithManyInputs xGetOutputsAddress addrs pgSize (decodeNTI cursor)
            case res of
                Left (e :: SomeException) -> do
                    err lg $ LG.msg $ "Error: xGetOutputsAddresses: " ++ show e
                    modifyResponse $ setResponseStatus 500 "Internal Server Error"
                    writeBS "INTERNAL_SERVER_ERROR"
                Right ops -> do
                    writeBS $
                        BSL.toStrict $
                        encodeResp pretty $
                        RespOutputsByAddresses (encodeNTI $ getNextCursor ops) (fromResultWithCursor <$> ops)
        Nothing -> throwBadRequest

getOutputsByScriptHash :: Handler App App ()
getOutputsByScriptHash = do
    sh <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getParam "scripthash")
    pgSize <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagesize")
    cursor <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "cursor")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    bp2pEnv <- getBitcoinP2P
    let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
    lg <- getLogger
    res <- LE.try $ xGetOutputsScriptHash (fromJust sh) pgSize (decodeNTI cursor)
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> do
            writeBS $
                BSL.toStrict $
                encodeResp pretty $
                RespOutputsByScriptHash (encodeNTI $ getNextCursor ops) (fromResultWithCursor <$> ops)

getOutputsByScriptHashes :: Handler App App ()
getOutputsByScriptHashes = do
    shs <- (fmap $ words . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "scripthash")
    case shs of
        Just sh -> do
            pgSize <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagesize")
            cursor <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "cursor")
            pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
            bp2pEnv <- getBitcoinP2P
            let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
            lg <- getLogger
            res <- LE.try $ runWithManyInputs xGetOutputsScriptHash sh pgSize (decodeNTI cursor)
            case res of
                Left (e :: SomeException) -> do
                    modifyResponse $ setResponseStatus 500 "Internal Server Error"
                    writeBS "INTERNAL_SERVER_ERROR"
                Right ops -> do
                    writeBS $
                        BSL.toStrict $
                        encodeResp pretty $
                        RespOutputsByScriptHashes (encodeNTI $ getNextCursor ops) (fromResultWithCursor <$> ops)
        Nothing -> throwBadRequest

getUTXOsByAddr :: Handler App App ()
getUTXOsByAddr = do
    lg <- getLogger
    addr <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getParam "address")
    pgSize <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagesize")
    cursor <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "cursor")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    bp2pEnv <- getBitcoinP2P
    let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
    res <- LE.try $ xGetUTXOsAddress (fromJust addr) pgSize (decodeOP cursor)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetUTXOsAddress: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> do
            writeBS $
                BSL.toStrict $
                encodeResp pretty $ RespUTXOsByAddress (encodeOP $ getNextCursor ops) (fromResultWithCursor <$> ops)

getUTXOsByAddrs :: Handler App App ()
getUTXOsByAddrs = do
    addresses <- (fmap $ words . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "address")
    case addresses of
        Just (addrs :: [String]) -> do
            pgSize <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagesize")
            cursor <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "cursor")
            pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
            bp2pEnv <- getBitcoinP2P
            let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
            lg <- getLogger
            res <- LE.try $ runWithManyInputs xGetUTXOsAddress addrs pgSize (decodeOP cursor)
            case res of
                Left (e :: SomeException) -> do
                    err lg $ LG.msg $ "Error: xGetUTXOsAddress: " ++ show e
                    modifyResponse $ setResponseStatus 500 "Internal Server Error"
                    writeBS "INTERNAL_SERVER_ERROR"
                Right ops -> do
                    writeBS $
                        BSL.toStrict $
                        encodeResp pretty $
                        RespUTXOsByAddresses (encodeOP $ getNextCursor ops) (fromResultWithCursor <$> ops)
        Nothing -> throwBadRequest

getUTXOsByScriptHash :: Handler App App ()
getUTXOsByScriptHash = do
    sh <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getParam "scripthash")
    pgSize <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagesize")
    cursor <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "cursor")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    bp2pEnv <- getBitcoinP2P
    let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
    lg <- getLogger
    res <- LE.try $ xGetUTXOsScriptHash (fromJust sh) pgSize (decodeOP cursor)
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> do
            writeBS $
                BSL.toStrict $
                encodeResp pretty $ RespUTXOsByScriptHash (encodeOP $ getNextCursor ops) (fromResultWithCursor <$> ops)

getUTXOsByScriptHashes :: Handler App App ()
getUTXOsByScriptHashes = do
    shs <- (fmap $ words . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "scripthash")
    case shs of
        Just sh -> do
            pgSize <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagesize")
            cursor <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "cursor")
            pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
            bp2pEnv <- getBitcoinP2P
            let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
            lg <- getLogger
            res <- LE.try $ runWithManyInputs xGetUTXOsScriptHash sh pgSize (decodeOP cursor)
            case res of
                Left (e :: SomeException) -> do
                    modifyResponse $ setResponseStatus 500 "Internal Server Error"
                    writeBS "INTERNAL_SERVER_ERROR"
                Right ops -> do
                    writeBS $
                        BSL.toStrict $
                        encodeResp pretty $
                        RespUTXOsByScriptHashes (encodeOP $ getNextCursor ops) (fromResultWithCursor <$> ops)
        Nothing -> throwBadRequest

getMNodesByTxID :: Handler App App ()
getMNodesByTxID = do
    txId <- (DT.unpack . DTE.decodeUtf8 . fromJust) <$> getParam "txid"
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetMerkleBranch txId
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> do
            writeBS $ BSL.toStrict $ encodeResp pretty $ RespMerkleBranchByTxID ops

getOutpointsByName :: Handler App App ()
getOutpointsByName = do
    name <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getParam "name")
    isProducer <- (fmap $ read . DT.unpack . DT.toTitle . DTE.decodeUtf8) <$> (getQueryParam "isProducer")
    pretty <- (maybe True (read . DT.unpack . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetAllegoryNameBranch (fromJust name) (fromJust isProducer)
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespAllegoryNameBranch ops

relayTx :: RPCReqParams' -> Handler App App ()
relayTx RelayTx {..} = do
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xRelayTx rTx
    case res of
        Left (e :: BlockSyncException) -> do
            case e of
                ParentProcessingException e -> do
                    modifyResponse $ setResponseStatus 400 "Bad Request"
                    writeBS $ "Rejected relay: exception while processing parent(s) of transaction: " <> S.pack e
                RelayFailureException -> do
                    modifyResponse $ setResponseStatus 500 "Internal Server Error"
                    writeBS "Failed to relay transaction to any Nexa peer"
                _ -> do
                    modifyResponse $ setResponseStatus 500 "Internal Server Error"
                    writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespRelayTx ops
relayTx _ = throwBadRequest

relayMultipleTx :: RPCReqParams' -> Handler App App ()
relayMultipleTx RelayMultipleTx {..} = do
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xRelayMultipleTx rTxns
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespRelayMultipleTx ops
relayMultipleTx _ = throwBadRequest

getOutpointByName :: RPCReqParams' -> Handler App App ()
getOutpointByName (AllegoryNameQuery nameArray isProducer) = do
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetOutpointByName nameArray isProducer
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS (S.pack $ show e)
        Right (forName, outpoint, script, confirmed, isProducer) ->
            writeBS $
            BSL.toStrict $
            encodeResp pretty $ RespOutpointByName forName outpoint (DT.unpack script) confirmed isProducer
getOutpointByName _ = throwBadRequest

findNameReseller :: RPCReqParams' -> Handler App App ()
findNameReseller (AllegoryNameQuery nameArray isProducer) = do
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xFindNameReseller nameArray isProducer
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS (S.pack $ show e)
        Right (forName, protocol, uri, confirmed, isProducer) ->
            writeBS $ BSL.toStrict $ encodeResp pretty $ RespFindNameReseller forName protocol uri confirmed isProducer
findNameReseller _ = throwBadRequest

getPurchasedNames :: RPCReqParams' -> Handler App App ()
getPurchasedNames (GetPurchasedNames nameArray pgSize cursor) = do
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetPurchasedNames nameArray pgSize cursor
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS (S.pack $ show e)
        Right (names, nextCursor) -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespPurchasedNames names nextCursor
getPurchasedNames _ = throwBadRequest

getCurrentUser :: Handler App App ()
getCurrentUser = do
    sk <- (fmap $ DTE.decodeUtf8) <$> (getParam "sessionKey")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetUserBySessionKey (fromJust sk)
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right u@(Just us) -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespUser u
        Right Nothing -> throwNotFound

getUserByUsername :: Handler App App ()
getUserByUsername = do
    uname <- (fmap $ DTE.decodeUtf8) <$> (getParam "username")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xGetUserByUsername (fromJust uname)
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right u@(Just us) -> writeBS $ BSL.toStrict $ encodeResp pretty $ RespUser u
        Right Nothing -> throwNotFound

deleteUserByUsername :: Handler App App ()
deleteUserByUsername = do
    uname <- (fmap $ DTE.decodeUtf8) <$> (getParam "username")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xDeleteUserByUsername (fromJust uname)
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right () -> do
            modifyResponse $ setResponseStatus 200 "Deleted"
            writeBS $ "User deleted"

updateUserByUsername :: UpdateUserByUsername' -> Handler App App ()
updateUserByUsername updates = do
    uname <- (fmap $ DTE.decodeUtf8) <$> (getParam "username")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    res <- LE.try $ xUpdateUserByUsername (fromJust uname) updates
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right True -> do
            modifyResponse $ setResponseStatus 200 "Updated"
            writeBS $ "User updated"
        Right False -> throwNotFound

getTxByProtocol :: Handler App App ()
getTxByProtocol = do
    proto <- getParam "protocol"
    lg <- getLogger
    pgSize <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagesize")
    cursor <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "cursor")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    props <- getQueryProps
    res <-
        LE.try $
        xGetTxIDByProtocol (DTE.decodeUtf8 $ fromJust proto) props pgSize (decodeNTI cursor) >>=
        (\c -> (encodeNTI $ getNextCursor c, ) <$> xGetTxHashes (fromResultWithCursor <$> c))
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxProtocol: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (nt, txs) -> do
            let rawTxs =
                    (\RawTxRecord {..} ->
                         (TxRecord txId size txBlockInfo <$>
                          (txToTx' <$> (Extra.hush $ S.decodeLazy txSerialized) <*> (pure $ fromMaybe [] txOutputs) <*>
                           (pure txInputs)) <*>
                          (pure fees) <*>
                          (pure txMerkleBranch))) <$>
                    txs
            writeBS $ BSL.toStrict $ encodeResp pretty $ RespTransactionsByProtocol nt (catMaybes rawTxs)
  where
    getQueryProps = do
        prop2 <- (fmap DTE.decodeUtf8) <$> getQueryParam "prop1"
        prop3 <- (fmap DTE.decodeUtf8) <$> getQueryParam "prop2"
        prop4 <- (fmap DTE.decodeUtf8) <$> getQueryParam "prop3"
        prop5 <- (fmap DTE.decodeUtf8) <$> getQueryParam "prop4"
        pure $ catMaybes [prop2, prop3, prop4, prop5]

getTxByProtocols :: Handler App App ()
getTxByProtocols = do
    allMap <- getQueryParams
    lg <- getLogger
    pgSize <- (fmap $ read . DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "pagesize")
    cursor <- (fmap $ DT.unpack . DTE.decodeUtf8) <$> (getQueryParam "cursor")
    pretty <- (maybe True (read . DT.unpack . DT.toTitle . DTE.decodeUtf8)) <$> (getQueryParam "pretty")
    let protocols = DTE.decodeUtf8 <$> (fromJust $ Map.lookup "protocol" allMap)
    let props = getQueryProps allMap
    res <-
        LE.try $
        traverse
            (\(proto, ind) -> xGetTxIDByProtocol proto (fromMaybe [] $ indexMaybe props ind) pgSize (decodeNTI cursor))
            (zip protocols [0 ..]) >>=
        ((\c -> (encodeNTI $ getNextCursor c, ) <$> xGetTxHashes (fromResultWithCursor <$> c)) . concat)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxProtocol: " ++ show e
            debug lg $ LG.msg $ "Error: xGetTxProtocol: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (nt, txs) -> do
            let rawTxs =
                    (\RawTxRecord {..} ->
                         (TxRecord txId size txBlockInfo <$>
                          (txToTx' <$> (Extra.hush $ S.decodeLazy txSerialized) <*> (pure $ fromMaybe [] txOutputs) <*>
                           (pure txInputs)) <*>
                          (pure fees) <*>
                          (pure txMerkleBranch))) <$>
                    txs
            writeBS $ BSL.toStrict $ encodeResp pretty $ RespTransactionsByProtocols nt (catMaybes rawTxs)
  where
    getQueryProps allMap = do
        let prop2 = DTE.decodeUtf8 <$> (fromMaybe [] $ Map.lookup "prop1" allMap)
            prop3 = DTE.decodeUtf8 <$> (fromMaybe [] $ Map.lookup "prop2" allMap)
            prop4 = DTE.decodeUtf8 <$> (fromMaybe [] $ Map.lookup "prop3" allMap)
            prop5 = DTE.decodeUtf8 <$> (fromMaybe [] $ Map.lookup "prop4" allMap)
        foldr
            (\(p, i) acc -> (catMaybes [Just p, indexMaybe prop3 i, indexMaybe prop4 i, indexMaybe prop5 i]) : acc)
            []
            (zip prop2 [0 ..])

--- |
-- Helper functions
withAuth :: Handler App App () -> Handler App App ()
withAuth onSuccess = do
    rq <- getRequest
    env <- gets _env
    let mh = getHeader "Authorization" rq
    let h = parseAuthorizationHeader mh
    case h of
        Just sk -> putRequest $ rqSetParam "sessionKey" [sk] rq
        Nothing -> return ()
    uok <- liftIO $ testAuthHeader env h Nothing
    modifyResponse (setContentType "application/json")
    if uok
        then onSuccess
        else case h of
                 Nothing -> throwChallenge
                 Just _ -> throwDenied

withAuthAs :: DT.Text -> Handler App App () -> Handler App App ()
withAuthAs role onSuccess = do
    rq <- getRequest
    env <- gets _env
    let mh = getHeader "Authorization" rq
    let h = parseAuthorizationHeader mh
    case h of
        Just sk -> putRequest $ rqSetParam "sessionKey" [sk] rq
        Nothing -> return ()
    uok <- liftIO $ testAuthHeader env h $ Just role
    modifyResponse (setContentType "application/json")
    if uok
        then onSuccess
        else case h of
                 Nothing -> throwChallenge
                 Just _ -> throwDenied

withReq :: Aeson.FromJSON a => (a -> Handler App App ()) -> Handler App App ()
withReq handler = do
    rq <- getRequest
    let ct = getHeader "content-type" rq <|> (getHeader "Content-Type" rq) <|> (getHeader "Content-type" rq)
    if ct == Just "application/json"
        then do
            bsReq <- readRequestBody (8 * 2048)
            case Aeson.eitherDecode bsReq of
                Right r -> handler r
                Left err -> do
                    modifyResponse $ setResponseStatus 400 "Bad Request"
                    writeBS "Error: failed to decode request body JSON"
        else throwBadRequest

parseAuthorizationHeader :: Maybe B.ByteString -> Maybe B.ByteString
parseAuthorizationHeader bs =
    case bs of
        Nothing -> Nothing
        Just x ->
            case (S.split ' ' x) of
                ("Bearer":y) ->
                    if S.length (S.intercalate "" y) > 0
                        then Just $ S.intercalate "" y
                        else Nothing
                _ -> Nothing

testAuthHeader :: XokenNodeEnv -> Maybe B.ByteString -> Maybe DT.Text -> IO Bool
testAuthHeader _ Nothing _ = pure False
testAuthHeader env (Just sessionKey) role = do
    let dbe = dbHandles env
        conn = xCqlClientState (dbe)
        lg = loggerEnv env
        bp2pEnv = bitcoinP2PEnv env
        sKey = DT.pack $ S.unpack sessionKey
    userData <- liftIO $ H.lookup (userDataCache bp2pEnv) sKey
    case userData of
        Just (name, quota, used, exp, roles) -> do
            curtm <- liftIO $ getCurrentTime
            if exp > curtm && quota > used
                then do
                    if (used + 1) `mod` 100 == 0
                        then do
                            let str = " UPDATE xoken.user_permission SET api_used = ? WHERE username = ? "
                                qstr = str :: Q.QueryString Q.W (Int32, DT.Text) ()
                                p = getSimpleQueryParam (used + 1, name)
                            res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr p)
                            case res of
                                Left (SomeException e) -> do
                                    err lg $ LG.msg $ "Error: UPDATE'ing into 'user_permission': " ++ show e
                                    throw e
                                Right _ -> return ()
                        else return ()
                    liftIO $ H.insert (userDataCache bp2pEnv) sKey (name, quota, used + 1, exp, roles)
                    case role of
                        Nothing -> return True
                        Just rl -> return $ rl `elem` roles
                else do
                    liftIO $ H.delete (userDataCache bp2pEnv) sKey
                    return False
        Nothing -> do
            let str =
                    " SELECT username, api_quota, api_used, session_key_expiry_time, permissions FROM xoken.user_permission WHERE session_key = ? ALLOW FILTERING "
                qstr = str :: Q.QueryString Q.R (Identity DT.Text) (DT.Text, Int32, Int32, UTCTime, Set DT.Text)
                p = getSimpleQueryParam $ Identity $ sKey
            res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
            case res of
                Left (SomeException e) -> do
                    err lg $ LG.msg $ "Error: SELECT'ing from 'user_permission': " ++ show e
                    throw e
                Right op -> do
                    if length op == 0
                        then return False
                        else do
                            case op !! 0 of
                                (name, quota, used, exp, roles) -> do
                                    curtm <- liftIO $ getCurrentTime
                                    if exp > curtm && quota > used
                                        then do
                                            liftIO $
                                                H.insert
                                                    (userDataCache bp2pEnv)
                                                    sKey
                                                    (name, quota, used + 1, exp, fromSet roles)
                                            case role of
                                                Nothing -> return True
                                                Just rl -> return $ rl `elem` (fromSet roles)
                                        else return False

throwChallenge :: Handler App App ()
throwChallenge = do
    modifyResponse $
        (setResponseStatus 401 "Unauthorized") . (setHeader "WWW-Authenticate" "Basic realm=my-authentication")
    writeBS ""

throwDenied :: Handler App App ()
throwDenied = do
    modifyResponse $ setResponseStatus 403 "Access Denied"
    writeBS "Access Denied"

throwBadRequest :: Handler App App ()
throwBadRequest = do
    modifyResponse $ setResponseStatus 400 "Bad Request"
    writeBS "Bad Request"

throwNotFound :: Handler App App ()
throwNotFound = do
    modifyResponse $ setResponseStatus 404 "Not Found"
    writeBS "Not Found"
