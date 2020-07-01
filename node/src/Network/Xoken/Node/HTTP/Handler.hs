{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE Rank2Types #-}

module Network.Xoken.Node.HTTP.Handler where

import Arivi.P2P.Config (decodeHex, encodeHex)
import qualified Control.Error.Util as Extra
import Control.Exception (SomeException(..), throw, try)
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.State.Class
import qualified Data.Aeson as Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as C
import qualified Data.Either as Either
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
import Database.CQL.IO as Q
import Database.CQL.Protocol
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.Data
    ( BlockRecord(..)
    , RPCResponseBody(..)
    , RawTxRecord(..)
    , TxRecord(..)
    , addressToScriptOutputs
    , aoAddress
    , coinbaseTxToMessage
    , RPCReqParams(..)
    , RPCReqParams'(..)
    )
import Network.Xoken.Node.Env
import Network.Xoken.Node.HTTP.Types
import Network.Xoken.Node.P2P.Common (generateSessionKey)
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.XokenService
import Snap
import System.Logger as LG
import Text.Read (readMaybe)
import qualified Xoken.NodeConfig as NC

authClient :: RPCReqParams -> Handler App App ()
authClient AuthenticateReq {..} = do
    resp <- LE.try $ login (DT.pack username) (BC.pack password)
    case resp of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ar -> writeBS $ BSL.toStrict $ Aeson.encode $ AuthenticateResp ar
authClient _ = throwBadRequest

getBlockByHash :: Handler App App ()
getBlockByHash = do
    hash <- getParam "hash"
    lg <- getLogger
    res <- LE.try $ xGetBlockHash (DTE.decodeUtf8 $ fromJust hash)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (Just rec) -> writeBS $ BSL.toStrict $ Aeson.encode $ RespBlockByHash rec
        Right Nothing -> do
            modifyResponse $ setResponseStatus 400 "Bad Request"
            writeBS "400 error"

getBlocksByHash :: Handler App App ()
getBlocksByHash = do
    allMap <- getQueryParams
    lg <- getLogger
    res <- LE.try $ xGetBlocksHashes (DTE.decodeUtf8 <$> (fromJust $ Map.lookup "hash" allMap))
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right rec -> writeBS $ BSL.toStrict $ Aeson.encode $ RespBlocksByHashes rec

getBlockByHeight :: Handler App App ()
getBlockByHeight = do
    height <- getParam "height"
    lg <- getLogger
    res <- LE.try $ xGetBlockHeight (read $ DT.unpack $ DTE.decodeUtf8 $ fromJust height)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHeight: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (Just rec) -> writeBS $ BSL.toStrict $ Aeson.encode $ RespBlockByHeight rec
        Right Nothing -> do
            modifyResponse $ setResponseStatus 400 "Bad Request"
            writeBS "400 error"

getBlocksByHeight :: Handler App App ()
getBlocksByHeight = do
    allMap <- getQueryParams
    lg <- getLogger
    res <-
        LE.try $
        xGetBlocksHeights (read . DT.unpack . DTE.decodeUtf8 <$> (fromJust $ Map.lookup "height" allMap))
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHeight: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right rec -> writeBS $ BSL.toStrict $ Aeson.encode $ RespBlocksByHeight rec

getRawTxById :: Handler App App ()
getRawTxById = do
    txId <- getParam "id"
    lg <- getLogger
    res <- LE.try $ xGetTxHash (DTE.decodeUtf8 $ fromJust txId)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (Just rec) -> writeBS $ BSL.toStrict $ Aeson.encode $ RespRawTransactionByTxID rec
        Right Nothing -> do
            modifyResponse $ setResponseStatus 400 "Bad Request"
            writeBS "400 error"

getRawTxByIds :: Handler App App ()
getRawTxByIds = do
    allMap <- getQueryParams
    lg <- getLogger
    res <- LE.try $ xGetTxHashes (DTE.decodeUtf8 <$> (fromJust $ Map.lookup "id" allMap))
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right rec -> writeBS $ BSL.toStrict $ Aeson.encode $ RespRawTransactionsByTxIDs rec

getTxById :: Handler App App ()
getTxById = do
    txId <- getParam "id"
    lg <- getLogger
    res <- LE.try $ xGetTxHash (DTE.decodeUtf8 $ fromJust txId)
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (Just RawTxRecord {..}) -> do
            case S.decodeLazy txSerialized of
                Right rt -> writeBS $ BSL.toStrict $ Aeson.encode $ RespTransactionByTxID (TxRecord txId txBlockInfo rt)
                Left err -> do
                    modifyResponse $ setResponseStatus 400 "Bad Request"
                    writeBS "400 error"
        Right Nothing -> do
            modifyResponse $ setResponseStatus 400 "Bad Request"
            writeBS "400 error"

getTxByIds :: Handler App App ()
getTxByIds = do
    allMap <- getQueryParams
    lg <- getLogger
    res <- LE.try $ xGetTxHashes (DTE.decodeUtf8 <$> (fromJust $ Map.lookup "id" allMap))
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetTxHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right txs -> do
            let rawTxs =
                    (\RawTxRecord {..} -> (TxRecord txId txBlockInfo) <$> (Extra.hush $ S.decodeLazy txSerialized)) <$>
                    txs
            writeBS $ BSL.toStrict $ Aeson.encode $ RespTransactionsByTxIDs $ catMaybes rawTxs

getOutputsByAddr :: RPCReqParams' -> Handler App App ()
getOutputsByAddr GetOutputsByAddress {..} = do
    bp2pEnv <- getBitcoinP2P
    let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
    lg <- getLogger
    res <-
        LE.try $
        case convertToScriptHash net gaAddrOutputs of
            Just o -> xGetOutputsAddress o gaPageSize gaNominalTxIndex
            Nothing -> return []
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetOutputsAddress: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> do
            writeBS $ BSL.toStrict $ Aeson.encode $ RespOutputsByAddress $ (\ao -> ao {aoAddress = gaAddrOutputs}) <$> ops
getOutputsByAddr _ = throwBadRequest

getOutputsByAddrs :: RPCReqParams' -> Handler App App ()
getOutputsByAddrs GetOutputsByAddresses{..} = do
    bp2pEnv <- getBitcoinP2P
    let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
    lg <- getLogger
    let (shs, shMap) =
            L.foldl'
                (\(arr, m) x ->
                     case convertToScriptHash net x of
                         Just addr -> (addr : arr, Map.insert addr x m)
                         Nothing -> (arr, m))
                ([], Map.empty)
                gasAddrOutputs
    res <- LE.try $ xGetOutputsAddresses shs gasPageSize gasNominalTxIndex
    case res of
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetOutputsAddress: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> do
            writeBS $
                BSL.toStrict $
                Aeson.encode $
                RespOutputsByAddresses $ (\ao -> ao {aoAddress = fromJust $ Map.lookup (aoAddress ao) shMap}) <$> ops
getOutputsByAddrs _ = throwBadRequest

getOutputsByScriptHash :: RPCReqParams' -> Handler App App ()
getOutputsByScriptHash GetOutputsByScriptHash{..} = do
    bp2pEnv <- getBitcoinP2P
    let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
    lg <- getLogger
    res <- LE.try $ xGetOutputsAddress gaScriptHashOutputs gaScriptHashPageSize gaScriptHashNominalTxIndex
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> do
            writeBS $ BSL.toStrict $ Aeson.encode $ RespOutputsByScriptHash $ addressToScriptOutputs <$> ops
getOutputsByScriptHash _ = throwBadRequest

getOutputsByScriptHashes :: RPCReqParams' -> Handler App App ()
getOutputsByScriptHashes GetOutputsByScriptHashes{..} = do
    bp2pEnv <- getBitcoinP2P
    let net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
    lg <- getLogger
    res <- LE.try $ xGetOutputsAddresses gasScriptHashOutputs gasScriptHashPageSize gasScriptHashNominalTxIndex
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> do
            writeBS $ BSL.toStrict $ Aeson.encode $ RespOutputsByScriptHashes $ addressToScriptOutputs <$> ops
getOutputsByScriptHashes _ = throwBadRequest

getMNodesByTxID :: Handler App App ()
getMNodesByTxID = do
    txId <- (DT.unpack . DTE.decodeUtf8 . fromJust) <$> getParam "txId"
    res <- LE.try $ xGetMerkleBranch txId
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> do
            writeBS $ BSL.toStrict $ Aeson.encode $ RespMerkleBranchByTxID ops

getOutpointsByName :: RPCReqParams' -> Handler App App ()
getOutpointsByName GetAllegoryNameBranch{..} = do
    res <- LE.try $ xGetAllegoryNameBranch gaName gaIsProducer
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops ->
            writeBS $ BSL.toStrict $ Aeson.encode $ RespAllegoryNameBranch ops
getOutpointsByName _ = throwBadRequest

getRelayTx :: Handler App App ()
getRelayTx = do
    tx <- fromJust <$> getParam "tx"
    res <- LE.try $ xRelayTx tx
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops ->
            writeBS $ BSL.toStrict $ Aeson.encode $ RespRelayTx ops

getPartiallySignedAllegoryTx :: RPCReqParams' -> Handler App App ()
getPartiallySignedAllegoryTx GetPartiallySignedAllegoryTx{..} = do
    res <- LE.try $ xGetPartiallySignedAllegoryTx gpsaPaymentInputs gpsaName gpsaOutputOwner gpsaOutputChange
    case res of
        Left (e :: SomeException) -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right ops -> do
            writeBS $ BSL.toStrict $ Aeson.encode $ RespPartiallySignedAllegoryTx ops
getPartiallySignedAllegoryTx _ = throwBadRequest

--- |
-- Helper functions
withAuth :: Handler App App () -> Handler App App ()
withAuth onSuccess = do
    rq <- getRequest
    env <- gets _env
    let mh = getHeader "Authorization" rq
    let h = parseAuthorizationHeader mh
    uok <- liftIO $ testAuthHeader env h
    modifyResponse (setContentType "application/json")
    if uok
        then onSuccess
        else case h of
                 Nothing -> throwChallenge
                 Just _ -> throwDenied

withReq :: Aeson.FromJSON a => (a -> Handler App App ()) -> Handler App App ()
withReq handler = do
    bsReq <- readRequestBody (8 * 2048)
    case Aeson.eitherDecode bsReq of
        Right r -> handler r
        Left err -> do
            modifyResponse $ setResponseStatus 400 "Bad Request"
            writeBS "400 error"

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

testAuthHeader :: XokenNodeEnv -> Maybe B.ByteString -> IO Bool
testAuthHeader _ Nothing = pure False
testAuthHeader env (Just sessionKey) = do
    let dbe = dbHandles env
    let conn = keyValDB (dbe)
    let lg = loggerEnv env
    let str =
            " SELECT api_quota, api_used, session_key_expiry_time FROM xoken.user_permission WHERE session_key = ? ALLOW FILTERING "
        qstr = str :: Q.QueryString Q.R (Q.Identity DT.Text) (Int32, Int32, UTCTime)
        p = Q.defQueryParams Q.One $ Identity $ (DTE.decodeUtf8 sessionKey)
    res <- liftIO $ try $ Q.runClient conn (Q.query (Q.prepared qstr) p)
    case res of
        Left (SomeException e) -> do
            err lg $ LG.msg $ "Error: SELECT'ing from 'user_permission': " ++ show e
            throw e
        Right (op) -> do
            if length op == 0
                then return False
                else do
                    case op !! 0 of
                        (quota, used, exp) -> do
                            curtm <- liftIO $ getCurrentTime
                            if exp > curtm && quota > used
                                then return True
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
