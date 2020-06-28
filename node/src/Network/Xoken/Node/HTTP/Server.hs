{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.HTTP.Server where

import Arivi.P2P.Config (encodeHex, decodeHex)
import Control.Exception (SomeException(..), throw, try)
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.IO.Class
import Control.Monad.State.Class
import qualified Data.Aeson as Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as C
import Data.Int
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
import Network.Xoken.Node.Data (BlockRecord(..), coinbaseTxToMessage)
import Network.Xoken.Node.Env
import Network.Xoken.Node.HTTP.Types
import Network.Xoken.Node.P2P.Common (generateSessionKey)
import Network.Xoken.Node.P2P.Types
import Snap
import System.Logger as LG

appInit :: XokenNodeEnv -> SnapletInit App App
appInit env =
    makeSnaplet "v1" "API's" Nothing $ do
        addRoutes apiRoutes
        return $ App env

apiRoutes :: [(B.ByteString, Handler App App ())]
apiRoutes = [("/v1/auth", method POST login), ("/v1/block/hash/:hash", method GET (withAuth getHash))]

login :: Handler App App ()
login = do
    rq <- getRequest
    let allParams = rqPostParams rq
        user = S.intercalate " " <$> Map.lookup "username" allParams
        pass = S.intercalate " " <$> Map.lookup "password" allParams
    if isNothing user || isNothing pass
        then do
            modifyResponse $ setResponseStatus 400 "Bad Request"
            writeBS "400 error"
        else do
            env <- gets _env
            let dbe = dbHandles env
            let conn = keyValDB (dbe)
            let lg = loggerEnv env
            let hashedPasswd = encodeHex ((S.encode $ sha256 $ fromJust pass))
                str =
                    " SELECT password, api_quota, api_used, session_key_expiry_time FROM xoken.user_permission WHERE username = ? "
                qstr = str :: Q.QueryString Q.R (Identity DT.Text) (DT.Text, Int32, Int32, UTCTime)
                p = Q.defQueryParams Q.One $ Identity $ (DTE.decodeUtf8 $ fromJust user)
            res <- liftIO $ try $ Q.runClient conn (Q.query (Q.prepared qstr) p)
            case res of
                Left (SomeException e) -> do
                    err lg $ LG.msg $ "Error: SELECT'ing from 'user_permission': " ++ show e
                    throw e
                Right (op) -> do
                    if length op == 0
                        then writeBS $ BSL.toStrict $ Aeson.encode $ AuthResp Nothing 0 0
                        else do
                            case (op !! 0) of
                                (sk, _, _, _) -> do
                                    if (sk /= hashedPasswd)
                                        then writeBS $ BSL.toStrict $ Aeson.encode $ AuthResp Nothing 0 0
                                        else do
                                            tm <- liftIO $ getCurrentTime
                                            newSessionKey <- liftIO $ generateSessionKey
                                            let str1 =
                                                    "UPDATE xoken.user_permission SET session_key = ?, session_key_expiry_time = ? WHERE username = ? "
                                                qstr1 = str1 :: Q.QueryString Q.W (DT.Text, UTCTime, DT.Text) ()
                                                par1 =
                                                    Q.defQueryParams
                                                        Q.One
                                                        ( newSessionKey
                                                        , (addUTCTime (nominalDay * 30) tm)
                                                        , DTE.decodeUtf8 $ fromJust user)
                                            res1 <- liftIO $ try $ Q.runClient conn (Q.write (qstr1) par1)
                                            case res1 of
                                                Right () -> return ()
                                                Left (SomeException e) -> do
                                                    err lg $
                                                        LG.msg $ "Error: UPDATE'ing into 'user_permission': " ++ show e
                                                    throw e
                                            writeBS $
                                                BSL.toStrict $
                                                Aeson.encode $ AuthResp (Just $ DT.unpack newSessionKey) 1 100

getHash :: Handler App App ()
getHash = do
    hash <- getParam "hash"
    env <- gets _env
    let dbe = dbHandles env
        conn = keyValDB (dbe)
        lg = loggerEnv env
        str =
            "SELECT block_hash,block_height,block_header,block_size,tx_count,coinbase_tx from xoken.blocks_by_hash where block_hash = ?"
        qstr =
            str :: Q.QueryString Q.R (Identity DT.Text) (DT.Text, Int32, DT.Text, Maybe Int32, Maybe Int32, Maybe Blob)
        p = Q.defQueryParams Q.One $ Identity $ DTE.decodeUtf8 $ fromJust hash
    res <- LE.try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            if length iop == 0
                then do
                    modifyResponse $ setResponseStatus 400 "Bad Request"
                    writeBS "400 error"
                else do
                    let (hs, ht, hdr, size, txc, cbase) = iop !! 0
                    case Aeson.eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 hdr of
                        Right bh ->
                            writeBS $
                            BSL.toStrict $
                            Aeson.encode $
                            BlockRecord
                                (fromIntegral ht)
                                (DT.unpack hs)
                                bh
                                (maybe (-1) fromIntegral size)
                                (maybe (-1) fromIntegral txc)
                                ("")
                                (maybe "" (coinbaseTxToMessage . fromBlob) cbase)
                                (maybe "" (C.unpack . fromBlob) cbase)
                        Left err -> do
                            liftIO $ print $ "Decode failed with error: " <> show err
                            modifyResponse $ setResponseStatus 400 "Bad Request"
                            writeBS "400 error"
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"

--- Helpers
withAuth :: Handler App App () -> Handler App App ()
withAuth onSuccess = do
    rq <- getRequest
    env <- gets _env
    let mh = getHeader "Authorization" rq
    let h = parseAuthorizationHeader mh
    uok <- liftIO $ testAuthHeader env h
    if uok
        then onSuccess
        else case h of
                 Nothing -> throwChallenge
                 Just _ -> throwDenied

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
