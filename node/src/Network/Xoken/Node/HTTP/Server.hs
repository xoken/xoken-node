{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.HTTP.Server where

import Arivi.P2P.Config (decodeHex, encodeHex)
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
import Network.Xoken.Node.XokenService
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
apiRoutes = [ ("/v1/auth", method POST authClient)
            , ("/v1/block/hash/:hash", method GET (withAuth getBlockByHash))
            , ("/v1/block/hash", method GET (withAuth getBlocksByHash))
            , ("/v1/block/height/:height", method GET (withAuth getBlockByHeight))
            ]

authClient :: Handler App App ()
authClient = do
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
            resp <- LE.try $ login dbe lg (DTE.decodeUtf8 $ fromJust user) (fromJust pass)
            case resp of
                Left (e :: SomeException) -> do
                    modifyResponse $ setResponseStatus 500 "Internal Server Error"
                    writeBS "INTERNAL_SERVER_ERROR"
                Right ar -> writeBS $ BSL.toStrict $ Aeson.encode ar

getBlockByHash :: Handler App App ()
getBlockByHash = do
    hash <- getParam "hash"
    env <- gets _env
    let dbe = dbHandles env
        lg = loggerEnv env
    res <- LE.try $ xGetBlockHash dbe lg (DTE.decodeUtf8 $ fromJust hash)
    case res of
      Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
      Right (Just rec) -> writeBS $ BSL.toStrict $ Aeson.encode rec
      Right Nothing -> do
            modifyResponse $ setResponseStatus 400 "Bad Request"
            writeBS "400 error"

getBlocksByHash :: Handler App App ()
getBlocksByHash = do
    allMap <- getQueryParams
    env <- gets _env
    let dbe = dbHandles env
        lg = loggerEnv env
    res <- LE.try $ xGetBlocksHashes dbe lg (DTE.decodeUtf8 <$> (fromJust $ Map.lookup "hash" allMap))
    case res of
      Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHash: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
      Right rec -> writeBS $ BSL.toStrict $ Aeson.encode rec

getBlockByHeight :: Handler App App ()
getBlockByHeight = do
    height <- getParam "height"
    env <- gets _env
    let dbe = dbHandles env
        lg = loggerEnv env
    res <- LE.try $ xGetBlockHeight dbe lg (read $ DT.unpack $ DTE.decodeUtf8 $ fromJust height)
    case res of
      Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHeight: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
      Right (Just rec) -> writeBS $ BSL.toStrict $ Aeson.encode rec
      Right Nothing -> do
            modifyResponse $ setResponseStatus 400 "Bad Request"
            writeBS "400 error"

getBlocksByHeight :: Handler App App ()
getBlocksByHeight = do
    allMap <- getQueryParams
    env <- gets _env
    let dbe = dbHandles env
        lg = loggerEnv env
    res <- LE.try $ xGetBlocksHeights dbe lg (read . DT.unpack . DTE.decodeUtf8 <$> (fromJust $ Map.lookup "height" allMap))
    case res of
      Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlocksHeight: " ++ show e
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
      Right rec -> writeBS $ BSL.toStrict $ Aeson.encode rec

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
