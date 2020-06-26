{-# LANGUAGE OverloadedStrings #-}

module Network.Xoken.Node.HTTP.Server where

import Arivi.P2P.Config (encodeHex)
import Control.Exception (SomeException(..), throw, try)
import Control.Monad.IO.Class
import Control.Monad.State.Class
import qualified Data.Aeson as Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BSL
import Data.Int
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
apiRoutes = [("/auth", method POST login)]

login :: Handler App App ()
login = do
    user <- getPostParam "username"
    pass <- getPostParam "password"
    if isNothing user || isNothing pass
        then do
            modifyResponse $ setResponseStatus 400 "Bad Request"
            writeBS "400 error"
            r <- getResponse
            finishWith r
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
                                                        (newSessionKey, (addUTCTime (nominalDay * 30) tm), DTE.decodeUtf8 $ fromJust user)
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
