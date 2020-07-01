{-# LANGUAGE OverloadedStrings #-}

module Network.Xoken.Node.HTTP.Server where

import qualified Data.ByteString as B
import Network.Xoken.Node.Env
import Network.Xoken.Node.HTTP.Handler
import Network.Xoken.Node.HTTP.Types
import Snap

appInit :: XokenNodeEnv -> SnapletInit App App
appInit env =
    makeSnaplet "v1" "API's" Nothing $ do
        addRoutes apiRoutes
        return $ App env

apiRoutes :: [(B.ByteString, Handler App App ())]
apiRoutes =
    [ ("/v1/auth", method POST (withReq authClient))
    , ("/v1/block/hash/:hash", method GET (withAuth getBlockByHash))
    , ("/v1/block/hash", method GET (withAuth getBlocksByHash))
    , ("/v1/block/height/:height", method GET (withAuth getBlockByHeight))
    , ("/v1/block/height", method GET (withAuth getBlocksByHeight))
    , ("/v1/rawtx/:id", method GET (withAuth getRawTxById))
    , ("/v1/rawtx", method GET (withAuth getRawTxByIds))
    , ("/v1/tx/:id", method GET (withAuth getTxById))
    , ("/v1/tx", method GET (withAuth getTxByIds))
    , ("/v1/output/address", method POST (withAuth $ withReq getOutputsByAddr))
    , ("/v1/outputs/addresses", method POST (withAuth $ withReq getOutputsByAddrs))
    , ("/v1/output/scriptHash", method POST (withAuth $ withReq getOutputsByScriptHash))
    , ("/v1/output/scriptHashes", method POST (withAuth $ withReq getOutputsByScriptHashes))
    , ("/v1/mnodes/:txId", method GET (withAuth getMNodesByTxID))
    , ("/v1/outpoints", method POST (withAuth $ withReq getOutpointsByName))
    , ("/v1/relaytx/:tx", method GET (withAuth getRelayTx))
    , ("/v1/partialsign", method POST (withAuth $ withReq getPartiallySignedAllegoryTx))
    ]
