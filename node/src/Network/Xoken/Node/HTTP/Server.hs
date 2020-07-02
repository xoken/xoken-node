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
    , ("/v1/chain/info/", method GET (withAuth getChainInfo))
    , ("/v1/block/hash/:hash", method GET (withAuth getBlockByHash))
    , ("/v1/block/hashes", method GET (withAuth getBlocksByHash))
    , ("/v1/block/height/:height", method GET (withAuth getBlockByHeight))
    , ("/v1/block/heights", method GET (withAuth getBlocksByHeight))
    , ("/v1/rawtransaction/:id", method GET (withAuth getRawTxById))
    , ("/v1/rawtransactions", method GET (withAuth getRawTxByIds))
    , ("/v1/transactionSpendStatus/:txid/:index", method GET (withAuth getTxOutputSpendStatus))
    , ("/v1/transaction/:id", method GET (withAuth getTxById))
    , ("/v1/transactions", method GET (withAuth getTxByIds))
    , ("/v1/address/:address", method GET (withAuth getOutputsByAddr))
    , ("/v1/addresses", method GET (withAuth getOutputsByAddrs))
    , ("/v1/scripthash/:scriptHash", method GET (withAuth getOutputsByScriptHash))
    , ("/v1/scripthashes", method GET (withAuth getOutputsByScriptHashes))
    , ("/v1/merklebranch/:txid", method GET (withAuth getMNodesByTxID))
    , ("/v1/allegory/:name", method GET (withAuth getOutpointsByName))
    , ("/v1/relaytx", method POST (withAuth $ withReq relayTx))
    , ("/v1/partialsign", method POST (withAuth $ withReq getPartiallySignedAllegoryTx))
    ]
