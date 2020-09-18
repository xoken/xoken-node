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
    , ("/v1/user", method POST (withAuthAs "admin" $ withReq addUser))
    , ("/v1/user/:username", method GET (withAuthAs "admin" getUserByUsername))
    , ("/v1/user/:username", method DELETE (withAuthAs "admin" deleteUserByUsername))
    , ("/v1/user/:username", method PUT (withAuthAs "admin" $ withReq updateUserByUsername))
    , ("/v1/user/", method GET (withAuth getCurrentUser))
    , ("/v1/chain/info/", method GET (withAuth getChainInfo))
    , ("/v1/chain/headers/", method GET (withAuth getChainHeaders))
    , ("/v1/block/hash/:hash", method GET (withAuth getBlockByHash))
    , ("/v1/block/hashes", method GET (withAuth getBlocksByHash))
    , ("/v1/block/height/:height", method GET (withAuth getBlockByHeight))
    , ("/v1/block/heights", method GET (withAuth getBlocksByHeight))
    , ("/v1/block/txids/:hash", method GET (withAuth getTxIDsByBlockHash))
    , ("/v1/rawtransaction/:id", method GET (withAuth getRawTxById))
    , ("/v1/rawtransactions", method GET (withAuth getRawTxByIds))
    , ("/v1/transaction/:id", method GET (withAuth getTxById))
    , ("/v1/transactions", method GET (withAuth getTxByIds))
    , ("/v1/transaction/:txid/index/:index", method GET (withAuth getTxOutputSpendStatus))
    , ("/v1/address/:address/outputs", method GET (withAuth getOutputsByAddr))
    , ("/v1/addresses/outputs/", method GET (withAuth getOutputsByAddrs))
    , ("/v1/scripthash/:scripthash/outputs", method GET (withAuth getOutputsByScriptHash))
    , ("/v1/scripthashes/outputs", method GET (withAuth getOutputsByScriptHashes))
    , ("/v1/address/:address/utxos", method GET (withAuth getUTXOsByAddr))
    , ("/v1/addresses/utxos", method GET (withAuth getUTXOsByAddrs))
    , ("/v1/scripthash/:scripthash/utxos", method GET (withAuth getUTXOsByScriptHash))
    , ("/v1/scripthashes/utxos", method GET (withAuth getUTXOsByScriptHashes))
    , ("/v1/merklebranch/:txid", method GET (withAuth getMNodesByTxID))
    , ("/v1/allegory/:name", method GET (withAuth getOutpointsByName))
    , ("/v1/producer", method POST (withAuth $ withReq getProducer))
    , ("/v1/relaytx", method POST (withAuth $ withReq relayTx))
    , ("/v1/relaymultipletx", method POST (withAuth $ withReq relayMultipleTx))
    ]
