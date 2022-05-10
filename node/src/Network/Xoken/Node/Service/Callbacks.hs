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

module Network.Xoken.Node.Service.Callbacks where

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
import qualified Control.Concurrent.Async.Lifted as LA (async, concurrently, mapConcurrently, wait)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Concurrent.STM.TVar
import qualified Control.Error.Util as Extra
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
import Data.Function ((&))
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
import qualified Data.Set as SE
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
import Network.HTTP.Simple as HS
import qualified Network.Simple.TCP.TLS as TLS
import Network.Xoken.Address.Base58
import Network.Xoken.Block.Common
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.Allegory
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Service.Transaction
import Network.Xoken.Util (bsToInteger, integerToBS)
import Numeric (showHex)
import Streamly as S
import Streamly.Prelude (drain, each, nil, (|:))
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Text.Read
import Xoken
import qualified Xoken.NodeConfig as NC

xGetCallbackByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> DT.Text -> m (Maybe MapiCallback)
xGetCallbackByUsername name cbName = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        context = DT.append (DT.pack "USER/") name
        key = DT.append cbName context
    cachedCallback <- liftIO $ TSH.lookup (callbacksDataCache bp2pEnv) (context)
    case cachedCallback of
        Just cp -> return cachedCallback
        Nothing -> do
            let str = "SELECT context, callback_name, callback_url, auth_type, auth_key, events, created_time from xoken.callbacks where context = ? AND callback_group = ? AND callback_name = ? "
            let qstr =
                    str ::
                        Q.QueryString
                            Q.R
                            (DT.Text, DT.Text, DT.Text)
                            ( DT.Text
                            , DT.Text
                            , DT.Text
                            , DT.Text
                            , DT.Text
                            , Set (DT.Text)
                            , UTCTime
                            )
            let p = getSimpleQueryParam $ (context, DT.pack "mAPI", cbName)
            res <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query qstr p)
            case res of
                Right iop -> do
                    if length iop == 0
                        then return Nothing
                        else do
                            let (context, callbackName, callbackUrl, authType, authKey, events, createdTime) = iop !! 0
                            let eventsT = L.map (\ev -> read (DT.unpack ev) :: CallbackEvent) (Q.fromSet events)
                            let mp = Just $ MapiCallback context (DT.pack "mAPI") callbackName callbackUrl authType authKey eventsT createdTime
                            return mp
                Left (e :: SomeException) -> do
                    err lg $ LG.msg $ "Error: xGetCallbackByUsername: " ++ show e
                    throw KeyValueDBLookupException

xDeleteCallbackByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> DT.Text -> m ()
xDeleteCallbackByUsername name cbName = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState dbe
        context = DT.append (DT.pack "USER/") name
        key = DT.append cbName context
        str = "DELETE FROM xoken.callbacks WHERE context = ? and callback_group = ? and callback_name = ? "
        qstr = str :: Q.QueryString Q.W (DT.Text, DT.Text, DT.Text) ()
        par = getSimpleQueryParam (context, DT.pack "mAPI", cbName)
    res <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr par)
    case res of
        Right _ -> do
            liftIO $ (TSH.delete $ userPolicies $ policyDataCache bp2pEnv) key
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xDeletePolicyByUsername: " ++ show e
            throw e

xUpdateCallbackByUsername :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> DT.Text -> DT.Text -> DT.Text -> DT.Text -> [DT.Text] -> m (Maybe MapiCallback)
xUpdateCallbackByUsername name cbName cbUrl authType authToken events = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    tm <- liftIO $ getCurrentTime
    mapM_
        ( \ev -> do
            let evs = DT.unpack ev
            case evs of
                "MerkleProofSingle" -> return ()
                "MerkleProofComposite" -> return ()
                "DoubleSpendingCheck" -> return ()
                "AncestorsDiscovered" -> return ()
                _ -> throw InvalidMessageTypeException
        )
        events

    let str = "INSERT INTO xoken.callbacks ( context, callback_group, callback_name, callback_url, auth_type, auth_key, events, created_time) VALUES (?,?,?,?,?,?,?,?)"
        context = DT.append (DT.pack "USER/") name
        key = DT.append cbName context
        qstr = str :: Q.QueryString Q.W (DT.Text, DT.Text, DT.Text, DT.Text, DT.Text, DT.Text, [DT.Text], UTCTime) ()
        skTime = (addUTCTime (nominalDay * 30) tm)
        par = getSimpleQueryParam (context, DT.pack "mAPI", cbName, cbUrl, authType, authToken, events, skTime)
    let conn = xCqlClientState dbe
    res' <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr par)
    case res' of
        Right _ -> do
            let eventsT = L.map (\ev -> read (DT.unpack ev) :: CallbackEvent) events
            liftIO $
                TSH.insert
                    (callbacksDataCache bp2pEnv)
                    key
                    (MapiCallback context (DT.pack "mAPI") cbName cbUrl authType authToken eventsT skTime)
            cbRec <- xGetCallbackByUsername name cbName
            return cbRec
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xUpdatePolicyByUsername (updating data): " ++ show e
            throw e

triggerCallbacks :: (HasXokenNodeEnv env m, MonadIO m) => m ()
triggerCallbacks = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    loadCallbacksCache -- TODO: this needs to be called when booting up, letting it remain here for now
    debug lg $ LG.msg $ "triggerCallbacks: inside : " ++ show ""
    cbDataCache <- liftIO $ TSH.toList $ callbacksDataCache bp2pEnv
    xx <-
        LE.try $
            S.drain $
                aheadly $
                    (do S.fromList cbDataCache)
                        & S.mapM
                            ( \(userid, mapicb) -> do
                                merkleRootBlockInfoCache <- liftIO $ TSH.new 1
                                compositeProofs <- liftIO $ TSH.new 4
                                processCallbacks userid "0000" merkleRootBlockInfoCache compositeProofs --dummy starting txid
                                return (userid, merkleRootBlockInfoCache, compositeProofs)
                            )
                        & S.mapM
                            ( \(userid, merkleRootBlockInfoCache, compositeProofs) -> do
                                postCompositeMerkleCallbacks userid merkleRootBlockInfoCache compositeProofs
                            )
                        & S.maxBuffer (5) --  nodeConfig p2pEnv)
                        & S.maxThreads (5) -- nodeConfig p2pEnv)
    debug lg $ LG.msg $ "triggerCallbacks: done: " ++ show ""
    case xx of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error while triggerCallbacks: " ++ show e
            throw e
type MerkleRootBlockInfoCache = (TSH.TSHashTable DT.Text (DT.Text, Int32))
type CompositeProofs = (TSH.TSHashTable DT.Text ((DT.Text, Int32, Int32), (DT.Text, Maybe [MerkleBranchNode'], AState)))

postCompositeMerkleCallbacks :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> MerkleRootBlockInfoCache -> CompositeProofs -> m ()
postCompositeMerkleCallbacks userid mrbiCache compProofs = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    cpList <- liftIO $ TSH.toList $ compProofs
    let cbname = fst3 $ snd $ snd $ head cpList
    let callbacksGroups = L.groupBy (\a b -> (fst3 $ fst $ snd a) == (fst3 $ fst $ snd b)) cpList
    mapM_
        ( \cpSubL -> do
            branches <-
                mapM
                    ( \(txid, ((bhash, bht, txindx), (_, merkleBranch, state))) -> do
                        return $ CallbackMerkleBranches (DT.unpack txid) (fromIntegral txindx) state merkleBranch
                    )
                    cpSubL
            mapicb <- xGetCallbackByUsername userid cbname
            cbTimestamp <- liftIO getCurrentTime
            let mpc = fromJust $ mapicb
            let token = BC.pack $ DT.unpack $ mcAuthKey mpc
                url = DT.unpack $ mcCallbackUrl mpc
                req' = buildRequest url token $ BC.pack "POST"
                cbApiVersion = "1.0"
                cbMinerID = NC.minerID $ nodeConfig bp2pEnv
                cbBlockHash = DT.unpack $ fst3 $ fst $ snd $ head $ cpSubL
                cbBlockHeight = fromIntegral $ snd3 $ fst $ snd $ head $ cpSubL
                cbCallBackType = MerkleProofComposite
                cbMerkleRoot = "" -- TODO
                cbMerkleBranches = branches
                mpr =
                    CallbackCompositeMerkleProof
                        cbApiVersion
                        cbTimestamp
                        cbMinerID
                        cbBlockHash
                        cbBlockHeight
                        cbCallBackType
                        cbMerkleRoot
                        cbMerkleBranches

                req = setRequestBodyJSON (mpr) req'
            response <- httpLBS req
            let status = getResponseStatusCode response
            if status == 200
                then return ()
                else return ()
        )
        callbacksGroups

processCallbacks :: (HasXokenNodeEnv env m, MonadIO m) => DT.Text -> DT.Text -> MerkleRootBlockInfoCache -> CompositeProofs -> m ()
processCallbacks userid txid mrbiCache compProofs = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    debug lg $ LG.msg $ "processCallbacks: inside: " ++ (show txid) ++ " userid: " ++ (show userid)
    let conn = xCqlClientState dbe
        net = NC.bitcoinNetwork $ nodeConfig bp2pEnv
        str =
            "SELECT userid, txid, block_hash, block_height, callback_name, flags_bitmask, state FROM xoken.callback_registrations WHERE user_id=? AND txid > ? ORDER BY txid ASC"
        qstr = str :: Q.QueryString Q.R (DT.Text, DT.Text) (DT.Text, DT.Text, DT.Text, Int32, DT.Text, Int32, DT.Text)
        uqstr = getSimpleQueryParam $ (userid, txid)
    eResp <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query qstr (uqstr{pageSize = (Just 100)}))
    case eResp of
        Right mb -> do
            let (_, cursor, _, _, _, _, _) = last mb
            mapM_
                ( \(userid, txid, blkhash, blkht, cbname, flags, state) -> do
                    mapicb <- xGetCallbackByUsername userid cbname
                    case mapicb of
                        Just mpc -> do
                            mb <- xGetMerkleBranch (DT.unpack txid)
                            ce <- liftIO $ TSH.lookup mrbiCache $ DT.pack $ nodeValue $ last mb
                            (bhash, bht, txindx) <- case ce of
                                Just et -> do
                                    let flags = L.map (isLeftNode) mb
                                    return (fst et, snd et, calcMerkleTreeTxIndex flags) -- TODO: calculate Tx index with left/right flags
                                Nothing -> do
                                    let toStr = "SELECT block_info FROM xoken.transactions WHERE tx_id=?"
                                        toQStr = toStr :: Q.QueryString Q.R (Identity DT.Text) (Identity (DT.Text, Int32, Int32))
                                        par = getSimpleQueryParam (Identity txid)
                                    res1 <- liftIO $ LE.try $ query conn (Q.RqQuery $ Q.Query toQStr par)
                                    case res1 of
                                        Right rx -> do
                                            let (bhash', bht', txindx') = runIdentity $ rx !! 0
                                            liftIO $ TSH.insert mrbiCache txid (bhash', bht')
                                            return (bhash', bht', txindx')
                                        Left (e :: SomeException) -> do
                                            err lg $ LG.msg $ "Error: getTxOutputsFromTxId: " ++ show e
                                            throw KeyValueDBLookupException

                            let cbMerkleBranch =
                                    if L.length mb > 0
                                        then Just mb
                                        else Nothing

                            if MerkleProofComposite `elem` (mcEvents mpc)
                                then do
                                    let cbState = fromJust $ A.decode $ C.pack $ DT.unpack state
                                    liftIO $ TSH.insert compProofs txid ((bhash, bht, txindx), (cbname, cbMerkleBranch, cbState))
                                else do
                                    cbTimestamp <- liftIO getCurrentTime
                                    let token = BC.pack $ DT.unpack $ mcAuthKey mpc
                                        url = DT.unpack $ mcCallbackUrl mpc
                                        req' = buildRequest url token $ BC.pack "POST"
                                        cbApiVersion = "1.0"
                                        cbMinerID = NC.minerID $ nodeConfig bp2pEnv
                                        cbBlockHash = DT.unpack bhash
                                        cbBlockHeight = fromIntegral bht
                                        cbTxid = DT.unpack txid
                                        cbCallBackType = MerkleProofSingle
                                        cbMerkleRoot = "" -- TODO
                                        cbTxIndex = fromIntegral txindx
                                        cbState = fromJust $ A.decode $ C.pack $ DT.unpack state
                                        mpr =
                                            CallbackSingleMerkleProof
                                                cbApiVersion
                                                cbTimestamp
                                                cbMinerID
                                                cbBlockHash
                                                cbBlockHeight
                                                cbTxid
                                                cbCallBackType
                                                cbMerkleRoot
                                                cbTxIndex
                                                cbState
                                                cbMerkleBranch

                                        req = setRequestBodyJSON (mpr) req'
                                    response <- httpLBS req
                                    let status = getResponseStatusCode response
                                    if status == 200
                                        then do
                                            return ()
                                        else do return ()
                )
                mb
            if L.length mb < 100
                then return ()
                else processCallbacks userid cursor mrbiCache compProofs
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getTxIDByProtocol: " ++ show e
            throw KeyValueDBLookupException

buildRequest :: String -> BC.ByteString -> BC.ByteString -> HS.Request
buildRequest url token method =
    setRequestMethod method $
        setRequestHeader "token" [token] $
            setRequestSecure True $
                setRequestPort 443 $
                    parseRequest_ (url)
