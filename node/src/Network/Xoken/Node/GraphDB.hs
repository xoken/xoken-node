{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Xoken.Node.GraphDB where

import Arivi.P2P.P2PEnv as PE hiding (option)
import Codec.Serialise
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.Reader
import Control.Monad.Trans (liftIO)
import Control.Monad.Trans.Reader (ReaderT(..))
import Data.Aeson (ToJSON(..), (.=), decode, encode, object)
import qualified Data.ByteString.Base16 as B16 (decode, encode)
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Char
import Data.Hashable
import Data.List ((\\), filter, intersect, nub, nubBy, union, zip4)
import Data.Map.Strict as M (fromList)
import Data.Maybe (fromJust)
import Data.Monoid ((<>))
import Data.Pool (Pool, createPool)
import Data.Text (Text, append, concat, filter, intercalate, map, null, pack, replace, take, takeWhileEnd, unpack)
import Data.Time.Clock
import Data.Word
import Database.Bolt as BT
import qualified Database.CQL.IO as Q
import GHC.Generics
import Network.Socket hiding (send)
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.Data.Allegory as AL
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction
import System.Random
import Text.Read

data MerkleBranchNode =
    MerkleBranchNode
        { _nodeValue :: Text
        , _isLeftNode :: Bool
        }

instance ToJSON Value where
    toJSON (N _) = toJSON ()
    toJSON (B b) = toJSON b
    toJSON (I i) = toJSON i
    toJSON (F d) = toJSON d
    toJSON (T t) = toJSON t
    toJSON (L l) = toJSON l
    toJSON (M m) = toJSON m
    toJSON _ = undefined -- we do not need Maps and Structures in this example

-- |Create pool of connections (4 stripes, 500 ms timeout, 1 resource per stripe)
constructState :: BoltCfg -> IO ServerState
constructState bcfg = do
    pool <- createPool (BT.connect bcfg) BT.close 32 500 32
    return (ServerState pool)

-- | Convert record to MerkleBranchNode
toMerkleBranchNode :: Monad m => Record -> m (MerkleBranchNode)
toMerkleBranchNode r = do
    txid :: Text <- (r `at` "txid") >>= exact
    isLeft :: Bool <- (r `at` "isleft") >>= exact
    return (MerkleBranchNode txid isLeft)

-- | Convert record to Name & ScriptOp
toNameScriptOp :: Monad m => Record -> m ((Text, Text))
toNameScriptOp r = do
    outpoint :: Text <- (r `at` "outpoint") >>= exact
    script :: Text <- (r `at` "script") >>= exact
    return ((outpoint, script))

-- Fetch the Merkle branch/proof
queryMerkleBranch :: Text -> BoltActionT IO [MerkleBranchNode]
queryMerkleBranch leaf = do
    records <- queryP cypher params
    merkleBranch <- traverse toMerkleBranchNode records
    return merkleBranch
  where
    cypher =
        "MATCH (me:mnode{ v: {leaf} })-[:SIBLING]->(sib)  RETURN sib.v AS txid, sib.l AS isleft  UNION " <>
        "MATCH p=(start:mnode {v: {leaf}})-[:PARENT*]->(end:mnode) WHERE NOT (end)-[:PARENT]->() " <>
        "UNWIND tail(nodes(p)) AS elem  RETURN elem.v as txid, elem.l AS isleft"
    params = fromList [("leaf", T leaf)]

-- |Returns Neo4j DB version
queryGraphDBVersion :: BoltActionT IO [Text]
queryGraphDBVersion = do
    records <- queryP cypher params
    x <- traverse (`at` "version") records
    return $ x >>= exact
  where
    cypher =
        "call dbms.components() yield name, versions, edition unwind versions as version return name, version, edition"
    params = fromList []

-- Fetch the Allegory Name branch
queryAllegoryNameBranch :: Text -> Bool -> BoltActionT IO [Text]
queryAllegoryNameBranch name isProducer = do
    records <- queryP cypher params
    x <- traverse (`at` "outpoint") records
    return $ x >>= exact
  where
    cypher =
        " MATCH p=(pointer:namestate {name: {namestr}})-[:REVISION]-()-[:INPUT*]->(start:nutxo) " <>
        " WHERE NOT (start)-[:INPUT]->() " <>
        " UNWIND tail(nodes(p)) AS elem " <>
        " RETURN elem.outpoint as outpoint "
    params =
        if isProducer
            then fromList [("namestr", T (name <> pack "|producer"))]
            else fromList [("namestr", T (name <> pack "|owner"))]

-- Fetch the Allegory Name branch with scriptOutput
queryAllegoryNameBranchScriptOp :: Text -> Bool -> BoltActionT IO [(Text, Text)]
queryAllegoryNameBranchScriptOp name isProducer = do
    records <- queryP cypher params
    x <- traverse toNameScriptOp records
    return x
  where
    cypher =
        " MATCH p=(pointer:namestate {name: {namestr}})-[:REVISION]-()-[:INPUT*]->(start:nutxo) " <>
        " WHERE NOT (start)-[:INPUT]->() " <>
        " UNWIND tail(nodes(p)) AS elem " <>
        " RETURN elem.outpoint as outpoint "
    params =
        if isProducer
            then fromList [("namestr", T (name <> pack "|producer"))]
            else fromList [("namestr", T (name <> pack "|owner"))]

updateAllegoryStateTrees :: Tx -> Allegory -> BoltActionT IO ()
updateAllegoryStateTrees tx allegory = do
    case action allegory of
        OwnerAction oin oout proxy -> do
            let iop = prevOutput $ (txIn tx !! (index $ oin))
            let iops = append (txHashToHex $ outPointHash $ iop) $ pack (":" ++ show (outPointIndex $ iop))
            let oops = append (txHashToHex $ txHash tx) $ pack (":" ++ show (index $ owner oout))
            let scr = scriptOutput ((txOut tx) !! (index $ owner oout))
            let cypher =
                    " MATCH (x:namestate)-[r:REVISION]->(a:nutxo) WHERE a.outpoint = {in_op} AND x.name = {nn_str} " <>
                    (if length proxy == 0
                         then case oVendorEndpoint oout of
                                  Just e ->
                                      " CREATE (b:nutxo { outpoint: {out_op}, name:{name}, script: {scr} , vendor:{endpoint} }) "
                                  Nothing -> " CREATE (b:nutxo { outpoint: {out_op}, name:{name}, script: {scr} }) "
                         else case oVendorEndpoint oout of
                                  Just e ->
                                      " CREATE (b:nutxo { outpoint: {out_op}, name:{name}, script: {scr} , proxy:{proxies}, vendor:{endpoint} }) "
                                  Nothing ->
                                      " CREATE (b:nutxo { outpoint: {out_op}, name:{name}, script: {scr} , proxy:{proxies} }) ") <>
                    " , (b)-[:INPUT]->(a) , (x)-[:REVISION]->(b)  DELETE r "
            let params =
                    fromList
                        [ ("in_op", T $ iops)
                        , ("out_op", T $ oops)
                        , ("scr", T $ pack $ C.unpack $ B16.encode scr)
                        , ("name", T $ pack $ Prelude.map (\x -> chr x) (name allegory))
                        , ("proxies", T $ pack $ BL.unpack $ Data.Aeson.encode proxy)
                        , ("endpoint", T $ pack $ BL.unpack $ Data.Aeson.encode $ oVendorEndpoint oout)
                        , ("nn_str", T $ pack $ Prelude.map (\x -> chr x) (name allegory) ++ "|owner")
                        ]
            res <- LE.try $ queryP cypher params
            case res of
                Left (e :: SomeException) -> do
                    liftIO $ print ("[ERROR] updateAllegoryStateTrees (Prod) " ++ show e)
                    throw e
                Right (records) -> return ()
        ProducerAction pin pout oout extn -> do
            let iop = prevOutput $ (txIn tx !! (index $ pin))
            let iops = append (txHashToHex $ outPointHash $ iop) $ pack (":" ++ show (outPointIndex $ iop))
            -- Producer (required)
            let pop = index $ producer pout
            let val = append (txHashToHex $ txHash tx) $ pack (":" ++ show (pop))
            let pproScr = scriptOutput ((txOut tx) !! pop)
            let tstr =
                    case pVendorEndpoint pout of
                        Just e ->
                            "(ppro:nutxo { outpoint: {op_ppro}, script: {ppro_scr} , name:{name}, producer: True , vendor:{endpoint_prod}}) , (ppro)-[:INPUT]->(a) "
                        Nothing ->
                            "(ppro:nutxo { outpoint: {op_ppro}, script: {ppro_scr} , name:{name}, producer: True }) , (ppro)-[:INPUT]->(a) "
            let cyProStr = tstr
            let poutPro =
                    ( cyProStr
                    , [ ("op_ppro", T $ val)
                      , ("ppro_scr", T $ pack $ C.unpack $ B16.encode pproScr)
                      , ("endpoint_prod", T $ pack $ BL.unpack $ Data.Aeson.encode $ pVendorEndpoint pout)
                      , ("nn_pr_str", T $ pack $ Prelude.map (\x -> chr x) (name allegory) ++ "|producer")
                      ])
            -- Owner (optional)
            let poutOwn =
                    case oout of
                        Nothing -> Nothing
                        Just poo -> do
                            let pop = index $ owner $ poo
                            let val = append (txHashToHex $ txHash tx) $ pack (":" ++ show pop)
                            let pownScr = scriptOutput ((txOut tx) !! pop)
                            let mstr =
                                    " OPTIONAL MATCH (nsown:namestate)-[:REVISION]->() WHERE nsown.name = {nn_ow_str}  WITH rootname + collect(nsown) AS owner_exists "
                            let cstr = " , (nsown:namestate { name:{nn_ow_str} }) , (nsown)-[:REVISION]->(pown)  "
                            let tstr =
                                    case oVendorEndpoint poo of
                                        Just e ->
                                            " (pown:nutxo { outpoint: {op_pown}, script: {pown_scr} , name:{name}, producer: False , vendor:{endpoint_owner} }) ,(pown)-[:INPUT]->(a) "
                                        Nothing ->
                                            " (pown:nutxo { outpoint: {op_pown}, script: {pown_scr} , name:{name}, producer: False }) ,(pown)-[:INPUT]->(a) "
                            let cyOwnStr = (mstr, tstr)
                            let parOwn =
                                    [ ("op_pown", T $ val)
                                    , ("pown_scr", T $ pack $ C.unpack $ B16.encode pownScr)
                                    , ("endpoint_owner", T $ pack $ BL.unpack $ Data.Aeson.encode $ oVendorEndpoint poo)
                                    , ("nn_ow_str", T $ pack $ Prelude.map (\x -> chr x) (name allegory) ++ "|owner")
                                    ]
                            Just (cyOwnStr, parOwn)
            let extensions =
                    Prelude.map
                        (\(x, ind) -> do
                             let mstr =
                                     " OPTIONAL MATCH (ns_<j>:namestate)-[:REVISION]->() WHERE ns_<j>.name = {nn_<j>_str}  WITH <previous> + collect(ns_<j>) AS owner_extn_exists_<i> "
                             let cstr = " , (ns_<j>:namestate { name:{nn_<j>_str} }) , (ns_<j>)-[:REVISION]->(<j>)  "
                             let (ss, eop) =
                                     case x of
                                         OwnerExtension ow cp ->
                                             ( case oVendorEndpoint ow of
                                                   Just ep ->
                                                       ( mstr
                                                       , "(<j>:nutxo { outpoint: {op_<j>}, script: {pext_<j>_scr} , name:{name_ext_<j>}, producer: False , vendor:{endpoint_<j>}}) ,(<j>)-[:INPUT]->(a) " ++
                                                         cstr)
                                                   Nothing ->
                                                       ( mstr
                                                       , "(<j>:nutxo { outpoint: {op_<j>}, script: {pext_<j>_scr} , name:{name_ext_<j>}, producer: False }) ,(<j>)-[:INPUT]->(a) " ++
                                                         cstr)
                                             , index $ owner $ ow)
                                         ProducerExtension pr cp ->
                                             ( ( mstr
                                               , "(<j>:nutxo { outpoint: {op_<j>}, script: {pext_<j>_scr} , name:{name_ext_<j>}, producer: True }) ,(<j>)-[:INPUT]->(a) " ++
                                                 cstr)
                                             , index $ producer $ pr)
                             let val = append (txHashToHex $ txHash tx) $ pack (":" ++ show (eop))
                             let pextScr = scriptOutput ((txOut tx) !! eop)
                             let cyExtStr =
                                     ( replace
                                           ("<previous>")
                                           (pack $
                                            if (ind == 1)
                                                then "owner_exists"
                                                else "owner_extn_exists_" ++ (show $ ind - 1))
                                           (replace
                                                ("<i>")
                                                (pack $ (show ind))
                                                (replace ("<j>") (pack $ numrepl (show eop)) $ pack $ fst ss))
                                     , replace ("<j>") (pack $ numrepl (show eop)) $ pack $ snd ss)
                             let opr = "op_" ++ (numrepl (show eop))
                             let next = "name_ext_" ++ (numrepl (show eop))
                             let eep = "endpoint_" ++ (numrepl (show eop))
                             let mep = "nn_" ++ (numrepl (show eop)) ++ "_str"
                             let pextv = "pext_" ++ (numrepl (show eop)) ++ "_scr"
                             let parExt =
                                     [ (pack opr, T $ val)
                                     , (pack pextv, T $ pack $ C.unpack $ B16.encode pextScr)
                                     , ( pack next
                                       , T $ pack $ Prelude.map (\x -> chr x) (name allegory ++ [codePoint x]))
                                     , ( pack mep
                                       , T $
                                         pack $
                                         Prelude.map (\x -> chr x) (name allegory) ++
                                         case x of
                                             OwnerExtension _ cp -> [chr cp] ++ "|owner"
                                             ProducerExtension _ cp -> [chr cp] ++ "|producer")
                                     ] ++
                                     case x of
                                         OwnerExtension ow cp ->
                                             case oVendorEndpoint ow of
                                                 Just ep -> [(pack eep, T $ pack $ BL.unpack $ Data.Aeson.encode $ ep)]
                                                 Nothing -> []
                                         ProducerExtension pr cp ->
                                             case pVendorEndpoint pr of
                                                 Just ep -> [(pack eep, T $ pack $ BL.unpack $ Data.Aeson.encode $ ep)]
                                                 Nothing -> []
                             (cyExtStr, parExt))
                        (zip extn [1 ..])
            let cypher =
                    " OPTIONAL MATCH (rootn:namestate) WHERE rootn.name={rtname} WITH collect(rootn) AS rootname " <>
                    (case poutOwn of
                         Just po -> (fst $ fst po)
                         Nothing -> " ") <>
                    (Data.Text.intercalate (" ") $ fst $ unzip $ fst $ unzip $ extensions) <>
                    replace ("<j>") (pack $ show $ length extn) " UNWIND owner_extn_exists_<j> AS owner_or_extn_exists " <>
                    " WITH owner_or_extn_exists " <>
                    " UNWIND  CASE WHEN (owner_or_extn_exists.name={rtname}) THEN [1] ELSE [] END AS relation " <>
                    " MATCH (x:namestate)-[r:REVISION]->(a:nutxo) WHERE a.outpoint = {in_op} AND x.name = {nn_pr_str} " <>
                    " CREATE " <>
                    fst poutPro <>
                    (case poutOwn of
                         Just po -> " , " <> (snd $ fst po) <> " , "
                         Nothing -> " , ") <>
                    (Data.Text.intercalate (" , ") $ snd $ unzip $ fst $ unzip $ extensions) <>
                    " , (x)-[:REVISION]->(ppro) DELETE r  "
            let params =
                    fromList $
                    [ ("rtname", T $ "")
                    , ("in_op", T $ iops)
                    , ("name", T $ pack $ Prelude.map (\x -> chr x) (name allegory))
                    ] ++
                    (case poutOwn of
                         Just po -> (snd poutPro) ++ (snd po)
                         Nothing -> (snd poutPro)) ++
                    (Prelude.concat $ snd $ unzip extensions)
            liftIO $ print (cypher)
            liftIO $ print (params)
            res <- LE.try $ queryP cypher params
            case res of
                Left (e :: SomeException) -> do
                    liftIO $ print ("[ERROR] updateAllegoryStateTrees (Prod) " ++ show e)
                    throw e
                Right (records) -> return ()
  where
    numrepl txt =
        Prelude.map
            (\x ->
                 case x of
                     '0' -> 'k'
                     '1' -> 'l'
                     '2' -> 'm'
                     '3' -> 'n'
                     '4' -> 'o'
                     '5' -> 'p'
                     '6' -> 'q'
                     '7' -> 'r'
                     '8' -> 's'
                     '9' -> 't'
                     otherwise -> x)
            txt
    bool2Text cond =
        if cond
            then Data.Text.pack " TRUE "
            else Data.Text.pack " FALSE "

insertMerkleSubTree :: [MerkleNode] -> [MerkleNode] -> BoltActionT IO ()
insertMerkleSubTree leaves inodes = do
    res <- LE.try $ queryP cypher params
    case res of
        Left (e :: SomeException)
            -- liftIO $ print ("[ERROR] insertMerkleSubTree " ++ show e)
            -- liftIO $ print (show leaves)
            -- liftIO $ print (show inodes)
            -- liftIO $ print (show cypher)
            -- liftIO $ print (show params)
         -> do
            throw e
        Right (records) -> return ()
  where
    lefts = Prelude.map (leftChild) inodes
    rights = Prelude.map (rightChild) inodes
    nodes = Prelude.map (node) inodes
    matchReq =
        ((lefts `union` rights) \\ ((lefts `intersect` nodes) `union` (rights `intersect` nodes))) \\
        (Prelude.map (node) leaves)
    matchTemplate = "  (<i>:mnode { v: {<i>}}) "
    createTemplate = " (<i>:mnode { v: {<i>} , l: <f> }) "
    parentRelnTempl = " (<c>)-[:PARENT]->(<p>) "
    siblingRelnTempl = " (<m>)-[:SIBLING]->(<s>) , (<s>)-[:SIBLING]->(<m>)"
    cyCreateLeaves =
        Data.Text.intercalate (" , ") $
        Prelude.map (\(repl, il) -> replace ("<i>") (repl) (replace ("<f>") (il) (pack createTemplate))) $
        zip (vars $ Prelude.map (node) leaves) (Prelude.map (bool2Text . isLeft) leaves)
    cyMatch =
        Data.Text.intercalate (" , ") $
        Prelude.map (\repl -> replace ("<i>") (repl) (pack matchTemplate)) (vars matchReq)
    cyCreateT =
        Data.Text.intercalate (" , ") $
        Prelude.map (\(repl, il) -> replace ("<i>") (repl) (replace ("<f>") (il) (pack createTemplate))) $
        zip (vars nodes) (Prelude.map (bool2Text . isLeft) inodes)
    cyRelationLeft =
        Data.Text.intercalate (" , ") $
        Prelude.map
            (\(rc, rp) -> replace ("<p>") (rp) (replace ("<c>") (rc) (pack parentRelnTempl)))
            (zip (vars lefts) (vars nodes))
    cyRelationRight =
        Data.Text.intercalate (" , ") $
        Prelude.map
            (\(rc, rp) -> replace ("<p>") (rp) (replace ("<c>") (rc) (pack parentRelnTempl)))
            (zip (vars rights) (vars nodes))
    cySiblingReln =
        if length leaves == 2
            then replace
                     ("<m>")
                     (var $ node $ leaves !! 0)
                     (replace ("<s>") (var $ node $ leaves !! 1) (pack siblingRelnTempl))
            else replace -- must be a single lone leaf
                     ("<m>")
                     (var $ node $ leaves !! 0)
                     (replace ("<s>") (var $ node $ leaves !! 0) (pack siblingRelnTempl))
    cyCreate =
        if length leaves > 0
            then if length inodes > 0
                     then Data.Text.intercalate (" , ") $
                          Data.List.filter
                              (not . Data.Text.null)
                              [cyCreateLeaves, cyCreateT, cyRelationLeft, cyRelationRight, cySiblingReln]
                     else Data.Text.intercalate (" , ") $
                          Data.List.filter (not . Data.Text.null) [cyCreateLeaves, cySiblingReln]
            else cyCreateT
    parCreateLeaves = Prelude.map (\(i, x) -> (i, T $ txtTx $ node x)) (zip (vars $ Prelude.map (node) leaves) leaves)
    parCreateSiblingReln =
        Prelude.map
            (\(i, x) -> (i, T $ txtTx $ node x))
            (zip (vars $ Prelude.map (node) leaves) (leaves ++ reverse leaves))
    parCreateArr =
        Prelude.map
            (\(i, j, k, x) -> [(i, T $ txtTx $ node x), (j, T $ txtTx $ leftChild x), (k, T $ txtTx $ rightChild x)])
            (zip4 (vars nodes) (vars lefts) (vars rights) inodes)
    parCreate = Prelude.concat parCreateArr
    params =
        fromList $
        if length leaves > 0
            then parCreateLeaves <> parCreate <> parCreateSiblingReln
            else parCreate
    cypher =
        if length matchReq == 0
            then " CREATE " <> cyCreate
            else " MATCH " <> cyMatch <> " CREATE " <> cyCreate
    txtTx i = txHashToHex $ TxHash $ fromJust i
    vars m = Prelude.map (\x -> Data.Text.filter (isAlpha) $ numrepl $ Data.Text.take 8 $ txtTx x) (m)
    var m = Data.Text.filter (isAlpha) $ numrepl $ Data.Text.take 8 $ txtTx m
    numrepl txt =
        Data.Text.map
            (\x ->
                 case x of
                     '0' -> 'k'
                     '1' -> 'l'
                     '2' -> 'm'
                     '3' -> 'n'
                     '4' -> 'o'
                     '5' -> 'p'
                     '6' -> 'q'
                     '7' -> 'r'
                     '8' -> 's'
                     '9' -> 't'
                     otherwise -> x)
            txt
    bool2Text cond =
        if cond
            then Data.Text.pack " TRUE "
            else Data.Text.pack " FALSE "
--
