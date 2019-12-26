{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Network.Xoken.Node.GraphDB where

import Arivi.P2P.P2PEnv as PE hiding (option)
import Codec.Serialise
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import Control.Monad.Reader
import Control.Monad.Trans (liftIO)
import Control.Monad.Trans.Reader (ReaderT(..))
import Data.Aeson (ToJSON(..), (.=), object)
import Data.Char
import Data.Hashable
import Data.List ((\\), filter, intersect, nub, nubBy, union, zip4)
import Data.Map.Strict as M (fromList)
import Data.Maybe (fromJust)
import Data.Monoid ((<>))
import Data.Pool (Pool, createPool)
import qualified Data.Set as SE
import Data.Text (Text, concat, filter, intercalate, null, pack, replace, take, takeWhileEnd, unpack)
import Data.Time.Clock
import Data.Word
import Database.Bolt as BT
import qualified Database.CQL.IO as Q
import GHC.Generics
import Network.Socket hiding (send)
import Network.Xoken.Crypto.Hash
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

data Movie =
    Movie
        { _id :: Int
        , _title :: Text
        , _released :: Int
        , _tagline :: Text
        }
    deriving (Show, Eq)

data MovieInfo =
    MovieInfo
        { _mTitle :: Text
        , _cast :: [Cast]
        }
    deriving (Show, Eq)

data Cast =
    Cast
        { _name :: Text
        , _job :: Text
        , _role :: Value
        }
    deriving (Show, Eq)

data MNode =
    MNode
        { _mnTitle :: Text
        , _label :: Text
        }
    deriving (Show, Eq, Ord)

data MRel =
    MRel
        { _source :: Int
        , _target :: Int
        }
    deriving (Show, Eq)

data MGraph =
    MGraph
        { _nodes :: [MNode]
        , _links :: [MRel]
        }
    deriving (Show, Eq)

instance ToJSON Value where
    toJSON (N _) = toJSON ()
    toJSON (B b) = toJSON b
    toJSON (I i) = toJSON i
    toJSON (F d) = toJSON d
    toJSON (T t) = toJSON t
    toJSON (L l) = toJSON l
    toJSON _ = undefined -- we do not need Maps and Structures in this example

instance ToJSON Movie where
    toJSON (Movie i t r tl) = object ["id" .= i, "title" .= t, "released" .= r, "tagline" .= tl]

instance ToJSON Cast where
    toJSON (Cast n j r) = object ["name" .= n, "job" .= j, "role" .= r]

instance ToJSON MovieInfo where
    toJSON (MovieInfo t c) = object ["title" .= t, "cast" .= c]

instance ToJSON MNode where
    toJSON (MNode t l) = object ["title" .= t, "label" .= l]

instance ToJSON MRel where
    toJSON (MRel s t) = object ["source" .= s, "target" .= t]

instance ToJSON MGraph where
    toJSON (MGraph n r) = object ["nodes" .= n, "links" .= r]

-- |Create pool of connections (4 stripes, 500 ms timeout, 1 resource per stripe)
constructState :: BoltCfg -> IO ServerState
constructState bcfg = do
    pool <- createPool (BT.connect bcfg) BT.close 4 500 1
    return (ServerState pool)

-- | Convert record to MerkleBranchNode
toMerkleBranchNode :: Monad m => Record -> m (MerkleBranchNode)
toMerkleBranchNode r = do
    txid :: Text <- (r `at` "txid") >>= exact
    isLeft :: Bool <- (r `at` "isleft") >>= exact
    return (MerkleBranchNode txid isLeft)

-- toMerkleBranch :: Monad m => Value -> m MerkleBranchNode
-- toMerkleBranch (L [T nodeVal, B isLeft]) = return $ MerkleBranchNode nodeVal isLeft
-- toMerkleBranch _ = fail "Not a Cast value"
-- -- |Converts some BOLT value to 'Movie'
-- toMovie2 :: Monad m => Value -> m Movie
-- toMovie2 v = do
--     node :: Node <- exact v
--     let props = nodeProps node
--     let identity = nodeIdentity node
--     title :: Text <- (props `at` "title") >>= exact
--     released :: Int <- (props `at` "released") >>= exact
--     tagline :: Text <- (props `at` "tagline") >>= exact
--     return $ Movie identity title released tagline
--
-- -- |Converts some BOLT value to 'Cast'
-- toCast :: Monad m => Value -> m Cast
-- toCast (L [T name, T job, role']) = return $ Cast name job role'
-- toCast _ = fail "Not a Cast value"
--
-- -- |Converts some BOLT value to 'Movie'
-- toMovie :: Monad m => Value -> m Movie
-- toMovie v = do
--     node :: Node <- exact v
--     let props = nodeProps node
--     let identity = nodeIdentity node
--     title :: Text <- (props `at` "title") >>= exact
--     released :: Int <- (props `at` "released") >>= exact
--     tagline :: Text <- (props `at` "tagline") >>= exact
--     return $ Movie identity title released tagline
--
-- -- |Create movie node and actors node from single record
-- toNodes :: Monad m => Record -> m (MNode, [MNode])
-- toNodes r = do
--     title :: Text <- (r `at` "movie") >>= exact
--     casts :: [Text] <- (r `at` "cast") >>= exact
--     return (MNode title "movie", (`MNode` "actor") <$> casts)
--
-- --
--
-- -- -- |Reader monad over IO to store connection pool
-- -- type WebM = ReaderT ServerState IO
-- -- |Search movie by title pattern
-- querySearch :: Text -> BoltActionT IO [undefined]
-- querySearch q = do
--     records <- queryP cypher params
--     nodes <- traverse (`at` "movie") records
--     traverse toMovie nodes
--     return [undefined] -- remove this
--   where
--     cypher = "MATCH (movie:Movie) WHERE movie.title =~ {title} RETURN movie"
--     params = fromList [("title", T $ "(?i).*" <> q <> ".*")]
--
-- -- |Returns movie by title
-- queryMovie :: Text -> BoltActionT IO undefined
-- queryMovie title = do
--     result <- head <$> queryP cypher params
--     T title <- result `at` "title"
--     L members <- result `at` "cast"
--     cast <- traverse toCast members
--     return undefined -- $ MovieInfo title cast
--   where
--     cypher =
--         "MATCH (movie:Movie {title:{title}}) " <> "OPTIONAL MATCH (movie)<-[r]-(person:Person) " <>
--         "RETURN movie.title as title," <>
--         "collect([person.name, " <>
--         "         head(split(lower(type(r)), '_')), r.roles]) as cast " <>
--         "LIMIT 1"
--     params = fromList [("title", T title)]
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

insertMerkleSubTree :: [MerkleNode] -> [MerkleNode] -> BoltActionT IO ()
insertMerkleSubTree leaves inodes = do
    records <- queryP cypher params
    return ()
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
        replace ("<m>") (var $ node $ leaves !! 0) (replace ("<s>") (var $ node $ leaves !! 1) (pack siblingRelnTempl))
    cyCreate =
        if length leaves == 2
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
        if length leaves == 2
            then parCreateLeaves <> parCreate <> parCreateSiblingReln
            else parCreate
    cypher =
        if length inodes == 0
            then " CREATE " <> cyCreate
            else " MATCH " <> cyMatch <> " CREATE " <> cyCreate
    txtTx i = txHashToHex $ TxHash $ fromJust i
    vars m = Prelude.map (\x -> Data.Text.filter (isAlpha) $ Data.Text.take 24 $ txtTx x) (m)
    var m = Data.Text.filter (isAlpha) $ Data.Text.take 24 $ txtTx m
    bool2Text cond =
        if cond
            then Data.Text.pack " TRUE "
            else Data.Text.pack " FALSE "
--
