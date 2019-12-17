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

-- import Database.Bolt (Node(..), Record, RecordValue(..), Value(..), at)
import qualified Database.CQL.IO as Q
import GHC.Generics
import Network.Socket hiding (send)
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction
import System.Random
import Text.Read

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

-- |Converts some BOLT value to 'Cast'
toCast :: Monad m => Value -> m Cast
toCast (L [T name, T job, role']) = return $ Cast name job role'
toCast _ = fail "Not a Cast value"

-- |Converts some BOLT value to 'Movie'
toMovie :: Monad m => Value -> m Movie
toMovie v = do
    node :: Node <- exact v
    let props = nodeProps node
    let identity = nodeIdentity node
    title :: Text <- (props `at` "title") >>= exact
    released :: Int <- (props `at` "released") >>= exact
    tagline :: Text <- (props `at` "tagline") >>= exact
    return $ Movie identity title released tagline

-- |Create movie node and actors node from single record
toNodes :: Monad m => Record -> m (MNode, [MNode])
toNodes r = do
    title :: Text <- (r `at` "movie") >>= exact
    casts :: [Text] <- (r `at` "cast") >>= exact
    return (MNode title "movie", (`MNode` "actor") <$> casts)

--
-- |Create pool of connections (4 stripes, 500 ms timeout, 1 resource per stripe)
constructState :: BoltCfg -> IO ServerState
constructState bcfg = do
    pool <- createPool (BT.connect bcfg) BT.close 4 500 1
    return (ServerState pool)

-- -- |Reader monad over IO to store connection pool
-- type WebM = ReaderT ServerState IO
-- |Search movie by title pattern
querySearch :: Text -> BoltActionT IO [undefined]
querySearch q = do
    records <- queryP cypher params
    nodes <- traverse (`at` "movie") records
    traverse toMovie nodes
    return [undefined] -- remove this
  where
    cypher = "MATCH (movie:Movie) WHERE movie.title =~ {title} RETURN movie"
    params = fromList [("title", T $ "(?i).*" <> q <> ".*")]

-- |Returns movie by title
queryMovie :: Text -> BoltActionT IO undefined
queryMovie title = do
    result <- head <$> queryP cypher params
    T title <- result `at` "title"
    L members <- result `at` "cast"
    cast <- traverse toCast members
    return undefined -- $ MovieInfo title cast
  where
    cypher =
        "MATCH (movie:Movie {title:{title}}) " <> "OPTIONAL MATCH (movie)<-[r]-(person:Person) " <>
        "RETURN movie.title as title," <>
        "collect([person.name, " <>
        "         head(split(lower(type(r)), '_')), r.roles]) as cast " <>
        "LIMIT 1"
    params = fromList [("title", T title)]

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

-- MATCH  (Ax:mnode), (Ay:mnode)  ,  (Bx:mnode), (By:mnode)  WHERE  Ax.v = {leftChildA} AND Ay.v = {rightChildA}  AND  Bx.v = {leftChildB} AND By.v = {rightChildB}  MERGE  (aa:mnode { v: {node_aa}}), (bb:mnode { v: {node_bb}})  ,  (Az:mnode { v: {nodeA}}) , (Ax)-[:PARENT]->(Az), (Ay)-[:PARENT]->(Az)  ,  (Bz:mnode { v: {nodeB}}) , (Bx)-[:PARENT]->(Bz), (By)-[:PARENT]->(Bz)
insertMerkleSubTree :: [MerkleNode] -> [MerkleNode] -> BoltActionT IO ()
insertMerkleSubTree create match = do
    liftIO $ print (cypher)
    records <- queryP cypher params
    return ()
  where
    lefts = Prelude.map (leftChild) match
    rights = Prelude.map (rightChild) match
    nodes = Prelude.map (node) match
    matchReq = (lefts `union` rights) \\ ((lefts `intersect` nodes) `union` (rights `intersect` nodes))
    cyCreateLeaves = " (aa:mnode { v: {node_aa}}), (bb:mnode { v: {node_bb}}) "
    parCreateLeaves =
        [ (pack $ "node_aa", T $ txHashToHex $ TxHash $ fromJust $ node $ create !! 0)
        , (pack $ "node_bb", T $ txHashToHex $ TxHash $ fromJust $ node $ create !! 1)
        ]
    matchTemplate = "  (<i>:mnode) "
    whereTemplate = " <i>.v = {<i>} "
    createTemplate = " (<i>:mnode { v: {<i>}}) "
    relationTemplate = " (<c>)-[:PARENT]->(<p>) "
    cyMatch =
        Data.Text.intercalate (" , ") $
        Prelude.map (\repl -> replace ("<i>") (repl) (pack matchTemplate)) (vars matchReq)
    cyWhere =
        Data.Text.intercalate (" AND ") $
        Prelude.map (\repl -> replace ("<i>") (repl) (pack whereTemplate)) (vars matchReq)
    cyCreateT =
        Data.Text.intercalate (" , ") $ Prelude.map (\repl -> replace ("<i>") (repl) (pack createTemplate)) (vars nodes)
    cyRelationLeft =
        Data.Text.intercalate (" , ") $
        Prelude.map
            (\(rc, rp) -> replace ("<p>") (rp) (replace ("<c>") (rc) (pack relationTemplate)))
            (zip (vars lefts) (vars nodes))
    cyRelationRight =
        Data.Text.intercalate (" , ") $
        Prelude.map
            (\(rc, rp) -> replace ("<p>") (rp) (replace ("<c>") (rc) (pack relationTemplate)))
            (zip (vars rights) (vars nodes))
    cyCreate =
        if length create == 2
            then if length match > 0
                     then Data.Text.intercalate (" , ") $
                          Data.List.filter
                              (not . Data.Text.null)
                              [cyCreateLeaves, cyCreateT, cyRelationLeft, cyRelationRight]
                     else cyCreateLeaves
            else cyCreateT
    parCreateArr =
        Prelude.map
            (\(i, j, k, x) ->
                 [ (i, T $ txHashToHex $ TxHash $ fromJust $ node $ x)
                 , (j, T $ txHashToHex $ TxHash $ fromJust $ leftChild $ x)
                 , (k, T $ txHashToHex $ TxHash $ fromJust $ rightChild $ x)
                 ])
            (zip4 (vars nodes) (vars lefts) (vars rights) match)
    parCreate = Prelude.concat parCreateArr
    params =
        fromList $
        if length create == 2
            then parCreateLeaves <> parCreate
            else parCreate
    cypher =
        if length match == 0
            then " CREATE " <> cyCreate
            else " MATCH " <> cyMatch <> " WHERE " <> cyWhere <> " CREATE " <> cyCreate
    txtTx i = txHashToHex $ TxHash $ fromJust i
    vars m = Prelude.map (\x -> Data.Text.filter (isAlpha) $ Data.Text.take 20 $ txtTx x) (m)
--
