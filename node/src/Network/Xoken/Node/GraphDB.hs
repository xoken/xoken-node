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
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Monad.Reader
import Control.Monad.Trans (liftIO)
import Control.Monad.Trans.Reader (ReaderT(..))
import Data.Aeson (ToJSON(..), (.=), object)
import Data.Char
import Data.Hashable
import Data.List (nub)
import Data.Map.Strict as M
import Data.Maybe (fromJust)
import Data.Monoid ((<>))
import Data.Pool (Pool, createPool)
import Data.Text (Text)
import Data.Time.Clock
import Data.Word
import Database.Bolt as BT
import Database.Bolt (Node(..), Record, RecordValue(..), Value(..), at)
import qualified Database.CQL.IO as Q
import GHC.Generics
import Network.Socket hiding (send)
import Network.Xoken.Crypto.Hash
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
-- import Type
-- |A pool of connections to Neo4j server
data ServerState =
    ServerState
        { pool :: Pool Pipe
        }

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

-- |Returns movies with all it's actors
queryGraph :: Int -> BoltActionT IO Int
queryGraph limit = do
    records <- queryP cypher params
    nodeTuples <- traverse toNodes records
    let movies = fst <$> nodeTuples
    let actors = nub $ concatMap snd nodeTuples
    let actorIdx = fromJust . (`Prelude.lookup` zip actors [0 ..])
    let modifyTpl (m, as) = (m, actorIdx <$> as)
    let indexMap = fromList $ modifyTpl <$> nodeTuples
    let mkTuples (m, t) = (`MRel` t) <$> indexMap ! m
    let relations = concatMap mkTuples $ zip movies [length actors ..]
    return 12 -- $ MGraph (actors <> movies) relations
  where
    cypher =
        "MATCH (m:Movie)<-[:ACTED_IN]-(a:Person) " <> "RETURN m.title as movie, collect(a.name) as cast " <>
        "LIMIT {limit}"
    params = fromList [("limit", I limit)]

insertMerkleSubTree :: Bool -> [Hash256] -> BoltActionT IO ()
insertMerkleSubTree match hashes = do
    records <- queryP cypher params
    return ()
  where
    ind = Prelude.map (\x -> chr x) [1 .. (length hashes)]
    inputs = zip hashes ind
    cypher =
        "MATCH (m:Movie)<-[:ACTED_IN]-(a:Person) " <> "RETURN m.title as movie, collect(a.name) as cast " <>
        "LIMIT {limit}"
    params = fromList [("limit", I 100)]
