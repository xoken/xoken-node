{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DuplicateRecordFields #-}

module Network.Xoken.Node.HTTP.Types where

import qualified Control.Exception as CE
import Control.Lens (makeLenses)
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class
import Control.Monad.State.Class
import Data.Aeson
import Data.Text
import GHC.Generics
import Network.Xoken.Node.Env
import Snap

data App =
    App
        { _env :: XokenNodeEnv
        }

instance HasBitcoinP2P (Handler App App) where
    getBitcoinP2P = bitcoinP2PEnv <$> gets _env

instance HasDatabaseHandles (Handler App App) where
    getDB = dbHandles <$> gets _env

instance HasAllegoryEnv (Handler App App) where
    getAllegory = allegoryEnv <$> gets _env

instance HasLogger (Handler App App) where
    getLogger = loggerEnv <$> gets _env

instance MC.MonadThrow (Handler App App) where
    throwM = liftIO . CE.throwIO

stripLensPrefixOptions :: Options
stripLensPrefixOptions = defaultOptions {fieldLabelModifier = Prelude.drop 1}

data QueryRequest =
    QueryRequest
        { _where :: Maybe Value
        , _return :: Maybe Value
        , _on :: NodeRelation
        }
    deriving (Generic)

instance ToJSON QueryRequest where
    toJSON = genericToJSON stripLensPrefixOptions

instance FromJSON QueryRequest where
    parseJSON = genericParseJSON stripLensPrefixOptions

data NodeRelation =
    NodeRelation
        { _from :: Node
        , _via :: Maybe Relation
        , _to :: Maybe Node
        }
    deriving (Generic)

instance ToJSON NodeRelation where
    toJSON = genericToJSON stripLensPrefixOptions

instance FromJSON NodeRelation where
    parseJSON = genericParseJSON stripLensPrefixOptions

data Node =
    Node
        { node :: Text
        , alias :: Text
        }
    deriving (Generic, FromJSON, ToJSON)

data Relation =
    Relation
        { relationship :: Text
        , direction :: Maybe Bool
        , alias :: Text
        }
    deriving (Generic, FromJSON, ToJSON)

aliasNode :: Node -> Text
aliasNode x = alias (x :: Node)

aliasRel :: Relation -> Text
aliasRel x = alias (x :: Relation)

makeLenses ''App
