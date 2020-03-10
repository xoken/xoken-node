{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Network.Xoken.Node.Data.Allegory where

import Control.Exception
import Control.Monad (guard)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as B16
import Data.ByteString.Char8 as C
import Data.Foldable
import Data.Maybe
import Data.Text as T
import qualified Data.Text.Encoding as E
import Data.Yaml
import GHC.Generics

data Allegory =
    Allegory
        { version :: Int
        , namespaceId :: String
        , localName :: String
        , action :: Action
        }
    deriving (Show, Generic)

data Action
    = ProducerAction
          { prInput :: !ProducerInput
          , prOutput :: !ProducerOutput
          }
    | OwnerAction
          { ownInput :: !OwnerInput
          , ownOutput :: !OwnerOutput
          }
    deriving (Show, Generic)

data OwnerOutput =
    OwnerOutput
        { ownerO :: !Index
        , proxyProviders :: Maybe [ProxyProvider]
        }
    deriving (Show, Generic)

data OwnerInput =
    OwnerInput
        { owner :: !Index
        }
    deriving (Show, Generic)

data ProducerOutput =
    ProducerOutput
        { producerO :: !Index
        , extensions :: Maybe [Extension]
        , ownerOutput :: Maybe [OwnerOutput]
        }
    deriving (Show, Generic)

data ProducerInput =
    ProducerInput
        { producer :: !Index
        }
    deriving (Show, Generic)

data Index =
    Index
        { index :: !Int
        }
    deriving (Show, Generic)

data Extension =
    Extension
        { indexE :: !Int
        , codePoint :: !String
        }
    deriving (Show, Generic)

data ProxyProvider =
    ProxyProvider
        { service :: !String
        , mode :: !String
        , endpoint :: !Endpoint'
        , registration :: Registration
        }
    deriving (Show, Generic)

data Endpoint'
    = XokenP2P'
          { protocol :: !String
          , nodeid :: !String
          }
    | HTTPS'
          { protocol :: !String
          , uri :: !String
          }
    deriving (Show, Generic)

data Registration =
    Registration
        { addressCommitment :: String
        , providerUtxoCommitment :: String
        , signature :: String
        , expiry :: Int
        }
    deriving (Show, Generic)

instance FromJSON Endpoint' where
    parseJSON =
        withObject "XokenP2P' or HTTPS'" $ \o ->
            asum [XokenP2P' <$> o .: "protocol" <*> o .: "nodeid", HTTPS' <$> o .: "protocol" <*> o .: "uri"]

instance FromJSON Action where
    parseJSON =
        withObject "ProducerAction or OwnerAction" $ \o ->
            asum [ProducerAction <$> o .: "input" <*> o .: "output", OwnerAction <$> o .: "input" <*> o .: "output"]

instance FromJSON Allegory where
    parseJSON (Object v) =
        Allegory <$> v .: "version" <*> v .: "namespace-identifier" <*> v .: "local-name" <*> v .: "action"
    parseJSON _ = error "Can't parse XokenP2P' "

instance FromJSON ProducerInput where
    parseJSON (Object v) = ProducerInput <$> v .: "producer"
    parseJSON _ = error "Can't parse Inputs' "

instance FromJSON OwnerInput where
    parseJSON (Object v) = OwnerInput <$> v .: "producer"
    parseJSON _ = error "Can't parse Inputs' "

instance FromJSON ProducerOutput where
    parseJSON (Object v) = ProducerOutput <$> v .: "producer" <*> v .: "extensions" <*> v .: "owner"
    parseJSON _ = error "Can't parse XokenP2P' "

instance FromJSON OwnerOutput where
    parseJSON (Object v) = OwnerOutput <$> v .: "index" <*> v .: "proxy-providers"
    parseJSON _ = error "Can't parse XokenP2P' "

instance FromJSON Index

instance FromJSON Extension where
    parseJSON (Object v) = Extension <$> v .: "index" <*> v .: "code-point"
    parseJSON _ = error "Can't parse Inputs' "

instance FromJSON ProxyProvider

instance FromJSON Registration

instance ToJSON Extension

instance ToJSON ProxyProvider

instance ToJSON Endpoint'

instance ToJSON Registration
