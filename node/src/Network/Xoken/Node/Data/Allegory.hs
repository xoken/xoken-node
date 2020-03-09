{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Network.Xoken.Node.Data.Allegory
    ( module Network.Xoken.Node.Data.Allegory
    ) where

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
        , inputs :: Inputs'
        , outputs :: Outputs'
        }
    deriving (Show, Generic)

data Inputs' =
    Inputs'
        { iOwner :: Maybe Owner
        , iProducer :: Maybe Producer
        }
    deriving (Show, Generic)

data Outputs' =
    Outputs'
        { oOwner :: Maybe Owner
        , oProducer :: Maybe Producer
        , extensions :: Maybe [Extension]
        , proxyProviders :: Maybe [ProxyProvider]
        }
    deriving (Show, Generic)

data Producer =
    Producer
        { indexP :: !Int
        }
    deriving (Show, Generic)

data Owner =
    Owner
        { indexO :: !Int
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

instance FromJSON Allegory where
    parseJSON (Object v) =
        Allegory <$> v .: "version" <*> v .: "namespace-identifier" <*> v .: "local-name" <*> v .: "inputs" <*>
        v .: "outputs"
    parseJSON _ = error "Can't parse XokenP2P' "

instance FromJSON Inputs' where
    parseJSON (Object v) = Inputs' <$> v .:? "owner" <*> v .:? "producer"
    parseJSON _ = error "Can't parse Inputs' "

instance FromJSON Outputs' where
    parseJSON (Object v) =
        Outputs' <$> v .:? "owner" <*> v .:? "producer" <*> v .:? "proxy-providers" <*> v .:? "extensions"
    parseJSON _ = error "Can't parse Outputs' "

instance FromJSON Owner where
    parseJSON (Object v) = Owner <$> v .: "index"
    parseJSON _ = error "Can't parse Inputs' "

instance FromJSON Producer where
    parseJSON (Object v) = Producer <$> v .: "index"
    parseJSON _ = error "Can't parse Inputs' "

instance FromJSON Extension where
    parseJSON (Object v) = Extension <$> v .: "index" <*> v .: "code-point"
    parseJSON _ = error "Can't parse Inputs' "

instance FromJSON ProxyProvider

instance FromJSON Registration

instance ToJSON Allegory

instance ToJSON Inputs'

instance ToJSON Outputs'

instance ToJSON Owner

instance ToJSON Producer

instance ToJSON Extension

instance ToJSON ProxyProvider

instance ToJSON Endpoint'

instance ToJSON Registration
