{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Network.Xoken.Node.Data.Allegory where

import Codec.Serialise
import Data.Aeson
import Data.Yaml ()
import GHC.Generics

data Allegory =
    Allegory
        { version :: !Int
        , name :: ![Int]
        , action :: !Action
        }
    deriving (Show, Generic, Eq, Serialise)

data Action
    = ProducerAction
          { producerInput :: !Index
          , producerOutput :: !ProducerOutput
          , pOwnerOutput :: !(Maybe OwnerOutput)
          , extensions :: ![Extension]
          }
    | OwnerAction
          { ownerInput :: !Index
          , ownerOutput :: !OwnerOutput
          , registrations :: ![Registration]
          }
    deriving (Show, Generic, Eq, Serialise)

data ProducerOutput =
    ProducerOutput
        { producer :: !Index
        , pVendorEndpoint :: !(Maybe Endpoint)
        }
    deriving (Show, Generic, Eq, Serialise)

data OwnerOutput =
    OwnerOutput
        { owner :: !Index
        , oVendorEndpoint :: !(Maybe Endpoint)
        }
    deriving (Show, Generic, Eq, Serialise)

data Index =
    Index
        { index :: !Int
        }
    deriving (Show, Generic, Eq, Serialise)

data Extension
    = OwnerExtension
          { ownerOutputEx :: !OwnerOutput
          , codePoint :: !Int
          }
    | ProducerExtension
          { producerOutputEx :: !ProducerOutput
          , codePoint :: !Int
          }
    deriving (Show, Generic, Eq, Serialise)

data Registration =
    AllPayRegistration
        { service :: !String
        , mode :: !String
        , identityPublicKey :: !String
        , publicKeyAuthEncrypt :: !String
        , publicKeySigning :: !String
        }
    deriving (Show, Generic, Eq, Serialise)

data Endpoint =
    Endpoint
        { protocol :: !String
        , uri :: !String
        }
    deriving (Show, Generic, Eq, Serialise)

instance ToJSON Registration

instance ToJSON Endpoint

instance FromJSON Endpoint
--
-- instance ToJSON OwnerInput
--
-- instance FromJSON Endpoint' where
--     parseJSON =
--         withObject "XokenP2P' or HTTPS'" $ \o ->
--             asum [XokenP2P' <$> o .: "protocol" <*> o .: "nodeid", HTTPS' <$> o .: "protocol" <*> o .: "uri"]
--
-- instance FromJSON ProducerTransfer where
--     parseJSON = withObject "OwnerT or ProducerT" $ \o -> asum [OwnerT <$> o .: "owner", ProducerT <$> o .: "producer"]
--
-- instance FromJSON AllegoryAction where
--     parseJSON =
--         withObject "ProducerAction or OwnerAction" $ \v ->
--             asum
--                 [ ProducerAction <$> v .: "version" <*> v .: "namespace-identifier" <*> v .: "local-name" <*>
--                   v .: "source" <*>
--                   v .: "extensions" <*>
--                   v .: "transfers"
--                 , OwnerAction <$> v .: "version" <*> v .: "namespace-identifier" <*> v .: "local-name" <*> v .: "source" <*>
--                   v .: "proxy-providers" <*>
--                   v .: "transfers"
--                 ]
--
-- instance FromJSON OwnerTransfer where
--     parseJSON (Object v) = OwnerTransfer <$> v .: "owner"
--     parseJSON _ = error "Can't parse Inputs' "
--
-- instance FromJSON Owner where
--     parseJSON (Object v) = Owner <$> v .: "owner"
--     parseJSON _ = error "Can't parse Inputs' "
--
-- instance FromJSON Producer where
--     parseJSON (Object v) = Producer <$> v .: "producer"
--     parseJSON _ = error "Can't parse Inputs' "
--
-- instance FromJSON Index
--
-- instance FromJSON Extension where
--     parseJSON =
--         withObject "OwnerExtension or ProducerExtension" $ \o ->
--             asum
--                 [ OwnerExtension <$> o .: "owner" <*> o .: "code-point"
--                 , ProducerExtension <$> o .: "producer" <*> o .: "code-point"
--                 ]
--
-- instance FromJSON ProxyProvider
--
-- instance FromJSON Registration
--
-- instance ToJSON ProxyProvider
--
-- instance ToJSON Endpoint'
--
-- instance ToJSON Registration
