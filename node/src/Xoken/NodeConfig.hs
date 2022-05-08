{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Xoken.NodeConfig
    ( module Xoken.NodeConfig
    ) where

import Arivi.P2P.Kademlia.Types
import Control.Exception
import Control.Monad (guard)
import Crypto.Secp256k1
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as B16
import Data.ByteString.Char8 as C
import Data.Int
import Data.Maybe
import Data.Text as T
import qualified Data.Text.Encoding as E
import Data.Word
import Data.Yaml
import GHC.Generics
import Network.Socket
import Network.Xoken.Constants
import Network.Xoken.Constants
import System.Logger

data NodeConfig =
    NodeConfig
        { bitcoinNetwork :: Network
        , logLevel :: Level
        , logFileName :: T.Text
        , bitcoinNodeListenIP :: String
        , bitcoinNodeListenPort :: PortNumber
        , endPointTLSListenIP :: String
        , endPointTLSListenPort :: PortNumber
        , endPointHTTPSListenIP :: String
        , endPointHTTPSListenPort :: PortNumber
        , allegoryVendorSecretKey :: SecKey
        , maxBitcoinPeerCount :: Int
        , unresponsivePeerConnTimeoutSecs :: Int
        , acivatePeerDiscovery :: Bool
        , maxTMTBuilderThreads :: Int
        , txProcInputDependenciesWait :: Int
        , tlsCertificatePath :: FilePath
        , tlsKeyfilePath :: FilePath
        , tlsCertificateStorePath :: FilePath
        , neo4jUsername :: T.Text
        , neo4jPassword :: T.Text
        , allegoryNameUtxoSatoshis :: Int
        , allegoryTxFeeSatsProducerAction :: Int
        , allegoryTxFeeSatsOwnerAction :: Int
        , txOutputValuesCacheKeyBits :: Int
        , unconfirmedTxCacheKeyBits :: Int
        , blockProcessingTimeout :: Int
        , recentTxReceiveTimeout :: Int
        , getDataResponseTimeout :: Int
        , maxStreamsXCql :: Int16
        , stripesXCql :: Int
        , maxConnectionsXCql :: Int
        , maxTxProcessingThreads :: Int
        , maxTxProcessingBuffer :: Int
        , maxInsertsProtocol :: Int
        , blocksFetchWindow :: [BlockFetchWindow]
        , parallelBlockProcessing :: Int
        , staticPeerList :: [String]
        , mAPICallbacksEnable :: Bool
        , minerID :: String
        }
    deriving (Show, Generic)

data BlockFetchWindow =
    BlockFetchWindow
        { heightBegin :: Int
        , heightEnd :: Int
        , windowSize :: Int8
        }
    deriving (Show, Generic)

instance FromJSON BlockFetchWindow

instance FromJSON ByteString where
    parseJSON = withText "ByteString" $ \t -> pure $ fromJust (decodeHex t)

instance FromJSON PortNumber where
    parseJSON v = fromInteger <$> parseJSON v

instance FromJSON SecKey where
    parseJSON v = fromJust <$> secKey <$> (parseJSON v :: Parser ByteString)

instance FromJSON Network where
    parseJSON v = fromJust <$> netByName <$> parseJSON v

instance FromJSON Level where
    parseJSON v = read <$> parseJSON v

instance FromJSON NodeConfig

readConfig :: FilePath -> IO NodeConfig
readConfig path = do
    config <- decodeFileEither path :: IO (Either ParseException NodeConfig)
    case config of
        Left e -> throw e
        Right con -> return con

-- | Decode string of human-readable hex characters.
decodeHex :: Text -> Maybe ByteString
decodeHex text =
    let (x, b) = B16.decode (E.encodeUtf8 text)
     in guard (b == BS.empty) >> return x
