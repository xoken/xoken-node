{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ConstraintKinds #-}

module Network.Xoken.Node.P2P.Types where

import Control.Concurrent.MVar
import Control.Concurrent.STM
import qualified Data.ByteString as B
import Data.Functor.Identity
import Data.Time.Clock
import Data.Word
import qualified Database.CQL.IO as Q
import Network.Socket hiding (send)
import Network.Xoken.Block
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network
import Network.Xoken.Transaction
import System.Random
import Text.Read

-- | Type alias for a combination of hostname and port.
type HostPort = (Host, Port)

-- | Type alias for a hostname.
type Host = String

-- | Type alias for a port number.
type Port = Int

data DatabaseHandles =
    DatabaseHandles
        { keyValDB :: !Q.ClientState
        }

-- | Data structure representing an bitcoin peer.
data BitcoinPeer =
    BitcoinPeer
        { bpAddress :: !SockAddr
      -- ^ network address
        , bpSocket :: !(Maybe Socket)
      -- ^ live stream socket
        , bpSockLock :: !(MVar Bool)
      -- ^ socket write lock
        , bpConnected :: !Bool
      -- ^ peer is connected and ready
        , bpVersion :: !(Maybe Version)
      -- ^ protocol version
        , bpNonce :: !Word64
      -- ^ random nonce sent during handshake
        , bpPing :: !(Maybe (UTCTime, Word64))
      -- ^ last sent ping time and nonce
        , ingressStreamState :: !(TVar (Maybe IngressStreamState))
      -- ^ Block stream processing state
        }

-- | General node configuration.
data BitcoinNodeConfig =
    BitcoinNodeConfig
        { bncMaxPeers :: !Int
      -- ^ maximum number of connected peers allowed
        , bncPeers :: ![HostPort]
      -- ^ static list of peers to connect to
        , bncDiscover :: !Bool
      -- ^ activate peer discovery
        , bncNetAddr :: !NetworkAddress
      -- ^ network address for the local host
        , bncNet :: !Network
      -- ^ network constants
        , bncTimeout :: !Int
      -- ^ timeout in seconds
        }

data BlockInfo =
    BlockInfo
        { blockHash :: !BlockHash
        , blockHeight :: !Int
        }

data IngressStreamState =
    IngressStreamState
        { blockIngest :: BlockIngestState
        , blockInfo :: Maybe BlockInfo
        }

data BlockIngestState =
    BlockIngestState
        { unspentBytes :: !B.ByteString
        , txPayloadLeft :: !Int
        , txTotalCount :: !Int
        , txProcessed :: !Int
        , checksum :: !CheckSum32
        }
    deriving (Show)
