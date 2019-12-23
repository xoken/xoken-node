{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ConstraintKinds #-}

module Network.Xoken.Node.P2P.Types where

import Control.Concurrent.MVar
import Control.Concurrent.STM
import qualified Data.ByteString as B
import Data.Functor.Identity
import Data.Int
import qualified Data.Map.Strict as M
import Data.Pool
import Data.Time.Clock
import Data.Word
import Database.Bolt as BT
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
        , graphDB :: !ServerState
        }

-- | Data structure representing an bitcoin peer.
data BitcoinPeer =
    BitcoinPeer
        { bpAddress :: !SockAddr
      -- ^ network address
        , bpSocket :: !(Maybe Socket)
      -- ^ live stream socket
        , bpReadMsgLock :: !(MVar Bool)
      -- ^  read message lock
        , bpWriteMsgLock :: !(MVar Bool)
      -- ^ write message lock
        , bpConnected :: !Bool
      -- ^ peer is connected and ready
        , bpVersion :: !(Maybe Version)
      -- ^ protocol version
        , bpNonce :: !Word64
      -- ^ random nonce sent during handshake
        , bpPing :: !(Maybe (UTCTime, Word64))
      -- ^ last sent ping time and nonce
        , bpIngressState :: !(TVar (Maybe IngressStreamState))
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
        { biBlockHash :: !BlockHash
        , biBlockHeight :: !BlockHeight
        }
    deriving (Show)

data MerkleNode =
    MerkleNode
        { node :: Maybe Hash256
        , leftChild :: Maybe Hash256
        , rightChild :: Maybe Hash256
        , isLeft :: Bool
        }
    deriving (Show, Eq, Ord)

type HashCompute = (M.Map Int8 (MerkleNode), [MerkleNode])

emptyMerkleNode :: MerkleNode
emptyMerkleNode = MerkleNode {node = Nothing, leftChild = Nothing, rightChild = Nothing, isLeft = False}

data IngressStreamState =
    IngressStreamState
        { issBlockIngest :: !BlockIngestState
        , issBlockInfo :: !(Maybe BlockInfo)
        , merkleTreeHeight :: !Int8
        , merkleTreeCurIndex :: !Int8
        , merklePrevNodesMap :: !HashCompute
        }
    deriving (Show)

data BlockIngestState =
    BlockIngestState
        { binUnspentBytes :: !B.ByteString
        , binTxPayloadLeft :: !Int
        , binTxTotalCount :: !Int
        , binTxProcessed :: !Int
        , binChecksum :: !CheckSum32
        }
    deriving (Show)

data BlockSyncStatus
    = RequestSent !UTCTime
    | RequestQueued
    | BlockReceived
    deriving (Eq, Ord, Show)

-- import Type
-- |A pool of connections to Neo4j server
data ServerState =
    ServerState
        { pool :: Pool Pipe
        }
