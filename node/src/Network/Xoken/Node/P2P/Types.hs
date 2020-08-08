{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ConstraintKinds #-}

module Network.Xoken.Node.P2P.Types where

import Control.Concurrent.MSem as MS
import Control.Concurrent.MSemN as MSN
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM
import Control.Concurrent.STM.TSem
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

type CqlConnection = Pool Socket

data DatabaseHandles =
    DatabaseHandles
        { keyValDB :: !Q.ClientState
        , graphDB :: !ServerState
        , connection :: !(CqlConnection)
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
        , bpIngressMsgCount :: !(TVar Int)
      -- ^ recent msg count for detecting stale peer connections
        , bpLastTxRecvTime :: !(TVar (Maybe UTCTime))
      -- ^ last tx recv time
        , bpLastGetDataSent :: !(TVar (Maybe UTCTime))
      -- ^ block 'GetData' sent time
        , bpBlockFetchWindow :: !(TVar Int)
      -- number of outstanding blocks
        , bpTxSem :: !(MSemN Int)
        -- number of outstanding transactions
        }

instance Show BitcoinPeer where
    show p = (show $ bpAddress p) ++ " : " ++ (show $ bpConnected p)

data BlockInfo =
    BlockInfo
        { biBlockHash :: !BlockHash
        , biBlockHeight :: !BlockHeight
        }
    deriving (Show)

data MerkleNode =
    MerkleNode
        { node :: !(Maybe Hash256)
        , leftChild :: !(Maybe Hash256)
        , rightChild :: !(Maybe Hash256)
        , isLeft :: !Bool
        }
    deriving (Show, Eq, Ord)

type HashCompute = (M.Map Int8 (MerkleNode), [MerkleNode])

emptyMerkleNode :: MerkleNode
emptyMerkleNode = MerkleNode {node = Nothing, leftChild = Nothing, rightChild = Nothing, isLeft = False}

data IngressStreamState =
    IngressStreamState
        { issBlockIngest :: !BlockIngestState
        , issBlockInfo :: !(Maybe BlockInfo)
        }
    deriving (Show)

data BlockIngestState =
    BlockIngestState
        { binUnspentBytes :: !B.ByteString
        , binTxPayloadLeft :: !Int
        , binTxTotalCount :: !Int
        , binTxIngested :: !Int
        , binBlockSize :: !Int
        , binChecksum :: !CheckSum32
        }
    deriving (Show)

data BlockSyncStatus
    = RequestSent !UTCTime
    | RequestQueued
    | RecentTxReceiveTime !(UTCTime, Int)
    | BlockReceiveComplete !UTCTime
    | BlockProcessingComplete
    deriving (Eq, Ord, Show)

-- |A pool of connections to Neo4j server
data ServerState =
    ServerState
        { pool :: !(Pool Pipe)
        }
