{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MonoLocalBinds #-}

module Network.Xoken.Node.Env where

import Arivi.P2P.P2PEnv as PE hiding (option)
import Codec.Serialise
import Control.Concurrent.Event
import Control.Concurrent.MSem
import Control.Concurrent.MVar
import Control.Concurrent.STM.TQueue
import Control.Concurrent.STM.TVar
import Control.Monad.Catch
import Control.Monad.Reader
import Control.Monad.Trans.Control
import Crypto.Secp256k1
import qualified Data.ByteString.Lazy.Char8 as C
import qualified Data.HashTable.IO as H
import Data.Hashable
import Data.Int
import qualified Data.Map.Strict as M
import Data.Text
import Data.Time.Clock
import Data.Word
import GHC.Generics
import Network.Socket hiding (send)
import Network.Xoken.Block.Common
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction
import StmContainers.Map as SM
import StmContainers.Set as SS
import System.Logger
import System.Random
import Text.Read
import Xoken.NodeConfig

type HashTable k v = H.BasicHashTable k v

type HasXokenNodeEnv env m
     = ( HasBitcoinP2P m
       , HasDatabaseHandles m
       , HasLogger m
       , HasAllegoryEnv m
       , MonadReader env m
       , MonadBaseControl IO m
       , MonadThrow m)

data XokenNodeEnv =
    XokenNodeEnv
        { bitcoinP2PEnv :: !BitcoinP2P
        , dbHandles :: !DatabaseHandles
        , loggerEnv :: !Logger
        , allegoryEnv :: !AllegoryEnv
        }

data AllegoryEnv =
    AllegoryEnv
        { allegorySecretKey :: !SecKey
        }

data BitcoinP2P =
    BitcoinP2P
        { nodeConfig :: !NodeConfig
        , bitcoinPeers :: !(TVar (M.Map SockAddr BitcoinPeer))
        , blacklistedPeers :: !(TVar (M.Map SockAddr BitcoinPeer))
        , bestBlockUpdated :: !(MVar Bool)
        , headersWriteLock :: !(MVar Bool)
        , blockSyncStatusMap :: !(TSH.TSHashTable BlockHash (BlockSyncStatus, BlockHeight))
        , blockTxProcessingLeftMap :: !(TSH.TSHashTable BlockHash ((TSH.TSHashTable TxHash Int), Int))
        , epochType :: !(TVar Bool)
        , unconfirmedTxCache :: !(TSH.TSHashTable TxShortHash (Bool, TxHash))
        , txOutputValuesCache :: !(TSH.TSHashTable TxShortHash (TxHash, [(Word32, (Text, Text, Int64))]))
        , peerReset :: !(MVar Bool, TVar Int)
        , merkleQueueMap :: !(TSH.TSHashTable BlockHash (TQueue (TxHash, Bool)))
        , txSynchronizer :: !(TSH.TSHashTable (TxHash, Word32) (MVar (Text, Text, Int64)))
        , maxTMTBuilderThreadLock :: !(MSem Int)
        , indexUnconfirmedTx :: !(TVar Bool)
        , userDataCache :: !(HashTable Text (Text, Int32, Int32, UTCTime, [Text])) -- (name, quota, used, expiry time, roles)
        , txProcFailAttempts :: !(TVar Int)
        , bestSyncedBlock :: !(TVar (Maybe BlockInfo))
        }

class HasBitcoinP2P m where
    getBitcoinP2P :: m (BitcoinP2P)

class HasLogger m where
    getLogger :: m (Logger)

class HasDatabaseHandles m where
    getDB :: m (DatabaseHandles)

class HasAllegoryEnv m where
    getAllegory :: m (AllegoryEnv)

data ServiceEnv =
    ServiceEnv
        { xokenNodeEnv :: !XokenNodeEnv
        -- , p2pEnv :: !(P2PEnv m r t rmsg pmsg)
        }

data ServiceResource =
    AriviService
        {
        }
    deriving (Eq, Ord, Show, Generic)

type ServiceTopic = String

instance Serialise ServiceResource

instance Hashable ServiceResource

type HasService env m
     = ( HasXokenNodeEnv env m
       , HasP2PEnv env m ServiceResource ServiceTopic RPCMessage PubNotifyMessage
       , MonadReader env m
       , MonadBaseControl IO m)
