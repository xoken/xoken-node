{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MonoLocalBinds #-}

module Network.Xoken.Node.Env where

import Arivi.P2P.P2PEnv as PE hiding (option)
import Codec.Serialise
import Control.Concurrent.MVar

import Control.Concurrent.STM.TVar
import Control.Monad.Catch
import Control.Monad.Reader
import Control.Monad.Trans.Control
import qualified Data.ByteString.Lazy.Char8 as C
import qualified Data.HashTable.IO as H
import Data.Hashable
import qualified Data.Map.Strict as M
import Data.Time.Clock
import Data.Word
import qualified Database.CQL.IO as Q
import GHC.Generics
import Network.Socket hiding (send)
import Network.Xoken.Block.Common
import Network.Xoken.Node.Data
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction
import System.Logger
import System.Random
import Text.Read

type HashTable k v = H.BasicHashTable k v

type HasXokenNodeEnv env m
     = (HasBitcoinP2P m, HasDatabaseHandles m, HasLogger m, MonadReader env m, MonadBaseControl IO m, MonadThrow m)

data XokenNodeEnv =
    XokenNodeEnv
        { bitcoinP2PEnv :: !BitcoinP2P
        , dbHandles :: !DatabaseHandles
        , loggerEnv :: !Logger
        }

data BitcoinP2P =
    BitcoinP2P
        { bitcoinNodeConfig :: !BitcoinNodeConfig
        , bitcoinPeers :: !(TVar (M.Map SockAddr BitcoinPeer))
        , bestBlockUpdated :: !(MVar Bool)
        , headersWriteLock :: !(MVar Bool)
        , blockSyncStatusMap :: !(TVar (M.Map BlockHash (BlockSyncStatus, BlockHeight)))
        , epochType :: !(TVar Bool)
        , unconfirmedTxCache :: !(HashTable TxShortHash (Bool, TxHash))
        , indexUnconfirmedTx :: !Bool
        }

class HasBitcoinP2P m where
    getBitcoinP2P :: m (BitcoinP2P)

class HasLogger m where
    getLogger :: m (Logger)

class HasDatabaseHandles m where
    getDB :: m (DatabaseHandles)

data ServiceEnv m r t rmsg pmsg =
    ServiceEnv
        { xokenNodeEnv :: !XokenNodeEnv
        , p2pEnv :: !(P2PEnv m r t rmsg pmsg)
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
