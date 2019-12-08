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
import Control.Concurrent.QSem
import Control.Concurrent.STM.TVar
import Control.Monad.Reader
import Data.Hashable
import qualified Data.Map.Strict as M
import Data.Time.Clock
import Data.Word
import qualified Database.CQL.IO as Q
import GHC.Generics
import Network.Socket hiding (send)
import Network.Xoken.Block.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction
import System.Logger
import System.Random
import Text.Read

data BitcoinP2PEnv =
    BitcoinP2PEnv
        { bitcoinNodeConfig :: !BitcoinNodeConfig
        , bitcoinPeers :: !(TVar (M.Map SockAddr BitcoinPeer))
        , bestBlockUpdated :: !(MVar Bool)
        , headersWriteLock :: !(MVar Bool)
        , blockFetchBalance :: !QSem
        , blockSyncStatusMap :: !(TVar (M.Map BlockHash (BlockSyncStatus, BlockHeight)))
        , logger :: Logger
        }

class HasBitcoinP2PEnv m where
    getBitcoinP2PEnv :: m (BitcoinP2PEnv)

data DBEnv =
    DBEnv
        { dbHandles :: !DatabaseHandles
        }

class HasDBEnv m where
    getDBEnv :: m (DBEnv)

data ServiceEnv m r t rmsg pmsg =
    ServiceEnv
        { dbEnv :: DBEnv
        , p2pEnv :: P2PEnv m r t rmsg pmsg
        , bitcoinP2PEnv :: BitcoinP2PEnv
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
     = (HasDBEnv m, HasP2PEnv env m ServiceResource ServiceTopic String String, HasBitcoinP2PEnv m, MonadReader env m)
