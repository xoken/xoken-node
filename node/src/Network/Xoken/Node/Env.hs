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
import Control.Monad.Reader
import Data.Hashable
import qualified Data.Map.Strict as M
import Data.Time.Clock
import Data.Word
import qualified Database.CQL.IO as Q
import GHC.Generics
import Network.Socket hiding (send)
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction
import System.Random
import Text.Read

-- import Data.Functor.Identity
-- import Network.Xoken.Block
-- import Network.Xoken.Constants
-- import Network.Xoken.Network
data BitcoinP2PEnv =
    BitcoinP2PEnv
        { bitcoinNodeConfig :: !BitcoinNodeConfig
        , bitcoinPeers :: !(TVar (M.Map SockAddr BitcoinPeer))
        , bestBlockUpdated :: !(MVar Bool)
        }

class HasBitcoinP2PEnv env where
    getBitcoinP2PEnv :: env -> BitcoinP2PEnv

instance HasBitcoinP2PEnv (ServiceEnv m r t rmsg pmsg) where
    getBitcoinP2PEnv = bitcoinP2PEnv

data DBEnv =
    DBEnv
        { dbHandles :: !DatabaseHandles
        }

class HasDBEnv env where
    getDBEnv :: env -> DBEnv

instance HasDBEnv (ServiceEnv m r t rmsg pmsg) where
    getDBEnv = dbEnv

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
     = ( HasDBEnv env
       , HasP2PEnv env m ServiceResource ServiceTopic String String
       , HasBitcoinP2PEnv env
       , MonadReader env m)
