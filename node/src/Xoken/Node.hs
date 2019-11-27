{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}

module Xoken.Node
    ( BlockData(..)
    , Transaction(..)
    , Input(..)
    , Output(..)
    , Spender(..)
    , BlockRef(..)
    , Unspent(..)
    , BlockTx(..)
    , XPubBal(..)
    , XPubUnspent(..)
    , Balance(..)
    , PeerInformation(..)
    , HealthCheck(..)
    , Event(..)
    , TxAfterHeight(..)
    , JsonSerial(..)
    , BinSerial(..)
    , TxId(..)
    , UnixTime
    , BlockPos
    , fromTransaction
    , toTransaction
    , transactionData
    , isCoinbase
    , confirmed
    ) where

import Conduit
import Control.Monad
import qualified Control.Monad.Except as E
import Control.Monad.Logger
import Control.Monad.Trans.Maybe
import Data.Foldable
import Data.Function
import qualified Data.HashMap.Strict as H
import Data.List
import Data.Maybe
import Data.Serialize (decode)
import qualified Data.Text as T
import Data.Word (Word32)
import Network.Socket (SockAddr(..))
import Network.Xoken.Node.AriviService
import Network.Xoken.Node.Data
import System.Random
import UnliftIO
import Xoken
