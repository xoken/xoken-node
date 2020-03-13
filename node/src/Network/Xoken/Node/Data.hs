{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Network.Xoken.Node.Data where

import Codec.Serialise
import Conduit
import Control.Applicative
import Control.Arrow (first)
import Control.Monad
import Control.Monad.Trans.Maybe
import Data.Aeson as A
import qualified Data.Aeson.Encoding as A
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy.Char8 as C
import Data.ByteString.Short (ShortByteString)
import qualified Data.ByteString.Short as B.Short
import Data.Default
import Data.Foldable
import Data.Functor.Identity
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as M
import Data.Hashable
import Data.Int
import qualified Data.IntMap as I
import Data.IntMap.Strict (IntMap)
import Data.Maybe
import Data.Serialize as S
import Data.String.Conversions
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as T.Lazy
import Data.Word
import qualified Database.CQL.IO as Q
import GHC.Generics
import Network.Socket (SockAddr(SockAddrUnix))
import Paths_xoken_node as P
import UnliftIO
import UnliftIO.Exception
import qualified Web.Scotty.Trans as Scotty
import Xoken as H

encodeShort :: Serialize a => a -> ShortByteString
encodeShort = B.Short.toShort . S.encode

decodeShort :: Serialize a => ShortByteString -> a
decodeShort bs =
    case S.decode (B.Short.fromShort bs) of
        Left e -> error e
        Right a -> a

data RPCMessage
    = RPCRequest
          { rqMethod :: String
          , rqParams :: Maybe RPCReqParams
          }
    | RPCResponse
          { rsStatusCode :: Int16
          , rsStatusMessage :: Maybe String
          , rsBody :: Maybe RPCResponseBody
          }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data RPCReqParams
    = GetBlockByHeight
          { gbHeight :: Int
          }
    | GetBlocksByHeight
          { gbHeights :: [Int]
          }
    | GetBlockByHash
          { gbBlockHash :: String
          }
    | GetBlocksByHashes
          { gbBlockHashes :: [String]
          }
    | GetTransactionByTxID
          { gtTxHash :: String
          }
    | GetTransactionsByTxIDs
          { gtTxHashes :: [String]
          }
    | GetOutputsByAddress
          { gaAddrOutputs :: String
          }
    | GetOutputsByAddresses
          { gasAddrOutputs :: [String]
          }
    | GetMerkleBranchByTxID
          { gmbMerkleBranch :: String
          }
    | RelayTx
          { rTx :: ByteString
          }
    deriving (Generic, Show, Hashable, Eq, Serialise)

data RPCResponseBody
    = RespBlockByHeight
          { block :: BlockRecord
          }
    | RespBlocksByHeight
          { blocks :: [BlockRecord]
          }
    | RespBlockByHash
          { block :: BlockRecord
          }
    | RespBlocksByHashes
          { blocks :: [BlockRecord]
          }
    | RespTransactionByTxID
          { tx :: TxRecord
          }
    | RespTransactionsByTxIDs
          { txs :: [TxRecord]
          }
    | RespOutputsByAddress
          { saddressOutputs :: [AddressOutputs]
          }
    | RespOutputsByAddresses
          { maddressOutputs :: [AddressOutputs]
          }
    | RespMerkleBranchByTxID
          { merkleBranch :: [MerkleBranchNode']
          }
    | RespRelayTx
          { rrTx :: Bool
          }
    deriving (Generic, Show, Hashable, Eq, Serialise)

data BlockRecord =
    BlockRecord
        { rbHeight :: Int
        , rbHash :: String
        , rbHeader :: String
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

data TxRecord =
    TxRecord
        { txId :: String
        , txBlockInfo :: BlockInfo'
        , txSerialized :: C.ByteString
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data AddressOutputs =
    AddressOutputs
        { aoAddress :: String
        , aoOutput :: OutPoint'
        , aoBlockInfo :: BlockInfo'
        , aoIsBlockConfirmed :: Bool
        , aoIsOutputSpent :: Bool
        , aoIsTypeReceive :: Bool
        , aoOtherAddress :: String
        , aoPrevOutpoint :: OutPoint'
        , aoValue :: Int64
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data OutPoint' =
    OutPoint'
        { opTxHash :: String
        , opIndex :: Int
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data BlockInfo' =
    BlockInfo'
        { binfBlockHash :: String
        , binfTxIndex :: Int
        , binfBlockHeight :: Int
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data MerkleBranchNode' =
    MerkleBranchNode'
        { nodeValue :: String
        , isLeftNode :: Bool
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data PubNotifyMessage =
    PubNotifyMessage
        { psBody :: ByteString
        }
    deriving (Show, Generic, Eq, Serialise)
