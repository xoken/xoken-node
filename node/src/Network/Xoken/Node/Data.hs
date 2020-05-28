{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Network.Xoken.Node.Data where

import Debug.Trace
import Codec.Compression.GZip as GZ
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
import Data.ByteString.Base64.Lazy as B64L
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as C
import Data.ByteString.Short (ShortByteString)
import qualified Data.ByteString.Short as B.Short
import Data.Default
import Data.Foldable
import Data.Functor.Identity
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
    | GetAllegoryNameBranch
          { gaName :: String
          , gaIsProducer :: Bool
          }
    | RelayTx
          { rTx :: ByteString
          }
    | GetPartiallySignedAllegoryTx
          { gpsaPaymentInputs :: [OutPoint']
          , gpsaName :: String
          , gpsaIsProducer :: Bool
          , gpsaOutputOwner :: (String, Int)
          , gpsaOutputChange :: (String, Int)
          }
    deriving (Generic, Show, Hashable, Eq, Serialise, ToJSON)

instance FromJSON RPCReqParams where
    parseJSON (Object o) =
        (GetBlockByHeight <$> o .: "gbHeight") <|> (GetBlocksByHeight <$> o .: "gbHeights") <|>
        (GetBlockByHash <$> o .: "gbBlockHash") <|>
        (GetBlocksByHashes <$> o .: "gbBlockHashes") <|>
        (GetTransactionByTxID <$> o .: "gtTxHash") <|>
        (GetTransactionsByTxIDs <$> o .: "gtTxHashes") <|>
        (GetOutputsByAddress <$> o .: "gaAddrOutputs") <|>
        (GetOutputsByAddresses <$> o .: "gasAddrOutputs") <|>
        (GetMerkleBranchByTxID <$> o .: "gmbMerkleBranch") <|>
        (GetAllegoryNameBranch <$> o .: "gaName" <*> o .: "gaIsProducer") <|>
        (RelayTx . BL.toStrict . GZ.decompress . B64L.decodeLenient . BL.fromStrict . T.encodeUtf8 <$> o .: "rTx") <|>
        (GetPartiallySignedAllegoryTx <$> o .: "gpsaPaymentInputs" <*> o .: "gpsaName" <*> o .: "gpsaIsProducer" <*>
         o .: "gpsaOutputOwner" <*>
         o .: "gpsaOutputChange")

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
    | RespAllegoryNameBranch
          { nameBranch :: [OutPoint']
          }
    | RespRelayTx
          { rrTx :: Bool
          }
    | RespPartiallySignedAllegoryTx
          { psaTx :: ByteString
          }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance ToJSON RPCResponseBody where
  toJSON (RespBlockByHeight b) = object ["block" .= b]
  toJSON (RespBlocksByHeight bs) = object ["blocks" .= bs]
  toJSON (RespBlockByHash b) = object ["block" .= b]
  toJSON (RespBlocksByHashes bs) = object ["blocks" .= bs]
  toJSON (RespTransactionByTxID tx) = object ["tx" .= tx]
  toJSON (RespTransactionsByTxIDs txs) = object ["txs" .= txs]
  toJSON (RespOutputsByAddress sa) = object ["saddressOutputs" .= sa]
  toJSON (RespOutputsByAddresses ma) = object ["maddressOutputs" .= ma]
  toJSON (RespMerkleBranchByTxID mb) = object ["merkleBranch" .= mb]
  toJSON (RespAllegoryNameBranch nb) = object ["nameBranch" .= nb]
  toJSON (RespRelayTx rrTx) = object ["rrTx" .= rrTx]
  toJSON (RespPartiallySignedAllegoryTx ps) = object ["psaTx" .= (T.decodeUtf8 . BL.toStrict . B64L.encode . GZ.compress . BL.fromStrict $ ps)]

data BlockRecord =
    BlockRecord
        { rbHeight :: Int
        , rbHash :: String
        , rbHeader :: String
        }
    deriving (Generic, Show, Hashable, Eq, Serialise, ToJSON)

data TxRecord =
    TxRecord
        { txId :: String
        , txBlockInfo :: BlockInfo'
        , txSerialized :: C.ByteString
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON TxRecord where
    toJSON (TxRecord tId tBI tS) =
        object
            [ "txId" .= tId
            , "txBlockInfo" .= tBI
            , "txSerialized" .= (T.decodeUtf8 . BL.toStrict . B64L.encode . GZ.compress $ tS) -- decodeUtf8 won't because of B64 encode
            ]

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
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data OutPoint' =
    OutPoint'
        { opTxHash :: String
        , opIndex :: Int
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, FromJSON, ToJSON)

data BlockInfo' =
    BlockInfo'
        { binfBlockHash :: String
        , binfTxIndex :: Int
        , binfBlockHeight :: Int
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data MerkleBranchNode' =
    MerkleBranchNode'
        { nodeValue :: String
        , isLeftNode :: Bool
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data PubNotifyMessage =
    PubNotifyMessage
        { psBody :: ByteString
        }
    deriving (Show, Generic, Eq, Serialise)

-- Internal message posting --
data XDataReq
    = XDataRPCReq
          { reqId :: Int
          , method :: String
          , params :: Maybe RPCReqParams
          }
    | XCloseConnection
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance FromJSON XDataReq where
  parseJSON (Object o) =
    (XDataRPCReq <$> o .: "reqId" <*> o .: "method" <*> o .:? "params")
    <|> (pure XCloseConnection)

data XDataResp =
    XDataRPCResp
        { matchId :: Int
        , statusCode :: Int16
        , statusMessage :: Maybe String
        , respBody :: Maybe RPCResponseBody
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)
