{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Xoken.Node.Data where

import Codec.Serialise
import Conduit
import Control.Applicative
import Control.Arrow (first)
import Control.Monad
import Control.Monad.Trans.Maybe
import Data.Aeson as A
import qualified Data.Aeson.Encode.Pretty as AP
import qualified Data.Aeson.Encoding as A
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.ByteString.Base64 as B64
import Data.ByteString.Base64.Lazy as B64L
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as C
import Data.ByteString.Short (ShortByteString)
import qualified Data.ByteString.Short as B.Short
import Data.Char (ord)
import Data.Default
import Data.Foldable
import Data.Functor.Identity
import Data.Hashable
import Data.Hashable.Time
import Data.Int
import qualified Data.IntMap as I
import Data.IntMap.Strict (IntMap)
import Data.Maybe
import Data.Serialize as S
import Data.String.Conversions
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as T.Lazy
import Data.Time.Clock (UTCTime)
import Data.Word
import Database.XCQL.Protocol as Q
import GHC.Generics
import Network.Socket (SockAddr(SockAddrUnix))
import Network.Xoken.Address.Base58
import Paths_xoken_node as P
import Prelude as P
import Text.Regex.TDFA
import UnliftIO
import UnliftIO.Exception
import qualified Web.Scotty.Trans as Scotty
import Xoken as H

encodeShort :: Serialize a => a -> ShortByteString
encodeShort = B.Short.toShort . S.encode

decodeShort :: Serialize a => ShortByteString -> a
decodeShort bs =
    case S.decode (B.Short.fromShort bs) of
        Left e -> P.error e
        Right a -> a

data RPCMessage
    = RPCRequest
          { rqMethod :: String
          , rqParams :: RPCReqParams
          }
    | RPCResponse
          { rsStatusCode :: Int16
          , pretty :: Bool
          , rsResp :: Either RPCError (Maybe RPCResponseBody)
          }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data RPCError =
    RPCError
        { rsStatusMessage :: RPCErrors
        , rsErrorData :: Maybe String
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data XRPCRequest
    = CBORRPCRequest
          { reqId :: Int
          , method :: String
          , params :: RPCReqParams
          }
    | JSONRPCRequest
          { method :: String
          , params :: RPCReqParams
          , jsonrpc :: String
          , id :: Int
          }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance FromJSON XRPCRequest where
    parseJSON = genericParseJSON (defaultOptions {sumEncoding = UntaggedValue})

data XRPCResponse
    = CBORRPCResponse
          { matchId :: Int
          , statusCode :: Int16
          , statusMessage :: Maybe String
          , respBody :: Maybe RPCResponseBody
          }
    | JSONRPCSuccessResponse
          { jsonrpc :: String
          , result :: Maybe RPCResponseBody
          , id :: Int
          }
    | JSONRPCErrorResponse
          { id :: Int
          , error :: ErrorResponse
          , jsonrpc :: String
          }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON XRPCResponse where
    toJSON = genericToJSON (defaultOptions {sumEncoding = UntaggedValue})

data ErrorResponse =
    ErrorResponse
        { code :: Int
        , message :: String
        , _data :: Maybe String
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON ErrorResponse where
    toJSON (ErrorResponse c m d) = object ["code" .= c, "message" .= m, "data" .= d]

data RPCReqParams
    = AuthenticateReq
          { username :: String
          , password :: String
          , prettyPrint :: Bool
          }
    | GeneralReq
          { sessionKey :: String
          , prettyPrint :: Bool
          , methodParams :: Maybe RPCReqParams'
          }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance FromJSON RPCReqParams where
    parseJSON (Object o) =
        (AuthenticateReq <$> o .: "username" <*> o .: "password" <*> o .:? "prettyPrint" .!= True) <|>
        (GeneralReq <$> o .: "sessionKey" <*> o .:? "prettyPrint" .!= True <*> o .:? "methodParams")

data RPCReqParams'
    = AddUser
          { auUsername :: String
          , auApiExpiryTime :: Maybe UTCTime
          , auApiQuota :: Maybe Int32
          , auFirstName :: String
          , auLastName :: String
          , auEmail :: String
          , auRoles :: Maybe [String]
          }
    | GetBlockByHeight
          { gbHeight :: Int
          }
    | GetBlocksByHeights
          { gbHeights :: [Int]
          }
    | GetBlockByHash
          { gbBlockHash :: String
          }
    | GetBlocksByHashes
          { gbBlockHashes :: [String]
          }
    | GetChainHeaders
          { gcHeight :: Int32
          , gcPageSize :: Int
          }
    | GetTxIDsByBlockHash
          { gtTxBlockHash :: String
          , gtPageSize :: Int32
          , gtPageNumber :: Int32
          }
    | GetTransactionByTxID
          { gtTxHash :: String
          }
    | GetTransactionsByTxIDs
          { gtTxHashes :: [String]
          }
    | GetRawTransactionByTxID
          { gtRTxHash :: String
          }
    | GetRawTransactionsByTxIDs
          { gtRTxHashes :: [String]
          }
    | GetOutputsByAddress
          { gaAddrOutputs :: String
          , gaPageSize :: Maybe Int32
          , gaCursor :: Maybe String
          }
    | GetOutputsByAddresses
          { gasAddrOutputs :: [String]
          , gasPageSize :: Maybe Int32
          , gasCursor :: Maybe String
          }
    | GetOutputsByScriptHash
          { gaScriptHashOutputs :: String
          , gaScriptHashPageSize :: Maybe Int32
          , gaScriptHashCursor :: Maybe String
          }
    | GetOutputsByScriptHashes
          { gasScriptHashOutputs :: [String]
          , gasScriptHashPageSize :: Maybe Int32
          , gasScriptHashCursor :: Maybe String
          }
    | GetUTXOsByAddress
          { guaAddrOutputs :: String
          , guaPageSize :: Maybe Int32
          , guaCursor :: Maybe String
          }
    | GetUTXOsByScriptHash
          { guScriptHashOutputs :: String
          , guScriptHashPageSize :: Maybe Int32
          , guScriptHashCursor :: Maybe String
          }
    | GetUTXOsByAddresses
          { guasAddrOutputs :: [String]
          , guasPageSize :: Maybe Int32
          , guasCursor :: Maybe String
          }
    | GetUTXOsByScriptHashes
          { gusScriptHashOutputs :: [String]
          , gusScriptHashPageSize :: Maybe Int32
          , gusScriptHashCursor :: Maybe String
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
    | RelayMultipleTx
          { rTxns :: [ByteString]
          }
    | GetProducer
          { gpName :: [Int]
          }
    | GetTxOutputSpendStatus
          { gtssHash :: String
          , gtssIndex :: Int32
          }
    | UserByUsername
          { uUsername :: String
          }
    | UpdateUserByUsername
          { uuUsername :: String
          , uuUpdateData :: UpdateUserByUsername'
          }
    deriving (Generic, Show, Hashable, Eq, Serialise, ToJSON)

instance FromJSON RPCReqParams' where
    parseJSON (Object o) =
        (GetBlockByHeight <$> o .: "height") <|> (GetBlocksByHeights <$> o .: "heights") <|>
        (GetBlockByHash <$> o .: "hash") <|>
        (GetBlocksByHashes <$> o .: "hashes") <|>
        (GetTxIDsByBlockHash <$> o .: "hash" <*> o .:? "pageSize" .!= 100 <*> o .:? "pageNumber" .!= 1) <|>
        (GetTransactionByTxID <$> o .: "txid") <|>
        (GetTransactionsByTxIDs <$> o .: "txids") <|>
        (GetRawTransactionByTxID <$> o .: "txid") <|>
        (GetRawTransactionsByTxIDs <$> o .: "txids") <|>
        (GetOutputsByAddress <$> o .: "address" <*> o .:? "pageSize" <*> o .:? "cursor") <|>
        (GetOutputsByAddresses <$> o .: "addresses" <*> o .:? "pageSize" <*> o .:? "cursor") <|>
        (GetOutputsByScriptHash <$> o .: "scriptHash" <*> o .:? "pageSize" <*> o .:? "cursor") <|>
        (GetOutputsByScriptHashes <$> o .: "scriptHashes" <*> o .:? "pageSize" <*> o .:? "cursor") <|>
        (GetUTXOsByAddress <$> o .: "address" <*> o .:? "pageSize" <*> o .:? "cursor") <|>
        (GetUTXOsByAddresses <$> o .: "addresses" <*> o .:? "pageSize" <*> o .:? "cursor") <|>
        (GetUTXOsByScriptHash <$> o .: "scriptHash" <*> o .:? "pageSize" <*> o .:? "cursor") <|>
        (GetUTXOsByScriptHashes <$> o .: "scriptHashes" <*> o .:? "pageSize" <*> o .:? "cursor") <|>
        (GetMerkleBranchByTxID <$> o .: "txid") <|>
        (GetAllegoryNameBranch <$> o .: "name" <*> o .: "isProducer") <|>
        (RelayTx . B64.decodeLenient . T.encodeUtf8 <$> o .: "rawTx") <|>
        (RelayMultipleTx . (B64.decodeLenient . T.encodeUtf8 <$>) <$> o .: "rawTransactions") <|>
        (GetProducer <$> o .: "name") <|>
        (AddUser <$> o .: "username" <*> o .:? "apiExpiryTime" <*> o .:? "apiQuota" <*> o .: "firstName" <*>
         o .: "lastName" <*>
         o .: "email" <*>
         o .:? "roles") <|>
        (GetTxOutputSpendStatus <$> o .: "txid" <*> o .: "index") <|>
        (UserByUsername <$> o .: "username") <|>
        (UpdateUserByUsername <$> o .: "username" <*> o .: "updateData") <|>
        (GetChainHeaders <$> o .:? "startBlockHeight" .!= 1 <*> o .:? "pageSize" .!= 2000)

data RPCResponseBody
    = AuthenticateResp
          { auth :: AuthResp
          }
    | RespAddUser
          { addUser :: AddUserResp
          }
    | RespBlockByHeight
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
    | RespChainInfo
          { chainInfo :: ChainInfo
          }
    | RespChainHeaders
          { chainHeaders :: [ChainHeader]
          }
    | RespTxIDsByBlockHash
          { txids :: [String]
          }
    | RespTransactionByTxID
          { tx :: TxRecord
          }
    | RespTransactionsByTxIDs
          { txs :: [TxRecord]
          }
    | RespRawTransactionByTxID
          { rawTx :: RawTxRecord
          }
    | RespRawTransactionsByTxIDs
          { rawTxs :: [RawTxRecord]
          }
    | RespOutputsByAddress
          { nextCursor :: Maybe String
          , saddressOutputs :: [AddressOutputs]
          }
    | RespOutputsByAddresses
          { nextCursor :: Maybe String
          , maddressOutputs :: [AddressOutputs]
          }
    | RespOutputsByScriptHash
          { nextCursor :: Maybe String
          , sscriptOutputs :: [ScriptOutputs]
          }
    | RespOutputsByScriptHashes
          { nextCursor :: Maybe String
          , mscriptOutputs :: [ScriptOutputs]
          }
    | RespUTXOsByAddress
          { nextCursor :: Maybe String
          , saddressUTXOs :: [AddressOutputs]
          }
    | RespUTXOsByAddresses
          { nextCursor :: Maybe String
          , maddressUTXOs :: [AddressOutputs]
          }
    | RespUTXOsByScriptHash
          { nextCursor :: Maybe String
          , sscriptUTXOs :: [ScriptOutputs]
          }
    | RespUTXOsByScriptHashes
          { nextCursor :: Maybe String
          , mscriptUTXOs :: [ScriptOutputs]
          }
    | RespMerkleBranchByTxID
          { merkleBranch :: [MerkleBranchNode']
          }
    | RespAllegoryNameBranch
          { nameBranch :: [(OutPoint', [MerkleBranchNode'])]
          }
    | RespRelayTx
          { rrTx :: Bool
          }
    | RespRelayMultipleTx
          { rrMultipleTx :: [Bool]
          }
    | RespGetProducer
          { producerName :: [Int]
          , producerOutpoint :: OutPoint'
          , producerScript :: String
          }
    | RespTxOutputSpendStatus
          { spendStatus :: Maybe TxOutputSpendStatus
          }
    | RespUser
          { user :: Maybe User
          }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance ToJSON RPCResponseBody where
    toJSON (AuthenticateResp a) = object ["auth" .= a]
    toJSON (RespAddUser usr) = object ["user" .= usr]
    toJSON (RespBlockByHeight b) = object ["block" .= b]
    toJSON (RespBlocksByHeight bs) = object ["blocks" .= bs]
    toJSON (RespBlockByHash b) = object ["block" .= b]
    toJSON (RespBlocksByHashes bs) = object ["blocks" .= bs]
    toJSON (RespChainInfo ci) = object ["chainInfo" .= ci]
    toJSON (RespChainHeaders chs) = object ["blockHeaders" .= chs]
    toJSON (RespTxIDsByBlockHash txids) = object ["txids" .= txids]
    toJSON (RespTransactionByTxID tx) = object ["tx" .= tx]
    toJSON (RespTransactionsByTxIDs txs) = object ["txs" .= txs]
    toJSON (RespRawTransactionByTxID tx) = object ["rawTx" .= tx]
    toJSON (RespRawTransactionsByTxIDs txs) = object ["rawTxs" .= txs]
    toJSON (RespOutputsByAddress nc sa) = object ["nextCursor" .= nc, "outputs" .= sa]
    toJSON (RespOutputsByAddresses nc ma) = object ["nextCursor" .= nc, "outputs" .= ma]
    toJSON (RespOutputsByScriptHash nc sa) = object ["nextCursor" .= nc, "outputs" .= sa]
    toJSON (RespOutputsByScriptHashes nc ma) = object ["nextCursor" .= nc, "outputs" .= ma]
    toJSON (RespUTXOsByAddress nc sa) = object ["nextCursor" .= nc, "utxos" .= sa]
    toJSON (RespUTXOsByAddresses nc ma) = object ["nextCursor" .= nc, "utxos" .= ma]
    toJSON (RespUTXOsByScriptHash nc sa) = object ["nextCursor" .= nc, "utxos" .= sa]
    toJSON (RespUTXOsByScriptHashes nc ma) = object ["nextCursor" .= nc, "utxos" .= ma]
    toJSON (RespMerkleBranchByTxID mb) = object ["merkleBranch" .= mb]
    toJSON (RespAllegoryNameBranch nb) = object ["nameBranch" .= nb]
    toJSON (RespRelayTx rrTx) = object ["txBroadcast" .= rrTx]
    toJSON (RespRelayMultipleTx rrMultipleTx) = object ["txnsBroadcast" .= rrMultipleTx]
    toJSON (RespGetProducer name op scr) = object ["name" .= name, "outpoint" .= op, "script" .= scr]
    toJSON (RespTxOutputSpendStatus ss) = object ["spendStatus" .= ss]
    toJSON (RespUser u) = object ["user" .= u]

data UpdateUserByUsername' =
    UpdateUserByUsername'
        { uuPassword :: Maybe String
        , uuFirstName :: Maybe String
        , uuLastName :: Maybe String
        , uuEmail :: Maybe String
        , uuApiQuota :: Maybe Int32
        , uuRoles :: Maybe [String]
        , uuApiExpiryTime :: Maybe UTCTime
        }
    deriving (Generic, Show, Hashable, Eq, Serialise, ToJSON)

instance FromJSON UpdateUserByUsername' where
    parseJSON (Object o) =
        (UpdateUserByUsername' <$> o .:? "password" <*> o .:? "firstName" <*> o .:? "lastName" <*> o .:? "email" <*>
         o .:? "apiQuota" <*>
         o .:? "roles" <*>
         o .:? "apiExpiryTime")

data AuthResp =
    AuthResp
        { sessionKey :: Maybe String
        , callsUsed :: Int
        , callsRemaining :: Int
        }
    deriving (Generic, Show, Hashable, Eq, Serialise, ToJSON)

data AddUserResp =
    AddUserResp
        { aurUser :: User
        , aurPassword :: String
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance ToJSON AddUserResp where
    toJSON (AddUserResp (User uname _ fname lname email roles apiQuota _ apiExpTime _ _) pwd) =
        object
            [ "username" .= uname
            , "password" .= pwd
            , "firstName" .= fname
            , "lastName" .= lname
            , "email" .= email
            , "roles" .= roles
            , "apiQuota" .= apiQuota
            , "apiExpiryTime" .= apiExpTime
            ]

data User =
    User
        { uUsername :: String
        , uHashedPassword :: String
        , uFirstName :: String
        , uLastName :: String
        , uEmail :: String
        , uRoles :: [String]
        , uApiQuota :: Int
        , uApiUsed :: Int
        , uApiExpiryTime :: UTCTime
        , uSessionKey :: String
        , uSessionKeyExpiry :: UTCTime
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance ToJSON User where
    toJSON (User uname _ fname lname email roles apiQuota apiUsed apiExpTime sKey sKeyExp) =
        object
            [ "username" .= uname
            , "firstName" .= fname
            , "lastName" .= lname
            , "email" .= email
            , "roles" .= roles
            , "callsRemaining" .= (apiQuota - apiUsed)
            , "callsUsed" .= apiUsed
            , "apiExpiryTime" .= apiExpTime
            , "sessionKey" .= sKey
            , "sessionKeyExpiry" .= sKeyExp
            ]

data ChainInfo =
    ChainInfo
        { ciChain :: String
        , ciChainWork :: String
        , ciHeaders :: Int32
        , ciBlocks :: Int32
        , ciBestBlockHash :: String
        , ciBestSyncedHash :: String
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance ToJSON ChainInfo where
    toJSON (ChainInfo ch cw hdr blk hs shs) =
        object
            [ "chain" .= ch
            , "chainwork" .= cw
            , "chainTip" .= hdr
            , "blocksSynced" .= blk
            , "chainTipHash" .= hs
            , "syncedBlockHash" .= shs
            ]

data BlockRecord =
    BlockRecord
        { rbHeight :: Int
        , rbHash :: String
        , rbHeader :: BlockHeader'
        , rbNextBlockHash :: String
        , rbSize :: Int
        , rbTxCount :: Int
        , rbGuessedMiner :: String
        , rbCoinbaseMessage :: String
        , rbCoinbaseTx :: C.ByteString
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance ToJSON BlockRecord where
    toJSON (BlockRecord ht hs hdr nbhs size ct gm cm cb) =
        object
            [ "height" .= ht
            , "hash" .= hs
            , "header" .= hdr
            , "nextBlockHash" .= nbhs
            , "size" .= size
            , "txCount" .= ct
            , "guessedMiner" .= gm
            , "coinbaseMessage" .= cm
            , "coinbaseTx" .= (T.decodeUtf8 . BL.toStrict . B64L.encode $ cb)
            ]

data BlockHeader' =
    BlockHeader'
        { blockVersion' :: Word32
        , prevBlock' :: BlockHash
        , merkleRoot' :: String
        , blockTimestamp' :: Timestamp
        , blockBits' :: Word32
        , bhNonce' :: Word32
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance FromJSON BlockHeader' where
    parseJSON (Object o) =
        (BlockHeader' <$> o .: "blockVersion" <*> o .: "prevBlock" <*> o .: "merkleRoot" <*> o .: "blockTimestamp" <*>
         o .: "blockBits" <*>
         o .: "bhNonce")

instance ToJSON BlockHeader' where
    toJSON (BlockHeader' v pb mr ts bb bn) =
        object
            [ "blockVersion" .= v
            , "prevBlock" .= pb
            , "merkleRoot" .= (reverse2 mr)
            , "blockTimestamp" .= ts
            , "blockBits" .= bb
            , "nonce" .= bn
            ]

data ChainHeader =
    ChainHeader
        { blockHeight :: Int32
        , blockHash :: String
        , blockHeader :: BlockHeader'
        , txCount :: Int32
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance ToJSON ChainHeader where
    toJSON (ChainHeader ht hs (BlockHeader' v pb mr ts bb bn) txc) =
        object
            [ "blockHeight" .= ht
            , "blockHash" .= hs
            , "blockVersion" .= v
            , "prevBlock" .= pb
            , "merkleRoot" .= (reverse2 mr)
            , "blockTimestamp" .= ts
            , "difficulty" .= (convertBitsToDifficulty bb)
            , "nonce" .= bn
            , "txCount" .= txc
            ]

data RawTxRecord =
    RawTxRecord
        { txId :: String
        , size :: Int32
        , txBlockInfo :: Maybe BlockInfo'
        , txSerialized :: C.ByteString
        , txOutputs :: Maybe [TxOutput]
        , txInputs :: [TxInput]
        , fees :: Int64
        , txMerkleBranch :: Maybe [MerkleBranchNode']
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON RawTxRecord where
    toJSON (RawTxRecord tId sz tBI tS txo txi fee mrkl) =
        object
            [ "txId" .= tId
            , "size" .= sz
            , "txIndex" .= (binfTxIndex <$> tBI)
            , "blockHash" .= (binfBlockHash <$> tBI)
            , "blockHeight" .= (binfBlockHeight <$> tBI)
            , "txSerialized" .= (T.decodeUtf8 . BL.toStrict . B64L.encode $ tS)
            , "txOutputs" .= txo
            , "txInputs" .= txi
            , "fees" .= fee
            , "merkleBranch" .= mrkl
            ]

data TxRecord =
    TxRecord
        { txId :: String
        , size :: Int32
        , txBlockInfo :: Maybe BlockInfo'
        , tx :: Tx'
        , fees :: Int64
        , txMerkleBranch :: Maybe [MerkleBranchNode']
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON TxRecord where
    toJSON (TxRecord tId sz tBI tx' fee mrkl) =
        object
            [ "txId" .= tId
            , "size" .= sz
            , "txIndex" .= (binfTxIndex <$> tBI)
            , "blockHash" .= (binfBlockHash <$> tBI)
            , "blockHeight" .= (binfBlockHeight <$> tBI)
            , "tx" .= tx'
            , "fees" .= fee
            , "merkleBranch" .= mrkl
            ]

data Tx' =
    Tx'
        { txVersion :: Word32
        , txOuts :: [TxOutput]
        , txInps :: [TxInput]
        , txLockTime :: Word32
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data TxInput =
    TxInput
        { outpointTxID :: String
        , outpointIndex :: Int32
        , txInputIndex :: Int32
        , address :: String -- decode will succeed for P2PKH txn
        , value :: Int64
        , unlockingScript :: ByteString -- scriptSig
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data TxOutput =
    TxOutput
        { outputIndex :: Int32
        , address :: String -- decode will succeed for P2PKH txn
        , txSpendInfo :: Maybe SpendInfo
        , value :: Int64
        , lockingScript :: ByteString -- Script Pub Key
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data TxOutputSpendStatus =
    TxOutputSpendStatus
        { isSpent :: Bool
        , spendingTxID :: Maybe String
        , spendingTxBlockHt :: Maybe Int32
        , spendingTxIndex :: Maybe Int32
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON TxOutputSpendStatus where
    toJSON (TxOutputSpendStatus tis stxid stxht stxindex) =
        object ["isSpent" .= tis, "spendingTxID" .= stxid, "spendingTxBlockHt" .= stxht, "spendingTxIndex" .= stxindex]

data ResultWithCursor r c =
    ResultWithCursor
        { res :: r
        , cur :: c
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

-- ordering instance for ResultWithCursor
-- imp.: note the FLIP
instance (Ord c, Eq r) => Ord (ResultWithCursor r c) where
    compare rc1 rc2 = flip compare c1 c2
      where
        c1 = cur rc1
        c2 = cur rc2

data AddressOutputs =
    AddressOutputs
        { aoAddress :: String
        , aoOutput :: OutPoint'
        , aoBlockInfo :: Maybe BlockInfo'
        , aoSpendInfo :: Maybe SpendInfo
        , aoPrevOutpoint :: [(OutPoint', Int32, Int64)]
        , aoValue :: Int64
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON AddressOutputs where
    toJSON (AddressOutputs addr out bi ios po val) =
        object
            [ "address" .= addr
            , "outputTxHash" .= (opTxHash out)
            , "outputIndex" .= (opIndex out)
            , "txIndex" .= (binfTxIndex <$> bi)
            , "blockHash" .= (binfBlockHash <$> bi)
            , "blockHeight" .= (binfBlockHeight <$> bi)
            , "spendInfo" .= ios
            , "prevOutpoint" .= po
            , "value" .= val
            ]

data ScriptOutputs =
    ScriptOutputs
        { scScriptHash :: String
        , scOutput :: OutPoint'
        , scBlockInfo :: Maybe BlockInfo'
        , scSpendInfo :: Maybe SpendInfo
        , scPrevOutpoint :: [(OutPoint', Int32, Int64)]
        , scValue :: Int64
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON ScriptOutputs where
    toJSON (ScriptOutputs dh out bi ios po val) =
        object
            [ "scriptHash" .= dh
            , "outputTxHash" .= (opTxHash out)
            , "outputIndex" .= (opIndex out)
            , "txIndex" .= (binfTxIndex <$> bi)
            , "blockHash" .= (binfBlockHash <$> bi)
            , "blockHeight" .= (binfBlockHeight <$> bi)
            , "spendInfo" .= ios
            , "prevOutpoint" .= po
            , "value" .= val
            ]

data OutPoint' =
    OutPoint'
        { opTxHash :: String
        , opIndex :: Int32
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, FromJSON, ToJSON)

data BlockInfo' =
    BlockInfo'
        { binfBlockHash :: String
        , binfBlockHeight :: Int32
        , binfTxIndex :: Int32
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

data SpendInfo =
    SpendInfo
        { spendingTxId :: String
        , spendindTxIdx :: Int32
        , spendingBlockInfo :: BlockInfo'
        , spendInfo' :: [SpendInfo']
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON SpendInfo where
    toJSON (SpendInfo stid stidx bi si) =
        object
            [ "spendingTxId" .= stid
            , "spendingTxIndex" .= stidx
            , "spendingBlockHash" .= (binfBlockHash bi)
            , "spendingBlockHeight" .= (binfBlockHeight bi)
            , "spendData" .= si
            ]

data SpendInfo' =
    SpendInfo'
        { spendingOutputIndex :: Int32
        , outputAddress :: T.Text
        , value :: Int64
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data TxOutputData =
    TxOutputData
        { txid :: T.Text
        , txind :: Int32
        , address :: T.Text
        , value :: Int64
        , blockInfo :: Maybe BlockInfo'
        , inputs :: [((T.Text, Int32), Int32, (T.Text, Int64))]
        , spendInfo :: Maybe SpendInfo
        }
    deriving (Show, Generic, Eq, Serialise)

-- Internal message posting --
data XDataReq
    = XDataRPCReq
          { reqId :: Int
          , method :: String
          , params :: RPCReqParams
          , version :: Maybe String
          }
    | XDataRPCBadRequest
    | XCloseConnection
    deriving (Show, Generic, Hashable, Eq, Serialise)

data XDataResp =
    XDataRPCResp
        { matchId :: Int
        , statusCode :: Int16
        , statusMessage :: Maybe String
        , respBody :: Maybe RPCResponseBody
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data RPCErrors
    = INVALID_METHOD
    | PARSE_ERROR
    | INVALID_PARAMS
    | INTERNAL_ERROR
    | SERVER_ERROR
    | INVALID_REQUEST
    deriving (Generic, Hashable, Eq, Serialise)

instance Show RPCErrors where
    show e =
        case e of
            INVALID_METHOD -> "Error: Invalid method"
            PARSE_ERROR -> "Error: Parse error"
            INVALID_PARAMS -> "Error: Invalid params"
            INTERNAL_ERROR -> "Error: RPC error occurred"
            SERVER_ERROR -> "Error: Something went wrong"
            INVALID_REQUEST -> "Error: Invalid request"

-- can be replaced with Enum instance but in future other RPC methods might be handled then we might have to give different codes
getJsonRPCErrorCode :: RPCErrors -> Int
getJsonRPCErrorCode err =
    case err of
        SERVER_ERROR -> -32000
        INVALID_REQUEST -> -32600
        INVALID_METHOD -> -32601
        INVALID_PARAMS -> -32602
        INTERNAL_ERROR -> -32603
        PARSE_ERROR -> -32700

coinbaseTxToMessage :: C.ByteString -> String
coinbaseTxToMessage s =
    case C.length (C.pack regex) > 6 of
        True ->
            let sig = C.drop 4 $ C.pack regex
                sigLen = fromIntegral . ord . C.head $ sig
                htLen = fromIntegral . ord . C.head . C.tail $ sig
             in C.unpack . C.take (sigLen - htLen - 1) . C.drop (htLen + 2) $ sig
        False -> "False"
  where
    r :: String
    r = "\255\255\255\255[\NUL-\255]+"
    regex = ((C.unpack s) =~ r) :: String

validateEmail :: String -> Bool
validateEmail email =
    let emailRegex = "^[a-zA-Z0-9+._-]+@[a-zA-Z-]+\\.[a-z]+$" :: String
     in (email =~ emailRegex :: Bool) || (null email)

mergeTxInTxInput :: TxIn -> TxInput -> TxInput
mergeTxInTxInput (TxIn {..}) txInput = txInput {unlockingScript = scriptInput}

mergeTxOutTxOutput :: TxOut -> TxOutput -> TxOutput
mergeTxOutTxOutput (TxOut {..}) txOutput = txOutput {lockingScript = scriptOutput}

mergeAddrTxInTxInput :: String -> TxIn -> TxInput -> TxInput
mergeAddrTxInTxInput addr (TxIn {..}) txInput = txInput {unlockingScript = scriptInput, address = addr}

mergeAddrTxOutTxOutput :: String -> TxOut -> TxOutput -> TxOutput
mergeAddrTxOutTxOutput addr (TxOut {..}) txOutput = txOutput {lockingScript = scriptOutput, address = addr}

txToTx' :: Tx -> [TxOutput] -> [TxInput] -> Tx'
txToTx' (Tx {..}) txout txin = Tx' txVersion txout txin txLockTime

type TxIdOutputs = ((T.Text, Int32, Int32), Bool, Set ((T.Text, Int32), Int32, (T.Text, Int64)), Int64, T.Text)

type TxIdOutputs' = (Bool, Set ((T.Text, Int32), Int32, (T.Text, Int64)), Int64, T.Text)

genUnConfTxOutputData :: (T.Text, Int32, TxIdOutputs', Maybe TxIdOutputs') -> TxOutputData
genUnConfTxOutputData (txId, txIndex, (_, inps, val, addr), Nothing) =
    TxOutputData txId txIndex addr val Nothing (Q.fromSet inps) Nothing
genUnConfTxOutputData (txId, txIndex, (_, inps, val, addr), Just (_, oth, _, _)) =
    let other = Q.fromSet oth
        ((stid, _), stidx, _) = head $ other
        si = (\((_, soi), _, (ad, vl)) -> SpendInfo' soi ad vl) <$> other
     in TxOutputData txId txIndex addr val Nothing (Q.fromSet inps) Nothing

genTxOutputData :: (T.Text, Int32, TxIdOutputs, Maybe TxIdOutputs) -> TxOutputData
genTxOutputData (txId, txIndex, ((hs, ht, ind), _, inps, val, addr), Nothing) =
    TxOutputData txId txIndex addr val (Just $ BlockInfo' (T.unpack hs) ht ind) (Q.fromSet inps) Nothing
genTxOutputData (txId, txIndex, ((hs, ht, ind), _, inps, val, addr), Just ((shs, sht, sind), _, oth, _, _)) =
    let other = Q.fromSet oth
        ((stid, _), stidx, _) = head $ other
        si = (\((_, soi), _, (ad, vl)) -> SpendInfo' soi ad vl) <$> other
     in TxOutputData
            txId
            txIndex
            addr
            val
            (Just $ BlockInfo' (T.unpack hs) ht ind)
            (Q.fromSet inps)
            (Just $ SpendInfo (T.unpack stid) stidx (BlockInfo' (T.unpack shs) sht sind) si)

txOutputDataToOutput :: TxOutputData -> TxOutput
txOutputDataToOutput (TxOutputData {..}) = TxOutput txind (T.unpack address) spendInfo value ""

fromResultWithCursor :: ResultWithCursor r c -> r
fromResultWithCursor = (\(ResultWithCursor res cur) -> res)

reverse2 :: String -> String
reverse2 (x:y:xs) = reverse2 xs ++ [x, y]
reverse2 x = x

maxBoundOutput :: (T.Text, Int32)
maxBoundOutput = (T.pack "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", maxBound)

encodeResp :: ToJSON a => Bool -> a -> C.ByteString
encodeResp True = AP.encodePretty
encodeResp False = A.encode
