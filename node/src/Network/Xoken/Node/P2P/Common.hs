{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.P2P.Common where

import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.Async.Lifted as LA (async)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import qualified Data.Aeson as A (decode, eitherDecode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import Data.ByteString.Base64 as B64
import Data.ByteString.Builder
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Serialize
import Data.Serialize as S
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as DTE
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import qualified Database.CQL.IO as Q
import Network.Socket
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common -- (GetData(..), MessageCommand(..), NetworkAddress(..))
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Random
import Text.Format

data BlockSyncException
    = BlocksNotChainedException
    | MessageParsingException
    | KeyValueDBInsertException
    | BlockHashNotFoundException
    | DuplicateBlockHeaderException
    | InvalidMessageTypeException
    | InvalidBlocksException
    | EmptyHeadersMessageException
    | InvalidStreamStateException
    | InvalidBlockIngestStateException
    | InvalidMetaDataException
    | InvalidBlockHashException
    | UnexpectedDuringBlockProcException
    | InvalidBlockSyncStatusMapException
    | InvalidBlockInfoException
    | OutpointAddressNotFoundException
    | InvalidAddressException
    | TxIDNotFoundException
    | InvalidOutpointException
    | DBTxParseException
    | MerkleTreeComputeException
    | InvalidCreateListException
    | MerkleSubTreeAlreadyExistsException
    | MerkleSubTreeDBInsertException
    | ResourcePoolFetchException
    | DBInsertTimeOutException
    | MerkleTreeInvalidException
    | MerkleQueueNotFoundException
    deriving (Show)

instance Exception BlockSyncException

data PeerMessageException
    = SocketReadException
    | SocketConnectException SockAddr
    | DeflatedBlockParseException
    | ConfirmedTxParseException
    | PeerSocketNotConnectedException
    | ZeroLengthSocketReadException
    | NetworkMagicMismatchException
    deriving (Show)

instance Exception PeerMessageException

data AriviServiceException
    = KeyValueDBLookupException
    | GraphDBLookupException
    | InvalidOutputAddressException
    deriving (Show)

instance Exception AriviServiceException

--
--
-- | Create version data structure.
buildVersion :: Network -> Word64 -> BlockHeight -> NetworkAddress -> NetworkAddress -> Word64 -> Version
buildVersion net nonce height loc rmt time =
    Version
        { version = myVersion
        , services = naServices loc
        , timestamp = time
        , addrRecv = rmt
        , addrSend = loc
        , verNonce = nonce
        , userAgent = VarString (getXokenUserAgent net)
        , startHeight = height
        , relay = True
        }

-- | Our protocol version.
myVersion :: Word32
myVersion = 70015

msgOrder :: Message -> Message -> Ordering
msgOrder m1 m2 = do
    if msgType m1 == MCGetHeaders
        then LT
        else GT

sendEncMessage :: MVar Bool -> Socket -> BSL.ByteString -> IO ()
sendEncMessage writeLock sock msg = do
    a <- takeMVar writeLock
    (LB.sendAll sock msg) `catch` (\(e :: IOException) -> putStrLn ("caught: " ++ show e))
    putMVar writeLock a

-- | Computes the height of a Merkle tree.
computeTreeHeight ::
       Int -- ^ number of transactions (leaf nodes)
    -> Int8 -- ^ height of the merkle tree
computeTreeHeight ntx
    | ntx < 2 = 0
    | even ntx = 1 + computeTreeHeight (ntx `div` 2)
    | otherwise = computeTreeHeight $ ntx + 1

getTextVal :: (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text) -> Maybe T.Text
getTextVal (_, _, _, txt) = txt

getBoolVal :: (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text) -> Maybe Bool
getBoolVal (b, _, _, _) = b

getIntVal :: (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text) -> Maybe Int32
getIntVal (_, i, _, _) = i

getBigIntVal :: (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text) -> Maybe Int64
getBigIntVal (_, _, ts, _) = ts

divide :: Int -> Int -> Float
divide x y = (a / b)
  where
    a = fromIntegral x :: Float
    b = fromIntegral y :: Float

toInt :: Float -> Int
toInt x = round x

-- OP_RETURN Allegory/AllPay
frameOpReturn :: C.ByteString -> C.ByteString
frameOpReturn opReturn = do
    let prefix = (fst . B16.decode) "006a0f416c6c65676f72792f416c6c506179"
    let len = B.length opReturn
    let xx =
            if (len <= 0x4b)
                then word8 $ fromIntegral len
                else if (len <= 0xff)
                         then mappend (word8 0x4c) (word8 $ fromIntegral len)
                         else if (len <= 0xffff)
                                  then mappend (word8 0x4d) (word16LE $ fromIntegral len)
                                  else if (len <= 0x7fffffff)
                                           then mappend (word8 0x4e) (word32LE $ fromIntegral len)
                                           else word8 0x99 -- error scenario!!
    let bs = LC.toStrict $ toLazyByteString xx
    C.append (C.append prefix bs) opReturn

generateSessionKey :: IO (Text)
generateSessionKey = do
    g <- liftIO $ newStdGen
    let seed = show $ fst (random g :: (Word64, StdGen))
        sdb = B64.encode $ C.pack $ seed
    return $ encodeHex ((S.encode $ sha256 $ B.reverse sdb))

addNewUser ::
       Q.ClientState
    -> T.Text
    -> T.Text
    -> T.Text
    -> T.Text
    -> Maybe [String]
    -> Maybe Int32
    -> Maybe UTCTime
    -> IO (Maybe AddUserResp)
addNewUser conn uname fname lname email roles api_quota api_expiry_time = do
    let qstr =
            " SELECT password from xoken.user_permission where username = ? " :: Q.QueryString Q.R (Identity T.Text) (Identity T.Text)
        p = Q.defQueryParams Q.One (Identity uname)
    op <- Q.runClient conn (Q.query qstr p)
    tm <- liftIO $ getCurrentTime
    if L.length op == 1
        then return Nothing
        else do
            g <- liftIO $ newStdGen
            let seed = show $ fst (random g :: (Word64, StdGen))
                passwd = B64.encode $ C.pack $ seed
                hashedPasswd = encodeHex ((S.encode $ sha256 passwd))
                tempSessionKey = encodeHex ((S.encode $ sha256 $ B.reverse passwd))
                str =
                    "insert INTO xoken.user_permission ( username, password, first_name, last_name, emailid, created_time, permissions, api_quota, api_used, api_expiry_time, session_key, session_key_expiry_time) values (?, ?, ?, ?, ?, ? ,? ,? ,? ,? ,? ,? )"
                qstr =
                    str :: Q.QueryString Q.W ( T.Text
                                             , T.Text
                                             , T.Text
                                             , T.Text
                                             , T.Text
                                             , UTCTime
                                             , [T.Text]
                                             , Int32
                                             , Int32
                                             , UTCTime
                                             , T.Text
                                             , UTCTime) ()
                par =
                    Q.defQueryParams
                        Q.One
                        ( uname
                        , hashedPasswd
                        , fname
                        , lname
                        , email
                        , tm
                        , (fromMaybe ["read"] ((fmap . fmap) T.pack roles))
                        , (fromMaybe 10000 api_quota)
                        , 0
                        , (fromMaybe (addUTCTime (nominalDay * 365) tm) api_expiry_time)
                        , tempSessionKey
                        , (addUTCTime (nominalDay * 30) tm))
            res1 <- liftIO $ try $ Q.runClient conn (Q.write (qstr) par)
            case res1 of
                Right () -> do
                    putStrLn $ "Added user: " ++ (T.unpack uname)
                    return $
                        Just $
                        AddUserResp
                            (T.unpack uname)
                            (C.unpack passwd)
                            (T.unpack fname)
                            (T.unpack lname)
                            (T.unpack email)
                            (fromMaybe ["read"] roles)
                            (fromIntegral $ fromMaybe 10000 api_quota)
                            (fromMaybe (addUTCTime (nominalDay * 365) tm) api_expiry_time)
                Left (SomeException e) -> do
                    putStrLn $ "Error: INSERTing into 'user_permission': " ++ show e
                    throw e

-- | Calculates sum of chainworks for blocks with blockheight in input list
calculateChainWork :: (HasLogger m, MonadIO m) => [Int32] -> Q.ClientState -> m Integer
calculateChainWork blks conn = do
    lg <- getLogger
    let str = "SELECT block_height,block_header from xoken.blocks_by_height where block_height in ?"
        qstr = str :: Q.QueryString Q.R (Identity [Int32]) (Int32, Text)
        p = Q.defQueryParams Q.One $ Identity $ blks
    res <- liftIO $ try $ Q.runClient conn (Q.query qstr p)
    case res of
        Right iop -> do
            if L.length iop == 0
                then return 0
                else do
                    case traverse
                             (\(ht, hdr) ->
                                  case (A.eitherDecode $ BSL.fromStrict $ DTE.encodeUtf8 hdr) of
                                      (Right bh) -> Right $ bh
                                      Left err -> Left err)
                             (iop) of
                        Right hdrs -> return $ foldr (\x y -> y + (convertBitsToBlockWork $ blockBits $ x)) 0 hdrs
                        Left err -> do
                            liftIO $ print $ "decode failed for blockrecord: " <> show err
                            return (-1)
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: xGetBlockHeights: " ++ show e
            throw KeyValueDBLookupException
