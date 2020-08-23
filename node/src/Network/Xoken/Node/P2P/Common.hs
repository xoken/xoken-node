{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

module Network.Xoken.Node.P2P.Common where

import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.Async.Lifted as LA (async, wait)
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
import Data.Bits
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
import Data.Pool
import Data.Serialize
import Data.Serialize as S
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as DTE
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import Database.XCQL.Protocol as Q hiding (Version)
import Network.Socket
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Address.Base58 as B58
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common -- (GetData(..), MessageCommand(..), NetworkAddress(..))
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
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
    | UnexpectedDuringBlockProcException String
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
    | BlockAlreadySyncedException
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
    | UnresponsivePeerException
    deriving (Show)

instance Exception PeerMessageException

data AriviServiceException
    = KeyValueDBLookupException
    | GraphDBLookupException
    | InvalidOutputAddressException
    | KeyValPoolException
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

maskAfter :: Int -> String -> String
maskAfter n skey = (\x -> take n x ++ fmap (const '*') (drop n x)) skey

addNewUser ::
       CqlConnection
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
        p = getSimpleQueryParam (Identity uname)
    op <- query conn (Q.RqQuery $ Q.Query qstr p)
    tm <- liftIO $ getCurrentTime
    if L.length op == 1
        then return Nothing
        else do
            g <- liftIO $ newStdGen
            let seed = show $ fst (random g :: (Word64, StdGen))
                passwd = B.init $ B.init $ B64.encode $ C.pack $ seed
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
                    getSimpleQueryParam
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
            res1 <- liftIO $ try $ write conn (Q.RqQuery $ Q.Query (qstr) par)
            case res1 of
                Right _ -> do
                    putStrLn $ "Added user: " ++ (T.unpack uname)
                    return $
                        Just $
                        AddUserResp
                            (User
                                 (T.unpack uname)
                                 ""
                                 (T.unpack fname)
                                 (T.unpack lname)
                                 (T.unpack email)
                                 (fromMaybe ["read"] roles)
                                 (fromIntegral $ fromMaybe 10000 api_quota)
                                 0
                                 (fromMaybe (addUTCTime (nominalDay * 365) tm) api_expiry_time)
                                 (maskAfter 10 $ T.unpack tempSessionKey)
                                 (addUTCTime (nominalDay * 30) tm))
                            (C.unpack passwd)
                Left (SomeException e) -> do
                    putStrLn $ "Error: INSERTing into 'user_permission': " ++ show e
                    throw e

-- | Calculates sum of chainworks for blocks with blockheight in input list
calculateChainWork :: (HasLogger m, MonadIO m) => [Int32] -> CqlConnection -> m Integer
calculateChainWork blks conn = do
    lg <- getLogger
    let qstr :: Q.QueryString Q.R (Identity [Int32]) (Int32, Text)
        qstr = "SELECT block_height,block_header from xoken.blocks_by_height where block_height in ?"
        p = getSimpleQueryParam $ Identity $ blks
    res <- liftIO $ try $ query conn (Q.RqQuery $ Q.Query qstr p)
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
            err lg $ LG.msg $ "Error: calculateChainWork: " ++ show e ++ "\n" ++ show blks
            throw KeyValueDBLookupException

stripScriptHash :: ((Text, Int32), Int32, (Text, Text, Int64)) -> ((Text, Int32), Int32, (Text, Int64))
stripScriptHash (op, ii, (addr, scriptHash, satValue)) = (op, ii, (addr, satValue))

fromBytes :: B.ByteString -> Integer
fromBytes = B.foldl' f 0
  where
    f a b = a `shiftL` 8 .|. fromIntegral b

splitList :: [a] -> ([a], [a])
splitList xs = (f 1 xs, f 0 xs)
  where
    f n a = map fst . filter (odd . snd) . zip a $ [n ..]

getSimpleQueryParam :: Tuple a => a -> QueryParams a
getSimpleQueryParam a = Q.QueryParams Q.One False a Nothing Nothing Nothing Nothing

query :: (Tuple a, Tuple b) => CqlConnection -> Request Q.R a b -> IO [b]
query ps req = do
    withResource ps $ \(ht,sock) -> do
        -- getstreamid
        let i = 0
            sid = mkStreamId i
        case (Q.pack Q.V3 noCompression False sid req) of
            Right reqp -> do
                queryResp (ht,sock) reqp i
                resp' <- TSH.lookup ht i -- logic issue
                case resp' of
                    Just resp'' -> do
                        (h,x) <- takeMVar resp''
                        case Q.unpack noCompression h x of
                            Left e -> do
                                print $ "[Error] Query: unpack " ++ show e
                                throw KeyValPoolException
                            Right (RsError _ _ e) -> do
                                print $ "[Error] Query: RsError: " ++ show e
                                throw KeyValPoolException
                            Right response -> case response of
                                                (Q.RsResult _ _ (Q.RowsResult _ r)) -> return r
                                                response -> do
                                                    print $ "[Error] Query: Not a RowsResult!!"
                                                    throw KeyValPoolException
                    Nothing -> do
                        print $ "[Error] Query: Nothing in hashtable"
                        throw KeyValPoolException
            Left _ -> do
                print "[Error] Query: pack"
                throw KeyValPoolException

write :: (Q.Cql a, Tuple a) => CqlConnection -> Request Q.W a () -> IO ()
write ps req = do
    withResource ps $ \(ht,sock) -> do
        -- getstreamid
        let i = 0
            sid = mkStreamId i
        case (Q.pack Q.V3 noCompression False sid req) of
            Right reqp -> do
                queryResp (ht,sock) reqp i
                resp' <- TSH.lookup ht i -- logic issue
                case resp' of
                    Just resp'' -> do
                        (h,x) <- takeMVar resp''
                        case Q.unpack noCompression h x of
                            Left e -> do
                                print $ "[Error] Query: unpack " ++ show e
                                throw KeyValPoolException
                            Right (RsError _ _ e) -> do
                                print $ "[Error] Query: RsError: " ++ show e
                                throw KeyValPoolException
                            Right response -> case response of
                                                (Q.RsResult _ _ (Q.VoidResult)) -> return ()
                                                response -> do
                                                    print $ "[Error] Query: Not a VoidResult!!"
                                                    throw KeyValPoolException
                    Nothing -> do
                        print $ "[Error] Query: Nothing in hashtable"
                        throw KeyValPoolException
            Left _ -> do
                print "[Error] Query: pack"
                throw KeyValPoolException
            

queryResp :: CqlConn -> LC.ByteString -> Int -> IO ()
queryResp (ht,sock) req sid = do
    LB.sendAll sock req
    b <- LB.recv sock 9
    h' <- return $ header Q.V3 b
    case h' of
        Left s -> do
            print $ "[Error] Query: header error: " ++ s
            throw KeyValPoolException
        Right h -> do
            case headerType h of
                RqHeader -> do
                    print "[Error] Query: RqHeader"
                    throw KeyValPoolException
                RsHeader -> do
                    let len = lengthRepr (bodyLength h)
                    x <- LB.recv sock (fromIntegral len)
                    mv <- newMVar (h,x)
                    TSH.insert ht sid mv


connHandshake :: (Tuple a, Tuple b) => Socket -> Request k a b -> IO (Response k a b)
connHandshake sock req = do
    let i = mkStreamId 0
    case (Q.pack Q.V3 noCompression False i req) of
        Right qp -> do
            LB.sendAll sock qp
            b <- LB.recv sock 9
            h' <- return $ header Q.V3 b
            case h' of
                Left s -> do
                    print $ "[Error] Query: header error: " ++ s
                    throw KeyValPoolException
                Right h -> do
                    case headerType h of
                        RqHeader -> do
                            print "[Error] Query: RqHeader"
                            throw KeyValPoolException
                        RsHeader -> do
                            case Q.unpack noCompression h "" of
                                Right r@(RsReady _ _ Ready) -> return r
                                Left e -> do
                                    print "[Error] Query: unpack"
                                    throw KeyValPoolException
                                Right (RsError _ _ e) -> do
                                    print $ "[Error] Query: RsError: " ++ show e
                                    throw KeyValPoolException
                                Right _ -> do
                                    print $ "[Error] Query: Response is not ready"
                                    throw KeyValPoolException
        Left _ -> do
            print "[Error] Query: pack"
            throw KeyValPoolException
