{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.P2P.Common where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.Async.Lifted as LA (async, wait)
import Control.Concurrent.MSem as MS
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Lens
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
import qualified Data.HashTable as CHT
import Data.IORef
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
import Numeric.Lens (base)
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
    | TxIDNotFoundException (String, Maybe Int) String
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
    deriving (Show)

instance Exception AriviServiceException

data XCqlException
    = XCqlPackException String
    | XCqlUnpackException String
    | XCqlInvalidRowsResultException String
    | XCqlInvalidVoidResultException String
    | XCqlQueryErrorException String
    | XCqlNotReadyException
    | XCqlHeaderException String
    | XCqlInvalidHeaderException
    | XCqlEmptyHashtableException
    deriving (Show)

instance Exception XCqlException

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

sendEncMessage :: MVar () -> Socket -> BSL.ByteString -> IO ()
sendEncMessage writeLock sock msg = withMVar writeLock (\x -> (LB.sendAll sock msg))

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

-- Helper Functions
recvAll :: (MonadIO m) => Socket -> Int64 -> m BSL.ByteString
recvAll sock len = do
    if len > 0
        then do
            res <- liftIO $ try $ LB.recv sock len
            case res of
                Left (e :: IOException) -> throw SocketReadException
                Right mesg ->
                    if BSL.length mesg == len
                        then return mesg
                        else if BSL.length mesg == 0
                                 then throw ZeroLengthSocketReadException
                                 else BSL.append mesg <$> recvAll sock (len - BSL.length mesg)
        else return (BSL.empty)

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
       XCqlClientState
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
calculateChainWork :: (HasLogger m, MonadIO m) => [Int32] -> XCqlClientState -> m Integer
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

query :: (Tuple a, Tuple b) => XCqlClientState -> Request k a b -> IO [b]
query ps req = do
    res <- execQuery ps req
    case res of
        Left e -> do
            throw $ XCqlUnpackException $ show e
        Right (Q.RsError _ _ e) -> do
            throw $ XCqlQueryErrorException $ show e
        Right (Q.RsReady _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsReady"
        Right (Q.RsAuthenticate _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsAuthenticate"
        Right (Q.RsAuthChallenge _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsAuthChallenge"
        Right (Q.RsAuthSuccess _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsAuthSuccess"
        Right (Q.RsSupported _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsSupported"
        Right (Q.RsEvent _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsEvent"
        Right (Q.RsResult _ _ result) ->
            case result of
                (Q.RowsResult _ r) -> return r
                Q.VoidResult -> throw $ XCqlInvalidRowsResultException "Got VoidResult"
                (Q.SetKeyspaceResult k) -> throw $ XCqlInvalidRowsResultException "Got SetKeySpaceResult"
                (Q.PreparedResult _ _ _) -> throw $ XCqlInvalidRowsResultException "Got PreparedResult"
                (Q.SchemaChangeResult _) -> throw $ XCqlInvalidRowsResultException "Got SchemaChangeResult"

queryPrepared :: (Tuple a, Tuple b) => XCqlClientState -> Request k a b -> IO (QueryId k a b)
queryPrepared ps req = do
    res <- execQuery ps req
    case res of
        Left e -> do
            throw $ XCqlUnpackException $ show e
        Right (Q.RsError _ _ e) -> do
            throw $ XCqlQueryErrorException $ show e
        Right (Q.RsReady _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsReady" -- TODO change the exception
        Right (Q.RsAuthenticate _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsAuthenticate"
        Right (Q.RsAuthChallenge _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsAuthChallenge"
        Right (Q.RsAuthSuccess _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsAuthSuccess"
        Right (Q.RsSupported _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsSupported"
        Right (Q.RsEvent _ _ _) -> throw $ XCqlInvalidRowsResultException "Got RsEvent"
        Right (Q.RsResult _ _ result) ->
            case result of
                (Q.RowsResult _ r) -> throw $ XCqlInvalidRowsResultException "Got Rows Result"
                Q.VoidResult -> throw $ XCqlInvalidRowsResultException "Got VoidResult"
                (Q.SetKeyspaceResult k) -> throw $ XCqlInvalidRowsResultException "Got SetKeySpaceResult"
                (Q.PreparedResult q _ _) -> return q
                (Q.SchemaChangeResult _) -> throw $ XCqlInvalidRowsResultException "Got SchemaChangeResult"

write :: (Tuple a, Tuple b) => XCqlClientState -> Request k a b -> IO ()
write ps req = do
    res <- execQuery ps req
    case res of
        Left e -> do
            throw $ XCqlUnpackException $ show e
        Right (Q.RsError _ _ e) -> do
            throw $ XCqlQueryErrorException $ show e
        Right (Q.RsReady _ _ _) -> throw $ XCqlInvalidVoidResultException "Got RsReady"
        Right (Q.RsAuthenticate _ _ _) -> throw $ XCqlInvalidVoidResultException "Got RsAuthenticate"
        Right (Q.RsAuthChallenge _ _ _) -> throw $ XCqlInvalidVoidResultException "Got RsAuthChallenge"
        Right (Q.RsAuthSuccess _ _ _) -> throw $ XCqlInvalidVoidResultException "Got RsAuthSuccess"
        Right (Q.RsSupported _ _ _) -> throw $ XCqlInvalidVoidResultException "Got RsSupported"
        Right (Q.RsEvent _ _ _) -> throw $ XCqlInvalidVoidResultException "Got RsEvent"
        Right (Q.RsResult _ _ result) ->
            case result of
                Q.VoidResult -> return ()
                (Q.RowsResult _ r) -> throw $ XCqlInvalidVoidResultException "Got RowsResult"
                (Q.SetKeyspaceResult k) -> throw $ XCqlInvalidVoidResultException "Got SetKeySpaceResult"
                (Q.PreparedResult _ _ _) -> throw $ XCqlInvalidVoidResultException "Got PreparedResult"
                (Q.SchemaChangeResult _) -> throw $ XCqlInvalidVoidResultException "Got SchemaChangeResult"

getConnectionStreamID :: XCqlClientState -> IO (XCQLConnection, Int16)
getConnectionStreamID xcs = do
    rnd <- randomIO
    let vcs = xcs !! (rnd `mod` (L.length xcs - 1))
    let lock = xCqlWriteLock vcs
    sockm <- readIORef $ xCqlSocket vcs
    case sockm of
        Just s -> do
            i <-
                modifyMVar
                    lock
                    (\a -> do
                         if a == maxBound
                             then return (1, 1)
                             else return (a + 1, a + 1))
            return $ (vcs, i)
        Nothing -> do
            print "Hit a dud XCQL connection!"
            liftIO $ threadDelay (100000) -- 100ms sleep
            getConnectionStreamID xcs -- hopefully we find another good one

execQuery :: (Tuple a, Tuple b) => XCqlClientState -> Request k a b -> IO (Either String (Response k a b))
execQuery xcs req = do
    mv <- newEmptyMVar
    (conn, i) <- getConnectionStreamID xcs
    let ht = xCqlHashTable conn
    let lock = xCqlWriteLock conn
    sockm <- readIORef $ xCqlSocket conn
    let sock = fromJust sockm
    let sid = mkStreamId i
    case (Q.pack Q.V3 noCompression False sid req) of
        Right reqp -> do
            TSH.insert ht i mv
            withMVar lock (\_ -> LB.sendAll sock reqp)
        Left e -> do
            throw $ XCqlPackException $ show e
    (XCqlResponse h x) <- takeMVar mv
    TSH.delete ht i
    return $ Q.unpack noCompression h x

readResponse :: XCQLConnection -> IO ()
readResponse (XCQLConnection ht lock sk) =
    forever $ do
        skref <- readIORef sk
        let sock = fromJust skref
        b <- recvAll sock 9
        h' <- return $ header Q.V3 b
        -- print $ "readResponse " ++ show b
        case h' of
            Left s -> do
                writeIORef sk Nothing
                throw $ XCqlHeaderException (show s)
            Right h -> do
                case headerType h of
                    RqHeader -> do
                        writeIORef sk Nothing
                        throw XCqlInvalidHeaderException
                    RsHeader -> do
                        let len = lengthRepr (bodyLength h)
                            sid = fromIntegral $ fromStreamId $ streamId h
                        x <- recvAll sock (fromIntegral len)
                        mmv <- TSH.lookup ht sid
                        case mmv of
                            Just mv -> do
                                res <- tryPutMVar mv (XCqlResponse h x)
                                case res of
                                    True -> return ()
                                    False -> print "[Error] tryPutMVar False"
                            Nothing -> do
                                return ()

connHandshake :: (Tuple a, Tuple b) => Socket -> Request k a b -> IO (Response k a b)
connHandshake sock req = do
    let i = mkStreamId 0
    case (Q.pack Q.V3 noCompression False i req) of
        Right qp -> do
            LB.sendAll sock qp
            b <- recvAll sock 9
            h' <- return $ header Q.V3 b
            case h' of
                Left s -> do
                    throw $ XCqlHeaderException (show s)
                Right h -> do
                    case headerType h of
                        RqHeader -> do
                            throw XCqlInvalidHeaderException
                        RsHeader -> do
                            case Q.unpack noCompression h "" of
                                Right r@(RsReady _ _ Ready) -> return r
                                Left e -> do
                                    throw $ XCqlUnpackException (show e)
                                Right (RsError _ _ e) -> do
                                    throw $ XCqlQueryErrorException (show e)
                                Right _ -> do
                                    throw XCqlNotReadyException
        Left e -> do
            throw $ XCqlPackException (show e)

chunksOf :: Int -> BSL.ByteString -> [BSL.ByteString]
chunksOf k = go
  where
    go t =
        case LC.splitAt (toEnum k) t of
            (a, b)
                | BSL.null a -> []
                | otherwise -> a : go b

isSegmented :: BSL.ByteString -> Bool
isSegmented bs = (BSL.take 32 bs) == (LC.replicate 32 'f')

getSegmentCount :: BSL.ByteString -> Int
getSegmentCount = read . T.unpack . DTE.decodeUtf8 . BSL.toStrict . BSL.drop 32

getCompleteTx :: XCqlClientState -> T.Text -> Int -> IO (BSL.ByteString)
getCompleteTx conn hash segments =
    getTx conn ("SELECT tx_serialized from xoken.transactions where tx_id = ?") hash segments

getCompleteUnConfTx :: XCqlClientState -> T.Text -> Int -> IO (BSL.ByteString)
getCompleteUnConfTx conn hash segments =
    getTx conn ("SELECT tx_serialized from xoken.ep_transactions where tx_id = ?") hash segments

getTx :: XCqlClientState -> Q.QueryString Q.R (Identity Text) (Identity Blob) -> T.Text -> Int -> IO (BSL.ByteString)
getTx conn qstr hash segments = do
    queryI <- queryPrepared conn (Q.RqPrepare $ Q.Prepare qstr)
    foldM
        (\acc s -> do
             let p = getSimpleQueryParam $ Identity $ hash <> (T.pack $ show s)
             resp <- query conn (Q.RqExecute $ Q.Execute queryI p)
             return $
                 if L.length resp == 0 -- TODO: this shouldn't be occurring
                     then acc
                     else let sz = runIdentity $ resp !! 0
                           in acc <> fromBlob sz)
        BSL.empty
        [1 .. segments]

getProps :: B.ByteString -> [(Text, Text)]
getProps = reverse . go mempty 5 -- name, 4 properties
  where
    go acc 0 _ = acc
    go acc n b = do
        let (len, r) = B.splitAt 2 b
        let lenIntM = (T.unpack . DTE.decodeUtf8 $ len) ^? (base 16)
        if (not $ B.null r) && isJust lenIntM && lenIntM > Just 0 && lenIntM <= Just 252
            then go (( "prop" <> (T.pack $ show (6 - n)) -- starts at prop1
                     , (DTE.decodeUtf8 $ B.take (2 * (fromJust lenIntM)) r)) :
                     acc)
                     (n - 1)
                     (B.drop (2 * (fromJust lenIntM)) r)
            else acc

headMaybe :: [a] -> Maybe a
headMaybe [] = Nothing
headMaybe (x:xs) = Just x

indexMaybe :: [a] -> Int -> Maybe a
indexMaybe xs n
    | n < 0 = Nothing
    | n >= L.length xs = Nothing
    | otherwise = Just $ xs !! n

runInBatch :: (a -> IO b) -> [a] -> Int -> IO [b]
runInBatch fn inps batch = do
    go (take batch inps) (drop batch inps)
  where
    go cur next = do
        res <- mapConcurrently fn cur
        if Prelude.null next
            then return res
            else ((++) res) <$> go (take batch next) (drop batch next)
