{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.P2P.BlockSync
    ( createSocket
    , setupPeerConnection
    , handleIncomingMessages
    , runEgressStream
    ) where

import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short
import Data.Function ((&))
import Data.Functor.Identity
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Serialize
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
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
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Types
import Network.Xoken.P2P.Common
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Random

data MyException
    = BlocksNotChainedException
    | MessageParsingException
    | KeyValueDBInsertException
    | BlockHashNotFoundInDB
    | StaleBlockHeader
    deriving (Show)

instance Exception MyException

createSocket :: AddrInfo -> IO (Maybe Socket)
createSocket = createSocketWithOptions []

createSocketWithOptions :: [SocketOption] -> AddrInfo -> IO (Maybe Socket)
createSocketWithOptions options addr = do
    sock <- socket AF_INET Stream (addrProtocol addr)
    mapM_ (\option -> when (isSupportedSocketOption option) (setSocketOption sock option 1)) options
    res <- try $ connect sock (addrAddress addr)
    case res of
        Right () -> return $ Just sock
        Left (e :: IOException) -> do
            print ("TCP socket connect fail: " ++ show (addrAddress addr))
            return Nothing

sendEncMessage :: MVar Bool -> Socket -> BSL.ByteString -> IO ()
sendEncMessage writeLock sock msg = do
    a <- takeMVar writeLock
    (LB.sendAll sock msg) `catch` (\(e :: IOException) -> putStrLn ("caught: " ++ show e))
    putMVar writeLock a

setupPeerConnection :: (HasService env m) => m ()
setupPeerConnection = do
    bp2pEnv <- asks getBitcoinP2PEnv
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
        seeds = getSeeds net
        hints = defaultHints {addrSocketType = Stream}
        port = getDefaultPort net
    liftIO $ print (show seeds)
    let sd = map (\x -> Just (x :: HostName)) seeds
    addrs <- liftIO $ mapConcurrently (\x -> head <$> getAddrInfo (Just hints) (x) (Just (show port))) sd
    res <-
        liftIO $
        mapConcurrently
            (\y -> do
                 sock <- createSocket y
                 case sock of
                     Just sx -> do
                         fl <- doVersionHandshake net sx $ addrAddress y
                         mv <- newMVar True
                         let bp = BitcoinPeer (addrAddress y) sock mv fl Nothing 99999 Nothing
                         atomically $ modifyTVar (bitcoinPeers bp2pEnv) (M.insert (addrAddress y) bp)
                     Nothing -> do
                         mv <- newMVar True
                         let bp = BitcoinPeer (addrAddress y) sock mv False Nothing 99999 Nothing
                         atomically $ modifyTVar (bitcoinPeers bp2pEnv) (M.insert (addrAddress y) bp))
            addrs
    return ()

-- Helper Functions
recvAll :: Socket -> Int -> IO B.ByteString
recvAll sock len = do
    msg <- SB.recv sock len
    if B.length msg == len
        then return msg
        else B.append msg <$> recvAll sock (len - B.length msg)

readNextMessage :: Network -> Socket -> IO (Maybe Message)
readNextMessage net sock = do
    res <- liftIO $ try $ (SB.recv sock 24)
    case res of
        Right hdr -> do
            case (decode hdr) of
                Left e -> do
                    liftIO $ print ("Error decoding incoming message header: " ++ e)
                    return Nothing
                Right (MessageHeader _ _ len _) -> do
                    byts <-
                        if len == 0
                            then return hdr
                            else do
                                rs <- liftIO $ try $ (recvAll sock (fromIntegral len))
                                case rs of
                                    Left (e :: IOException) -> do
                                        liftIO $ print ("Error, reading: " ++ show e)
                                        throw e
                                    Right y -> do
                                        liftIO $ print (B.length y)
                                        return $ hdr `B.append` y
                    case runGet (getMessage net) $ byts of
                        Left e -> do
                            liftIO $ print ("Error, unexpected message header: " ++ e)
                            return Nothing
                        Right msg -> return $ Just msg
        Left (e :: IOException) -> do
            liftIO $ print ("socket read fail")
            return Nothing

doVersionHandshake :: Network -> Socket -> SockAddr -> IO (Bool)
doVersionHandshake net sock sa = do
    g <- liftIO $ getStdGen
    now <- round <$> liftIO getPOSIXTime
    myaddr <- head <$> getAddrInfo (Just defaultHints {addrSocketType = Stream}) (Just "192.168.0.106") (Just "3000")
    let nonce = fst (random g :: (Word64, StdGen))
        ad = NetworkAddress 0 $ addrAddress myaddr -- (SockAddrInet 0 0)
        bb = 1 :: Word32 -- ### TODO: getBestBlock ###
        rmt = NetworkAddress 0 sa
        ver = buildVersion net nonce bb ad rmt now
        em = runPut . putMessage net $ (MVersion ver)
    mv <- (newMVar True)
    sendEncMessage mv sock (BSL.fromStrict em)
    hs1 <- readNextMessage net sock
    case hs1 of
        Just (MVersion __) -> do
            hs2 <- readNextMessage net sock
            case hs2 of
                Just MVerAck -> do
                    let em2 = runPut . putMessage net $ (MVerAck)
                    sendEncMessage mv sock (BSL.fromStrict em2)
                    print ("Version handshake complete: " ++ show sa)
                    return True
                __ -> do
                    print "Error, unexpected message (2) during handshake"
                    return False
        __ -> do
            print "Error, unexpected message (1) during handshake"
            return False

produceGetHeadersMessage :: (HasService env m) => m (Message)
produceGetHeadersMessage = do
    liftIO $ print ("producing - called.")
    bp2pEnv <- asks getBitcoinP2PEnv
    dbe' <- asks getDBEnv
    liftIO $ takeMVar (bestBlockUpdated bp2pEnv) -- be blocked until a new best-block is updated in DB.
    let conn = keyValDB $ dbHandles dbe'
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    bb <- liftIO $ fetchBestBlock conn net
    let gh =
            GetHeaders
                { getHeadersVersion = myVersion
                , getHeadersBL = [fst bb]
                , getHeadersHashStop = "0000000000000000000000000000000000000000000000000000000000000000"
                }
    liftIO $ print ("producing a best-block: " ++ show bb)
    return (MGetHeaders gh)

sendRequestMessages :: (HasService env m) => Message -> m ()
sendRequestMessages msg = do
    liftIO $ print ("sendRequestMessages - called.")
    bp2pEnv <- asks getBitcoinP2PEnv
    dbe' <- asks getDBEnv
    let conn = keyValDB $ dbHandles dbe'
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    let conpr = L.filter (\x -> bpConnected (snd x)) (M.toList allpr)
    let pr = snd $ conpr !! 0
    case (bpSocket pr) of
        Just q -> do
            let em = runPut . putMessage net $ msg
            liftIO $ sendEncMessage (bpSockLock pr) q (BSL.fromStrict em)
            liftIO $ print ("sending out get data: " ++ show (bpAddress pr))
        Nothing -> liftIO $ print ("Error sending, no connections available")
    return ()

msgOrder :: Message -> Message -> Ordering
msgOrder m1 m2 = do
    if msgType m1 == MCGetHeaders
        then LT
        else GT

runEgressStream :: (HasService env m) => m ()
runEgressStream = do
    runStream $ (S.repeatM produceGetHeadersMessage) & (S.mapM sendRequestMessages)
        -- S.mergeBy msgOrder (S.repeatM produceGetHeadersMessage) (S.repeatM produceGetHeadersMessage) &
        --    S.mapM   sendRequestMessages

validateChainedBlockHeaders :: Headers -> Bool
validateChainedBlockHeaders hdrs = do
    let xs = headersList hdrs
        pairs = zip xs (drop 1 xs)
        res = map (\x -> (headerHash $ fst (fst x)) == (prevBlock $ fst (snd x))) pairs
    if all (== True) res
        then True
        else False

markBestBlock :: [(Int32, BlockHeaderCount)] -> Q.ClientState -> IO ()
markBestBlock indexed conn = do
    let str = "insert INTO xoken.blocks (blockhash, header, height, transactions) values (?, ? , ? , ?)"
        qstr = str :: Q.QueryString Q.W (Text, Text, Int32, Text) ()
        par =
            Q.defQueryParams
                Q.One
                ("best", blockHashToHex $ headerHash $ fst $ snd $ last indexed, (fst $ last indexed) :: Int32, "")
    res <- try $ Q.runClient conn (Q.write (Q.prepared qstr) par)
    case res of
        Right () -> return ()
        Left (e :: SomeException) ->
            print ("Error: Marking [Best] blockhash failed: " ++ show e) >> throw KeyValueDBInsertException
    return ()

fetchBestBlock :: Q.ClientState -> Network -> IO ((BlockHash, Int32))
fetchBestBlock conn net = do
    let str = "SELECT header, height from xoken.blocks where blockhash = ?" -- clever hack to use header for best-block hash
        qstr = str :: Q.QueryString Q.R (Identity Text) ((T.Text, Int32))
        p = Q.defQueryParams Q.One $ Identity "best"
    op <- Q.runClient conn (Q.query qstr p)
    if L.length op == 0
        then return ((headerHash $ getGenesisHeader net), 0)
        else do
            print ("Best-block from DB: " ++ show ((op !! 0)))
            case (hexToBlockHash $ fst (op !! 0)) of
                Just x -> return (x, snd (op !! 0))
                Nothing -> do
                    print ("block hash seems invalid, startin over from genesis") -- not optimal, but unforseen case.
                    return ((headerHash $ getGenesisHeader net), 0)

processHeaders :: (HasService env m) => Headers -> m ()
processHeaders hdrs = do
    dbe' <- asks getDBEnv
    bp2pEnv <- asks getBitcoinP2PEnv
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
        genesisHash = blockHashToHex $ headerHash $ getGenesisHeader net
        conn = keyValDB $ dbHandles dbe'
        str = "SELECT blockhash from xoken.blocks where blockhash = ?"
        qstr = str :: Q.QueryString Q.R (Identity Text) (Identity T.Text)
        headPrevHash = blockHashToHex $ prevBlock $ fst $ head $ headersList hdrs
        lastPrevHash = blockHashToHex $ prevBlock $ fst $ last $ headersList hdrs
        p = Q.defQueryParams Q.One $ Identity headPrevHash
    if headPrevHash /= genesisHash
        then do
            op <- Q.runClient conn (Q.query qstr p)
            if L.length op == 0
                then throw BlockHashNotFoundInDB
                else liftIO $ putStrLn $ "fetched from DB -  " ++ show (runIdentity (op !! 0))
        else liftIO $ putStrLn $ "No issue, very first Headers set"
    --
    let p = Q.defQueryParams Q.One $ Identity lastPrevHash
    op <- Q.runClient conn (Q.query qstr p)
    liftIO $ print (op)
    if L.length op == 0
        then liftIO $ print "LastPrevHash not in DB, processing Headers set.."
        else liftIO $ print "LastPrevHash found in DB, skipping Headers set!" >>= throw StaleBlockHeader
    --
    case validateChainedBlockHeaders hdrs of
        True -> do
            liftIO $ print ("Block headers validated..")
            bb <- liftIO $ fetchBestBlock conn net
            let indexed = zip [((snd bb) + 1) ..] (headersList hdrs)
                str = "insert INTO xoken.blocks (blockhash, header, height, transactions) values (?, ? , ? , ?)"
                qstr = str :: Q.QueryString Q.W (Text, Text, Int32, Text) ()
            w <-
                mapM
                    (\y -> do
                         let par =
                                 Q.defQueryParams
                                     Q.One
                                     ( (blockHashToHex $ headerHash $ fst $ snd y)
                                     , (T.pack $ LC.unpack $ A.encode $ fst $ snd y)
                                     , (fst y)
                                     , (""))
                         res <- liftIO $ try $ Q.runClient conn (Q.write (Q.prepared qstr) par)
                         case res of
                             Right () -> return ()
                             Left (e :: SomeException) ->
                                 liftIO $
                                 print ("Error: Block headers [INSERT] Failed: " ++ show e) >>
                                 throw KeyValueDBInsertException)
                    indexed
            liftIO $ markBestBlock indexed conn
            liftIO $ putMVar (bestBlockUpdated bp2pEnv) True
            return ()
        False -> liftIO $ print ("Error: _BlocksNotChainedException_") >> throw BlocksNotChainedException
    return ()

messageHandler :: (HasService env m) => (Maybe Message) -> m (Maybe Message)
messageHandler mm = do
    case mm of
        Just msg -> do
            liftIO $ print (msgType msg)
            case (msg) of
                MHeaders hdrs -> do
                    processHeaders hdrs
                    return mm
                MInv inv -> do
                    return mm
                MTx tx -> do
                    return mm
                _ -> do
                    return mm
        Nothing -> do
            liftIO $ print "Error, invalid message"
            return Nothing

readNextMessage' :: (HasService env m) => Network -> Socket -> m (Maybe Message)
readNextMessage' net sock = liftIO $ readNextMessage net sock

logMessage :: (HasService env m) => Message -> m ()
logMessage mg = do
    liftIO $ print (mg)
    return ()

handleIncomingMessages :: (HasService env m) => m ()
handleIncomingMessages = do
    bp2pEnv <- asks getBitcoinP2PEnv
    let net = bncNet $ bitcoinNodeConfig bp2pEnv
    allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    let conpr = L.filter (\x -> bpConnected (snd x)) (M.toList allpr)
    let pr = snd $ conpr !! 0
    case (bpSocket pr) of
        Just s -> do
            liftIO $ print ("reading from:  " ++ show (bpAddress pr))
            runStream $ S.repeatM (readNextMessage' net s) & S.mapMaybeM (messageHandler) & S.mapM (logMessage)
                -- & S.mapM undefined
            undefined
        Nothing -> undefined
    return ()
