{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

import Arivi.Crypto.Utils.PublicKey.Signature as ACUPS
import Arivi.Crypto.Utils.PublicKey.Utils
import Arivi.Crypto.Utils.Random
import Arivi.Env
import Arivi.Network
import Arivi.P2P
import qualified Arivi.P2P.Config as Config
import Arivi.P2P.Kademlia.Types
import Arivi.P2P.P2PEnv as PE hiding (option)
import Arivi.P2P.PubSub.Types
import Arivi.P2P.RPC.Types
import Arivi.P2P.ServiceRegistry
import Control.Arrow
import Control.Concurrent
import Control.Concurrent (threadDelay)
import qualified Control.Concurrent.Async as A (async, uninterruptibleCancel)
import Control.Concurrent.Async.Lifted as LA (async, race, wait, waitAnyCancel, withAsync)
import Control.Concurrent.Event as EV
import Control.Concurrent.MSem as MS
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TQueue as TB
import Control.Concurrent.STM.TVar
import Control.Exception (throw)
import Control.Monad
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Except
import Control.Monad.Logger
import Control.Monad.Loops
import Control.Monad.Reader
import qualified Control.Monad.STM as CMS (atomically)
import Control.Monad.Trans.Control
import Control.Monad.Trans.Maybe
import Data.Aeson.Encoding (encodingToLazyByteString, fromEncoding)
import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import Data.ByteString.Base64 as B64
import Data.ByteString.Builder
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as CL
import Data.Char
import Data.Default
import Data.Function
import Data.Functor.Identity
import qualified Data.HashTable as CHT
import qualified Data.HashTable.IO as H
import Data.IORef
import Data.Int
import Data.List
import Data.Map.Strict as M
import Data.Maybe
import Data.Pool
import Data.Serialize as S
import Data.Serialize as Serialize
import Data.String.Conv
import Data.String.Conversions
import qualified Data.Text as DT
import qualified Data.Text as T
import qualified Data.Text.Encoding as DTE
import qualified Data.Text.Lazy as TL
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Typeable
import Data.Version
import Data.Word
import Data.Word (Word32)
import qualified Database.Bolt as BT
import qualified Database.XCQL.Protocol as Q
import Network.Simple.TCP
import Network.Socket
import Network.Xoken.Node.AriviService
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.HTTP.Server
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.ChainSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.PeerManager
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.UnconfTxSync
import Network.Xoken.Node.TLSServer
import Options.Applicative
import Paths_xoken_node as P
import qualified Snap as Snap
import StmContainers.Map as SM
import System.Directory (doesDirectoryExist, doesFileExist)
import System.Environment (getArgs)
import System.Exit
import System.FilePath
import System.IO.Unsafe
import qualified System.Logger as LG
import qualified System.Logger.Class as LGC
import System.Posix.Daemon
import System.Random
import Text.Read (readMaybe)
import Xoken
import Xoken.Node
import Xoken.NodeConfig as NC
import Prelude as P

newtype AppM a
    = AppM (ReaderT (ServiceEnv) (IO) a)
    deriving (Functor, Applicative, Monad, MonadReader (ServiceEnv), MonadIO, MonadThrow, MonadCatch)

deriving instance MonadBase IO AppM

deriving instance MonadBaseControl IO AppM

instance HasBitcoinP2P AppM where
    getBitcoinP2P = asks (bitcoinP2PEnv . xokenNodeEnv)

instance HasDatabaseHandles AppM where
    getDB = asks (dbHandles . xokenNodeEnv)

instance HasAllegoryEnv AppM where
    getAllegory = asks (allegoryEnv . xokenNodeEnv)

instance HasLogger AppM where
    getLogger = asks (loggerEnv . xokenNodeEnv)

runAppM :: ServiceEnv -> AppM a -> IO a
runAppM env (AppM app) = runReaderT app env

data ConfigException
    = ConfigParseException
    | RandomSecretKeyException
    deriving (Eq, Ord, Show)

instance Exception ConfigException

type HashTable k v = H.BasicHashTable k v

defaultConfig :: IO ()
defaultConfig = do
    (sk, _) <- ACUPS.generateKeyPair
    let bootstrapPeer =
            Peer
                ( (fst . B16.decode)
                    "a07b8847dc19d77f8ef966ba5a954dac2270779fb028b77829f8ba551fd2f7ab0c73441456b402792c731d8d39c116cb1b4eb3a18a98f4b099a5f9bdffee965c"
                )
                (NodeEndPoint "51.89.40.95" 5678 5678)
    let config =
            Config.Config 5678 5678 sk [bootstrapPeer] (generateNodeId sk) "127.0.0.1" (T.pack "./arivi.log") 20 5 3
    Config.makeConfig config "./arivi-config.yaml"

makeGraphDBResPool :: T.Text -> T.Text -> IO (ServerState)
makeGraphDBResPool uname pwd = do
    let gdbConfig = def{BT.user = uname, BT.password = pwd}
    gdbState <- constructState gdbConfig
    a <- withResource' (pool gdbState) (`BT.run` queryGraphDBVersion)
    putStrLn $ "Connected to Neo4j database, version " ++ show (a !! 0)
    return gdbState

initXCql :: NC.NodeConfig -> IO (XCqlClientState)
initXCql nodeConf = do
    let hints = defaultHints{addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream}
        startCql :: Q.Request k () ()
        startCql = Q.RqStartup $ Q.Startup Q.Cqlv300 (Q.algorithm Q.noCompression) --(Q.CqlVersion "3.4.4") Q.None
    mapM
        ( \cn -> do
            (addr : _) <- getAddrInfo (Just hints) (Just "127.0.0.1") (Just "9042")
            s <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
            Network.Socket.connect s (addrAddress addr)
            connHandshake s startCql
            t <- TSH.new 1
            l <- newMVar (1 :: Int16)
            sk <- newIORef $ Just s
            let xcqlc = XCQLConnection t l sk
            A.async (readResponse xcqlc)
            return xcqlc
        )
        [1 .. (maxConnectionsXCql nodeConf)]

runThreads :: Config.Config -> NC.NodeConfig -> BitcoinP2P -> XCqlClientState -> LG.Logger -> [FilePath] -> IO ()
runThreads config nodeConf bp2p conn lg certPaths = do
    gdbState <- makeGraphDBResPool (neo4jUsername nodeConf) (neo4jPassword nodeConf)
    let dbh = DatabaseHandles gdbState conn
    let allegoryEnv = AllegoryEnv $ allegoryVendorSecretKey nodeConf
    let xknEnv = XokenNodeEnv bp2p dbh lg allegoryEnv
    let serviceEnv = ServiceEnv xknEnv
    epHandler <- newTLSEndpointServiceHandler
    -- start TLS endpoint
    async $ startTLSEndpoint epHandler (endPointTLSListenIP nodeConf) (endPointTLSListenPort nodeConf) certPaths
    -- start HTTP endpoint
    let snapConfig =
            Snap.defaultConfig & Snap.setSSLBind (DTE.encodeUtf8 $ DT.pack $ endPointHTTPSListenIP nodeConf)
                & Snap.setSSLPort (fromEnum $ endPointHTTPSListenPort nodeConf)
                & Snap.setSSLKey (certPaths !! 1)
                & Snap.setSSLCert (head certPaths)
                & Snap.setSSLChainCert False
    async $ Snap.serveSnaplet snapConfig (appInit xknEnv)
    catch (withResource' (pool $ graphDB dbh) (`BT.run` initAllegoryRoot genesisTx)) $ \(e :: SomeException) ->
        if "ConstraintValidationFailed" `isInfixOf` (show e)
            then putStrLn "Allegory root previously initialised"
            else do
                putStrLn $ "[ERROR] initAllegoryRoot: " <> show e
                throw e
    -- run main workers
    runAppM
        serviceEnv
        ( do
            bp2pEnv <- getBitcoinP2P
            withAsync setupSeedPeerConnection $ \a -> do
                withAsync runEgressChainSync $ \b -> do
                    withAsync runBlockSync $ \c -> do
                        withAsync commitBlockCacheQueue $ \d -> do
                            withAsync (handleNewConnectionRequest epHandler) $ \e -> do
                                withAsync runPeerSync $ \f -> do
                                    withAsync runSyncStatusChecker $ \g -> do
                                        withAsync runWatchDog $ \h -> do
                                            withAsync runTMTDaemon $ \i -> do
                                                _ <- LA.waitAnyCancel [a, b, c, d, e, f, g, h, i]
                                                return ()
        )
    liftIO $ destroyAllResources $ pool gdbState
    liftIO $ putStrLn $ "node recovering from fatal DB connection failure!"
    return ()

runSyncStatusChecker :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runSyncStatusChecker = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    conn <- xCqlClientState <$> getDB
    liftIO $ threadDelay (300 * 1000000)
    forever $ do
        isSynced <- checkBlocksFullySynced conn
        liftIO $ CMS.atomically $ writeTVar (indexUnconfirmedTx bp2pEnv) isSynced
        liftIO $ threadDelay (60 * 1000000)

runWatchDog :: (HasXokenNodeEnv env m, MonadIO m) => m ()
runWatchDog = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = xCqlClientState (dbe)
    LG.debug lg $ LG.msg $ LG.val "Starting watchdog"
    continue <- liftIO $ newIORef True
    whileM_ (liftIO $ readIORef continue) $ do
        liftIO $ threadDelay (120 * 1000000)
        fa <- liftIO $ readTVarIO (txProcFailAttempts bp2pEnv)
        if fa > 5
            then do
                LG.err lg $ LG.msg $ LG.val "Error: exceeded Tx proc fail attempt threshold, watchdog raise alert "
                liftIO $ writeIORef continue False
            else do
                tm <- liftIO $ getCurrentTime
                let ttime = (floor $ utcTimeToPOSIXSeconds tm) :: Int64
                    str = "insert INTO xoken.transactions ( tx_id, block_info, tx_serialized ) values (?, ?, ?)"
                    qstr = str :: Q.QueryString Q.W (T.Text, ((T.Text, Int32), Int32), Q.Blob) ()
                    par =
                        getSimpleQueryParam ("watchdog-last-check", ((T.pack $ show ttime, 0), 0), Q.Blob $ CL.pack "")
                ores <-
                    LA.race
                        (liftIO $ threadDelay (3000000)) -- worst case of 3 secs
                        (liftIO $ try $ write conn (Q.RqQuery $ Q.Query qstr par))
                case ores of
                    Right (eth) -> do
                        case eth of
                            Right _ -> return ()
                            Left (SomeException e) -> do
                                LG.err lg $ LG.msg ("Error: unable to insert, watchdog raise alert " ++ show e)
                                liftIO $ writeIORef continue False
                    Left () -> do
                        LG.err lg $ LG.msg $ LG.val "Error: insert timed-out, watchdog raise alert "
                        liftIO $ writeIORef continue False

runNode :: Config.Config -> NC.NodeConfig -> XCqlClientState -> BitcoinP2P -> [FilePath] -> IO ()
runNode config nodeConf conn bp2p certPaths = do
    lg <-
        LG.new
            ( LG.setOutput
                (LG.Path $ T.unpack $ NC.logFileName nodeConf)
                (LG.setLogLevel (logLevel nodeConf) LG.defSettings)
            )
    runThreads config nodeConf bp2p conn lg certPaths

data Config = Config
    { configNetwork :: !Network
    , configDebug :: !Bool
    , configUnconfirmedTx :: !Bool
    }

defPort :: Int
defPort = 3000

defNetwork :: Network
defNetwork = bsvTest

netNames :: String
netNames = intercalate "|" (Data.List.map getNetworkName allNets)

defaultAdminUser :: XCqlClientState -> IO ()
defaultAdminUser conn = do
    let qstr =
            " SELECT password from xoken.user_permission where username = 'admin' " :: Q.QueryString Q.R () (Identity T.Text)
        p = getSimpleQueryParam ()
    op <- query conn (Q.RqQuery $ Q.Query qstr p)
    if length op == 1
        then return ()
        else do
            tm <- liftIO $ getCurrentTime
            usr <-
                addNewUser
                    conn
                    "admin"
                    "default"
                    "user"
                    ""
                    (Just ["admin"])
                    (Just 100000000)
                    (Just (addUTCTime (nominalDay * 365) tm))
            putStrLn $ "******************************************************************* "
            putStrLn $ "  Creating default Admin user!"
            putStrLn $ "  Please note down admin password NOW, will not be shown again."
            putStrLn $ "  Password : " ++ (aurPassword $ fromJust usr)
            putStrLn $ "******************************************************************* "

defBitcoinP2P :: NodeConfig -> Int64 -> IO (BitcoinP2P)
defBitcoinP2P nodeCnf ept = do
    bitcoinPeers <- newTVarIO M.empty
    blacklistedPeers <- newTVarIO M.empty
    bestBlockUpdated <- newMVar True
    headersWriteLock <- newMVar True
    blockSyncStatusMap <- TSH.new 2
    blockTxProcessingLeftMap <- TSH.new 4
    epochType <- newTVarIO False
    epochTimestamp <- newTVarIO ept
    unconfirmedTxCache <- TSH.new 16
    rpf <- newEmptyMVar
    rpc <- newTVarIO 0
    let peerReset = (rpf, rpc)
    merkleQueueMap <- TSH.new 4
    txSynchronizer <- TSH.new 32
    maxTMTBuilderThreadLock <- MS.new $ maxTMTBuilderThreads nodeCnf
    indexUnconfirmedTx <- newTVarIO False
    userDataCache <- H.new
    txProcFailAttempts <- newTVarIO 0
    bestSyncedBlock <- newTVarIO Nothing
    protocolInfo <- TSH.new 4
    blockCacheLock <- newMVar ()
    peerFetchQueue <- liftIO $ newTQueueIO
    blockPeerMap <- TSH.new 1
    --
    return $
        BitcoinP2P
            nodeCnf
            bitcoinPeers
            blacklistedPeers
            bestBlockUpdated
            headersWriteLock
            blockSyncStatusMap
            blockTxProcessingLeftMap
            epochType
            epochTimestamp
            unconfirmedTxCache
            peerReset
            merkleQueueMap
            txSynchronizer
            maxTMTBuilderThreadLock
            indexUnconfirmedTx
            userDataCache
            txProcFailAttempts
            bestSyncedBlock
            protocolInfo
            blockCacheLock
            peerFetchQueue
            blockPeerMap

--

initNexa :: IO ()
initNexa = do
    putStrLn $ "Starting Xoken Nexa"
    b <- doesFileExist "arivi-config.yaml"
    unless b defaultConfig
    cnf <- Config.readConfig "arivi-config.yaml"
    nodeCnf <- NC.readConfig "node-config.yaml"
    !conn <- initXCql nodeCnf
    defaultAdminUser conn
    tm <- getCurrentTime
    bp2p <- defBitcoinP2P nodeCnf (floor $ utcTimeToPOSIXSeconds tm)
    let certFP = tlsCertificatePath nodeCnf
        keyFP = tlsKeyfilePath nodeCnf
        csrFP = tlsCertificateStorePath nodeCnf
    cfp <- doesFileExist certFP
    kfp <- doesFileExist keyFP
    csfp <- doesDirectoryExist csrFP
    unless (cfp && kfp && csfp) $ P.error "Error: missing TLS certificate or keyfile"
    -- launch node --
    runNode cnf nodeCnf conn bp2p [certFP, keyFP, csrFP]

relaunch :: IO ()
relaunch =
    forever $ do
        let pid = "/tmp/nexa.pid.1"
        running <- isRunning pid
        if running
            then threadDelay (30 * 1000000)
            else do
                runDetached (Just pid) (ToFile "nexa.log") initNexa
                threadDelay (5000000)

main :: IO ()
main = do
    let pid = "/tmp/nexa.pid.0"
    runDetached (Just pid) (ToFile "nexa.log") relaunch
