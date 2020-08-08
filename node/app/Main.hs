{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE BlockArguments #-}

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
import Control.Concurrent (threadDelay)
import Control.Concurrent
import Control.Concurrent.Async.Lifted as LA (async, race, wait, withAsync)
import Control.Concurrent.Event as EV
import Control.Concurrent.MSem as MS
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TBQueue as TB
import Control.Concurrent.STM.TVar
import Control.Exception (throw)
import Control.Monad
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
import Data.Default
import Data.Function
import Data.Functor.Identity
import qualified Data.HashTable.IO as H
import Data.IORef
import Data.Int
import Data.List
import Data.Map.Strict as M
import Data.Maybe
import Data.Maybe
import Data.Pool
import Data.Serialize as Serialize
import Data.Serialize as S
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
import Data.Word (Word32)
import Data.Word
import qualified Database.Bolt as BT
import qualified Database.CQL.IO as Q
import Database.CQL.Protocol
import Network.Simple.TCP
import Network.Socket
import Network.Xoken.Node.AriviService
import Network.Xoken.Node.Data
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
import Prelude as P
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

newtype AppM a =
    AppM (ReaderT (ServiceEnv AppM ServiceResource ServiceTopic RPCMessage PubNotifyMessage) (LoggingT IO) a)
    deriving ( Functor
             , Applicative
             , Monad
             , MonadReader (ServiceEnv AppM ServiceResource ServiceTopic RPCMessage PubNotifyMessage)
             , MonadIO
             , MonadThrow
             , MonadCatch
             , MonadLogger
             )

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

instance HasNetworkEnv AppM where
    getEnv = asks (ariviNetworkEnv . nodeEndpointEnv . p2pEnv)

instance HasSecretKey AppM

instance HasKbucket AppM where
    getKb = asks (kbucket . kademliaEnv . p2pEnv)

instance HasStatsdClient AppM where
    getStatsdClient = asks (statsdClient . p2pEnv)

instance HasNodeEndpoint AppM where
    getEndpointEnv = asks (nodeEndpointEnv . p2pEnv)
    getNetworkConfig = asks (PE._networkConfig . nodeEndpointEnv . p2pEnv)
    getHandlers = asks (handlers . nodeEndpointEnv . p2pEnv)
    getNodeIdPeerMapTVarP2PEnv = asks (tvarNodeIdPeerMap . nodeEndpointEnv . p2pEnv)

instance HasPRT AppM where
    getPeerReputationHistoryTableTVar = asks (tvPeerReputationHashTable . prtEnv . p2pEnv)
    getServicesReputationHashMapTVar = asks (tvServicesReputationHashMap . prtEnv . p2pEnv)
    getP2PReputationHashMapTVar = asks (tvP2PReputationHashMap . prtEnv . p2pEnv)
    getReputedVsOtherTVar = asks (tvReputedVsOther . prtEnv . p2pEnv)
    getKClosestVsRandomTVar = asks (tvKClosestVsRandom . prtEnv . p2pEnv)

runAppM :: ServiceEnv AppM ServiceResource ServiceTopic RPCMessage PubNotifyMessage -> AppM a -> LoggingT IO a
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
                ((fst . B16.decode)
                     "a07b8847dc19d77f8ef966ba5a954dac2270779fb028b77829f8ba551fd2f7ab0c73441456b402792c731d8d39c116cb1b4eb3a18a98f4b099a5f9bdffee965c")
                (NodeEndPoint "51.89.40.95" 5678 5678)
    let config =
            Config.Config 5678 5678 sk [bootstrapPeer] (generateNodeId sk) "127.0.0.1" (T.pack "./arivi.log") 20 5 3
    Config.makeConfig config "./arivi-config.yaml"

makeGraphDBResPool :: T.Text -> T.Text -> IO (ServerState)
makeGraphDBResPool uname pwd = do
    let gdbConfig = def {BT.user = uname, BT.password = pwd}
    gdbState <- constructState gdbConfig
    a <- withResource (pool gdbState) (`BT.run` queryGraphDBVersion)
    putStrLn $ "Connected to Neo4j database, version " ++ show (a !! 0)
    return gdbState

runThreads ::
       Config.Config
    -> NC.NodeConfig
    -> BitcoinP2P
    -> Q.ClientState
    -> LG.Logger
    -> (P2PEnv AppM ServiceResource ServiceTopic RPCMessage PubNotifyMessage)
    -> [FilePath]
    -> IO ()
runThreads config nodeConf bp2p conn lg p2pEnv certPaths = do
    gdbState <- makeGraphDBResPool (neo4jUsername nodeConf) (neo4jPassword nodeConf)
    let dbh = DatabaseHandles conn gdbState
    let allegoryEnv = AllegoryEnv $ allegoryVendorSecretKey nodeConf
    let xknEnv = XokenNodeEnv bp2p dbh lg allegoryEnv
    let serviceEnv = ServiceEnv xknEnv p2pEnv
    epHandler <- newTLSEndpointServiceHandler
    -- start TLS endpoint
    async $ startTLSEndpoint epHandler (endPointTLSListenIP nodeConf) (endPointTLSListenPort nodeConf) certPaths
    -- start HTTP endpoint
    let snapConfig =
            Snap.defaultConfig & Snap.setSSLBind (DTE.encodeUtf8 $ DT.pack $ endPointHTTPSListenIP nodeConf) &
            Snap.setSSLPort (fromEnum $ endPointHTTPSListenPort nodeConf) &
            Snap.setSSLKey (certPaths !! 1) &
            Snap.setSSLCert (head certPaths) &
            Snap.setSSLChainCert False
    async $ Snap.serveSnaplet snapConfig (appInit xknEnv)
    withResource (pool $ graphDB dbh) (`BT.run` initAllegoryRoot genesisTx)
    -- run main workers
    runFileLoggingT (toS $ Config.logFile config) $
        runAppM
            serviceEnv
            (do initP2P config
                bp2pEnv <- getBitcoinP2P
                withAsync runEpochSwitcher $ \_ -> do
                    withAsync setupSeedPeerConnection $ \_ -> do
                        withAsync runEgressChainSync $ \_ -> do
                            withAsync runBlockCacheQueue $ \_ -> do
                                withAsync (handleNewConnectionRequest epHandler) $ \_ -> do
                                    withAsync runPeerSync $ \_ -> do
                                        withAsync runSyncStatusChecker $ \_ -> do
                                            withAsync runWatchDog $ \z -> do
                                                _ <- LA.wait z
                                                return ())
    liftIO $ Q.shutdown conn
    liftIO $ destroyAllResources $ pool gdbState
    liftIO $ putStrLn $ "node recovering from fatal DB connection failure!"
    return ()

runSyncStatusChecker :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runSyncStatusChecker = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    conn <- keyValDB <$> getDB
    -- wait 300 seconds before first check
    liftIO $ threadDelay (300 * 1000000)
    forever $ do
        isSynced <- checkBlocksFullySynced conn
        LG.debug lg $
            LG.msg $
            LG.val $
            C.pack $
            "Sync Status Checker: All blocks synced? " ++
            if isSynced
                then "Yes"
                else "No"
        liftIO $ CMS.atomically $ writeTVar (indexUnconfirmedTx bp2pEnv) isSynced
        liftIO $ threadDelay (60 * 1000000)

runWatchDog :: (HasXokenNodeEnv env m, MonadIO m) => m ()
runWatchDog = do
    dbe <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let conn = keyValDB (dbe)
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
                    qstr = str :: Q.QueryString Q.W (T.Text, ((T.Text, Int32), Int32), Blob) ()
                    par =
                        Q.defQueryParams Q.One ("watchdog-last-check", ((T.pack $ show ttime, 0), 0), Blob $ CL.pack "")
                ores <-
                    LA.race
                        (liftIO $ threadDelay (3000000)) -- worst case of 3 secs
                        (liftIO $ try $ Q.runClient conn (Q.write (Q.prepared qstr) par))
                case ores of
                    Right (eth) -> do
                        case eth of
                            Right () -> return ()
                            Left (SomeException e) -> do
                                LG.err lg $ LG.msg ("Error: unable to insert, watchdog raise alert " ++ show e)
                                liftIO $ writeIORef continue False
                    Left () -> do
                        LG.err lg $ LG.msg $ LG.val "Error: insert timed-out, watchdog raise alert "
                        liftIO $ writeIORef continue False

runNode :: Config.Config -> NC.NodeConfig -> Q.ClientState -> BitcoinP2P -> [FilePath] -> IO ()
runNode config nodeConf conn bp2p certPaths = do
    p2pEnv <- mkP2PEnv config globalHandlerRpc globalHandlerPubSub [AriviService] []
    lg <-
        LG.new
            (LG.setOutput
                 (LG.Path $ T.unpack $ NC.logFileName nodeConf)
                 (LG.setLogLevel (logLevel nodeConf) LG.defSettings))
    runThreads config nodeConf bp2p conn lg p2pEnv certPaths

data Config =
    Config
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

defaultAdminUser :: Q.ClientState -> IO ()
defaultAdminUser conn = do
    let qstr =
            " SELECT password from xoken.user_permission where username = 'admin' " :: Q.QueryString Q.R () (Identity T.Text)
        p = Q.defQueryParams Q.One ()
    op <- Q.runClient conn (Q.query qstr p)
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

makeKeyValDBConn :: IO (Q.ClientState)
makeKeyValDBConn = do
    let logg = Q.stdoutLogger Q.LogWarn
        stng = Q.setMaxStreams 2048 $ Q.setMaxConnections 256 $ Q.setPoolStripes 12 $ Q.setLogger logg Q.defSettings
        stng2 = Q.setRetrySettings Q.eagerRetrySettings stng
        qstr = "SELECT cql_version from system.local" :: Q.QueryString Q.R () (Identity T.Text)
        p = Q.defQueryParams Q.One ()
    conn <- Q.init stng2
    op <- Q.runClient conn (Q.query qstr p)
    putStrLn $ "Connected to Cassandra database, version " ++ show (runIdentity (op !! 0))
    return conn

defBitcoinP2P :: NodeConfig -> IO (BitcoinP2P)
defBitcoinP2P nodeCnf = do
    g <- newTVarIO M.empty
    bp <- newTVarIO M.empty
    mv <- newMVar True
    hl <- newMVar True
    st <- SM.newIO -- newMVar M.empty
    tl <- SM.newIO -- newMVar M.empty
    ep <- newTVarIO False
    tc <- H.new
    vc <- H.new
    rpf <- newEmptyMVar
    rpc <- newTVarIO 0
    mq <- SM.newIO -- newTVarIO M.empty
    ts <- SM.newIO -- newMVar M.empty
    tbt <- MS.new $ maxTMTBuilderThreads nodeCnf
    iut <- newTVarIO False
    udc <- H.new
    tpfa <- newTVarIO 0
    bfq <- liftIO $ newTBQueueIO 100
    return $ BitcoinP2P nodeCnf g bp mv hl st tl ep tc vc (rpf, rpc) mq ts tbt iut udc tpfa bfq

initNexa :: IO ()
initNexa = do
    putStrLn $ "Starting Xoken Nexa"
    conn <- makeKeyValDBConn
    defaultAdminUser conn
    b <- doesFileExist "arivi-config.yaml"
    unless b defaultConfig
    cnf <- Config.readConfig "arivi-config.yaml"
    nodeCnf <- NC.readConfig "node-config.yaml"
    bp2p <- defBitcoinP2P nodeCnf
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
    initNexa
    -- let pid = "/tmp/nexa.pid.0"
    -- runDetached (Just pid) (ToFile "nexa.log") relaunch
