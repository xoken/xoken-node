{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}

import Arivi.Crypto.Utils.PublicKey.Signature as ACUPS
import Arivi.Crypto.Utils.PublicKey.Utils
import Arivi.Env
import Arivi.Network
import Arivi.P2P
import qualified Arivi.P2P.Config as Config
import Arivi.P2P.P2PEnv as PE hiding (option)
import Arivi.P2P.PubSub.Types
import Arivi.P2P.RPC.Types
import Arivi.P2P.ServiceRegistry
import Conduit
import Control.Arrow
import Control.Concurrent (threadDelay)
import Control.Exception ()
import Control.Monad
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Logger
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.Trans.Control
import Control.Monad.Trans.Maybe
import Data.Aeson.Encoding (encodingToLazyByteString, fromEncoding)
import Data.Bits
import Data.ByteString.Builder
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as C
import Data.Char
import Data.Function
import Data.Int
import Data.List
import Data.Map.Strict as M
import Data.Maybe
import Data.Serialize as Serialize
import Data.String.Conv
import Data.String.Conversions
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import Data.Typeable
import Data.Version
import Data.Word (Word32)
import Database.RocksDB as R
import NQE
import Network.HTTP.Types
import Network.Simple.TCP
import Network.Xoken.Node.AriviService
import Options.Applicative
import Paths_xoken_node as P
import StmContainers.Map as H
import System.Directory (doesPathExist)
import System.Environment (getArgs)
import System.Exit
import System.FilePath
import System.IO.Unsafe
import Text.Read (readMaybe)
import UnliftIO
import UnliftIO.Directory
import Web.Scotty.Trans
import Xoken
import Xoken.Node
import Xoken.P2P

newtype AppM a =
    AppM (ReaderT (ServiceEnv AppM ServiceResource ServiceTopic String String) (LoggingT IO) a)
    deriving ( Functor
             , Applicative
             , Monad
             , MonadReader (ServiceEnv AppM ServiceResource ServiceTopic String String)
             , MonadIO
             , MonadThrow
             , MonadCatch
             , MonadLogger
             )

deriving instance MonadBase IO AppM

deriving instance MonadBaseControl IO AppM

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

runAppM :: ServiceEnv AppM ServiceResource ServiceTopic String String -> AppM a -> LoggingT IO a
runAppM env (AppM app) = runReaderT app env

defaultConfig :: FilePath -> IO ()
defaultConfig path = do
    (sk, _) <- ACUPS.generateKeyPair
    let config =
            Config.Config
                5678
                5678
                sk
                []
                (generateNodeId sk)
                "127.0.0.1"
                (T.pack (path <> "/node.log"))
                20
                5
                3
                9090
                9091
    Config.makeConfig config (path <> "/config.yaml")

runNode :: Config.Config -> IO ()
runNode config = do
    p2pEnv <- mkP2PEnv config globalHandlerRpc globalHandlerPubSub [AriviService] []
    que <- atomically $ newTChan
    mmap <- newTVarIO $ M.empty
    let serviceEnv = ServiceEnv p2pEnv
    runFileLoggingT (toS $ Config.logFile config) $ runAppM serviceEnv $ initP2P config
    liftIO $ threadDelay 5999999999
    return ()

--
--
--
data Config =
    Config
        { configDir :: !FilePath
        , configPort :: !Int
        , configNetwork :: !Network
        , configDiscover :: !Bool
        , configPeers :: ![(Host, Maybe Port)]
        , configVersion :: !Bool
        , configCache :: !FilePath
        , configDebug :: !Bool
        , configReqLog :: !Bool
        , configMaxLimits :: !MaxLimits
        }

defPort :: Int
defPort = 3000

defNetwork :: Network
defNetwork = btc

netNames :: String
netNames = intercalate "|" (Data.List.map getNetworkName allNets)

defMaxLimits :: MaxLimits
defMaxLimits =
    MaxLimits
        {maxLimitCount = 10000, maxLimitFull = 500, maxLimitOffset = 50000, maxLimitDefault = 100, maxLimitGap = 20}

config :: Parser Config
config = do
    configDir <-
        option str $
        metavar "DIR" <> long "dir" <> short 'd' <> help "Data directory" <> showDefault <> value "xoken-node"
    configPort <-
        option auto $
        metavar "INT" <> long "listen" <> short 'l' <> help "Listening port" <> showDefault <> value defPort
    configNetwork <-
        option (eitherReader networkReader) $
        metavar netNames <> long "net" <> short 'n' <> help "Network to connect to" <> showDefault <> value defNetwork
    configDiscover <- switch $ long "auto" <> short 'a' <> help "Peer discovery"
    configPeers <-
        many . option (eitherReader peerReader) $
        metavar "HOST" <> long "peer" <> short 'p' <> help "Network peer (as many as required)"
    configCache <- option str $ long "cache" <> short 'c' <> help "RAM drive directory for acceleration" <> value ""
    configVersion <- switch $ long "version" <> short 'v' <> help "Show version"
    configDebug <- switch $ long "debug" <> help "Show debug messages"
    configReqLog <- switch $ long "reqlog" <> help "HTTP request logging"
    maxLimitCount <-
        option auto $
        metavar "INT" <> long "maxlimit" <> help "Max limit for listings (0 for no limit)" <> showDefault <>
        value (maxLimitCount defMaxLimits)
    maxLimitFull <-
        option auto $
        metavar "INT" <> long "maxfull" <> help "Max limit for full listings (0 for no limit)" <> showDefault <>
        value (maxLimitFull defMaxLimits)
    maxLimitOffset <-
        option auto $
        metavar "INT" <> long "maxoffset" <> help "Max offset (0 for no limit)" <> showDefault <>
        value (maxLimitOffset defMaxLimits)
    maxLimitDefault <-
        option auto $
        metavar "INT" <> long "deflimit" <> help "Default limit (0 for max)" <> showDefault <>
        value (maxLimitDefault defMaxLimits)
    maxLimitGap <-
        option auto $
        metavar "INT" <> long "gap" <> help "Extended public key gap" <> showDefault <> value (maxLimitGap defMaxLimits)
    pure Config {configMaxLimits = MaxLimits {..}, ..}

networkReader :: String -> Either String Network
networkReader s
    | s == getNetworkName bsv = Right bsv
    | s == getNetworkName btc = Right btc
    | s == getNetworkName btcTest = Right btcTest
    | s == getNetworkName btcRegTest = Right btcRegTest
    | s == getNetworkName bch = Right bch
    | s == getNetworkName bchTest = Right bchTest
    | s == getNetworkName bchRegTest = Right bchRegTest
    | otherwise = Left "Network name invalid"

peerReader :: String -> Either String (Host, Maybe Port)
peerReader s = do
    let (host, p) = span (/= ':') s
    when (Data.List.null host) (Left "Peer name or address not defined")
    port <-
        case p of
            [] -> return Nothing
            ':':p' ->
                case readMaybe p' of
                    Nothing -> Left "Peer port number cannot be read"
                    Just n -> return (Just n)
            _ -> Left "Peer information could not be parsed"
    return (host, port)

main :: IO ()
main = do
    putStrLn $ "Starting Xoken node"
    let path = "."
    b <- System.Directory.doesPathExist (path <> "/config.yaml")
    unless b (defaultConfig path)
    cnf <- Config.readConfig (path <> "/config.yaml")
    runNode cnf
    --
    conf <- liftIO (execParser opts)
    when (configVersion conf) . liftIO $ do
        putStrLn $ showVersion P.version
        exitSuccess
    when (Data.List.null (configPeers conf) && not (configDiscover conf)) . liftIO $
        die "ERROR: Specify peers to connect or enable peer discovery."
    run conf
  where
    opts =
        info (helper <*> config) $
        fullDesc <> progDesc "Blockchain store and API" <>
        Options.Applicative.header ("xoken-node version " <> showVersion P.version)

cacheDir :: Network -> FilePath -> Maybe FilePath
cacheDir net "" = Nothing
cacheDir net ch = Just (ch </> getNetworkName net </> "cache")

run :: MonadUnliftIO m => Config -> m ()
run Config { configPort = port
           , configNetwork = net
           , configDiscover = disc
           , configPeers = peers
           , configCache = cache_path
           , configDir = db_dir
           , configDebug = deb
           , configMaxLimits = limits
           , configReqLog = reqlog
           } =
    runStderrLoggingT . filterLogger l . flip UnliftIO.finally clear $ do
        $(logInfoS) "Main" $ "Creating working directory if not found: " <> cs wd
        createDirectoryIfMissing True wd
        db <-
            do dbh <-
                   open
                       (wd </> "db")
                       R.defaultOptions
                           { createIfMissing = True
                           , compression = SnappyCompression
                           , maxOpenFiles = -1
                           , writeBufferSize = 2 `shift` 30
                           }
               return BlockDB {blockDB = dbh, blockDBopts = defaultReadOptions}
        cdb <-
            case cd of
                Nothing -> return Nothing
                Just ch -> do
                    $(logInfoS) "Main" $ "Deleting cache directory: " <> cs ch
                    removePathForcibly ch
                    $(logInfoS) "Main" $ "Creating cache directory: " <> cs ch
                    createDirectoryIfMissing True ch
                    dbh <- open ch R.defaultOptions {createIfMissing = True}
                    return $ Just BlockDB {blockDB = dbh, blockDBopts = defaultReadOptions}
        $(logInfoS) "Main" "Populating cache (if active)..."
        ldb <- newLayeredDB db cdb
        $(logInfoS) "Main" "Finished populating cache"
        ipcSvcHandler <- liftIO $ newIPCServiceHandler
        async $ loopRPC ldb (rpcQueue ipcSvcHandler) net
        withPublisher $ \pub ->
            let scfg =
                    StoreConfig
                        { storeConfMaxPeers = 20
                        , storeConfInitPeers = Data.List.map (second (fromMaybe (getDefaultPort net))) peers
                        , storeConfDiscover = disc
                        , storeConfDB = ldb
                        , storeConfNetwork = net
                        , storeConfListen = (`sendSTM` pub) . Event
                        }
             in withStore scfg $ \str ->
                    let wcfg =
                            WebConfig
                                { webPort = port
                                , webNetwork = net
                                , webDB = ldb
                                , webPublisher = pub
                                , webStore = str
                                , webMaxLimits = limits
                                , webReqLog = reqlog
                                }
                     in setupIPCServer wcfg ipcSvcHandler
  where
    l _ lvl
        | deb = True
        | otherwise = LevelInfo <= lvl
    clear =
        case cd of
            Nothing -> return ()
            Just ch -> do
                $(logInfoS) "Main" $ "Deleting cache directory: " <> cs ch
                removePathForcibly ch
    wd = db_dir </> getNetworkName net
    cd =
        case cache_path of
            "" -> Nothing
            ch -> Just (ch </> getNetworkName net </> "cache")
