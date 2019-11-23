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
import Control.Arrow
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted (async)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception ()
import Control.Monad
import Control.Monad.Base
import Control.Monad.Catch
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
import Data.Functor.Identity
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
import qualified Database.CQL.IO as Q
import Network.Simple.TCP
import Network.Socket
import Network.Xoken.Node.AriviService
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.Types
import Options.Applicative
import Paths_xoken_node as P
import StmContainers.Map as H
import System.Directory (doesPathExist)
import System.Environment (getArgs)
import System.Exit
import System.FilePath
import System.IO.Unsafe
import Text.Read (readMaybe)
import Xoken
import Xoken.Node

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

runNode :: Config.Config -> DatabaseHandles -> BitcoinP2PEnv -> IO ()
runNode config dbh bp2p = do
    p2pEnv <- mkP2PEnv config globalHandlerRpc globalHandlerPubSub [AriviService] []
    let dbEnv = DBEnv dbh
        serviceEnv = ServiceEnv dbEnv p2pEnv bp2p
    runFileLoggingT (toS $ Config.logFile config) $
        runAppM
            serviceEnv
            (do initP2P config
                async $ setupPeerConnection
                liftIO $ threadDelay (20 * 1000000)
                liftIO $ putStrLn $ "............"
                async $ runEgressStream
                async $ initPeerListeners)
    -- runFileLoggingT (toS $ Config.logFile config) $ runAppM serviceEnv $ setupPeerConnection
    liftIO $ threadDelay 5999999999
    return ()

getLayeredDB :: IO (DBHandles)
getLayeredDB = do
    return (undefined)

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
        }

defPort :: Int
defPort = 3000

defNetwork :: Network
defNetwork = bsvTest

netNames :: String
netNames = intercalate "|" (Data.List.map getNetworkName allNets)

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
    pure Config {..}

networkReader :: String -> Either String Network
networkReader s
    | s == getNetworkName bsv = Right bsv
    | s == getNetworkName bsvTest = Right bsvTest
    | s == getNetworkName bsvSTN = Right bsvSTN
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
    let logg = Q.stdoutLogger Q.LogWarn
        stng = Q.setLogger logg Q.defSettings
        qstr = "SELECT cql_version from system.local" :: Q.QueryString Q.R () (Identity T.Text)
        p = Q.defQueryParams Q.One ()
    conn <- Q.init stng
    op <- Q.runClient conn (Q.query qstr p)
    putStrLn $ "Connected to Cassandra-DB version " ++ show (runIdentity (op !! 0))
    --
    conf <- liftIO (execParser opts)
    when (configVersion conf) . liftIO $ do
        putStrLn $ showVersion P.version
        exitSuccess
    when (Data.List.null (configPeers conf) && not (configDiscover conf)) . liftIO $
        die "ERROR: Specify peers to connect or enable peer discovery."
    --
    let path = "."
    b <- System.Directory.doesPathExist (path <> "/config.yaml")
    unless b (defaultConfig path)
    cnf <- Config.readConfig (path <> "/config.yaml")
    let nodeConfig =
            BitcoinNodeConfig
                5 -- maximum connected peers allowed
                [] -- static list of peers to connect to
                False -- activate peer discovery
                (NetworkAddress 0 (SockAddrInet 0 0)) -- local host n/w addr
                (configNetwork conf) -- network constants
                60 -- timeout in seconds
    g <- newTVarIO M.empty
    mv <- newMVar True
    runNode cnf (DatabaseHandles conn) (BitcoinP2PEnv nodeConfig g mv)
  where
    opts =
        info (helper <*> config) $
        fullDesc <> progDesc "Blockchain store and API" <>
        Options.Applicative.header ("xoken-node version " <> showVersion P.version)
