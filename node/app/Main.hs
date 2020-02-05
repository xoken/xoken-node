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
import Arivi.P2P.Kademlia.Types
import Arivi.P2P.P2PEnv as PE hiding (option)
import Arivi.P2P.PubSub.Types
import Arivi.P2P.RPC.Types
import Arivi.P2P.ServiceRegistry
import Control.Arrow
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted (async, wait, withAsync)
import Control.Concurrent.MVar
import Control.Concurrent.QSem
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
import qualified Data.ByteString.Base16 as B16
import Data.ByteString.Builder
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as CL
import Data.Char
import Data.Default
import Data.Function
import Data.Functor.Identity
import qualified Data.HashTable.IO as H
import Data.Int
import Data.List
import Data.Map.Strict as M
import Data.Maybe
import Data.Pool
import Data.Serialize as Serialize
import Data.String.Conv
import Data.String.Conversions
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import Data.Typeable
import Data.Version
import Data.Word (Word32)
import qualified Database.Bolt as BT
import qualified Database.CQL.IO as Q
import Network.Simple.TCP
import Network.Socket
import Network.Xoken.Node.AriviService
import Network.Xoken.Node.Data
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.ChainSync
import Network.Xoken.Node.P2P.PeerManager
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.UnconfTxSync
import Options.Applicative
import Paths_xoken_node as P
import System.Directory (doesPathExist)
import System.Environment (getArgs)
import System.Exit
import System.FilePath
import System.IO.Unsafe
import qualified System.Logger as LG
import qualified System.Logger.Class as LG
import Text.Read (readMaybe)
import Xoken
import Xoken.Node

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

defaultConfig :: FilePath -> IO ()
defaultConfig path = do
    (sk, _) <- ACUPS.generateKeyPair
    let bootstrapPeer =
            Peer
                ((fst . B16.decode)
                     "a07b8847dc19d77f8ef966ba5a954dac2270779fb028b77829f8ba551fd2f7ab0c73441456b402792c731d8d39c116cb1b4eb3a18a98f4b099a5f9bdffee965c")
                (NodeEndPoint "51.89.40.95" 5678 5678)
    let config =
            Config.Config
                5678
                5678
                sk
                [bootstrapPeer]
                (generateNodeId sk)
                "127.0.0.1"
                (T.pack (path <> "/node.log"))
                20
                5
                3
                "127.0.0.1"
                9090
    Config.makeConfig config (path <> "/config.yaml")

makeGraphDBResPool :: IO (ServerState)
makeGraphDBResPool = do
    let gdbConfig = def {BT.user = "neo4j", BT.password = "admin123"}
    gdbState <- constructState gdbConfig
    a <- withResource (pool gdbState) (`BT.run` queryGraphDBVersion)
    putStrLn $ "Connected to Neo4j database, version " ++ show (a !! 0)
    return gdbState

runThreads ::
       Config.Config
    -> BitcoinP2P
    -> Q.ClientState
    -> LG.Logger
    -> (P2PEnv AppM ServiceResource ServiceTopic RPCMessage PubNotifyMessage)
    -> IO ()
runThreads config bp2p conn lg p2pEnv =
    forever $ do
        gdbState <- makeGraphDBResPool
        let dbh = DatabaseHandles conn gdbState
        let xknEnv = XokenNodeEnv bp2p dbh lg
        let serviceEnv = ServiceEnv xknEnv p2pEnv
        runFileLoggingT (toS $ Config.logFile config) $
            runAppM
                serviceEnv
                (do initP2P config
                    liftIO $ print ("begin of loop..")
                    bp2pEnv <- getBitcoinP2P
                    liftIO $ tryTakeMVar (fst $ peerReset bp2pEnv)
                    withAsync runEpochSwitcher $ \a -> do
                        withAsync setupSeedPeerConnection $ \b -> do
                            withAsync runEgressChainSync $ \c -> do
                                withAsync runEgressBlockSync $ \d -> do
                                    withAsync runPeerSync $ \e -> do
                                        resetPeers
                                        liftIO $ print ("exiting..")
                    liftIO $ print ("end of loop..")
                    liftIO $ destroyAllResources (pool gdbState)
                    liftIO $ threadDelay (10 * 1000000))

runNode :: Config.Config -> Q.ClientState -> BitcoinP2P -> Bool -> IO ()
runNode config conn bp2p dbg = do
    p2pEnv <- mkP2PEnv config globalHandlerRpc globalHandlerPubSub [AriviService] []
    let ll =
            if dbg
                then LG.Debug
                else LG.Info
    lg <- LG.new (LG.setOutput (LG.Path "xoken.log") (LG.setLogLevel ll LG.defSettings))
    runThreads config bp2p conn lg p2pEnv

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

config :: Parser Config
config = do
    configNetwork <-
        option (eitherReader networkReader) $
        metavar netNames <> long "net" <> short 'n' <> help "Network to connect to" <> showDefault <> value defNetwork
    configDebug <- switch $ long "debug" <> short 'd' <> help "Show debug messages"
    configUnconfirmedTx <- switch $ long "unconfirmedTx" <> short 'u' <> help "Index unconfirmed txs."
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
    putStrLn $ "Starting Xoken Nexa"
    let logg = Q.stdoutLogger Q.LogWarn
        stng = Q.setMaxStreams 1024 $ Q.setMaxConnections 10 $ Q.setPoolStripes 12 $ Q.setLogger logg Q.defSettings
        stng2 = Q.setRetrySettings Q.eagerRetrySettings stng
        qstr = "SELECT cql_version from system.local" :: Q.QueryString Q.R () (Identity T.Text)
        p = Q.defQueryParams Q.One ()
    conn <- Q.init stng2
    op <- Q.runClient conn (Q.query qstr p)
    putStrLn $ "Connected to Cassandra database, version " ++ show (runIdentity (op !! 0))
    conf <- liftIO $ execParser opts
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
    hl <- newMVar True
    st <- newTVarIO M.empty
    ep <- newTVarIO False
    tc <- H.new
    rpf <- newEmptyMVar
    rpc <- newTVarIO 0
    runNode cnf (conn) (BitcoinP2P nodeConfig g mv hl st ep tc (configUnconfirmedTx conf) (rpf, rpc)) (configDebug conf)
  where
    opts =
        info (helper <*> config) $
        fullDesc <> progDesc "Blockchain store and API" <>
        Options.Applicative.header ("xoken-node version " <> showVersion P.version)
