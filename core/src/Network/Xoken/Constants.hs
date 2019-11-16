{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

{-|
Module      : Network.Xoken.Constants
Copyright   : No rights reserved
License     : UNLICENSE
Maintainer  : xenog@protonmail.com
Stability   : experimental
Portability : POSIX

Network constants for various networks, including Bitcoin SegWit (BTC), Bitcoin
Cash (BCH), and corresponding public test and private regression test networks.
-}
module Network.Xoken.Constants
    ( Network(..)
    , bsv
    , bsvTest
    , bsvSTN
    , allNets
    , netByName
    , netByIdent
    ) where

import Data.ByteString (ByteString)
import Data.List
import Data.Maybe
import Data.Serialize
import Data.String
import Data.Text (Text)
import Data.Version
import Data.Word (Word32, Word64, Word8)
import GHC.Generics (Generic)
import Network.Xoken.Block.Common
import Paths_xoken_core
import Text.Read

-- | Version of Xoken Core package.
versionString :: IsString a => a
versionString = fromString (showVersion version)

-- | Constants for network.
data Network =
    Network
      -- | lowercase alphanumeric and dashes
        { getNetworkName :: !String
      -- | network Haskell identifier
        , getNetworkIdent :: !String
      -- | prefix for 'Base58' P2PKH addresses
        , getAddrPrefix :: !Word8
      -- | prefix for 'Base58' P2SH addresses
        , getScriptPrefix :: !Word8
      -- | prefix for WIF private key
        , getSecretPrefix :: !Word8
      -- | prefix for extended public key
        , getExtPubKeyPrefix :: !Word32
      -- | prefix for extended private key
        , getExtSecretPrefix :: !Word32
      -- | network magic
        , getNetworkMagic :: !Word32
      -- | genesis block header
        , getGenesisHeader :: !BlockHeader
      -- | maximum block size in bytes
        , getMaxBlockSize :: !Int
      -- | maximum amount of satoshi
        , getMaxSatoshi :: !Word64
      -- | user agent string
        , getXokenUserAgent :: !ByteString
      -- | default port for P2P connections
        , getDefaultPort :: !Int
      -- | allow min difficulty blocks (testnet)
        , getAllowMinDifficultyBlocks :: !Bool
      -- | do not retarget difficulty (regtest)
        , getPowNoRetargetting :: !Bool
      -- | proof-of-work target higest possible value
        , getPowLimit :: !Integer
      -- | block at which BIP34 activates
        , getBip34Block :: !(BlockHeight, BlockHash)
      -- | block at which BIP65 activates
        , getBip65Height :: !BlockHeight
      -- | block at which BIP66 activates
        , getBip66Height :: !BlockHeight
      -- | time between difficulty retargets
        , getTargetTimespan :: !Word32
      -- | time between blocks
        , getTargetSpacing :: !Word32
      -- | checkpoints
        , getCheckpoints :: ![(BlockHeight, BlockHash)]
      -- | BIP44 derivation path root
        , getBip44Coin :: !Word32
      -- | peer-to-peer network seeds
        , getSeeds :: ![String]
      -- | fork id for replay protection
        , getSigHashForkId :: !(Maybe Word32)
      -- | EDA start block height
        , getEdaBlockHeight :: !(Maybe Word32)
      -- | DAA start block height
        , getDaaBlockHeight :: !(Maybe Word32)
      -- | segregated witness active
        , getSegWit :: !Bool
      -- | 'CashAddr' prefix (for Bitcoin Cash)
        , getCashAddrPrefix :: !(Maybe Text)
      -- | 'Bech32' prefix (for SegWit network)
        , getBech32Prefix :: !(Maybe Text)
      -- | Replace-By-Fee (BIP-125)
        , getReplaceByFee :: !Bool
      -- | Subsidy halving interval
        , getHalvingInterval :: !Word32
        }
    deriving (Eq, Generic)

instance Serialize Network where
    put net = putWord32be $ getNetworkMagic net
    get = do
        magic <- getWord32be
        case find ((== magic) . getNetworkMagic) allNets of
            Nothing -> fail $ "Network magic unknown: " <> show magic
            Just net -> return net

instance Show Network where
    show = getNetworkIdent

instance Read Network where
    readPrec = do
        Ident str <- lexP
        maybe pfail return (netByIdent str)

instance IsString Network where
    fromString = fromMaybe (error "Network name invalid") . netByName

-- | Query known networks by name.
netByName :: String -> Maybe Network
netByName str = find ((== str) . getNetworkName) allNets

-- | Query known networks by Haskell identifier.
netByIdent :: String -> Maybe Network
netByIdent str = find ((== str) . getNetworkIdent) allNets

-- | Bitcoin SegWit network. Symbol: BTC.
bsv :: Network
bsv =
    Network
        { getNetworkName = "bsv"
        , getNetworkIdent = "bsv"
        , getAddrPrefix = 0
        , getScriptPrefix = 5
        , getSecretPrefix = 128
        , getExtPubKeyPrefix = 0x0488b21e
        , getExtSecretPrefix = 0x0488ade4
        , getNetworkMagic = 0xe3e1f3e8
        , getGenesisHeader =
              BlockHeader
                  0x01
                  "0000000000000000000000000000000000000000000000000000000000000000"
                  "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a"
                  1231006505
                  0x1d00ffff
                  2083236893
            -- Hash 000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f
        , getMaxBlockSize = 20000000000
        , getMaxSatoshi = 2100000000000000
        , getXokenUserAgent = "/xoken-bsv:" <> versionString <> "/"
        , getDefaultPort = 8333
        , getAllowMinDifficultyBlocks = False
        , getPowNoRetargetting = False
        , getPowLimit = 0x00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        , getBip34Block = (227931, "000000000000024b89b42a942fe0d9fea3bb44ab7bd1b19115dd6a759c0808b8")
        , getBip65Height = 388381
        , getBip66Height = 363725
        , getTargetTimespan = 14 * 24 * 60 * 60
        , getTargetSpacing = 10 * 60
        , getCheckpoints =
              [ (11111, "0000000069e244f73d78e8fd29ba2fd2ed618bd6fa2ee92559f542fdb26e7c1d")
              , (33333, "000000002dd5588a74784eaa7ab0507a18ad16a236e7b1ce69f00d7ddfb5d0a6")
              , (74000, "0000000000573993a3c9e41ce34471c079dcf5f52a0e824a81e7f953b8661a20")
              , (105000, "00000000000291ce28027faea320c8d2b054b2e0fe44a773f3eefb151d6bdc97")
              , (134444, "00000000000005b12ffd4cd315cd34ffd4a594f430ac814c91184a0d42d2b0fe")
              , (168000, "000000000000099e61ea72015e79632f216fe6cb33d7899acb35b75c8303b763")
              , (193000, "000000000000059f452a5f7340de6682a977387c17010ff6e6c3bd83ca8b1317")
              , (210000, "000000000000048b95347e83192f69cf0366076336c639f9b7228e9ba171342e")
              , (216116, "00000000000001b4f4b433e81ee46494af945cf96014816a4e2370f11b23df4e")
              , (225430, "00000000000001c108384350f74090433e7fcf79a606b8e797f065b130575932")
              , (250000, "000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214")
              , (279000, "0000000000000001ae8c72a0b0c301f67e3afca10e819efa9041e458e9bd7e40")
              , (295000, "00000000000000004d9b4ef50f0f9d686fd69db2e03af35a100370c64632a983")
              -- UAHF fork block.
              , (478558, "0000000000000000011865af4122fe3b144e2cbeea86142e8ff2fb4107352d43")
                -- Nov, 13 DAA activation block.
              , (504031, "0000000000000000011ebf65b60d0a3de80b8175be709d653b4c1a1beeb6ab9c")
                -- Monolith activation.
              , (530359, "0000000000000000011ada8bd08f46074f44a8f155396f43e38acf9501c49103")
              ]
        , getSeeds =
              [ "bitcoinsv.io"
              , "seed.bitcoinsv.io"
              , "cascharia.com"
              , "seed.cascharia.com"
              , "satoshisvision.network"
              , "seed.satoshisvision.network"
              ]
        , getBip44Coin = 0
        , getSigHashForkId = Nothing
        , getEdaBlockHeight = Just 478558
        , getDaaBlockHeight = Just 504031
        , getSegWit = False
        , getCashAddrPrefix = Nothing
        , getBech32Prefix = Nothing
        , getReplaceByFee = False
        , getHalvingInterval = 210000
        }

bsvTest :: Network
bsvTest = undefined

bsvSTN :: Network
bsvSTN = undefined

-- | List of all networks supported by this library.
allNets :: [Network]
allNets = [bsv, bsvTest, bsvSTN] --, btc, bch, btcTest, bchTest, btcRegTest, bchRegTest]
