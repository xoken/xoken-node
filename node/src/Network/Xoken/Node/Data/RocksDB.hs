{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE LambdaCase #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Network.Xoken.Node.Data.RocksDB where

import Conduit
import Control.Monad.Reader (MonadReader, ReaderT)
import qualified Control.Monad.Reader as R
import qualified Data.ByteString.Short as B.Short
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as I
import Data.Word
import Database.RocksDB (DB, ReadOptions)
import Database.RocksDB.Query
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.KeyValue
import UnliftIO
import Xoken

dataVersion :: Word32
dataVersion = 15

-- isInitializedDB :: MonadIO m => m (Either InitException Bool)
-- isInitializedDB   =
--      undefined -- VersionKey >>= \case
--         Just v
--             | v == dataVersion -> return (Right True)
--             | otherwise -> return (Left (IncorrectVersion v))
--         Nothing -> return (Right False)
setInitDB :: MonadIO m => DB -> m ()
setInitDB db = insert db VersionKey dataVersion

getBestBlockDB :: MonadIO m => m (Maybe BlockHash)
getBestBlockDB = undefined -- BestKey

getBlocksAtHeightDB :: MonadIO m => BlockHeight -> m [BlockHash]
getBlocksAtHeightDB h = undefined -- (HeightKey h) >>= \case
        -- Nothing -> return []
        -- Just ls -> return ls

getBlockDB :: MonadIO m => BlockHash -> m (Maybe BlockData)
getBlockDB h = undefined -- (BlockKey h)

getTxDataDB :: MonadIO m => TxHash -> m (Maybe TxData)
getTxDataDB th = undefined -- (TxKey th)

getSpenderDB :: MonadIO m => OutPoint -> m (Maybe Spender)
getSpenderDB op = undefined -- $ SpenderKey op

deleteOrphanTx :: MonadIO m => TxHash -> m ()
deleteOrphanTx = undefined

getTxData :: MonadIO m => TxHash -> m (Maybe TxData)
getTxData = undefined
-- getSpendersDB :: MonadIO m => TxHash -> m (IntMap Spender)
-- getSpendersDB th = undefined --I.fromList . map (uncurry f) <$> liftIO (matchingAsList db opts (SpenderKeyS th))
--   where
--     f (SpenderKey op) s = undefined --(fromIntegral (outPointIndex op), s)
--     f _ _ = undefined --undefined
--
-- getBalanceDB :: MonadIO m => Address -> m (Maybe Balance)
-- getBalanceDB a = undefined --fmap (balValToBalance a) <$> undefined -- (BalKey a)
--
-- getMempoolDB :: (MonadIO m, MonadResource m) => BlockDB -> ConduitT i (UnixTime, TxHash) m ()
-- getMempoolDB = undefined --x .| mapC (uncurry f)
--   where
--     x = undefined --matching db opts MemKeyS
--     f (MemKey u t) () = undefined --(u, t)
--     f _ _ = undefined --undefined
--
-- getOrphansDB :: (MonadIO m, MonadResource m) => ConduitT i (UnixTime, Tx) m ()
-- getOrphansDB = undefined --matching db opts OrphanKeyS .| mapC snd
--
-- getOrphanTxDB :: MonadIO m => TxHash -> m (Maybe (UnixTime, Tx))
-- getOrphanTxDB h = undefined --undefined -- (OrphanKey h)
--
-- getAddressTxsDB :: (MonadIO m, MonadResource m) => Address -> Maybe BlockRef -> BlockDB -> ConduitT i BlockTx m ()
-- getAddressTxsDB a mbr = undefined --x .| mapC (uncurry f)
--   where
--     x =
--         case mbr of
--             Nothing -> matching db opts (AddrTxKeyA a)
--             Just br -> matchingSkip db opts (AddrTxKeyA a) (AddrTxKeyB a br)
--     f AddrTxKey {addrTxKeyT = undefined --t} () = undefined --t
--     f _ _ = undefined --undefined
--
-- getAddressBalancesDB :: (MonadIO m, MonadResource m) => ConduitT i Balance m ()
-- getAddressBalancesDB = undefined --matching db opts BalKeyS .| mapC (\(BalKey a, b) -> balValToBalance a b)
--
-- getUnspentsDB :: (MonadIO m, MonadResource m) => ConduitT i Unspent m ()
-- getUnspentsDB = undefined --matching db opts UnspentKeyB .| mapC (\(UnspentKey k, v) -> unspentFromDB k v)
--
-- getUnspentDB :: MonadIO m => OutPoint -> m (Maybe Unspent)
-- getUnspentDB p = undefined --fmap (unspentValToUnspent p) <$> undefined -- (UnspentKey p)
--
-- getAddressUnspentsDB :: (MonadIO m, MonadResource m) => Address -> Maybe BlockRef -> BlockDB -> ConduitT i Unspent m ()
-- getAddressUnspentsDB a mbr = undefined --x .| mapC (uncurry f)
--   where
--     x =
--         case mbr of
--             Nothing -> matching db opts (AddrOutKeyA a)
--             Just br -> matchingSkip db opts (AddrOutKeyA a) (AddrOutKeyB a br)
--     f AddrOutKey {addrOutKeyB = undefined --b, addrOutKeyP = undefined --p} OutVal {outValAmount = undefined --v, outValScript = undefined --s} =
--         Unspent {unspentBlock = undefined --b, unspentAmount = undefined --v, unspentScript = undefined --B.Short.toShort s, unspentPoint = undefined --p}
--     f _ _ = undefined --undefined
--
-- unspentFromDB :: OutPoint -> UnspentVal -> Unspent
-- unspentFromDB p UnspentVal {unspentValBlock = undefined --b, unspentValAmount = undefined --v, unspentValScript = undefined --s} =
--     Unspent {unspentBlock = undefined --b, unspentAmount = undefined --v, unspentPoint = undefined --p, unspentScript = undefined --s}
