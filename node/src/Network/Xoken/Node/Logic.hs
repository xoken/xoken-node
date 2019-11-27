{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Xoken.Node.Logic where

import Conduit
import Control.Monad
import Control.Monad.Except
import Control.Monad.Logger
import qualified Data.ByteString as B
import qualified Data.ByteString.Short as B.Short
import Data.Either (rights)
import qualified Data.IntMap.Strict as I
import Data.List
import Data.Maybe
import Data.Serialize
import Data.String
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Word

-- import Database.RocksDB
import Network.Xoken.Block.Headers (computeSubsidy)
import Network.Xoken.Node.Data

import UnliftIO
import Xoken

data ImportException
    = PrevBlockNotBest !Text
    | UnconfirmedCoinbase !Text
    | BestBlockUnknown
    | BestBlockNotFound !Text
    | BlockNotBest !Text
    | OrphanTx !Text
    | TxNotFound !Text
    | NoUnspent !Text
    | TxInvalidOp !Text
    | TxDeleted !Text
    | TxDoubleSpend !Text
    | AlreadyUnspent !Text
    | TxConfirmed !Text
    | OutputOutOfRange !Text
    | BalanceNotFound !Text
    | InsufficientBalance !Text
    | InsufficientZeroBalance !Text
    | InsufficientOutputs !Text
    | InsufficientFunds !Text
    | InitException !InitException
    | DuplicatePrevOutput !Text
    deriving (Show, Read, Eq, Ord, Exception)

sortTxs :: [Tx] -> [(Word32, Tx)]
sortTxs txs = go $ zip [0 ..] txs
  where
    go [] = []
    go ts =
        let (is, ds) = partition (all ((`notElem` map (txHash . snd) ts) . outPointHash . prevOutput) . txIn . snd) ts
         in is <> go ds

getTxOutput :: (MonadLogger m, MonadError ImportException m) => Word32 -> Transaction -> m StoreOutput
getTxOutput i tx = do
    unless (fromIntegral i < length (transactionOutputs tx)) $ do
        $(logErrorS) "BlockLogic" $
            "Output out of range " <> txHashToHex (txHash (transactionData tx)) <> " " <> fromString (show i)
        throwError . OutputOutOfRange . cs $
            show OutPoint {outPointHash = txHash (transactionData tx), outPointIndex = i}
    return $ transactionOutputs tx !! fromIntegral i

updateAddressCounts :: (Monad m, MonadError ImportException m) => Network -> [Address] -> (Word64 -> Word64) -> m ()
updateAddressCounts net as f = undefined
    -- forM_ as $ \a -> do
    --     b <-
    --         getBalance a >>= \case
    --             Nothing -> throwError (BalanceNotFound (addrText net a))
    --             Just b -> return b
    --     setBalance b {balanceTxCount = f (balanceTxCount b)}

txAddresses :: Transaction -> [Address]
txAddresses t =
    nub . rights $
    map (scriptToAddressBS . inputPkScript) (filter (not . isCoinbase) (transactionInputs t)) <>
    map (scriptToAddressBS . outputScript) (transactionOutputs t)

txDataAddresses :: TxData -> [Address]
txDataAddresses t =
    nub . rights $
    map (scriptToAddressBS . prevScript) (I.elems (txDataPrevs t)) <>
    map (scriptToAddressBS . scriptOutput) (txOut (txData t))

addrText :: Network -> Address -> Text
addrText net a = fromMaybe "???" $ addrToString net a
