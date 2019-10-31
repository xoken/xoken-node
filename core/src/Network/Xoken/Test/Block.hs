{-|
Module      : Network.Xoken.Test.Block
Copyright   : No rights reserved
License     : UNLICENSE
Maintainer  : xenog@protonmail.com
Stability   : experimental
Portability : POSIX
-}
module Network.Xoken.Test.Block where

import Network.Xoken.Block.Common
import Network.Xoken.Block.Merkle
import Network.Xoken.Constants
import Network.Xoken.Test.Crypto
import Network.Xoken.Test.Network
import Network.Xoken.Test.Transaction
import Test.QuickCheck

-- | Block full or arbitrary transactions.
arbitraryBlock :: Network -> Gen Block
arbitraryBlock net = do
    h <- arbitraryBlockHeader
    c <- choose (0, 10)
    txs <- vectorOf c (arbitraryTx net)
    return $ Block h txs

-- | Block header with random hash.
arbitraryBlockHeader :: Gen BlockHeader
arbitraryBlockHeader =
    BlockHeader <$> arbitrary <*> arbitraryBlockHash <*> arbitraryHash256 <*>
    arbitrary <*>
    arbitrary <*>
    arbitrary

-- | Arbitrary block hash.
arbitraryBlockHash :: Gen BlockHash
arbitraryBlockHash = BlockHash <$> arbitraryHash256

-- | Arbitrary 'GetBlocks' object with at least one block hash.
arbitraryGetBlocks :: Gen GetBlocks
arbitraryGetBlocks =
    GetBlocks <$> arbitrary <*> listOf1 arbitraryBlockHash <*>
    arbitraryBlockHash

-- | Arbitrary 'GetHeaders' object with at least one block header.
arbitraryGetHeaders :: Gen GetHeaders
arbitraryGetHeaders =
    GetHeaders <$> arbitrary <*> listOf1 arbitraryBlockHash <*>
    arbitraryBlockHash

-- | Arbitrary 'Headers' object with at least one block header.
arbitraryHeaders :: Gen Headers
arbitraryHeaders =
    Headers <$> listOf1 ((,) <$> arbitraryBlockHeader <*> arbitraryVarInt)

-- | Arbitrary 'MerkleBlock' with at least one hash.
arbitraryMerkleBlock :: Gen MerkleBlock
arbitraryMerkleBlock = do
    bh <- arbitraryBlockHeader
    ntx <- arbitrary
    hashes <- listOf1 arbitraryHash256
    c <- choose (1, 10)
    flags <- vectorOf (c * 8) arbitrary
    return $ MerkleBlock bh ntx hashes flags
