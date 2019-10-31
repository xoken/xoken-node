{-|
Module      : Network.Xoken.Test.Message
Copyright   : No rights reserved
License     : UNLICENSE
Maintainer  : xenog@protonmail.com
Stability   : experimental
Portability : POSIX
-}
module Network.Xoken.Test.Message where

import Network.Xoken.Constants
import Network.Xoken.Network.Message
import Network.Xoken.Test.Block
import Network.Xoken.Test.Crypto
import Network.Xoken.Test.Network
import Network.Xoken.Test.Transaction
import Test.QuickCheck

-- | Arbitrary 'MessageHeader'.
arbitraryMessageHeader :: Gen MessageHeader
arbitraryMessageHeader =
    MessageHeader <$> arbitrary <*> arbitraryMessageCommand <*> arbitrary <*>
    arbitraryCheckSum32

-- | Arbitrary 'Message'.
arbitraryMessage :: Network -> Gen Message
arbitraryMessage net =
    oneof
        [ MVersion <$> arbitraryVersion
        , return MVerAck
        , MAddr <$> arbitraryAddr1
        , MInv <$> arbitraryInv1
        , MGetData <$> arbitraryGetData
        , MNotFound <$> arbitraryNotFound
        , MGetBlocks <$> arbitraryGetBlocks
        , MGetHeaders <$> arbitraryGetHeaders
        , MTx <$> arbitraryTx net
        , MBlock <$> arbitraryBlock net
        , MMerkleBlock <$> arbitraryMerkleBlock
        , MHeaders <$> arbitraryHeaders
        , return MGetAddr
        , MFilterLoad <$> arbitraryFilterLoad
        , MFilterAdd <$> arbitraryFilterAdd
        , return MFilterClear
        , MPing <$> arbitraryPing
        , MPong <$> arbitraryPong
        , MAlert <$> arbitraryAlert
        , MReject <$> arbitraryReject
        , return MSendHeaders
        ]
