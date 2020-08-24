{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Network.Xoken.Node.HTTP.Types where

import Control.Lens (makeLenses)
import Control.Monad.State.Class
import Control.Monad.IO.Class
import qualified Control.Exception as CE
import qualified Control.Monad.Catch as MC
import Network.Xoken.Node.Env
import Snap

data App =
    App
        { _env :: XokenNodeEnv k a b
        }

instance HasBitcoinP2P (Handler App App) where
    getBitcoinP2P = bitcoinP2PEnv <$> gets _env

instance HasDatabaseHandles (Handler App App) where
    getDB = dbHandles <$> gets _env

instance HasAllegoryEnv (Handler App App) where
    getAllegory = allegoryEnv <$> gets _env

instance HasLogger (Handler App App) where
    getLogger = loggerEnv <$> gets _env

instance MC.MonadThrow (Handler App App) where
    throwM = liftIO . CE.throwIO

makeLenses ''App
