{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Xoken.Node.HTTP.Types where

import Control.Lens (makeLenses)
import Network.Xoken.Node.Env

data App =
    App
        { _env :: XokenNodeEnv
        }

makeLenses ''App
