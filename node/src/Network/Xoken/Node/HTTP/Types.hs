{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Xoken.Node.HTTP.Types where

import Control.Lens (makeLenses)
import Data.Aeson
import GHC.Generics
import Network.Xoken.Node.Env
import Snap.Snaplet

data App =
    App
        { _env :: XokenNodeEnv
        --, _auth :: Snaplet AuthService
        }

data AuthService = AuthService

data AuthReq =
    AuthReq
        { username :: String
        , password :: String
        }
    deriving (Generic, FromJSON)

data AuthResp =
    AuthResp
        { sessionKey :: Maybe String
        , callsUsed :: Int
        , callsRemaining :: Int
        }
    deriving (Generic, ToJSON)

makeLenses ''App
