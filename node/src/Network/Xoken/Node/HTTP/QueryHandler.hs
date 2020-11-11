{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE DeriveGeneric #-}

module Network.Xoken.Node.HTTP.QueryHandler where

import Data.Aeson
import Data.Either
import Data.HashMap.Strict
import Data.Maybe
import Data.Text
import qualified Data.Text.Encoding as DTE
import Data.Vector
import GHC.Generics
import Network.Xoken.Node.HTTP.Types
import Network.Xoken.Node.P2P.Common (addNewUser, generateSessionKey, getSimpleQueryParam, query, write)
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Service
import Prelude
import Snap

tshow :: Show a => a -> Text
tshow = Data.Text.pack . show

showValue :: Value -> Text
showValue (String t) = t
showValue (Number s) = tshow s
showValue (Bool b) = tshow b
showValue (Array v) = tshow $ showValue <$> Data.Vector.toList v
showValue Null = ""
showValue (Object o) = tshow o -- :TODO not correct, yet shouldn't be used

operators :: [Text]
operators = ["$or", "$and", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$eq", "$neq"]

showOp :: Text -> Text
showOp "$or" = " OR "
showOp "$and" = " AND "
showOp "$gt" = " > "
showOp "$gte" = " >= "
showOp "$lt" = " < "
showOp "$lte" = " <= "
showOp "$in" = " IN "
showOp "$nin" = " NOT IN "
showOp "$eq" = " == "
showOp "$neq" = " <> "
showOp x = x

stripLensPrefixOptions :: Options
stripLensPrefixOptions = defaultOptions {fieldLabelModifier = Prelude.drop 1}

data QueryRequest =
    QueryRequest
        { _where :: Maybe Value
        , _return :: Maybe Value
        }
    deriving (Generic)

instance ToJSON QueryRequest where
    toJSON = genericToJSON stripLensPrefixOptions

instance FromJSON QueryRequest where
    parseJSON = genericParseJSON stripLensPrefixOptions

encodeQuery :: HashMap Text Value -> Maybe Value -> Text
encodeQuery req ret =
    let m = "Match (protocol: Protocol)-[r:PRESENT_IN]->(block: Block) Where "
        withWhere = foldlWithKey' (\acc k v -> acc <> handleOp k v) m req
     in (fromMaybe withWhere $
         (\(Object hm) -> foldlWithKey' (\acc k v -> acc <> handleReturn k v) (withWhere <> " RETURN ") hm) <$> ret)

handleOp :: Text -> Value -> Text
handleOp op v =
    case op of
        "$or" ->
            case v of
                Array v ->
                    intercalate (showOp op) $
                    fmap
                        (\x ->
                             case x of
                                 Object y -> foldlWithKey' (\acc k v -> acc <> handleOp k v) "" y)
                        (Data.Vector.toList v)
                _ -> undefined
        "$and" ->
            case v of
                (Object y) ->
                    foldlWithKey'
                        (\acc k v ->
                             acc <>
                             if acc == ""
                                 then handleOp k v
                                 else showOp op <> " " <> handleOp k v)
                        ""
                        y
                _ -> undefined
        k ->
            let (t, va) = dec v
             in k <> showOp t <> showValue va

dec :: Value -> (Text, Value)
dec v =
    case v of
        Object hm ->
            if Prelude.length (keys hm) == 1
                then (Prelude.head $ keys hm, Prelude.head $ elems hm)
                else (tshow $ keys hm, String "dum")
        x -> (showValue x, String "dum")

handleReturn :: Text -> Value -> Text
handleReturn "relation" (Object hm) =
    foldlWithKey'
        (\acc k v ->
             acc <>
             (if acc == ""
                  then " r."
                  else " , r.") <>
             showValue v <> "(" <> k <> ")")
        ""
        hm
handleReturn x (Object hm) =
    foldlWithKey'
        (\acc k v ->
             acc <>
             (if acc == ""
                  then " "
                  else " , ") <>
             showValue v <> "(" <> k <> ")")
        ""
        hm

queryHandler :: QueryRequest -> Handler App App ()
queryHandler qr = do
    case _where qr of
        Just (Object hm) -> do
            let resp = encodeQuery hm (_return qr)
            writeBS $ DTE.encodeUtf8 resp
        _ -> undefined
