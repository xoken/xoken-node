{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Network.Xoken.Node.HTTP.QueryHandler where

import Control.Arrow
import Control.Exception
import Control.Monad.IO.Class
import Data.Aeson
import qualified Data.ByteString.Lazy as BSL
import Data.Either
import Data.HashMap.Strict
import qualified Data.HashMap.Strict as Map
import Data.List (find, nub)
import qualified Data.Map.Strict as MMap
import Data.Maybe
import Data.Pool
import Data.Text
import qualified Data.Text.Encoding as DTE
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Vector
import qualified Database.Bolt as BT
import GHC.Generics
import Network.Xoken.Node.Env
import Network.Xoken.Node.HTTP.Handler
import Network.Xoken.Node.HTTP.Types
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.Common (addNewUser, generateSessionKey, getSimpleQueryParam, query, write, withResource')
import Network.Xoken.Node.P2P.Types hiding (node)
import Network.Xoken.Node.Service
import Prelude
import Snap
import qualified Text.Read as TR

tshow :: Show a => a -> Text
tshow = Data.Text.pack . show

showValueNS :: Value -> Text
showValueNS (String t) = t
showValueNS (Number s) = tshow s
showValueNS (Bool b) = tshow b
showValueNS (Array v) = tshow $ showValueNS <$> Data.Vector.toList v
showValueNS Null = ""
showValueNS (Object o) = tshow o -- :TODO not correct, yet shouldn't be used

showValue :: Value -> Text
showValue (String t) = tshow t
showValue (Array v) = "[" <> (Data.Text.intercalate "," $ showValue <$> Data.Vector.toList v) <> "]"
showValue x = showValueNS x

operators :: [Text]
operators = ["$or", "$and", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$eq", "$neq"]

showOp :: Maybe Text -> Text -> Text
showOp Nothing "$or" = " OR "
showOp (Just k) "$or" = k <> " OR "
showOp Nothing "$and" = " AND "
showOp (Just k) "$and" = k <> " AND "
showOp Nothing "$gt" = " > "
showOp (Just k) "$gt" = k <> " > "
showOp Nothing "$gte" = " >= "
showOp (Just k) "$gte" = k <> " >= "
showOp Nothing "$lt" = " < "
showOp (Just k) "$lt" = k <> " < "
showOp Nothing "$lte" = " <= "
showOp (Just k) "$lte" = k <> " <= "
showOp Nothing "$in" = " IN "
showOp (Just k) "$in" = k <> " IN "
showOp (Just k) "$nin" = " NOT " <> k <> " IN "
showOp Nothing "$eq" = " = "
showOp (Just k) "$eq" = k <> " = "
showOp Nothing "$neq" = " <> "
showOp (Just k) "$neq" = k <> " <> "
showOp _ x = x

encodeQuery :: HashMap Text Value -> Maybe Value -> HashMap Text Value -> NodeRelation -> Text
encodeQuery req ret grouped nr =
    let m = getMatch nr <> " Where "
        withWhere = foldlWithKey' (\acc k v -> acc <> handleOp k v) m req
     in (fromMaybe withWhere $
         (\r ->
              let current =
                      case r of
                          (Array v) ->
                              intercalate
                                  " , "
                                  ((\(Object hm) ->
                                        foldlWithKey' (\acc k v -> acc <> handleReturn k v) (withWhere <> " RETURN ") hm) <$>
                                   (Data.Vector.toList v))
                          (Object hm) ->
                              foldlWithKey' (\acc k v -> acc <> handleReturn k v) (withWhere <> " RETURN ") hm
                  other = foldlWithKey' (\acc k v -> acc <> handleReturn k v) " , " grouped
               in current <>
                  if " , " == other
                      then ""
                      else other) <$>
         ret)

handleOp :: Text -> Value -> Text
handleOp op v =
    case op of
        "$or" ->
            case v of
                Array v ->
                    intercalate (showOp Nothing op) $
                    fmap
                        (\x ->
                             case x of
                                 Object y ->
                                     foldlWithKey'
                                         (\acc k v ->
                                              acc <>
                                              if k == "$or" || k == "$and"
                                                  then let res = handleOp k v
                                                        in if acc == ""
                                                               then res
                                                               else showOp Nothing op <> " " <> res
                                                  else if k == "p.name"
                                                           then let resp =
                                                                        case v of
                                                                            Object v' ->
                                                                                intercalate (showOp Nothing op) $
                                                                                elems $ mapWithKey (handleOpInd op) v'
                                                                            _ -> undefined
                                                                 in if acc == ""
                                                                        then resp
                                                                        else showOp Nothing op <> " " <> resp
                                                           else if acc == ""
                                                                    then handleOp' k v (showOp Nothing op)
                                                                    else showOp Nothing op <>
                                                                         " " <> handleOp' k v (showOp Nothing op))
                                         ""
                                         y)
                        (Data.Vector.toList v)
                _ -> undefined
        "$and" ->
            case v of
                (Object y) ->
                    foldlWithKey'
                        (\acc k v ->
                             acc <>
                             if k == "$or" || k == "$and"
                                 then let res = handleOp k v
                                       in if acc == ""
                                              then res
                                              else showOp Nothing op <> " " <> res
                                 else if k == "p.name"
                                          then let resp =
                                                       case v of
                                                           Object v' ->
                                                               intercalate (showOp Nothing op) $
                                                               elems $ mapWithKey (handleOpInd op) v'
                                                           _ -> undefined
                                                in if acc == ""
                                                       then resp
                                                       else showOp Nothing op <> " " <> resp
                                          else if acc == ""
                                                   then handleOp' k v (showOp Nothing op)
                                                   else showOp Nothing op <> " " <> handleOp' k v (showOp Nothing op))
                        ""
                        y
                _ -> undefined
        k ->
            let (t, va) = dec v
                resp =
                    case t of
                        "$in" -> handlePropsArray (k, va)
                        "$nin" -> handlePropsArray (k, va)
                        _ -> handleProps (k, va)
             in intercalate (showOp Nothing "$and") ((\(k', va') -> showOp (Just k') t <> showValue va') <$> resp)

handleOpInd :: Text -> Text -> Value -> Text
handleOpInd m op v =
    case op of
        "$and" -> handleOp op v
        "$or" -> handleOp op v
        "$in" ->
            intercalate
                (showOp Nothing "$and")
                ((\(k', va') -> showOp (Just k') op <> showValue va') <$> handlePropsArray ("p.name", v))
        "$nin" ->
            intercalate
                (showOp Nothing "$and")
                ((\(k', va') -> showOp (Just k') op <> showValue va') <$> handlePropsArray ("p.name", v))
        _ ->
            intercalate
                (showOp Nothing "$and")
                ((\(k', va') -> showOp (Just k') op <> showValue va') <$> handleProps ("p.name", v))

handleOp' :: Text -> Value -> Text -> Text
handleOp' k v op =
    case v of
        Object hm -> intercalate op $ (\(k', v') -> showOp (Just k) k' <> showValue v') <$> Map.toList hm
        _ -> ""

dec :: Value -> (Text, Value)
dec v =
    case v of
        Object hm ->
            if Prelude.length (keys hm) == 1
                then (Prelude.head $ keys hm, Prelude.head $ elems hm)
                else (tshow $ keys hm, String "dum")
        x -> (showValue x, String (showValue x))

handlePropsArray :: (Text, Value) -> [(Text, Value)]
handlePropsArray (k, v) =
    if k == "p.name"
        then case v of
                 Array x ->
                     let res =
                             Prelude.reverse $
                             Prelude.foldl
                                 (\acc (e, ind) -> do
                                      case e of
                                          Array y ->
                                              Prelude.foldl
                                                  (\acc (e', ind') -> ("p.prop" <> (tshow ind'), e') : acc)
                                                  acc
                                                  (Prelude.zip (Data.Vector.toList y) [1 ..])
                                          _ -> ("p.prop" <> (tshow ind), e) : acc)
                                 []
                                 (Prelude.zip (Data.Vector.toList x) [1 ..])
                      in Map.toList $
                         fmap
                             (Array . Data.Vector.fromList . Data.List.nub)
                             (Map.fromListWith (\a b -> a Prelude.++ b) (fmap (second pure) res))
                 String s -> [("p.prop1", v)]
                 _ -> [(k, v)]
        else [(k, v)]

handleProps :: (Text, Value) -> [(Text, Value)]
handleProps (k, v) =
    if k == "p.name"
        then case v of
                 Array x ->
                     Prelude.reverse $
                     Prelude.foldl
                         (\acc (e, ind) -> ("p.prop" <> (tshow ind), e) : acc)
                         []
                         (Prelude.zip (Data.Vector.toList x) [1 ..])
                 String s -> [("p.prop1", v)]
                 _ -> [(k, v)]
        else [(k, v)]

handleReturn :: Text -> Value -> Text
handleReturn "relation" (Object hm) =
    foldrWithKey
        (\k v acc ->
             acc <>
             (if acc == ""
                  then " r."
                  else " , r.") <>
             showValueNS v <> "(" <> k <> ")")
        ""
        hm
handleReturn x (Object hm) =
    foldrWithKey
        (\k v acc ->
             acc <>
             (if acc == ""
                  then " "
                  else " , ") <>
             (if showValueNS v == ""
                  then k
                  else showValueNS v <> "(" <> k <> ")"))
        ""
        hm

getMatch :: NodeRelation -> Text
getMatch nr =
    let m = "Match "
        f = "(" <> (aliasNode (_from nr :: Node)) <> ": " <> (node $ _from nr) <> " )"
        t =
            case _to nr of
                Just x -> "(" <> (aliasNode (x :: Node)) <> ": " <> (node x) <> " )"
                Nothing -> ""
        v =
            case _via nr of
                Just x ->
                    "-[" <>
                    (aliasRel (x :: Relation)) <>
                    ":" <>
                    (relationship x) <>
                    "]-" <>
                    (if direction x == Just True
                         then ">"
                         else "")
                Nothing ->
                    if isJust (_to nr)
                        then ","
                        else ""
     in m <> f <> v <> t

getMap :: [Grouped] -> HashMap Text Value
getMap groupeds =
    Prelude.foldl
        (\acc g -> do
             case g of
                 Day -> Map.insert "b.timestamp" (String "collect") $ Map.insert "b.month" (String "collect") acc
                 Month -> acc
                 Hour ->
                     Map.insert "b.timestamp" (String "collect") .
                     Map.insert "b.month" (String "collect") . Map.insert "b.absoluteHour" (String "collect") $
                     acc
                 Year -> acc)
        Map.empty
        groupeds

formatResponse :: [BT.Record] -> [BT.Record]
formatResponse = fmap go
  where
    go mapR = do
        let yearF = MMap.alter (fmap (\(BT.L ((BT.I y):xs)) -> BT.I (1970 + y))) "collect(b.year)"
        let month m =
                let x = Prelude.rem m 12
                 in if x == 0
                        then 12
                        else x
        let years mon = 1970 + (div mon 12)
        let monF =
                MMap.alter
                    (fmap
                         (\(BT.L ((BT.I mon):xs)) -> BT.T (pack $ show (month mon) <> "/" <> show (1970 + (div mon 12)))))
                    "collect(b.month)"
        let mon = MMap.lookup "collect(b.month)" mapR
        let dayF map' =
                case mon of
                    Just (BT.L ((BT.I m):xs)) ->
                        MMap.alter
                            (\(Just (BT.I x)) ->
                                 Just
                                     (BT.T
                                          (pack $
                                           show x <>
                                           "/" <>
                                           show
                                               (if month m == 0
                                                    then 12
                                                    else m) <>
                                           "/" <> show (years m))))
                            "b.day"
                            map'
                    Nothing -> map'
        yearF . monF . dayF $ mapR

handleResponse :: Grouped -> [BT.Record] -> [BT.Record]
handleResponse group = fmap go
  where
    go m =
        case group of
            Day ->
                case Data.List.find (isInfixOf "day") (MMap.keys m) of
                    Just k -> do
                        let (Just (BT.L ((BT.I mon):xs))) = MMap.lookup "collect(b.month)" m
                        let (Just (BT.L ((BT.I ts):txs))) = MMap.lookup "collect(b.timestamp)" m
                        let years = 1970 + (div mon 12)
                        let month = Prelude.rem mon 12
                        let epd = utctDay $ posixSecondsToUTCTime 0
                        let cd = utctDay $ posixSecondsToUTCTime (fromIntegral ts)
                        let (_, d) = diffGregorianDurationClip cd epd
                        MMap.alter (const Nothing) "collect(b.timestamp)" $
                            MMap.alter (const Nothing) "collect(b.month)" $
                            MMap.alter
                                (\(Just (BT.I x)) ->
                                     Just
                                         (BT.T
                                              (pack $
                                               show (d + 1) <>
                                               "/" <>
                                               show
                                                   (if month == 0
                                                        then 12
                                                        else month) <>
                                               "/" <> show years)))
                                k
                                m
                    Nothing -> m
            Month ->
                case Data.List.find (isInfixOf "month") (MMap.keys m) of
                    Just k -> do
                        let month m =
                                let x = Prelude.rem m 12
                                 in if x == 0
                                        then 12
                                        else x
                        MMap.alter
                            (\(Just (BT.I mon)) ->
                                 Just (BT.T (pack $ show (month mon) <> "/" <> show (1970 + (div mon 12)))))
                            k
                            m
                    Nothing -> m
            Year ->
                case Data.List.find (isInfixOf "year") (MMap.keys m) of
                    Just k -> MMap.alter (\(Just (BT.I year)) -> Just (BT.T (pack $ show (1970 + year)))) k m
                    Nothing -> m
            Hour ->
                case Data.List.find (isInfixOf "hour") (MMap.keys m) of
                    Just k -> do
                        let (Just (BT.L ((BT.I ts):txs))) = MMap.lookup "collect(b.timestamp)" m
                        let (Just (BT.L ((BT.I mon):mxs))) = MMap.lookup "collect(b.month)" m
                        let (Just (BT.L ((BT.I hr):hxs))) = MMap.lookup "collect(b.absoluteHour)" m
                        let epd = utctDay $ posixSecondsToUTCTime 0
                        let cd = utctDay $ posixSecondsToUTCTime (fromIntegral ts)
                        let (_, day) = diffGregorianDurationClip cd epd
                        let years = 1970 + (div mon 12)
                        let month =
                                let x = Prelude.rem mon 12
                                 in if x == 0
                                        then 12
                                        else x
                        MMap.alter (const Nothing) "collect(b.timestamp)" $
                            MMap.alter (const Nothing) "collect(b.month)" $
                            MMap.alter (const Nothing) "collect(b.absoluteHour)" $
                            MMap.alter
                                (\(Just (BT.I x)) ->
                                     Just
                                         (BT.T
                                              (pack $
                                               show (day + 1) <>
                                               "/" <> show month <> "/" <> show years <> " " <> show hr <> ":00:00")))
                                k
                                m
                    Nothing -> m

handleProtocol :: MMap.Map Text BT.Value -> MMap.Map Text BT.Value
handleProtocol = MMap.mapWithKey go
  where
    go k v =
        if Data.Text.isInfixOf "p.name" k
            then case v of
                     BT.T x ->
                         if Data.Text.isInfixOf "_" x
                             then BT.L $ BT.T <$> (Data.Text.split ((==) '_') x)
                             else v
                     BT.L x ->
                         BT.L $
                         (\y ->
                              case y of
                                  BT.T x' ->
                                      if Data.Text.isInfixOf "_" x'
                                          then BT.L $ BT.T <$> (Data.Text.split ((==) '_') x')
                                          else v
                                  _ -> y) <$>
                         x
                     _ -> v
            else v

queryHandler :: QueryRequest -> Snap.Handler App App ()
queryHandler qr = do
    offset <- (flip (>>=) (TR.readMaybe . unpack . DTE.decodeUtf8)) <$> (getQueryParam "offset")
    limitv <- (flip (>>=) (TR.readMaybe . unpack . DTE.decodeUtf8)) <$> (getQueryParam "limit")
    let skip = " SKIP " <> (show $ fromMaybe 0 offset)
    let limit = " LIMIT " <> (show $ fromMaybe 200 limitv)
    case _where qr of
        Just (Object hm) -> do
            let groupeds =
                    case _return qr of
                        (Just (Object rh)) ->
                            case Map.lookup "$fields" rh of
                                (Just (Object x)) -> grouped <$> Map.keys x
                                _ -> []
                        _ -> []
            let resp =
                    BT.query $
                    (encodeQuery
                         hm
                         (_return qr)
                         (Map.insert "$fields" (Object (getMap $ catMaybes groupeds)) Map.empty)
                         (_on qr)) <>
                    (pack $ skip <> limit)
            dbe <- getDB
            pres <- liftIO $ try $ withResource' (pool $ graphDB dbe) (`BT.run` resp)
            case pres of
                Right rtm -> writeBS $ BSL.toStrict $
                             encode $ fmap handleProtocol $ Prelude.foldr handleResponse rtm (catMaybes groupeds)
                Left (e :: SomeException) -> do
                    liftIO $ print $ "Error occurred: " Prelude.++ show e
                    throwBadRequest
        _ -> throwBadRequest
