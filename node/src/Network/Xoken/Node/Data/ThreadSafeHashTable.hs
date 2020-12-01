{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.Data.ThreadSafeHashTable
    ( TSHashTable(..)
    , new
    , insert
    , delete
    , Network.Xoken.Node.Data.ThreadSafeHashTable.lookup
    , mutate
    , Network.Xoken.Node.Data.ThreadSafeHashTable.mapM_
    , fromList
    , toList
    -- , lookupIndex
    -- , nextByIndex
    ) where

import Control.Concurrent.MVar
import Control.Monad.IO.Class
import Control.Monad.STM
import qualified Data.HashTable.IO as H
import Data.Hashable
import Data.Int
import qualified Data.List as L
import Data.Text as T
import Numeric as N

type HashTable k v = H.BasicHashTable k v

data TSHashTable k v =
    TSHashTable
        { hashTableList :: ![MVar (HashTable k v)]
        , size :: !Int16
        }

type GenericShortHash = Int

new :: Int16 -> IO (TSHashTable k v)
new size = do
    hlist <-
        mapM
            (\x -> do
                 hm <- H.new
                 newMVar hm)
            [1 .. size]
    return $ TSHashTable hlist size

insert :: (Eq k, Hashable k) => TSHashTable k v -> k -> v -> IO ()
insert tsh k v = do
    let index = (hash k) `mod` (fromIntegral $ size tsh)
    withMVar ((hashTableList tsh) !! index) (\hx -> H.insert hx k v)

delete :: (Eq k, Hashable k) => TSHashTable k v -> k -> IO ()
delete tsh k = do
    let index = (hash k) `mod` (fromIntegral $ size tsh)
    withMVar ((hashTableList tsh) !! index) (\hx -> H.delete hx k)

lookup :: (Eq k, Hashable k) => TSHashTable k v -> k -> IO (Maybe v)
lookup tsh k = do
    let index = (hash k) `mod` (fromIntegral $ size tsh)
    withMVar ((hashTableList tsh) !! index) (\hx -> H.lookup hx k)

mutate :: (Eq k, Hashable k) => TSHashTable k v -> k -> (Maybe v -> (Maybe v, a)) -> IO a
mutate tsh k f = do
    v <- Network.Xoken.Node.Data.ThreadSafeHashTable.lookup tsh k
    case f v of
        (Nothing, a) -> do
            delete tsh k
            return a
        (Just v, a) -> do
            insert tsh k v
            return a

mapM_ :: ((k, v) -> IO a) -> TSHashTable k v -> IO ()
mapM_ f tsh = do
    traverse (\ht -> liftIO $ withMVar ht (\hx -> H.mapM_ f hx)) (hashTableList tsh)
    return ()

toList :: (Eq k, Hashable k) => TSHashTable k v -> IO [(k, v)]
toList tsh = fmap L.concat $ mapM (\x -> withMVar x (\hx -> H.toList hx)) (hashTableList tsh)

fromList :: (Eq k, Hashable k) => Int16 -> [(k, v)] -> IO (TSHashTable k v)
fromList size kv = do
    tsh <- new size
    traverse (\(k, v) -> insert tsh k v) kv
    return tsh
--
-- lookupIndex :: (Eq k, Hashable k) => TSHashTable k v -> k -> IO (Maybe (Int, Word))
-- lookupIndex tsh k = lookupIndex' (hashTableList tsh) k 0
--   where
--     lookupIndex' [] _ _ = return Nothing
--     lookupIndex' (h:hs) k i = do
--         ind <- H.lookupIndex h k
--         case ind of
--             Nothing -> lookupIndex' hs k (i + 1)
--             Just w -> return $ Just $ (i, w)
--
-- nextByIndex :: (Eq k, Hashable k) => TSHashTable k v -> (Int, Word) -> IO (Maybe (k, v))
-- nextByIndex tsh (i, w) = do
--     let hts = hashTableList tsh
--     if L.length hts <= i
--         then return Nothing
--         else do
--             nbi <- H.nextByIndex (hts !! i) w
--             case nbi of
--                 Nothing -> return Nothing
--                 Just (_, k, v) -> return $ Just (k, v)
