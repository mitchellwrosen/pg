module PgUtils
  ( rowsByKey,
    rowsByMaybeKey,
  )
where

import Data.List qualified as List
import Data.Map.Strict qualified as Map
import Queue qualified

rowsByKey :: (Ord key) => (row -> key) -> [row] -> (key -> [row])
rowsByKey toKey rows =
  maybe [] Queue.toList . flip Map.lookup (List.foldl' f Map.empty rows)
  where
    f acc row =
      Map.alter (Just . maybe (Queue.singleton row) (Queue.enqueue row)) (toKey row) acc

rowsByMaybeKey :: (Ord key) => (row -> Maybe key) -> [row] -> (key -> [row])
rowsByMaybeKey toKey rows =
  maybe [] Queue.toList . flip Map.lookup (List.foldl' f Map.empty rows)
  where
    f acc row =
      case toKey row of
        Nothing -> acc
        Just key -> Map.alter (Just . maybe (Queue.singleton row) (Queue.enqueue row)) key acc
