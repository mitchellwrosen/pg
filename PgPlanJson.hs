{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module PgPlanJson where

import Data.Aeson (FromJSON, Object, Value, parseJSON, withObject, withText)
import Data.Aeson.Types (Parser, explicitParseField, parseField, parseFieldMaybe')
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import PgPlan
import Prelude hiding (filter)

instance FromJSON Analyze where
  parseJSON :: Value -> Parser Analyze
  parseJSON =
    withObject "Analyze" \object -> do
      executionTime <- parseField @Double object "Execution Time"
      plan <- parseField @Node object "Plan"
      planningTime <- parseField @Double object "Planning Time"
      pure Analyze {..}

instance FromJSON Node where
  parseJSON :: Value -> Parser Node
  parseJSON =
    withObject "Node" \object -> do
      info <- parseNodeInfo object
      let go :: (NodeInfo -> a -> Node) -> (Object -> Parser a) -> Parser Node
          go constructor parse = do
            info2 <- parse object
            pure (constructor info info2)
      parseField @Text object "Node Type" >>= \case
        "Aggregate" -> go AggregateNode parseAggregateNodeInfo
        "Append" -> go AppendNode parseAppendNodeInfo
        "Bitmap Heap Scan" -> go BitmapHeapScanNode parseBitmapHeapScanNodeInfo
        "Bitmap Index Scan" -> go BitmapIndexScanNode parseBitmapIndexScanNodeInfo
        "BitmapOr" -> pure (BitmapOrNode info)
        "CTE Scan" -> go CteScanNode parseCteScanNodeInfo
        "Function Scan" -> go FunctionScanNode parseFunctionScanNodeInfo
        "Gather" -> go GatherNode parseGatherNodeInfo
        "Hash Join" -> go HashJoinNode parseHashJoinNodeInfo
        "Hash" -> go HashNode parseHashNodeInfo
        "Index Only Scan" -> go IndexOnlyScanNode parseIndexOnlyScanNodeInfo
        "Index Scan" -> go IndexScanNode parseIndexScanNodeInfo
        "Limit" -> pure (LimitNode info)
        "Materialize" -> pure (MaterializeNode info)
        "Memoize" -> go MemoizeNode parseMemoizeNodeInfo
        "Merge Join" -> go MergeJoinNode parseMergeJoinNodeInfo
        "Nested Loop" -> go NestedLoopNode parseNestedLoopNodeInfo
        "Result" -> go ResultNode parseResultNodeInfo
        "Seq Scan" -> go SeqScanNode parseSeqScanNodeInfo
        "SetOp" -> go SetOpNode parseSetOpNodeInfo
        "Sort" -> go SortNode parseSortNodeInfo
        "Subquery Scan" -> go SubqueryScanNode parseSubqueryScanNodeInfo
        "Unique" -> pure (UniqueNode info)
        "Values Scan" -> go ValuesScanNode parseValuesScanNodeInfo
        typ -> fail ("Unknown node type: " ++ Text.unpack typ)

instance FromJSON NodeInfo where
  parseJSON :: Value -> Parser NodeInfo
  parseJSON =
    withObject "NodeInfo" parseNodeInfo

parseAggregateNodeInfo :: Object -> Parser AggregateNodeInfo
parseAggregateNodeInfo object = do
  diskUsage <- parseFieldMaybe' @Int object "Disk Usage"
  groupKey <- parseFieldMaybe' @[Text] object "Group Key"
  hashAggBatches <- parseFieldMaybe' @Int object "HashAgg Batches"
  partialMode <- parseField @Text object "Partial Mode"
  peakMemoryUsage <- parseFieldMaybe' @Int object "Peak Memory Usage"
  plannedPartitions <- parseFieldMaybe' @Int object "Planned Partitions"
  strategy <- explicitParseField parseAggregateStrategy object "Strategy"
  pure AggregateNodeInfo {..}

parseAggregateStrategy :: Value -> Parser AggregateStrategy
parseAggregateStrategy =
  withText "AggregateStrategy" \case
    "Hashed" -> pure AggregateStrategyHashed
    "Mixed" -> pure AggregateStrategyMixed
    "Plain" -> pure AggregateStrategyPlain
    "Sorted" -> pure AggregateStrategySorted
    strategy -> fail ("Unknown aggregate strategy: " ++ Text.unpack strategy)

parseAppendNodeInfo :: Object -> Parser AppendNodeInfo
parseAppendNodeInfo object = do
  subplansRemoved <- parseField @Int object "Subplans Removed"
  pure AppendNodeInfo {..}

parseBitmapHeapScanNodeInfo :: Object -> Parser BitmapHeapScanNodeInfo
parseBitmapHeapScanNodeInfo object = do
  alias <- parseField @Text object "Alias"
  exactHeapBlocks <- parseField @Int object "Exact Heap Blocks"
  lossyHeapBlocks <- parseField @Int object "Lossy Heap Blocks"
  recheckCond <- parseField @Text object "Recheck Cond"
  relationName <- parseField @Text object "Relation Name"
  rowsRemovedByIndexRecheck <- parseFieldMaybe' @Int object "Rows Removed by Index Recheck"
  pure BitmapHeapScanNodeInfo {..}

parseBitmapIndexScanNodeInfo :: Object -> Parser BitmapIndexScanNodeInfo
parseBitmapIndexScanNodeInfo object = do
  indexCond <- parseFieldMaybe' @Text object "Index Cond"
  indexName <- parseField @Text object "Index Name"
  pure BitmapIndexScanNodeInfo {..}

parseCteScanNodeInfo :: Object -> Parser CteScanNodeInfo
parseCteScanNodeInfo object = do
  alias <- parseField @Text object "Alias"
  cteName <- parseField @Text object "CTE Name"
  pure CteScanNodeInfo {..}

parseFunctionScanNodeInfo :: Object -> Parser FunctionScanNodeInfo
parseFunctionScanNodeInfo object = do
  alias <- parseField @Text object "Alias"
  filter <- parseFieldMaybe' @Text object "Filter"
  functionName <- parseField @Text object "Function Name"
  rowsRemovedByFilter <- parseFieldMaybe' @Int object "Rows Removed by Filter"
  pure FunctionScanNodeInfo {..}

parseGatherNodeInfo :: Object -> Parser GatherNodeInfo
parseGatherNodeInfo object = do
  singleCopy <- parseField @Bool object "Single Copy"
  workersLaunched <- parseField @Int object "Workers Launched"
  workersPlanned <- parseField @Int object "Workers Planned"
  pure GatherNodeInfo {..}

parseHashJoinNodeInfo :: Object -> Parser HashJoinNodeInfo
parseHashJoinNodeInfo object = do
  hashCond <- parseField @Text object "Hash Cond"
  innerUnique <- parseField @Bool object "Inner Unique"
  joinType <- explicitParseField parseJoinType object "Join Type"
  pure HashJoinNodeInfo {..}

parseHashNodeInfo :: Object -> Parser HashNodeInfo
parseHashNodeInfo object = do
  hashBatches <- parseField @Int object "Hash Batches"
  hashBuckets <- parseField @Int object "Hash Buckets"
  originalHashBatches <- parseField @Int object "Original Hash Batches"
  originalHashBuckets <- parseField @Int object "Original Hash Buckets"
  peakMemoryUsage <- parseField @Int object "Peak Memory Usage"
  pure HashNodeInfo {..}

parseIndexOnlyScanNodeInfo :: Object -> Parser IndexOnlyScanNodeInfo
parseIndexOnlyScanNodeInfo object = do
  alias <- parseField @Text object "Alias"
  heapFetches <- parseField @Int object "Heap Fetches"
  indexCond <- parseFieldMaybe' @Text object "Index Cond"
  indexName <- parseField @Text object "Index Name"
  relationName <- parseField @Text object "Relation Name"
  rowsRemovedByIndexRecheck <- parseFieldMaybe' @Int object "Rows Removed by Index Recheck"
  scanDirection <- parseField @Text object "Scan Direction"
  pure IndexOnlyScanNodeInfo {..}

parseIndexScanNodeInfo :: Object -> Parser IndexScanNodeInfo
parseIndexScanNodeInfo object = do
  alias <- parseField @Text object "Alias"
  indexCond <- parseFieldMaybe' @Text object "Index Cond"
  indexName <- parseField @Text object "Index Name"
  relationName <- parseField @Text object "Relation Name"
  rowsRemovedByIndexRecheck <- parseFieldMaybe' @Int object "Rows Removed by Index Recheck"
  scanDirection <- parseField @Text object "Scan Direction"
  pure IndexScanNodeInfo {..}

parseJoinType :: Value -> Parser JoinType
parseJoinType =
  withText "JoinType" \case
    "Anti" -> pure AntiJoin
    "Full" -> pure FullJoin
    "Inner" -> pure InnerJoin
    "Left" -> pure LeftJoin
    "Right Anti" -> pure RightAntiJoin
    "Right" -> pure RightJoin
    "Semi" -> pure SemiJoin
    ty -> fail ("Unknown join type: " ++ Text.unpack ty)

parseMemoizeNodeInfo :: Object -> Parser MemoizeNodeInfo
parseMemoizeNodeInfo object = do
  cacheEvictions <- parseField @Int object "Cache Evictions"
  cacheHits <- parseField @Int object "Cache Hits"
  cacheKey <- parseField @Text object "Cache Key"
  cacheMisses <- parseField @Int object "Cache Misses"
  cacheMode <- parseField @Text object "Cache Mode"
  cacheOverflows <- parseField @Int object "Cache Overflows"
  peakMemoryUsage <- parseField @Int object "Peak Memory Usage"
  pure MemoizeNodeInfo {..}

parseMergeJoinNodeInfo :: Object -> Parser MergeJoinNodeInfo
parseMergeJoinNodeInfo object = do
  innerUnique <- parseField @Bool object "Inner Unique"
  joinType <- explicitParseField parseJoinType object "Join Type"
  mergeCond <- parseField @Text object "Merge Cond"
  pure MergeJoinNodeInfo {..}

parseNestedLoopNodeInfo :: Object -> Parser NestedLoopNodeInfo
parseNestedLoopNodeInfo object = do
  innerUnique <- parseField @Bool object "Inner Unique"
  joinType <- explicitParseField parseJoinType object "Join Type"
  pure NestedLoopNodeInfo {..}

parseNodeInfo :: Object -> Parser NodeInfo
parseNodeInfo object = do
  actualLoops <- parseField @Int object "Actual Loops"
  actualRows <- parseField @Int object "Actual Rows"
  actualStartupTime <- parseField @Double object "Actual Startup Time"
  actualTotalTime <- parseField @Double object "Actual Total Time"
  asyncCapable <- parseFieldMaybe' @Bool object "Async Capable"
  parallelAware <- parseField @Bool object "Parallel Aware"
  parentRelationship <- parseFieldMaybe' @Text object "Parent Relationship"
  planRows <- parseField @Int object "Plan Rows"
  planWidth <- parseField @Int object "Plan Width"
  plans <- fromMaybe [] <$> parseFieldMaybe' @[Node] object "Plans"
  startupCost <- parseField @Double object "Startup Cost"
  subplanName <- parseFieldMaybe' @Text object "Subplan Name"
  totalCost <- parseField @Double object "Total Cost"
  pure NodeInfo {..}

parseResultNodeInfo :: Object -> Parser ResultNodeInfo
parseResultNodeInfo object = do
  oneTimeFilter <- parseFieldMaybe' @Text object "One-Time Filter"
  pure ResultNodeInfo {..}

parseSeqScanNodeInfo :: Object -> Parser SeqScanNodeInfo
parseSeqScanNodeInfo object = do
  alias <- parseField @Text object "Alias"
  filter <- parseFieldMaybe' @Text object "Filter"
  relationName <- parseField @Text object "Relation Name"
  rowsRemovedByFilter <- parseFieldMaybe' @Int object "Rows Removed by Filter"
  pure SeqScanNodeInfo {..}

parseSetOpCommand :: Value -> Parser SetOpCommand
parseSetOpCommand =
  withText "SetOpCommand" \case
    "Except" -> pure SetOpCommandExcept
    "Except All" -> pure SetOpCommandExceptAll
    "Intersect" -> pure SetOpCommandIntersect
    "Intersect All" -> pure SetOpCommandIntersectAll
    command -> fail ("Unknown set op command: " ++ Text.unpack command)

parseSetOpNodeInfo :: Object -> Parser SetOpNodeInfo
parseSetOpNodeInfo object = do
  command <- explicitParseField parseSetOpCommand object "Command"
  strategy <- explicitParseField parseSetOpStrategy object "Strategy"
  pure SetOpNodeInfo {..}

parseSetOpStrategy :: Value -> Parser SetOpStrategy
parseSetOpStrategy =
  withText "SetOpStrategy" \case
    "Hashed" -> pure SetOpStrategyHashed
    "Sorted" -> pure SetOpStrategySorted
    strategy -> fail ("Unknown set op strategy: " ++ Text.unpack strategy)

parseSortNodeInfo :: Object -> Parser SortNodeInfo
parseSortNodeInfo object = do
  sortKey <- parseField @[Text] object "Sort Key"
  sortMethod <- parseField @Text object "Sort Method"
  sortSpaceType <- parseField @Text object "Sort Space Type"
  sortSpaceUsed <- parseField @Int object "Sort Space Used"
  pure SortNodeInfo {..}

parseSubqueryScanNodeInfo :: Object -> Parser SubqueryScanNodeInfo
parseSubqueryScanNodeInfo object = do
  alias <- parseField @Text object "Alias"
  pure SubqueryScanNodeInfo {..}

parseValuesScanNodeInfo :: Object -> Parser ValuesScanNodeInfo
parseValuesScanNodeInfo object = do
  alias <- parseField @Text object "Alias"
  pure ValuesScanNodeInfo {..}
