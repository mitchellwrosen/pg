module PgPlan where

import Data.Text (Text)

data AggregateNodeInfo = AggregateNodeInfo
  { diskUsage :: !(Maybe Int),
    groupKey :: !(Maybe [Text]),
    hashAggBatches :: !(Maybe Int),
    partialMode :: !Text,
    peakMemoryUsage :: !(Maybe Int),
    plannedPartitions :: !(Maybe Int),
    strategy :: !AggregateStrategy
  }
  deriving stock (Show)

data AggregateStrategy
  = AggregateStrategyHashed
  | AggregateStrategyMixed
  | AggregateStrategyPlain
  | AggregateStrategySorted
  deriving stock (Show)

data Analyze = Analyze
  { executionTime :: !Double,
    plan :: !Node,
    planningTime :: !Double
  }
  deriving stock (Show)

data AppendNodeInfo = AppendNodeInfo
  { subplansRemoved :: !Int
  }
  deriving stock (Show)

data BitmapHeapScanNodeInfo = BitmapHeapScanNodeInfo
  { alias :: !Text,
    exactHeapBlocks :: !Int,
    lossyHeapBlocks :: !Int,
    recheckCond :: !Text,
    relationName :: !Text,
    rowsRemovedByIndexRecheck :: !(Maybe Int)
  }
  deriving stock (Show)

data BitmapIndexScanNodeInfo = BitmapIndexScanNodeInfo
  { indexCond :: !(Maybe Text),
    indexName :: !Text
  }
  deriving stock (Show)

data FunctionScanNodeInfo = FunctionScanNodeInfo
  { alias :: !Text,
    filter :: !(Maybe Text),
    functionName :: !Text,
    rowsRemovedByFilter :: !(Maybe Int)
  }
  deriving stock (Show)

data GatherNodeInfo = GatherNodeInfo
  { singleCopy :: !Bool,
    workersLaunched :: !Int,
    workersPlanned :: !Int
  }
  deriving stock (Show)

data HashJoinNodeInfo = HashJoinNodeInfo
  { hashCond :: !Text,
    innerUnique :: !Bool,
    joinType :: !JoinType
  }
  deriving stock (Show)

data HashNodeInfo = HashNodeInfo
  { hashBatches :: !Int,
    hashBuckets :: !Int,
    originalHashBatches :: !Int,
    originalHashBuckets :: !Int,
    peakMemoryUsage :: !Int
  }
  deriving stock (Show)

data IndexOnlyScanNodeInfo = IndexOnlyScanNodeInfo
  { alias :: !Text,
    heapFetches :: !Int,
    indexCond :: !(Maybe Text),
    indexName :: !Text,
    relationName :: !Text,
    rowsRemovedByIndexRecheck :: !(Maybe Int),
    scanDirection :: !Text
  }
  deriving stock (Show)

data IndexScanNodeInfo = IndexScanNodeInfo
  { alias :: !Text,
    indexCond :: !(Maybe Text),
    indexName :: !Text,
    relationName :: !Text,
    rowsRemovedByIndexRecheck :: !(Maybe Int),
    scanDirection :: !Text,
    subplanName :: !(Maybe Text)
  }
  deriving stock (Show)

data JoinType
  = AntiJoin
  | FullJoin
  | InnerJoin
  | LeftJoin
  | RightAntiJoin
  | RightJoin
  | SemiJoin
  deriving stock (Show)

data MemoizeNodeInfo = MemoizeNodeInfo
  { cacheEvictions :: !Int,
    cacheHits :: !Int,
    cacheKey :: !Text,
    cacheMisses :: !Int,
    cacheMode :: !Text,
    cacheOverflows :: !Int,
    peakMemoryUsage :: !Int
  }
  deriving stock (Show)

data MergeJoinNodeInfo = MergeJoinNodeInfo
  { innerUnique :: !Bool,
    joinType :: !JoinType,
    mergeCond :: !Text
  }
  deriving stock (Show)

data NestedLoopNodeInfo = NestedLoopNodeInfo
  { innerUnique :: !Bool,
    joinType :: !JoinType
  }
  deriving stock (Show)

data Node
  = AggregateNode !NodeInfo !AggregateNodeInfo
  | AppendNode !NodeInfo !AppendNodeInfo
  | BitmapHeapScanNode !NodeInfo !BitmapHeapScanNodeInfo
  | BitmapIndexScanNode !NodeInfo !BitmapIndexScanNodeInfo
  | BitmapOrNode !NodeInfo
  | FunctionScanNode !NodeInfo !FunctionScanNodeInfo
  | GatherNode !NodeInfo !GatherNodeInfo
  | HashJoinNode !NodeInfo !HashJoinNodeInfo
  | HashNode !NodeInfo !HashNodeInfo
  | IndexOnlyScanNode !NodeInfo !IndexOnlyScanNodeInfo
  | IndexScanNode !NodeInfo !IndexScanNodeInfo
  | LimitNode !NodeInfo
  | MaterializeNode !NodeInfo
  | MemoizeNode !NodeInfo !MemoizeNodeInfo
  | MergeJoinNode !NodeInfo !MergeJoinNodeInfo
  | NestedLoopNode !NodeInfo !NestedLoopNodeInfo
  | ResultNode !NodeInfo !ResultNodeInfo
  | SeqScanNode !NodeInfo !SeqScanNodeInfo
  | SetOpNode !NodeInfo !SetOpNodeInfo
  | SortNode !NodeInfo !SortNodeInfo
  | SubqueryScanNode !NodeInfo !SubqueryScanNodeInfo
  | UniqueNode !NodeInfo
  | ValuesScanNode !NodeInfo !ValuesScanNodeInfo
  deriving stock (Show)

data NodeInfo = NodeInfo
  { actualLoops :: !Int,
    actualRows :: !Int,
    actualStartupTime :: !Double,
    actualTotalTime :: !Double,
    asyncCapable :: !(Maybe Bool),
    parallelAware :: !Bool,
    parentRelationship :: !(Maybe Text),
    planRows :: !Int,
    planWidth :: !Int,
    plans :: ![Node],
    startupCost :: !Double,
    totalCost :: !Double
  }
  deriving stock (Show)

data ResultNodeInfo = ResultNodeInfo
  { oneTimeFilter :: !(Maybe Text)
  }
  deriving stock (Show)

data SeqScanNodeInfo = SeqScanNodeInfo
  { alias :: !Text,
    filter :: !(Maybe Text),
    relationName :: !Text,
    rowsRemovedByFilter :: !(Maybe Int)
  }
  deriving stock (Show)

data SetOpCommand
  = SetOpCommandExcept
  | SetOpCommandExceptAll
  | SetOpCommandIntersect
  | SetOpCommandIntersectAll
  deriving stock (Show)

data SetOpNodeInfo = SetOpNodeInfo
  { command :: !SetOpCommand,
    strategy :: !SetOpStrategy
  }
  deriving stock (Show)

data SetOpStrategy
  = SetOpStrategyHashed
  | SetOpStrategySorted
  deriving stock (Show)

data SortNodeInfo = SortNodeInfo
  { sortKey :: ![Text],
    sortMethod :: !Text,
    sortSpaceType :: !Text,
    sortSpaceUsed :: !Int
  }
  deriving stock (Show)

data SubqueryScanNodeInfo = SubqueryScanNodeInfo
  { alias :: !Text
  }
  deriving stock (Show)

data ValuesScanNodeInfo = ValuesScanNodeInfo
  { alias :: !Text
  }
  deriving stock (Show)
