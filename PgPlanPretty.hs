module PgPlanPretty
  ( prettyAnalyze,
  )
where

import Data.Fixed (mod')
import Data.Foldable (fold)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import GHC.Float.RealFracMethods (floorDoubleInt, roundDoubleInt)
import PgPlan
import PgPrettyUtils
import Prettyprinter
import Prettyprinter.Render.Terminal (AnsiStyle, Color (..), bold, color, colorDull, italicized)
import Prelude hiding (filter)

data Vnode = Vnode
  { actualRowsPerLoop :: !Int,
    children :: [Vnode],
    loops :: !Int,
    metadata :: ![Doc AnsiStyle],
    name :: !(Maybe Text),
    nodeName :: !Text,
    predictedRowsPerLoop :: !Int,
    relationship :: !(Maybe Text),
    rowBytes :: !Int,
    startupCost :: !Double,
    startupTimePerLoop :: !Double,
    summary :: !(Doc AnsiStyle),
    totalCost :: !Double,
    totalTimePerLoop :: !Double
  }

makeVnode :: Text -> NodeInfo -> Vnode
makeVnode nodeName info =
  Vnode
    { actualRowsPerLoop = info.actualRows,
      children = map nodeVnode info.plans,
      loops = info.actualLoops,
      metadata = [],
      name =
        case info.parentRelationship of
          Just "SubPlan" -> Just "Correlated subquery, run once for every row emitted by parent"
          _ -> info.subplanName,
      nodeName =
        let parallel = if info.parallelAware then ("Parallel " <>) else id
            async = if fromMaybe False info.asyncCapable then ("Async " <>) else id
         in parallel (async nodeName),
      predictedRowsPerLoop = info.planRows,
      relationship = info.parentRelationship,
      rowBytes = info.planWidth,
      startupCost = info.startupCost,
      startupTimePerLoop = info.actualStartupTime,
      summary = mempty,
      totalCost = info.totalCost,
      totalTimePerLoop = info.actualTotalTime
    }

prettyVnode :: Bool -> Vnode -> Doc AnsiStyle
prettyVnode indentAfterRows node =
  fold
    [ case node.relationship of
        Just "InitPlan" -> "● "
        _ -> "⮬ ",
      annRows
        ( prettyInt actualRows
            <> (if actualRows == 1 then " row" else " rows")
            <> ( if actualRows /= predictedRows
                   then " (" <> prettyInt predictedRows <> " predicted)"
                   else mempty
               )
        ),
      ", ",
      annBytes (prettyBytes node.rowBytes <> (if actualRows == 1 then mempty else " each")),
      (if indentAfterRows then nest 4 else id) $
        fold
          [ line,
            "╭─",
            line,
            case node.name of
              Nothing -> mempty
              Just s -> vbar <> annotate (color Black) ("# " <> pretty s) <> line,
            vbar,
            annotate bold (pretty node.nodeName),
            if node.loops == 1 then mempty else annotate italicized (" (" <> prettyInt node.loops <> "x)"),
            line,
            vsep (map (vbar <>) [node.summary]),
            line,
            vsep (map (vbar <>) node.metadata),
            if null node.metadata then mempty else line,
            vbar,
            annotate
              (colorDull Yellow)
              ( prettyMilliseconds startupTime
                  <> (if startupTime == totalTime then mempty else " → " <> prettyMilliseconds totalTime)
              ),
            annotate (color Black) " │ ",
            annotate
              (colorDull Green)
              ( "$"
                  <> prettyCost node.startupCost
                  <> (if node.startupCost == node.totalCost then mempty else " → $" <> prettyCost node.totalCost)
              ),
            line,
            "╰─",
            case node.children of
              [] -> mempty
              [child] -> line <> prettyVnode False child
              children -> line <> vsep (map (prettyVnode True) children)
          ]
    ]
  where
    actualRows = node.actualRowsPerLoop * node.loops
    predictedRows = node.predictedRowsPerLoop * node.loops
    startupTime = node.startupTimePerLoop * realToFrac @Int @Double node.loops
    totalTime = node.totalTimePerLoop * realToFrac @Int @Double node.loops

nodeVnode :: Node -> Vnode
nodeVnode = \case
  AggregateNode info info2 -> aggregateVnode info info2
  AppendNode info info2 -> appendVnode info info2
  BitmapHeapScanNode info info2 -> bitmapHeapScanVnode info info2
  BitmapIndexScanNode info info2 -> bitmapIndexScanVnode info info2
  BitmapOrNode info -> bitmapOrVnode info
  CteScanNode info info2 -> cteScanVnode info info2
  FunctionScanNode info info2 -> functionScanVnode info info2
  GatherNode info info2 -> gatherVnode info info2
  HashJoinNode info info2 -> hashJoinVnode info info2
  HashNode {} -> error "unexpected HashNode"
  IndexOnlyScanNode info info2 -> indexOnlyScanVnode info info2
  IndexScanNode info info2 -> indexScanVnode info info2
  LimitNode info -> limitVnode info
  MaterializeNode info -> materializeVnode info
  MemoizeNode info info2 -> memoizeVnode info info2
  MergeJoinNode info info2 -> mergeJoinVnode info info2
  NestedLoopNode info info2 -> nestedLoopVnode info info2
  ResultNode info info2 -> resultVnode info info2
  SeqScanNode info info2 -> seqScanVnode info info2
  SetOpNode info info2 -> setOpVnode info info2
  SortNode info info2 -> sortVnode info info2
  SubqueryScanNode info info2 -> subqueryScanVnode info info2
  UniqueNode info -> uniqueVnode info
  ValuesScanNode info info2 -> valuesScanVnode info info2

aggregateVnode :: NodeInfo -> AggregateNodeInfo -> Vnode
aggregateVnode info info2 =
  (makeVnode name info)
    { metadata =
        case info2.peakMemoryUsage of
          Nothing -> []
          Just mem -> ["Peak memory usage: " <> annBytes (prettyBytes (mem * 1_000))],
      summary =
        case info2.strategy of
          AggregateStrategyHashed ->
            "Group rows "
              <> case info2.groupKey of
                Nothing -> mempty
                Just key -> "by " <> prettyKey key <> " "
              <> "with a hash table"
          AggregateStrategyPlain -> "Reduce to a single value"
          AggregateStrategyMixed -> "Mixed aggregation" -- TODO make this better
          AggregateStrategySorted ->
            "Group sorted rows"
              <> case info2.groupKey of
                Nothing -> mempty
                Just key -> " by " <> prettyKey key
    }
  where
    name =
      case info2.strategy of
        AggregateStrategyHashed -> "HashAggregate"
        AggregateStrategyMixed -> "MixedAggregate"
        AggregateStrategyPlain -> "Aggregate"
        AggregateStrategySorted -> "GroupAggregate"

    prettyKey key =
      hsep
        ( punctuate
            comma
            (map (annotate italicized . pretty) key)
        )

appendVnode :: NodeInfo -> AppendNodeInfo -> Vnode
appendVnode info _info2 =
  (makeVnode "Append" info)
    { summary = "Emit all rows from first child, then second, and so on (async children interspersed)"
    }

bitmapHeapScanVnode :: NodeInfo -> BitmapHeapScanNodeInfo -> Vnode
bitmapHeapScanVnode info info2 =
  (makeVnode "Bitmap Heap Scan" info)
    { metadata = filterLine (Just info2.recheckCond) info2.rowsRemovedByIndexRecheck,
      summary =
        "Access bitmapped pages in table "
          <> annTable info2.relationName
          <> if info2.alias /= info2.relationName
            then " as " <> annAlias info2.alias
            else mempty
    }

bitmapIndexScanVnode :: NodeInfo -> BitmapIndexScanNodeInfo -> Vnode
bitmapIndexScanVnode info info2 =
  (makeVnode "Bitmap Index Scan" info)
    { summary =
        "Access "
          <> case info2.indexCond of
            Nothing -> "every page of index " <> annIndex info2.indexName
            Just cond -> "index " <> annIndex info2.indexName <> " at " <> annExpr cond
          <> ", build bitmap of pages to visit"
    }

bitmapOrVnode :: NodeInfo -> Vnode
bitmapOrVnode info =
  (makeVnode "Bitmap Or" info)
    { summary = "Bitwise-or the bitmaps"
    }

cteScanVnode :: NodeInfo -> CteScanNodeInfo -> Vnode
cteScanVnode info info2 =
  (makeVnode "CTE Scan" info)
    { summary = "Scan CTE " <> annotate italicized (pretty info2.cteName)
    }

functionScanVnode :: NodeInfo -> FunctionScanNodeInfo -> Vnode
functionScanVnode info info2 =
  (makeVnode "Function Scan" info)
    { metadata = filterLine info2.filter info2.rowsRemovedByFilter,
      summary = "Execute function " <> annotate italicized (pretty info2.functionName)
    }

gatherVnode :: NodeInfo -> GatherNodeInfo -> Vnode
gatherVnode info info2 =
  (makeVnode "Gather" info)
    { summary =
        "Parallelize with "
          <> pretty info2.workersLaunched
          <> " additional threads"
          <> if info2.workersLaunched /= info2.workersPlanned
            then " (of " <> pretty info2.workersPlanned <> " requested)"
            else mempty
    }

hashVnode :: Bool -> NodeInfo -> HashNodeInfo -> Vnode
hashVnode innerUnique info info2 =
  (makeVnode "Hash" info)
    { metadata = ["Peak memory usage: " <> annBytes (prettyBytes (info2.peakMemoryUsage * 1_000))],
      summary =
        "Build a "
          <> (if innerUnique then "k→v" else "k→vs")
          <> " hash table"
    }

hashJoinVnode :: NodeInfo -> HashJoinNodeInfo -> Vnode
hashJoinVnode info info2 =
  (makeVnode "Hash Join" info)
    { children =
        case info.plans of
          [outer, HashNode innerInfo innerInfo2] -> [nodeVnode outer, hashVnode info2.innerUnique innerInfo innerInfo2]
          _ -> error "bogus hash join",
      summary =
        fold
          [ prettyJoinType info2.joinType,
            " hash join",
            (if info2.innerUnique then " (inner unique)" else mempty),
            " on ",
            annExpr info2.hashCond
          ]
    }

indexOnlyScanVnode :: NodeInfo -> IndexOnlyScanNodeInfo -> Vnode
indexOnlyScanVnode info info2 =
  (makeVnode name info)
    { summary =
        fold
          [ "Access ",
            case info2.indexCond of
              Nothing -> "every page of index " <> annIndex info2.indexName
              Just cond -> "index " <> annIndex info2.indexName <> " at " <> annExpr cond,
            if info2.heapFetches > 0
              then
                "; access table "
                  <> annTable info2.relationName
                  <> " "
                  <> prettyInt info2.heapFetches
                  <> if info2.heapFetches == 1 then " time" else " times"
              else mempty
          ]
    }
  where
    name =
      case info2.scanDirection of
        ScanDirectionBackward -> "Index Only Scan Backward"
        ScanDirectionForward -> "Index Only Scan"

indexScanVnode :: NodeInfo -> IndexScanNodeInfo -> Vnode
indexScanVnode info info2 =
  (makeVnode name info)
    { summary =
        fold
          [ "Access ",
            case info2.indexCond of
              Nothing -> "every page of index " <> annIndex info2.indexName
              Just cond -> "index " <> annIndex info2.indexName <> " at " <> annExpr cond,
            "; for each row access table ",
            annTable info2.relationName
          ]
    }
  where
    name =
      case info2.scanDirection of
        ScanDirectionBackward -> "Index Scan Backward"
        ScanDirectionForward -> "Index Scan"

limitVnode :: NodeInfo -> Vnode
limitVnode info =
  let vnode = makeVnode "Limit" info
   in vnode
        { summary = "Limit " <> prettyInt vnode.actualRowsPerLoop
        }

materializeVnode :: NodeInfo -> Vnode
materializeVnode info =
  (makeVnode "Materialize" info)
    { summary = "Materialize"
    }

memoizeVnode :: NodeInfo -> MemoizeNodeInfo -> Vnode
memoizeVnode info _info2 =
  (makeVnode "Memoize" info)
    { summary = "Memoize"
    }

mergeJoinVnode :: NodeInfo -> MergeJoinNodeInfo -> Vnode
mergeJoinVnode info info2 =
  (makeVnode "Merge Join" info)
    { summary =
        fold
          [ prettyJoinType info2.joinType,
            " merge join",
            (if info2.innerUnique then " (inner unique)" else mempty),
            " on ",
            annExpr info2.mergeCond
          ]
    }

nestedLoopVnode :: NodeInfo -> NestedLoopNodeInfo -> Vnode
nestedLoopVnode info info2 =
  (makeVnode "Nested Loop" info)
    { summary =
        prettyJoinType info2.joinType
          <> " nested loop join"
          <> if info2.innerUnique then " (inner unique)" else mempty
    }

resultVnode :: NodeInfo -> ResultNodeInfo -> Vnode
resultVnode info _info2 =
  (makeVnode "Result" info)
    { summary = "Result"
    }

seqScanVnode :: NodeInfo -> SeqScanNodeInfo -> Vnode
seqScanVnode info info2 =
  (makeVnode "Seq Scan" info)
    { metadata = filterLine info2.filter info2.rowsRemovedByFilter,
      summary =
        "Access table "
          <> annTable info2.relationName
          <> (if info2.alias /= info2.relationName then " as " <> annAlias info2.alias else mempty)
    }

setOpVnode :: NodeInfo -> SetOpNodeInfo -> Vnode
setOpVnode info info2 =
  (makeVnode name info)
    { summary =
        fold
          [ let except = "rows that are in the first relation but not in the second"
                intersect = "rows that are in both relations"
             in hsep
                  [ "Emit",
                    case info2.command of
                      SetOpCommandExcept -> "distinct"
                      SetOpCommandExceptAll -> "all"
                      SetOpCommandIntersect -> "distinct"
                      SetOpCommandIntersectAll -> "all",
                    case info2.command of
                      SetOpCommandExcept -> except
                      SetOpCommandExceptAll -> except
                      SetOpCommandIntersect -> intersect
                      SetOpCommandIntersectAll -> intersect
                  ],
            case info2.strategy of
              SetOpStrategyHashed -> " using a hash table of row counts"
              SetOpStrategySorted -> mempty
          ]
    }
  where
    name =
      Text.unwords
        [ case info2.strategy of
            SetOpStrategyHashed -> "HashSetOp"
            SetOpStrategySorted -> "SetOp",
          case info2.command of
            SetOpCommandExcept -> "Except"
            SetOpCommandExceptAll -> "Except All"
            SetOpCommandIntersect -> "Intersect"
            SetOpCommandIntersectAll -> "Intersect All"
        ]

sortVnode :: NodeInfo -> SortNodeInfo -> Vnode
sortVnode info info2 =
  let vnode = makeVnode "Sort" info
   in vnode
        { metadata =
            [ case info2.sortSpaceType of
                "Memory" -> "Memory: " <> annBytes (prettyBytes (info2.sortSpaceUsed * 1_000))
                "Disk" -> "Disk: " <> annBytes (prettyBytes (info2.sortSpaceUsed * 1_000))
                ty -> error ("Unknown sort type: " ++ Text.unpack ty)
            ],
          summary =
            fold
              [ case info2.sortMethod of
                  "external merge" -> "On-disk mergesort on "
                  "quicksort" -> "Quicksort on "
                  "top-N heapsort" -> "Top-" <> prettyInt vnode.actualRowsPerLoop <> " heapsort on "
                  method -> error ("Unknown sort method: " <> Text.unpack method),
                hsep (punctuate comma (map (annotate italicized . pretty) info2.sortKey))
              ]
        }

subqueryScanVnode :: NodeInfo -> SubqueryScanNodeInfo -> Vnode
subqueryScanVnode info info2 =
  (makeVnode "Subquery Scan" info)
    { summary = "Scan subquery " <> annotate italicized (pretty info2.alias)
    }

uniqueVnode :: NodeInfo -> Vnode
uniqueVnode info =
  (makeVnode "Unique" info)
    { summary = "Only emit distinct rows"
    }

valuesScanVnode :: NodeInfo -> ValuesScanNodeInfo -> Vnode
valuesScanVnode info _info2 =
  (makeVnode "Values Scan" info)
    { summary = "Emit row literals"
    }

prettyAnalyze :: Analyze -> Doc AnsiStyle
prettyAnalyze analyze =
  fold
    [ prettyVnode False (nodeVnode analyze.plan),
      line,
      "Total time: ",
      prettyMilliseconds (analyze.planningTime + analyze.executionTime),
      " (",
      prettyMilliseconds analyze.planningTime,
      " to plan, ",
      prettyMilliseconds analyze.executionTime,
      " to execute)"
    ]

prettyCost :: Double -> Doc a
prettyCost cost =
  prettyInt dollars
    <> "."
    <> (if cents < 10 then "0" else mempty)
    <> pretty cents
  where
    dollars = floorDoubleInt cost
    cents = roundDoubleInt (mod' cost 1 * 100)

prettyJoinType :: JoinType -> Doc a
prettyJoinType = \case
  AntiJoin -> "Anti"
  FullJoin -> "Full"
  InnerJoin -> "Inner"
  LeftJoin -> "Left"
  RightAntiJoin -> "Right anti"
  RightJoin -> "Right"
  SemiJoin -> "Semi"

filterLine :: Maybe Text -> Maybe Int -> [Doc AnsiStyle]
filterLine filter maybeRemoved =
  case (filter, maybeRemoved) of
    (Just cond, Just removed) ->
      [ "Filter: "
          <> annExpr cond
          <> " removed "
          <> annRows (prettyInt removed <> (if removed == 1 then " row" else " rows"))
      ]
    (Just cond, Nothing) -> ["Filter: " <> annExpr cond]
    _ -> []

vbar :: Doc a
vbar =
  "│"

annAlias :: Text -> Doc AnsiStyle
annAlias =
  annotate italicized . pretty

annBytes :: Doc AnsiStyle -> Doc AnsiStyle
annBytes =
  annotate (color Green)

annExpr :: Text -> Doc AnsiStyle
annExpr =
  annotate (color Blue) . pretty

annIndex :: Text -> Doc AnsiStyle
annIndex =
  annotate (color Magenta <> italicized) . pretty

annRows :: Doc AnsiStyle -> Doc AnsiStyle
annRows =
  annotate (colorDull Cyan)

annTable :: Text -> Doc AnsiStyle
annTable =
  annotate (color Red <> italicized) . pretty
