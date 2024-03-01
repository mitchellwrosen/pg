module PgPlanPretty
  ( prettyAnalyze,
  )
where

import Data.Fixed (mod')
import Data.Foldable (fold)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Lazy.Builder qualified as Builder
import Data.Text.Lazy.Builder.RealFloat qualified as Builder
import GHC.Float.RealFracMethods (floorDoubleInt, roundDoubleInt)
import PgPlan
import Prettyprinter
import Prettyprinter.Render.Terminal (AnsiStyle, Color (..), bold, color, colorDull, italicized)
import Text.Printf (printf)
import Prelude hiding (filter)

data Vnode = Vnode
  { actualRows :: !Int,
    children :: [Vnode],
    loops :: !Int,
    metadata :: ![Doc AnsiStyle],
    predictedRows :: !Int,
    rowBytes :: !Int,
    startupCost :: !Double,
    startupTime :: !Double,
    summary :: !(Doc AnsiStyle),
    totalCost :: !Double,
    totalTime :: !Double
  }

makeVnode :: NodeInfo -> Vnode
makeVnode info =
  Vnode
    { actualRows = info.actualRows * info.actualLoops,
      children = map nodeVnode info.plans,
      loops = info.actualLoops,
      metadata = [],
      predictedRows = info.planRows * info.actualLoops,
      rowBytes = info.planWidth,
      startupCost = info.startupCost,
      startupTime = info.actualStartupTime * realToFrac @Int @Double info.actualLoops,
      summary = mempty,
      totalCost = info.totalCost,
      totalTime = info.actualTotalTime * realToFrac @Int @Double info.actualLoops
    }

prettyVnode :: Bool -> Vnode -> Doc AnsiStyle
prettyVnode indentAfterRows node =
  fold
    [ "⮬ ",
      annRows
        ( prettyInt node.actualRows
            <> (if node.actualRows == 1 then " row" else " rows")
            <> ( if node.actualRows /= node.predictedRows
                   then " (" <> prettyInt node.predictedRows <> " predicted)"
                   else mempty
               )
        ),
      ", ",
      annBytes (prettyBytes node.rowBytes <> (if node.actualRows == 1 then mempty else " each")),
      (if indentAfterRows then nest 4 else id) $
        fold
          [ line,
            "╭─",
            line,
            vbar,
            annotate
              bold
              ( if node.loops == 1
                  then node.summary
                  else prettyInt node.loops <> "x " <> node.summary
              ),
            line,
            vsep (map (vbar <>) node.metadata),
            if null node.metadata then mempty else line,
            vbar,
            annotate
              (colorDull Yellow)
              ( prettyMilliseconds node.startupTime
                  <> (if node.startupTime == node.totalTime then mempty else " → " <> prettyMilliseconds node.totalTime)
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

nodeVnode :: Node -> Vnode
nodeVnode = \case
  AggregateNode info info2 -> aggregateVnode info info2
  AppendNode info info2 -> appendVnode info info2
  BitmapHeapScanNode info info2 -> bitmapHeapScanVnode info info2
  BitmapIndexScanNode info info2 -> bitmapIndexScanVnode info info2
  BitmapOrNode info -> bitmapOrVnode info
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
  (makeVnode info)
    { metadata =
        case info2.peakMemoryUsage of
          Nothing -> []
          Just mem -> ["Peak memory usage: " <> annBytes (prettyKilobytes mem)],
      summary =
        case info2.strategy of
          AggregateStrategyHashed ->
            "Group rows "
              <> case info2.groupKey of
                Nothing -> mempty
                Just key -> "by " <> prettyKey key
              <> "with internal hash table"
          AggregateStrategyPlain -> "Reduce to a single value"
          AggregateStrategyMixed -> "Mixed aggregation" -- TODO make this better
          AggregateStrategySorted ->
            "Group sorted rows"
              <> case info2.groupKey of
                Nothing -> mempty
                Just key -> " by " <> prettyKey key
    }
  where
    prettyKey key =
      hsep
        ( punctuate
            comma
            (map (annotate italicized . pretty) key)
        )

appendVnode :: NodeInfo -> AppendNodeInfo -> Vnode
appendVnode info _info2 =
  (makeVnode info)
    { summary = "Emit all rows"
    }

bitmapHeapScanVnode :: NodeInfo -> BitmapHeapScanNodeInfo -> Vnode
bitmapHeapScanVnode info info2 =
  (makeVnode info)
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
  (makeVnode info)
    { summary =
        "Access "
          <> case info2.indexCond of
            Nothing -> "every page of index " <> annIndex info2.indexName
            Just cond -> "index " <> annIndex info2.indexName <> " at " <> annExpr cond
          <> ", build bitmap of pages to visit"
    }

bitmapOrVnode :: NodeInfo -> Vnode
bitmapOrVnode info =
  (makeVnode info)
    { summary = "Bitwise-or the bitmaps"
    }

functionScanVnode :: NodeInfo -> FunctionScanNodeInfo -> Vnode
functionScanVnode info info2 =
  (makeVnode info)
    { metadata = filterLine info2.filter info2.rowsRemovedByFilter,
      summary = "Execute function " <> annotate italicized (pretty info2.functionName)
    }

gatherVnode :: NodeInfo -> GatherNodeInfo -> Vnode
gatherVnode info info2 =
  (makeVnode info)
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
  (makeVnode info)
    { metadata = ["Peak memory usage: " <> annBytes (prettyKilobytes info2.peakMemoryUsage)],
      summary =
        "Build a "
          <> (if innerUnique then "k→v" else "k→vs")
          <> " hash table"
    }

hashJoinVnode :: NodeInfo -> HashJoinNodeInfo -> Vnode
hashJoinVnode info info2 =
  (makeVnode info)
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
  (makeVnode info)
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

indexScanVnode :: NodeInfo -> IndexScanNodeInfo -> Vnode
indexScanVnode info info2 =
  (makeVnode info)
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

limitVnode :: NodeInfo -> Vnode
limitVnode info =
  (makeVnode info)
    { summary = "Only emit a number of rows"
    }

materializeVnode :: NodeInfo -> Vnode
materializeVnode info =
  (makeVnode info)
    { summary = "Materialize"
    }

memoizeVnode :: NodeInfo -> MemoizeNodeInfo -> Vnode
memoizeVnode info _info2 =
  (makeVnode info)
    { summary = "Memoize"
    }

mergeJoinVnode :: NodeInfo -> MergeJoinNodeInfo -> Vnode
mergeJoinVnode info info2 =
  (makeVnode info)
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
  (makeVnode info)
    { summary =
        prettyJoinType info2.joinType
          <> " nested loop join"
          <> if info2.innerUnique then " (inner unique)" else mempty
    }

resultVnode :: NodeInfo -> ResultNodeInfo -> Vnode
resultVnode info _info2 =
  (makeVnode info)
    { summary = "Emit all rows"
    }

seqScanVnode :: NodeInfo -> SeqScanNodeInfo -> Vnode
seqScanVnode info info2 =
  (makeVnode info)
    { metadata = filterLine info2.filter info2.rowsRemovedByFilter,
      summary =
        "Access table "
          <> annTable info2.relationName
          <> (if info2.alias /= info2.relationName then " as " <> annAlias info2.alias else mempty)
    }

setOpVnode :: NodeInfo -> SetOpNodeInfo -> Vnode
setOpVnode info info2 =
  (makeVnode info)
    { summary =
        fold
          [ case info2.command of
              SetOpCommandExcept -> "Except "
              SetOpCommandExceptAll -> "Except all "
              SetOpCommandIntersect -> "Intersect "
              SetOpCommandIntersectAll -> "Intersect all ",
            case info2.strategy of
              SetOpStrategyHashed -> "(hashed)"
              SetOpStrategySorted -> "(sorted)"
          ]
    }

sortVnode :: NodeInfo -> SortNodeInfo -> Vnode
sortVnode info info2 =
  (makeVnode info)
    { metadata =
        [ case info2.sortSpaceType of
            "Memory" -> "Memory: " <> annBytes (prettyKilobytes info2.sortSpaceUsed)
            "Disk" -> "Disk: " <> annBytes (prettyKilobytes info2.sortSpaceUsed)
            ty -> error ("Unknown sort type: " ++ Text.unpack ty)
        ],
      summary =
        fold
          [ pretty @Text case info2.sortMethod of
              "external merge" -> "On-disk mergesort on "
              "quicksort" -> "Quicksort on "
              "top-N heapsort" -> "Top-N heapsort on "
              method -> error ("Unknown sort method: " <> Text.unpack method),
            hsep (punctuate comma (map (annotate italicized . pretty) info2.sortKey))
          ]
    }

subqueryScanVnode :: NodeInfo -> SubqueryScanNodeInfo -> Vnode
subqueryScanVnode info _info2 =
  (makeVnode info)
    { summary = "Subquery scan"
    }

uniqueVnode :: NodeInfo -> Vnode
uniqueVnode info =
  (makeVnode info)
    { summary = "Only emit distinct rows"
    }

valuesScanVnode :: NodeInfo -> ValuesScanNodeInfo -> Vnode
valuesScanVnode info _info2 =
  (makeVnode info)
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

prettyBytes :: Int -> Doc a
prettyBytes bytes =
  prettyInt bytes <> if bytes == 1 then " byte" else " bytes"

prettyCost :: Double -> Doc a
prettyCost cost =
  prettyInt dollars
    <> "."
    <> (if cents < 10 then "0" else mempty)
    <> pretty cents
  where
    dollars = floorDoubleInt cost
    cents = roundDoubleInt (mod' cost 1 * 100)

prettyDouble :: Int -> Double -> Doc a
prettyDouble i =
  pretty . Builder.toLazyText . Builder.formatRealFloat Builder.Fixed (Just i)

prettyInt :: Int -> Doc a
prettyInt =
  let loop acc d
        | d < 1000 = d : acc
        | otherwise = let (x, y) = divMod d 1000 in loop (y : acc) x
      pp [] = ""
      pp (n : ns) = Text.pack (show n) <> pps ns
      pps = Text.concat . map (("," <>) . pp1)
      pp1 = Text.pack . printf "%03d" :: Int -> Text
   in pretty . pp . loop []

prettyJoinType :: JoinType -> Doc a
prettyJoinType = \case
  AntiJoin -> "Anti"
  FullJoin -> "Full"
  InnerJoin -> "Inner"
  LeftJoin -> "Left"
  RightAntiJoin -> "Right anti"
  RightJoin -> "Right"
  SemiJoin -> "Semi"

prettyKilobytes :: Int -> Doc a
prettyKilobytes kb
  | kb < 995 = pretty kb <> " kb"
  | kb < 9_950 = prettyDouble 2 mb
  | kb < 99_500 = prettyDouble 1 mb
  | kb < 995_000 = prettyDouble 0 mb
  | kb < 9_950_000 = prettyDouble 2 gb
  | kb < 99_500_000 = prettyDouble 1 gb
  | otherwise = prettyDouble 0 gb
  where
    mb = realToFrac @Int @Double kb / 1_000
    gb = realToFrac @Int @Double kb / 1_000_000

prettyMilliseconds :: Double -> Doc AnsiStyle
prettyMilliseconds ms
  | us < 0.5 = "0 us"
  | us < 995 = prettyDouble 0 us <> " µs"
  | us < 9_950 = prettyDouble 2 ms <> " ms"
  | us < 99_500 = prettyDouble 1 ms <> " ms"
  | ms < 995 = prettyDouble 0 ms <> " ms"
  | ms < 9_950 = prettyDouble 2 s <> " s"
  | ms < 99_500 = prettyDouble 1 s <> " s"
  | otherwise = prettyDouble 0 s <> " s"
  where
    us = ms * 1000
    s = ms / 1_000

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
