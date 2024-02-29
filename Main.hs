{-# LANGUAGE RecordWildCards #-}

module Main (main) where

import Control.Applicative (asum)
import Control.Exception (bracket)
import Control.Monad (when)
import Data.Aeson qualified as Aeson
import Data.Aeson.Types qualified as Aeson (Parser, parseField, parseFieldMaybe')
import Data.ByteString qualified as ByteString
import Data.Fixed (mod')
import Data.Foldable (fold)
import Data.Function ((&))
import Data.Functor (void)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Data.Text.Lazy.Builder qualified as Builder
import Data.Text.Lazy.Builder.RealFloat qualified as Builder
import GHC.Float.RealFracMethods (floorDoubleInt, roundDoubleInt)
import Options.Applicative (optional)
import Options.Applicative qualified as Opt
import Prettyprinter qualified
import Prettyprinter.Render.Terminal qualified
import System.Directory qualified as Directory
import System.Exit (ExitCode (..), exitFailure, exitWith)
import System.IO qualified as IO
import System.Posix.ByteString qualified as Posix
import System.Process qualified as Process
import Text.Pretty.Simple (pPrint)
import Text.Printf (printf)
import Text.Read (readMaybe)
import Prelude hiding (filter, lines, read)

main :: IO ()
main = do
  commandLineInterface
    [ Opt.showHelpOnEmpty,
      Opt.showHelpOnError
    ]
    [ Opt.progDesc "Postgres utility knife."
    ]
    [ subcommands
        [ subcommand
            [ Opt.progDesc "Create a Postgres cluster."
            ]
            "create"
            (pure pgCreate),
          subcommand
            [ Opt.progDesc "Stop a Postgres cluster."
            ]
            "down"
            (pure pgDown),
          subcommand
            [ Opt.progDesc "Run a query on a Postgres cluster."
            ]
            "exec"
            (pgExec <$> textArg [Opt.metavar "QUERY"]),
          subcommand
            [ Opt.progDesc "Explain a query."
            ]
            "explain"
            ( pgExplain
                <$> textOpt [Opt.metavar "DBNAME", Opt.short 'd']
                <*> textArg [Opt.metavar "QUERY"]
            ),
          subcommand
            [ Opt.progDesc "Load a Postgres database."
            ]
            "load"
            ( pgLoad
                <$> textOpt [Opt.metavar "DBNAME", Opt.short 'd']
                <*> textArg [Opt.metavar "FILENAME"]
            ),
          subcommand
            [ Opt.progDesc "Connect to a Postgres cluster."
            ]
            "repl"
            (pure pgRepl),
          subcommand
            [ Opt.progDesc "Start a Postgres cluster."
            ]
            "up"
            (pure pgUp)
        ]
    ]

pgCreate :: IO ()
pgCreate = do
  stateDir <- getStateDir
  let clusterDir = stateDir <> "/data"
  Directory.doesDirectoryExist (Text.unpack clusterDir) >>= \case
    True -> Text.putStrLn ("There's already a Postgres cluster at " <> clusterDir)
    False -> do
      (out, err, code) <-
        process
          "initdb"
          [ "--encoding=UTF8",
            "--locale=en_US.UTF-8",
            "--no-sync",
            "--pgdata=" <> clusterDir
          ]
      when (code /= ExitSuccess) do
        Text.putStr out
        Text.putStr err
        exitWith code

pgDown :: IO ()
pgDown = do
  stateDir <- getStateDir
  let clusterDir = stateDir <> "/data"
  let postmasterFile = clusterDir <> "/postmaster.pid"
  Directory.doesFileExist (Text.unpack postmasterFile) >>= \case
    False -> Text.putStrLn ("There's no cluster running at " <> clusterDir)
    True ->
      (Text.lines <$> Text.readFile (Text.unpack postmasterFile)) >>= \case
        (readMaybe . Text.unpack -> Just pid) : _ -> Posix.signalProcess Posix.sigTERM pid
        _ -> Text.putStrLn ("Could not read PID from " <> postmasterFile)

pgExec :: Text -> IO ()
pgExec query = do
  stateDir <- getStateDir
  let clusterDir = stateDir <> "/data"
  Directory.doesFileExist (Text.unpack (clusterDir <> "/postmaster.pid")) >>= \case
    False -> Text.putStrLn ("There's no cluster running at " <> clusterDir)
    True -> do
      (out, err, code) <-
        process
          "psql"
          [ "--command=" <> query,
            "--dbname=postgres",
            "--host=" <> stateDir
          ]
      Text.putStr out
      Text.putStr err
      exitWith code

pgExplain :: Maybe Text -> Text -> IO ()
pgExplain maybeDatabase queryOrFilename = do
  stateDir <- getStateDir
  let clusterDir = stateDir <> "/data"
  Directory.doesFileExist (Text.unpack (clusterDir <> "/postmaster.pid")) >>= \case
    False -> Text.putStrLn ("There's no cluster running at " <> clusterDir)
    True -> do
      query <-
        Directory.doesFileExist (Text.unpack queryOrFilename) >>= \case
          False -> pure queryOrFilename
          True -> Text.readFile (Text.unpack queryOrFilename)
      (out, err, code) <-
        process
          "psql"
          [ "--command=EXPLAIN (ANALYZE, FORMAT JSON) " <> query,
            "--dbname=" <> fromMaybe "postgres" maybeDatabase,
            "--host=" <> stateDir,
            "--no-align",
            "--tuples-only"
          ]
      if code == ExitSuccess
        then do
          Text.putStr out
          case Aeson.eitherDecodeStrictText @[Analyze] out of
            Left parseError -> Text.putStrLn (Text.pack parseError)
            Right (head -> analyze) -> do
              pPrint analyze
              let bytesToDoc :: Int -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  bytesToDoc bytes =
                    commaSepIntDoc bytes <> if bytes == 1 then " byte" else " bytes"
                  commaSepIntDoc :: Int -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  commaSepIntDoc =
                    let loop acc d
                          | d < 1000 = d : acc
                          | otherwise = let (x, y) = divMod d 1000 in loop (y : acc) x
                        pp [] = ""
                        pp (n : ns) = Text.pack (show n) <> pps ns
                        pps = Text.concat . map (("," <>) . pp1)
                        pp1 = Text.pack . printf "%03d" :: Int -> Text
                     in Prettyprinter.pretty . pp . loop []
                  costToDoc :: Double -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  costToDoc cost =
                    let dollars = floorDoubleInt cost
                        cents = roundDoubleInt (mod' cost 1 * 100)
                     in commaSepIntDoc dollars
                          <> "."
                          <> (if cents < 10 then "0" else mempty)
                          <> Prettyprinter.pretty cents
                  kbToDoc :: Int -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  kbToDoc kb
                    | kb < 995 = Prettyprinter.pretty kb <> " kb"
                    | kb < 9_950 = doubleToDoc 2 mb
                    | kb < 99_500 = doubleToDoc 1 mb
                    | kb < 995_000 = doubleToDoc 0 mb
                    | kb < 9_950_000 = doubleToDoc 2 gb
                    | kb < 99_500_000 = doubleToDoc 1 gb
                    | otherwise = doubleToDoc 0 gb
                    where
                      mb = realToFrac @Int @Double kb / 1_000
                      gb = realToFrac @Int @Double kb / 1_000_000
                  timeToDoc :: Double -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  timeToDoc ms
                    | us < 0.5 = "0 us"
                    | us < 995 = doubleToDoc 0 us <> " µs"
                    | us < 9_950 = doubleToDoc 2 ms <> " ms"
                    | us < 99_500 = doubleToDoc 1 ms <> " ms"
                    | ms < 995 = doubleToDoc 0 ms <> " ms"
                    | ms < 9_950 = doubleToDoc 2 s <> " s"
                    | ms < 99_500 = doubleToDoc 1 s <> " s"
                    | otherwise = doubleToDoc 0 s <> " s"
                    where
                      us = ms * 1000
                      s = ms / 1_000
                  doubleToDoc i =
                    Prettyprinter.pretty . Builder.toLazyText . Builder.formatRealFloat Builder.Fixed (Just i)
                  nodeInfoToDoc :: Bool -> NodeInfo -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  nodeInfoToDoc evenOdd info =
                    -- how to show these?
                    -- parallelAware :: Bool
                    Prettyprinter.vsep
                      [ let startup = info.actualStartupTime * realToFrac @Int @Double info.actualLoops
                            total = info.actualTotalTime * realToFrac @Int @Double info.actualLoops
                         in vbar evenOdd
                              <> "∙ "
                              <> timeToDoc startup
                              <> (if startup == total then mempty else " → " <> timeToDoc total),
                        vbar evenOdd
                          <> "∙ $"
                          <> costToDoc info.startupCost
                          <> (if info.startupCost == info.totalCost then mempty else " → $" <> costToDoc info.totalCost)
                      ]
                  nodeToDoc :: Bool -> Node -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  nodeToDoc evenOdd = \case
                    AggregateNode info aggregateInfo ->
                      headerLine info
                        <> titleLine
                          evenOdd
                          info
                          ( case aggregateInfo.strategy of
                              AggregateStrategyHashed ->
                                headerLine info
                                  <> titleLine
                                    evenOdd
                                    info
                                    ( "Build a hash table"
                                        <> case aggregateInfo.groupKey of
                                          Nothing -> mempty
                                          Just key ->
                                            " keyed by "
                                              <> Prettyprinter.hsep
                                                ( Prettyprinter.punctuate
                                                    Prettyprinter.comma
                                                    ( map
                                                        ( Prettyprinter.annotate Prettyprinter.Render.Terminal.italicized
                                                            . Prettyprinter.pretty
                                                        )
                                                        key
                                                    )
                                                )
                                        <> ", then emit its values"
                                    )
                              AggregateStrategyPlain -> "Reduce to a single value"
                              AggregateStrategyMixed -> "Mixed aggregation" -- TODO make this better
                              AggregateStrategySorted -> "Sorted aggregation" -- TODO make this better
                          )
                        <> case aggregateInfo.peakMemoryUsage of
                          Nothing -> mempty
                          Just mem -> vbar evenOdd <> "Peak memory usage: " <> kbToDoc mem <> Prettyprinter.line
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    BitmapHeapScanNode info bitmapHeapScanInfo ->
                      headerLine info
                        <> titleLine
                          evenOdd
                          info
                          ("Access bitmapped pages in table " <> annTable bitmapHeapScanInfo.relationName)
                        <> filterLine (Just bitmapHeapScanInfo.recheckCond) bitmapHeapScanInfo.rowsRemovedByIndexRecheck
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    BitmapIndexScanNode info bitmapIndexScanInfo ->
                      headerLine info
                        <> titleLine
                          evenOdd
                          info
                          ( "Access "
                              <> case bitmapIndexScanInfo.indexCond of
                                Nothing -> "every page of index " <> annIndex bitmapIndexScanInfo.indexName
                                Just cond -> "index " <> annIndex bitmapIndexScanInfo.indexName <> " at " <> annExpr cond
                              <> ", build bitmap of pages to visit"
                          )
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    BitmapOrNode info ->
                      headerLine info
                        <> titleLine evenOdd info "Bitwise-or the bitmaps"
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    FunctionScanNode info functionScanInfo ->
                      headerLine info
                        <> titleLine
                          evenOdd
                          info
                          ( "Execute function "
                              <> Prettyprinter.annotate
                                Prettyprinter.Render.Terminal.italicized
                                (Prettyprinter.pretty functionScanInfo.functionName)
                          )
                        <> filterLine functionScanInfo.filter functionScanInfo.rowsRemovedByFilter
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    GatherNode info gatherInfo ->
                      headerLine info
                        <> titleLine
                          evenOdd
                          info
                          ( "Parallelize with "
                              <> Prettyprinter.pretty gatherInfo.workersLaunched
                              <> " additional threads"
                              <> if gatherInfo.workersLaunched /= gatherInfo.workersPlanned
                                then " (of " <> Prettyprinter.pretty gatherInfo.workersPlanned <> " requested)"
                                else mempty
                          )
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    HashJoinNode info hashJoinInfo ->
                      let (outer, innerInfo, innerHashInfo) =
                            case info.plans of
                              [x, HashNode y z] -> (x, y, z)
                              _ -> error "bogus hash join"
                       in headerLine info
                            <> titleLine
                              evenOdd
                              info
                              ( Prettyprinter.pretty hashJoinInfo.joinType
                                  <> " hash join"
                                  <> (if hashJoinInfo.innerUnique then " (inner unique)" else mempty)
                                  <> " on "
                                  <> annExpr hashJoinInfo.hashCond
                              )
                            <> nodeInfoToDoc evenOdd info
                            <> Prettyprinter.line
                            <> "╰─"
                            <> Prettyprinter.nest
                              2
                              ( Prettyprinter.line
                                  <> nodeToDoc (not evenOdd) outer
                                  <> Prettyprinter.line
                                  <> headerLine innerInfo
                                  <> titleLine
                                    (not evenOdd)
                                    innerInfo
                                    ( "Build a "
                                        <> (if hashJoinInfo.innerUnique then "k→v" else "k→vs")
                                        <> " hash table"
                                    )
                                  <> vbar (not evenOdd)
                                  <> "Peak memory usage: "
                                  <> kbToDoc innerHashInfo.peakMemoryUsage
                                  <> Prettyprinter.line
                                  <> nodeInfoToDoc (not evenOdd) innerInfo
                                  <> Prettyprinter.line
                                  <> "╰─"
                                  <> nodesToDoc (not evenOdd) innerInfo.plans
                              )
                    HashNode {} -> error "unexpected HashNode"
                    IndexOnlyScanNode info indexOnlyScanInfo ->
                      headerLine info
                        <> titleLine
                          evenOdd
                          info
                          ( "Access "
                              <> case indexOnlyScanInfo.indexCond of
                                Nothing -> "every page of index " <> annIndex indexOnlyScanInfo.indexName
                                Just cond ->
                                  "index "
                                    <> annIndex indexOnlyScanInfo.indexName
                                    <> " at "
                                    <> annExpr cond
                          )
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    IndexScanNode info indexScanInfo ->
                      headerLine info
                        <> titleLine
                          evenOdd
                          info
                          ( "Access "
                              <> case indexScanInfo.indexCond of
                                Nothing -> "every page of index " <> annIndex indexScanInfo.indexName
                                Just cond ->
                                  "index "
                                    <> annIndex indexScanInfo.indexName
                                    <> " at "
                                    <> annExpr cond
                              <> ", for each row access table "
                              <> annTable indexScanInfo.relationName
                          )
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    LimitNode info ->
                      headerLine info
                        <> titleLine evenOdd info "Only emit a number of rows"
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    MaterializeNode info ->
                      headerLine info
                        <> titleLine evenOdd info "Materialize"
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    MemoizeNode info _memoizeInfo ->
                      headerLine info
                        <> titleLine evenOdd info "Memoize"
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    MergeJoinNode info mergeJoinInfo ->
                      headerLine info
                        <> titleLine
                          evenOdd
                          info
                          ( Prettyprinter.pretty mergeJoinInfo.joinType
                              <> " merge join"
                              <> (if mergeJoinInfo.innerUnique then " (inner unique)" else mempty)
                              <> " on "
                              <> annExpr mergeJoinInfo.mergeCond
                          )
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    NestedLoopNode info nestedLoopInfo ->
                      headerLine info
                        <> titleLine
                          evenOdd
                          info
                          ( Prettyprinter.pretty nestedLoopInfo.joinType
                              <> " nested loop join"
                              <> if nestedLoopInfo.innerUnique then " (inner unique)" else mempty
                          )
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    ResultNode info ->
                      headerLine info
                        <> titleLine evenOdd info "Result"
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    SeqScanNode info seqScanInfo ->
                      headerLine info
                        <> titleLine
                          evenOdd
                          info
                          ("Access table " <> annTable seqScanInfo.relationName)
                        <> filterLine seqScanInfo.filter seqScanInfo.rowsRemovedByFilter
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    SortNode info sortInfo ->
                      headerLine info
                        <> titleLine
                          evenOdd
                          info
                          ( ( Prettyprinter.pretty @Text case sortInfo.sortMethod of
                                "external merge" -> "On-disk mergesort on "
                                "quicksort" -> "Quicksort on "
                                "top-N heapsort" -> "Top-N heapsort on "
                                method -> error ("Unknown sort method: " <> Text.unpack method)
                            )
                              <> Prettyprinter.hsep
                                ( Prettyprinter.punctuate
                                    Prettyprinter.comma
                                    ( map
                                        (Prettyprinter.annotate Prettyprinter.Render.Terminal.italicized . Prettyprinter.pretty)
                                        sortInfo.sortKey
                                    )
                                )
                          )
                        <> vbar evenOdd
                        <> case sortInfo.sortSpaceType of
                          "Memory" -> "Memory: " <> kbToDoc sortInfo.sortSpaceUsed
                          "Disk" -> "Disk: " <> kbToDoc sortInfo.sortSpaceUsed
                          ty -> error ("Unknown sort type: " ++ Text.unpack ty)
                        <> Prettyprinter.line
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    UniqueNode info ->
                      headerLine info
                        <> titleLine evenOdd info "Only emit distinct rows"
                        <> nodeInfoToDoc evenOdd info
                        <> Prettyprinter.line
                        <> "╰─"
                        <> nodesToDoc evenOdd info.plans
                    where
                      filterLine filter maybeRemoved =
                        case (filter, maybeRemoved) of
                          (Just cond, Just removed) ->
                            vbar evenOdd
                              <> "Filter: "
                              <> annExpr cond
                              <> " removed "
                              <> commaSepIntDoc removed
                              <> (if removed == 1 then " row" else " rows")
                              <> Prettyprinter.line
                          (Just cond, Nothing) ->
                            vbar evenOdd
                              <> "Filter: "
                              <> annExpr cond
                              <> Prettyprinter.line
                          _ -> mempty

                      headerLine info =
                        "⮬ "
                          <> commaSepIntDoc actualRows
                          <> (if actualRows == 1 then " row" else " rows")
                          <> ( if actualRows /= predictedRows
                                 then " (" <> commaSepIntDoc predictedRows <> " predicted)"
                                 else mempty
                             )
                          <> ", "
                          <> bytesToDoc info.planWidth
                          <> " each"
                          <> Prettyprinter.line
                          <> "╭─"
                          <> Prettyprinter.line
                        where
                          actualRows = info.actualRows * info.actualLoops
                          predictedRows = info.planRows * info.actualLoops

                  nodesToDoc :: Bool -> [Node] -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  nodesToDoc evenOdd = \case
                    [] -> mempty
                    nodes ->
                      Prettyprinter.nest
                        2
                        (Prettyprinter.line <> Prettyprinter.vsep (map (nodeToDoc (not evenOdd)) nodes))

                  titleLine evenOdd info title =
                    vbar evenOdd
                      <> Prettyprinter.annotate
                        Prettyprinter.Render.Terminal.bold
                        ( if info.actualLoops == 1
                            then title
                            else commaSepIntDoc info.actualLoops <> "x " <> title
                        )
                      <> Prettyprinter.line

                  vbar evenOdd = if evenOdd then "┊" else "│"
                  annExpr :: Text -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  annExpr =
                    Prettyprinter.annotate (Prettyprinter.Render.Terminal.color Prettyprinter.Render.Terminal.Blue)
                      . Prettyprinter.pretty
                  annIndex :: Text -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  annIndex =
                    Prettyprinter.annotate
                      ( Prettyprinter.Render.Terminal.color Prettyprinter.Render.Terminal.Magenta
                          <> Prettyprinter.Render.Terminal.italicized
                      )
                      . Prettyprinter.pretty
                  annTable :: Text -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  annTable =
                    Prettyprinter.annotate
                      ( Prettyprinter.Render.Terminal.color Prettyprinter.Render.Terminal.Red
                          <> Prettyprinter.Render.Terminal.italicized
                      )
                      . Prettyprinter.pretty

              let doc :: Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  doc =
                    nodeToDoc False analyze.plan
                      <> Prettyprinter.line
                      <> "Total time: "
                      <> timeToDoc (analyze.planningTime + analyze.executionTime)
                      <> " ("
                      <> timeToDoc analyze.planningTime
                      <> " to plan, "
                      <> timeToDoc analyze.executionTime
                      <> " to execute)"
              doc
                & Prettyprinter.layoutPretty (Prettyprinter.LayoutOptions Prettyprinter.Unbounded)
                & Prettyprinter.Render.Terminal.renderStrict
                & Text.putStrLn
        else do
          Text.putStr out
          Text.putStr err
      exitWith code

pgLoad :: Maybe Text -> Text -> IO ()
pgLoad maybeDatabase file = do
  stateDir <- getStateDir
  (out, err, code) <-
    process
      "pg_restore"
      [ "--dbname=" <> fromMaybe "postgres" maybeDatabase,
        "--host=" <> stateDir,
        file
      ]
  when (code /= ExitSuccess) do
    Text.putStr out
    Text.putStr err
    exitWith code

pgRepl :: IO ()
pgRepl = do
  stateDir <- getStateDir
  let clusterDir = stateDir <> "/data"
  Directory.doesFileExist (Text.unpack (clusterDir <> "/postmaster.pid")) >>= \case
    False -> Text.putStrLn ("There's no cluster running at " <> clusterDir)
    True -> do
      code <-
        foreground
          "psql"
          [ "--dbname=postgres",
            "--host=" <> stateDir
          ]
      exitWith code

pgUp :: IO ()
pgUp = do
  stateDir <- getStateDir
  let clusterDir = stateDir <> "/data"
  let logFile = stateDir <> "/log.txt"
  Directory.doesFileExist (Text.unpack (clusterDir <> "/postmaster.pid")) >>= \case
    False -> do
      Directory.createDirectoryIfMissing True (Text.unpack stateDir)
      background
        "postgres"
        [ "-D",
          clusterDir,
          -- disable fsync
          "-F",
          -- don't listen on IP
          "-h",
          "",
          -- unix socket directory
          "-k",
          stateDir
        ]
        logFile
      Posix.touchFile (Text.encodeUtf8 logFile) -- in case we get here before postgres creates it
      void (foreground "tail" ["-f", logFile])
    True -> Text.putStrLn ("There's already a cluster running at " <> clusterDir)

------------------------------------------------------------------------------------------------------------------------
-- EXPLAIN ANALYZE json utils

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

parseAggregateNodeInfo :: Aeson.Object -> Aeson.Parser AggregateNodeInfo
parseAggregateNodeInfo object = do
  diskUsage <- Aeson.parseFieldMaybe' @Int object "Disk Usage"
  groupKey <- Aeson.parseFieldMaybe' @[Text] object "Group Key"
  hashAggBatches <- Aeson.parseFieldMaybe' @Int object "HashAgg Batches"
  partialMode <- Aeson.parseField @Text object "Partial Mode"
  peakMemoryUsage <- Aeson.parseFieldMaybe' @Int object "Peak Memory Usage"
  plannedPartitions <- Aeson.parseFieldMaybe' @Int object "Planned Partitions"
  strategy <- Aeson.parseField @AggregateStrategy object "Strategy"
  pure AggregateNodeInfo {..}

data AggregateStrategy
  = AggregateStrategyHashed
  | AggregateStrategyMixed
  | AggregateStrategyPlain
  | AggregateStrategySorted
  deriving stock (Show)

instance Aeson.FromJSON AggregateStrategy where
  parseJSON =
    Aeson.withText "AggregateStrategy" \case
      "Hashed" -> pure AggregateStrategyHashed
      "Mixed" -> pure AggregateStrategyMixed
      "Plain" -> pure AggregateStrategyPlain
      "Sorted" -> pure AggregateStrategySorted
      strategy -> fail ("Unknown aggregate strategy: " ++ Text.unpack strategy)

data Analyze = Analyze
  { executionTime :: !Double,
    plan :: !Node,
    planningTime :: !Double
  }
  deriving stock (Show)

instance Aeson.FromJSON Analyze where
  parseJSON :: Aeson.Value -> Aeson.Parser Analyze
  parseJSON =
    Aeson.withObject "Analyze" \object -> do
      executionTime <- Aeson.parseField @Double object "Execution Time"
      plan <- Aeson.parseField @Node object "Plan"
      planningTime <- Aeson.parseField @Double object "Planning Time"
      pure Analyze {..}

data BitmapHeapScanNodeInfo = BitmapHeapScanNodeInfo
  { alias :: !Text,
    exactHeapBlocks :: !Int,
    lossyHeapBlocks :: !Int,
    recheckCond :: !Text,
    relationName :: !Text,
    rowsRemovedByIndexRecheck :: !(Maybe Int)
  }
  deriving stock (Show)

parseBitmapHeapScanNodeInfo :: Aeson.Object -> Aeson.Parser BitmapHeapScanNodeInfo
parseBitmapHeapScanNodeInfo object = do
  alias <- Aeson.parseField @Text object "Alias"
  exactHeapBlocks <- Aeson.parseField @Int object "Exact Heap Blocks"
  lossyHeapBlocks <- Aeson.parseField @Int object "Lossy Heap Blocks"
  recheckCond <- Aeson.parseField @Text object "Recheck Cond"
  relationName <- Aeson.parseField @Text object "Relation Name"
  rowsRemovedByIndexRecheck <- Aeson.parseFieldMaybe' @Int object "Rows Removed by Index Recheck"
  pure BitmapHeapScanNodeInfo {..}

data BitmapIndexScanNodeInfo = BitmapIndexScanNodeInfo
  { indexCond :: !(Maybe Text),
    indexName :: !Text
  }
  deriving stock (Show)

parseBitmapIndexScanNodeInfo :: Aeson.Object -> Aeson.Parser BitmapIndexScanNodeInfo
parseBitmapIndexScanNodeInfo object = do
  indexCond <- Aeson.parseFieldMaybe' @Text object "Index Cond"
  indexName <- Aeson.parseField @Text object "Index Name"
  pure BitmapIndexScanNodeInfo {..}

data FunctionScanNodeInfo = FunctionScanNodeInfo
  { alias :: !Text,
    filter :: !(Maybe Text),
    functionName :: !Text,
    rowsRemovedByFilter :: !(Maybe Int)
  }
  deriving stock (Show)

parseFunctionScanNodeInfo :: Aeson.Object -> Aeson.Parser FunctionScanNodeInfo
parseFunctionScanNodeInfo object = do
  alias <- Aeson.parseField @Text object "Alias"
  filter <- Aeson.parseFieldMaybe' @Text object "Filter"
  functionName <- Aeson.parseField @Text object "Function Name"
  rowsRemovedByFilter <- Aeson.parseFieldMaybe' @Int object "Rows Removed by Filter"
  pure FunctionScanNodeInfo {..}

data GatherNodeInfo = GatherNodeInfo
  { singleCopy :: !Bool,
    workersLaunched :: !Int,
    workersPlanned :: !Int
  }
  deriving stock (Show)

parseGatherNodeInfo :: Aeson.Object -> Aeson.Parser GatherNodeInfo
parseGatherNodeInfo object = do
  singleCopy <- Aeson.parseField @Bool object "Single Copy"
  workersLaunched <- Aeson.parseField @Int object "Workers Launched"
  workersPlanned <- Aeson.parseField @Int object "Workers Planned"
  pure GatherNodeInfo {..}

data HashJoinNodeInfo = HashJoinNodeInfo
  { hashCond :: !Text,
    innerUnique :: !Bool,
    joinType :: !Text
  }
  deriving stock (Show)

parseHashJoinNodeInfo :: Aeson.Object -> Aeson.Parser HashJoinNodeInfo
parseHashJoinNodeInfo object = do
  hashCond <- Aeson.parseField @Text object "Hash Cond"
  innerUnique <- Aeson.parseField @Bool object "Inner Unique"
  joinType <- Aeson.parseField @Text object "Join Type"
  pure HashJoinNodeInfo {..}

data HashNodeInfo = HashNodeInfo
  { hashBatches :: !Int,
    hashBuckets :: !Int,
    originalHashBatches :: !Int,
    originalHashBuckets :: !Int,
    peakMemoryUsage :: !Int
  }
  deriving stock (Show)

parseHashNodeInfo :: Aeson.Object -> Aeson.Parser HashNodeInfo
parseHashNodeInfo object = do
  hashBatches <- Aeson.parseField @Int object "Hash Batches"
  hashBuckets <- Aeson.parseField @Int object "Hash Buckets"
  originalHashBatches <- Aeson.parseField @Int object "Original Hash Batches"
  originalHashBuckets <- Aeson.parseField @Int object "Original Hash Buckets"
  peakMemoryUsage <- Aeson.parseField @Int object "Peak Memory Usage"
  pure HashNodeInfo {..}

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

parseIndexOnlyScanNodeInfo :: Aeson.Object -> Aeson.Parser IndexOnlyScanNodeInfo
parseIndexOnlyScanNodeInfo object = do
  alias <- Aeson.parseField @Text object "Alias"
  heapFetches <- Aeson.parseField @Int object "Heap Fetches"
  indexCond <- Aeson.parseFieldMaybe' @Text object "Index Cond"
  indexName <- Aeson.parseField @Text object "Index Name"
  relationName <- Aeson.parseField @Text object "Relation Name"
  rowsRemovedByIndexRecheck <- Aeson.parseFieldMaybe' @Int object "Rows Removed by Index Recheck"
  scanDirection <- Aeson.parseField @Text object "Scan Direction"
  pure IndexOnlyScanNodeInfo {..}

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

parseIndexScanNodeInfo :: Aeson.Object -> Aeson.Parser IndexScanNodeInfo
parseIndexScanNodeInfo object = do
  alias <- Aeson.parseField @Text object "Alias"
  indexCond <- Aeson.parseFieldMaybe' @Text object "Index Cond"
  indexName <- Aeson.parseField @Text object "Index Name"
  relationName <- Aeson.parseField @Text object "Relation Name"
  rowsRemovedByIndexRecheck <- Aeson.parseFieldMaybe' @Int object "Rows Removed by Index Recheck"
  scanDirection <- Aeson.parseField @Text object "Scan Direction"
  subplanName <- Aeson.parseFieldMaybe' @Text object "Subplan Name"
  pure IndexScanNodeInfo {..}

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

parseMemoizeNodeInfo :: Aeson.Object -> Aeson.Parser MemoizeNodeInfo
parseMemoizeNodeInfo object = do
  cacheEvictions <- Aeson.parseField @Int object "Cache Evictions"
  cacheHits <- Aeson.parseField @Int object "Cache Hits"
  cacheKey <- Aeson.parseField @Text object "Cache Key"
  cacheMisses <- Aeson.parseField @Int object "Cache Misses"
  cacheMode <- Aeson.parseField @Text object "Cache Mode"
  cacheOverflows <- Aeson.parseField @Int object "Cache Overflows"
  peakMemoryUsage <- Aeson.parseField @Int object "Peak Memory Usage"
  pure MemoizeNodeInfo {..}

data MergeJoinNodeInfo = MergeJoinNodeInfo
  { innerUnique :: !Bool,
    joinType :: !Text,
    mergeCond :: !Text
  }
  deriving stock (Show)

parseMergeJoinNodeInfo :: Aeson.Object -> Aeson.Parser MergeJoinNodeInfo
parseMergeJoinNodeInfo object = do
  innerUnique <- Aeson.parseField @Bool object "Inner Unique"
  joinType <- Aeson.parseField @Text object "Join Type"
  mergeCond <- Aeson.parseField @Text object "Merge Cond"
  pure MergeJoinNodeInfo {..}

data NestedLoopNodeInfo = NestedLoopNodeInfo
  { innerUnique :: !Bool,
    joinType :: !Text
  }
  deriving stock (Show)

parseNestedLoopNodeInfo :: Aeson.Object -> Aeson.Parser NestedLoopNodeInfo
parseNestedLoopNodeInfo object = do
  innerUnique <- Aeson.parseField @Bool object "Inner Unique"
  joinType <- Aeson.parseField @Text object "Join Type"
  pure NestedLoopNodeInfo {..}

data Node
  = AggregateNode !NodeInfo !AggregateNodeInfo
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
  | ResultNode !NodeInfo
  | SeqScanNode !NodeInfo !SeqScanNodeInfo
  | SortNode !NodeInfo !SortNodeInfo
  | UniqueNode !NodeInfo
  deriving stock (Show)

instance Aeson.FromJSON Node where
  parseJSON :: Aeson.Value -> Aeson.Parser Node
  parseJSON =
    Aeson.withObject "Node" \object -> do
      info <- parseNodeInfo object
      let go constructor parse = do
            info2 <- parse object
            pure (constructor info info2)
      Aeson.parseField @Text object "Node Type" >>= \case
        "Aggregate" -> go AggregateNode parseAggregateNodeInfo
        "Bitmap Heap Scan" -> go BitmapHeapScanNode parseBitmapHeapScanNodeInfo
        "Bitmap Index Scan" -> go BitmapIndexScanNode parseBitmapIndexScanNodeInfo
        "BitmapOr" -> pure (BitmapOrNode info)
        "Function Scan" -> go FunctionScanNode parseFunctionScanNodeInfo
        "Gather" -> go GatherNode parseGatherNodeInfo
        "Hash" -> go HashNode parseHashNodeInfo
        "Hash Join" -> go HashJoinNode parseHashJoinNodeInfo
        "Index Only Scan" -> go IndexOnlyScanNode parseIndexOnlyScanNodeInfo
        "Index Scan" -> go IndexScanNode parseIndexScanNodeInfo
        "Limit" -> pure (LimitNode info)
        "Materialize" -> pure (MaterializeNode info)
        "Memoize" -> go MemoizeNode parseMemoizeNodeInfo
        "Merge Join" -> go MergeJoinNode parseMergeJoinNodeInfo
        "Nested Loop" -> go NestedLoopNode parseNestedLoopNodeInfo
        "Result" -> pure (ResultNode info)
        "Seq Scan" -> go SeqScanNode parseSeqScanNodeInfo
        "Sort" -> go SortNode parseSortNodeInfo
        "Unique" -> pure (UniqueNode info)
        typ -> fail ("Unknown node type: " ++ Text.unpack typ)
    where

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

instance Aeson.FromJSON NodeInfo where
  parseJSON :: Aeson.Value -> Aeson.Parser NodeInfo
  parseJSON =
    Aeson.withObject "NodeInfo" parseNodeInfo

parseNodeInfo :: Aeson.Object -> Aeson.Parser NodeInfo
parseNodeInfo object = do
  actualLoops <- Aeson.parseField @Int object "Actual Loops"
  actualRows <- Aeson.parseField @Int object "Actual Rows"
  actualStartupTime <- Aeson.parseField @Double object "Actual Startup Time"
  actualTotalTime <- Aeson.parseField @Double object "Actual Total Time"
  asyncCapable <- Aeson.parseFieldMaybe' @Bool object "Async Capable"
  parallelAware <- Aeson.parseField @Bool object "Parallel Aware"
  parentRelationship <- Aeson.parseFieldMaybe' @Text object "Parent Relationship"
  planRows <- Aeson.parseField @Int object "Plan Rows"
  planWidth <- Aeson.parseField @Int object "Plan Width"
  plans <- fromMaybe [] <$> Aeson.parseFieldMaybe' @[Node] object "Plans"
  startupCost <- Aeson.parseField @Double object "Startup Cost"
  totalCost <- Aeson.parseField @Double object "Total Cost"
  pure NodeInfo {..}

data SeqScanNodeInfo = SeqScanNodeInfo
  { alias :: !Text,
    filter :: !(Maybe Text),
    relationName :: !Text,
    rowsRemovedByFilter :: !(Maybe Int)
  }
  deriving stock (Show)

parseSeqScanNodeInfo :: Aeson.Object -> Aeson.Parser SeqScanNodeInfo
parseSeqScanNodeInfo object = do
  alias <- Aeson.parseField @Text object "Alias"
  filter <- Aeson.parseFieldMaybe' @Text object "Filter"
  relationName <- Aeson.parseField @Text object "Relation Name"
  rowsRemovedByFilter <- Aeson.parseFieldMaybe' @Int object "Rows Removed by Filter"
  pure SeqScanNodeInfo {..}

data SortNodeInfo = SortNodeInfo
  { sortKey :: ![Text],
    sortMethod :: !Text,
    sortSpaceType :: !Text,
    sortSpaceUsed :: !Int
  }
  deriving stock (Show)

parseSortNodeInfo :: Aeson.Object -> Aeson.Parser SortNodeInfo
parseSortNodeInfo object = do
  sortKey <- Aeson.parseField @[Text] object "Sort Key"
  sortMethod <- Aeson.parseField @Text object "Sort Method"
  sortSpaceType <- Aeson.parseField @Text object "Sort Space Type"
  sortSpaceUsed <- Aeson.parseField @Int object "Sort Space Used"
  pure SortNodeInfo {..}

------------------------------------------------------------------------------------------------------------------------
-- Cli utils

commandLineInterface :: [Opt.PrefsMod] -> [Opt.InfoMod (IO a)] -> [Opt.Parser (IO a)] -> IO a
commandLineInterface prefs info parsers = do
  action <-
    Opt.customExecParser
      (Opt.prefs (fold prefs))
      (Opt.info (asum parsers) (fold info))
  action

subcommands :: [Opt.Mod Opt.CommandFields a] -> Opt.Parser a
subcommands commands =
  Opt.hsubparser (Opt.metavar "«command»" <> fold commands)

subcommand :: [Opt.InfoMod a] -> String -> Opt.Parser a -> Opt.Mod Opt.CommandFields a
subcommand info name parser =
  Opt.command name (Opt.info parser (fold info))

textArg :: [Opt.Mod Opt.ArgumentFields Text] -> Opt.Parser Text
textArg opts =
  Opt.strArgument (fold opts)

textOpt :: [Opt.Mod Opt.OptionFields Text] -> Opt.Parser (Maybe Text)
textOpt opts =
  optional (Opt.strOption (fold opts))

------------------------------------------------------------------------------------------------------------------------
-- Subprocess utils

background :: Text -> [Text] -> Text -> IO ()
background name args logfile = do
  withDevNull \devNull -> do
    IO.withBinaryFile (Text.unpack logfile) IO.WriteMode \logfileHandle -> do
      let config =
            Process.CreateProcess
              { Process.child_group = Nothing,
                Process.child_user = Nothing,
                Process.close_fds = False,
                Process.cmdspec = Process.RawCommand (Text.unpack name) (map Text.unpack args),
                Process.create_group = True,
                Process.create_new_console = False,
                Process.cwd = Nothing,
                Process.delegate_ctlc = False,
                Process.detach_console = False,
                Process.env = Nothing,
                Process.new_session = True,
                Process.std_err = Process.UseHandle logfileHandle,
                Process.std_in = Process.UseHandle devNull,
                Process.std_out = Process.UseHandle logfileHandle,
                Process.use_process_jobs = False
              }
      void (Process.createProcess config)

foreground :: Text -> [Text] -> IO ExitCode
foreground name args = do
  let config =
        Process.CreateProcess
          { Process.child_group = Nothing,
            Process.child_user = Nothing,
            Process.close_fds = False,
            Process.cmdspec = Process.RawCommand (Text.unpack name) (map Text.unpack args),
            Process.create_group = False,
            Process.create_new_console = False,
            Process.cwd = Nothing,
            Process.delegate_ctlc = True,
            Process.detach_console = False,
            Process.env = Nothing,
            Process.new_session = False,
            Process.std_err = Process.Inherit,
            Process.std_in = Process.Inherit,
            Process.std_out = Process.Inherit,
            Process.use_process_jobs = False
          }

  let cleanup (_, _, _, handle) = do
        Process.terminateProcess handle
        void (Process.waitForProcess handle)

  bracket (Process.createProcess config) cleanup \(_, _, _, handle) ->
    Process.waitForProcess handle

process :: Text -> [Text] -> IO (Text, Text, ExitCode)
process name args = do
  withDevNull \devNull ->
    withPipe \stdoutReadHandle stdoutWriteHandle ->
      withPipe \stderrReadHandle stderrWriteHandle -> do
        let config =
              Process.CreateProcess
                { Process.child_group = Nothing,
                  Process.child_user = Nothing,
                  Process.close_fds = False,
                  Process.cmdspec = Process.RawCommand (Text.unpack name) (map Text.unpack args),
                  Process.create_group = False,
                  Process.create_new_console = False,
                  Process.cwd = Nothing,
                  Process.delegate_ctlc = False,
                  Process.detach_console = False,
                  Process.env = Nothing,
                  Process.new_session = False,
                  Process.std_err = Process.UseHandle stderrWriteHandle,
                  Process.std_in = Process.UseHandle devNull,
                  Process.std_out = Process.UseHandle stdoutWriteHandle,
                  Process.use_process_jobs = False
                }

        let cleanup (_, _, _, handle) = do
              Process.terminateProcess handle
              void (Process.waitForProcess handle)

        bracket (Process.createProcess config) cleanup \(_, _, _, handle) -> do
          exitCode <- Process.waitForProcess handle
          out <- ByteString.hGetContents stdoutReadHandle
          err <- ByteString.hGetContents stderrReadHandle
          pure (Text.decodeUtf8 out, Text.decodeUtf8 err, exitCode)

withDevNull :: (IO.Handle -> IO a) -> IO a
withDevNull =
  bracket (IO.openBinaryFile "/dev/null" IO.WriteMode) IO.hClose

withPipe :: (IO.Handle -> IO.Handle -> IO a) -> IO a
withPipe action =
  bracket acquire release \(read, write) -> action read write
  where
    acquire = do
      (read, write) <- Process.createPipe
      IO.hSetBinaryMode read True
      IO.hSetBinaryMode write True
      IO.hSetBuffering read IO.NoBuffering
      IO.hSetBuffering write IO.NoBuffering
      pure (read, write)

    release (read, write) = do
      IO.hClose read
      IO.hClose write

------------------------------------------------------------------------------------------------------------------------
-- Directory utils

-- If in "$HOME/foo, returns "/foo"
getCurrentDirectoryRelativeToHome :: IO (Maybe Text)
getCurrentDirectoryRelativeToHome = do
  home <- Text.pack <$> Directory.getHomeDirectory
  cwd <- Text.pack <$> Directory.getCurrentDirectory
  pure (Text.stripPrefix home cwd)

-- If in "$HOME/foo", returns "$HOME/.local/state/<identifier>/foo
getStateDir :: IO Text
getStateDir = do
  currentDir <-
    getCurrentDirectoryRelativeToHome & onNothingM do
      Text.putStrLn "Error: not in home directory"
      exitFailure
  stateDir <- Directory.getXdgDirectory Directory.XdgState "pg-RnWBCkc"
  pure (Text.pack stateDir <> currentDir)

------------------------------------------------------------------------------------------------------------------------
-- Misc missing prelude functions

onNothingM :: (Monad m) => m a -> m (Maybe a) -> m a
onNothingM mx my =
  my >>= \case
    Nothing -> mx
    Just x -> pure x
