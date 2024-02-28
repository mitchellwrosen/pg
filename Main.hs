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
            "--locale=C.UTF-8",
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
pgExplain maybeDatabase query = do
  stateDir <- getStateDir
  let clusterDir = stateDir <> "/data"
  Directory.doesFileExist (Text.unpack (clusterDir <> "/postmaster.pid")) >>= \case
    False -> Text.putStrLn ("There's no cluster running at " <> clusterDir)
    True -> do
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
                    Prettyprinter.pretty bytes <> " bytes"
                  costToDoc :: Double -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  costToDoc cost =
                    let dollars = floorDoubleInt cost
                        cents = roundDoubleInt (mod' cost 1 * 100)
                        loop acc d
                          | d < 1000 = d : acc
                          | otherwise = let (x, y) = divMod d 1000 in loop (y : acc) x
                        pp [] = ""
                        pp [n] = Text.pack (show n)
                        pp (n : ns) = Text.pack (show n) <> "," <> pp ns
                     in Prettyprinter.pretty (pp (loop [] dollars))
                          <> "."
                          <> (if cents < 10 then "0" else mempty)
                          <> Prettyprinter.pretty cents
                  timeToDoc :: Double -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  timeToDoc ms
                    | us < 0.5 = "0 us"
                    | us < 995 = double 0 us <> " µs"
                    | us < 9_950 = double 2 ms <> " ms"
                    | us < 99_500 = double 1 ms <> " ms"
                    | ms < 995 = double 0 ms <> " ms"
                    | ms < 9_950 = double 2 s <> " s"
                    | ms < 99_500 = double 1 s <> " s"
                    | otherwise = double 0 s <> " s"
                    where
                      us = ms * 1000
                      s = ms / 1_000
                      double i =
                        Prettyprinter.pretty . Builder.toLazyText . Builder.formatRealFloat Builder.Fixed (Just i)
                  nodeInfoToDoc :: NodeInfo -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  nodeInfoToDoc info =
                    -- how to show these?
                    -- actualLoops :: Int
                    -- parallelAware :: Bool
                    Prettyprinter.vsep
                      [ "Time: "
                          <> timeToDoc (info.actualTotalTime * realToFrac @Int @Double info.actualLoops)
                          <> " ("
                          <> timeToDoc (info.actualStartupTime * realToFrac @Int @Double info.actualLoops)
                          <> " to start, "
                          <> timeToDoc ((info.actualTotalTime - info.actualStartupTime) * realToFrac @Int @Double info.actualLoops)
                          <> " to execute)",
                        "Cost: $" <> costToDoc info.totalCost <> " ($" <> costToDoc info.startupCost <> " to start, $" <> costToDoc (info.totalCost - info.startupCost) <> " to execute)",
                        "Rows: "
                          <> Prettyprinter.pretty (info.actualRows * info.actualLoops)
                          <> " ("
                          <> Prettyprinter.pretty (info.planRows * info.actualLoops)
                          <> " predicted), "
                          <> bytesToDoc info.planWidth
                          <> " each"
                      ]
                  nodeToDoc :: Node -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  nodeToDoc = \case
                    BitmapHeapScanNode info bitmapHeapScanInfo ->
                      Prettyprinter.annotate Prettyprinter.Render.Terminal.bold ("Bitmap heap scan " <> Prettyprinter.dquotes (Prettyprinter.pretty bitmapHeapScanInfo.relationName))
                        <> Prettyprinter.line
                        <> nodeInfoToDoc info
                        <> nodesToDoc info.plans
                    BitmapIndexScanNode info bitmapIndexScanInfo ->
                      Prettyprinter.annotate Prettyprinter.Render.Terminal.bold ("Bitmap index scan using " <> Prettyprinter.dquotes (Prettyprinter.pretty bitmapIndexScanInfo.indexName))
                        <> ( case bitmapIndexScanInfo.indexCond of
                               Nothing -> mempty
                               Just cond -> ", " <> Prettyprinter.pretty cond
                           )
                        <> Prettyprinter.line
                        <> nodeInfoToDoc info
                        <> nodesToDoc info.plans
                    IndexOnlyScanNode info indexScanInfo ->
                      Prettyprinter.annotate Prettyprinter.Render.Terminal.bold ("Scan index " <> Prettyprinter.dquotes (Prettyprinter.pretty indexScanInfo.indexName))
                        <> ( case indexScanInfo.indexCond of
                               Nothing -> mempty
                               Just cond -> ", " <> Prettyprinter.pretty cond
                           )
                        <> Prettyprinter.line
                        <> nodeInfoToDoc info
                        <> nodesToDoc info.plans
                    IndexScanNode info indexScanInfo ->
                      Prettyprinter.annotate Prettyprinter.Render.Terminal.bold ("Scan index " <> Prettyprinter.dquotes (Prettyprinter.pretty indexScanInfo.indexName) <> ", accessing table " <> Prettyprinter.dquotes (Prettyprinter.pretty indexScanInfo.relationName))
                        <> ( case indexScanInfo.indexCond of
                               Nothing -> mempty
                               Just cond -> Prettyprinter.line <> "Condition: " <> Prettyprinter.pretty cond
                           )
                        <> Prettyprinter.line
                        <> nodeInfoToDoc info
                        <> nodesToDoc info.plans
                    LimitNode info _limitInfo ->
                      Prettyprinter.annotate Prettyprinter.Render.Terminal.bold ("Take the first " <> (if info.actualRows == 1 then "row" else Prettyprinter.pretty info.actualRows <> " rows"))
                        <> Prettyprinter.line
                        <> nodeInfoToDoc info
                        <> nodesToDoc info.plans
                    ResultNode info -> Prettyprinter.annotate Prettyprinter.Render.Terminal.bold "Result" <> nodesToDoc info.plans
                    SeqScanNode info seqScanInfo ->
                      Prettyprinter.annotate Prettyprinter.Render.Terminal.bold ("Seq scan " <> Prettyprinter.dquotes (Prettyprinter.pretty seqScanInfo.relationName))
                        <> ( case seqScanInfo.filter of
                               Nothing -> mempty
                               Just s -> ", filter " <> Prettyprinter.pretty s
                           )
                        <> ( case seqScanInfo.rowsRemovedByFilter of
                               Nothing -> mempty
                               Just rows ->
                                 " (removed "
                                   <> Prettyprinter.pretty rows
                                   <> if rows == 1 then " row)" else " rows)"
                           )
                        <> Prettyprinter.line
                        <> nodeInfoToDoc info
                        <> nodesToDoc info.plans
                    SortNode info sortInfo ->
                      Prettyprinter.annotate Prettyprinter.Render.Terminal.bold ("Sort on " <> Prettyprinter.pretty sortInfo.sortKey <> " using " <> Prettyprinter.pretty sortInfo.sortMethod)
                        <> nodesToDoc info.plans
                  nodesToDoc :: [Node] -> Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  nodesToDoc = \case
                    [] -> mempty
                    nodes ->
                      Prettyprinter.line
                        <> Prettyprinter.vsep (map ((\d -> "  ➤ " <> Prettyprinter.align d) . nodeToDoc) nodes)
              let doc :: Prettyprinter.Doc Prettyprinter.Render.Terminal.AnsiStyle
                  doc =
                    nodeToDoc analyze.plan
                      <> Prettyprinter.line
                      <> timeToDoc analyze.planningTime
                      <> " to plan, "
                      <> timeToDoc analyze.executionTime
                      <> " to execute"
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
    asyncCapable :: !Bool,
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
  asyncCapable <- Aeson.parseField @Bool object "Async Capable"
  exactHeapBlocks <- Aeson.parseField @Int object "Exact Heap Blocks"
  lossyHeapBlocks <- Aeson.parseField @Int object "Lossy Heap Blocks"
  recheckCond <- Aeson.parseField @Text object "Recheck Cond"
  relationName <- Aeson.parseField @Text object "Relation Name"
  rowsRemovedByIndexRecheck <- Aeson.parseFieldMaybe' @Int object "Rows Removed by Index Recheck"
  pure BitmapHeapScanNodeInfo {..}

data BitmapIndexScanNodeInfo = BitmapIndexScanNodeInfo
  { asyncCapable :: !Bool,
    indexCond :: !(Maybe Text),
    indexName :: !Text,
    parentRelationship :: !Text
  }
  deriving stock (Show)

parseBitmapIndexScanNodeInfo :: Aeson.Object -> Aeson.Parser BitmapIndexScanNodeInfo
parseBitmapIndexScanNodeInfo object = do
  asyncCapable <- Aeson.parseField @Bool object "Async Capable"
  indexCond <- Aeson.parseFieldMaybe' @Text object "Index Cond"
  indexName <- Aeson.parseField @Text object "Index Name"
  parentRelationship <- Aeson.parseField @Text object "Parent Relationship"
  pure BitmapIndexScanNodeInfo {..}

data IndexOnlyScanNodeInfo = IndexOnlyScanNodeInfo
  { alias :: !Text,
    asyncCapable :: !Bool,
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
  asyncCapable <- Aeson.parseField @Bool object "Async Capable"
  heapFetches <- Aeson.parseField @Int object "Heap Fetches"
  indexCond <- Aeson.parseFieldMaybe' @Text object "Index Cond"
  indexName <- Aeson.parseField @Text object "Index Name"
  relationName <- Aeson.parseField @Text object "Relation Name"
  rowsRemovedByIndexRecheck <- Aeson.parseFieldMaybe' @Int object "Rows Removed by Index Recheck"
  scanDirection <- Aeson.parseField @Text object "Scan Direction"
  pure IndexOnlyScanNodeInfo {..}

data IndexScanNodeInfo = IndexScanNodeInfo
  { alias :: !Text,
    asyncCapable :: !Bool,
    indexCond :: !(Maybe Text),
    indexName :: !Text,
    relationName :: !Text,
    rowsRemovedByIndexRecheck :: !(Maybe Int),
    scanDirection :: !Text
  }
  deriving stock (Show)

parseIndexScanNodeInfo :: Aeson.Object -> Aeson.Parser IndexScanNodeInfo
parseIndexScanNodeInfo object = do
  alias <- Aeson.parseField @Text object "Alias"
  asyncCapable <- Aeson.parseField @Bool object "Async Capable"
  indexCond <- Aeson.parseFieldMaybe' @Text object "Index Cond"
  indexName <- Aeson.parseField @Text object "Index Name"
  relationName <- Aeson.parseField @Text object "Relation Name"
  rowsRemovedByIndexRecheck <- Aeson.parseFieldMaybe' @Int object "Rows Removed by Index Recheck"
  scanDirection <- Aeson.parseField @Text object "Scan Direction"
  pure IndexScanNodeInfo {..}

data LimitNodeInfo = LimitNodeInfo
  { asyncCapable :: !Bool
  }
  deriving stock (Show)

parseLimitNodeInfo :: Aeson.Object -> Aeson.Parser LimitNodeInfo
parseLimitNodeInfo object = do
  asyncCapable <- Aeson.parseField @Bool object "Async Capable"
  pure LimitNodeInfo {..}

data Node
  = BitmapHeapScanNode !NodeInfo !BitmapHeapScanNodeInfo
  | BitmapIndexScanNode !NodeInfo !BitmapIndexScanNodeInfo
  | IndexOnlyScanNode !NodeInfo !IndexOnlyScanNodeInfo
  | IndexScanNode !NodeInfo !IndexScanNodeInfo
  | LimitNode !NodeInfo !LimitNodeInfo
  | ResultNode !NodeInfo
  | SeqScanNode !NodeInfo !SeqScanNodeInfo
  | SortNode !NodeInfo !SortNodeInfo
  deriving stock (Show)

instance Aeson.FromJSON Node where
  parseJSON :: Aeson.Value -> Aeson.Parser Node
  parseJSON =
    Aeson.withObject "Node" \object -> do
      info <- parseNodeInfo object
      Aeson.parseField @Text object "Node Type" >>= \case
        "Bitmap Heap Scan" -> do
          bitmapHeapScanInfo <- parseBitmapHeapScanNodeInfo object
          pure (BitmapHeapScanNode info bitmapHeapScanInfo)
        "Bitmap Index Scan" -> do
          bitmapIndexScanInfo <- parseBitmapIndexScanNodeInfo object
          pure (BitmapIndexScanNode info bitmapIndexScanInfo)
        "Index Only Scan" -> do
          indexOnlyScanInfo <- parseIndexOnlyScanNodeInfo object
          pure (IndexOnlyScanNode info indexOnlyScanInfo)
        "Index Scan" -> do
          indexScanInfo <- parseIndexScanNodeInfo object
          pure (IndexScanNode info indexScanInfo)
        "Limit" -> do
          limitInfo <- parseLimitNodeInfo object
          pure (LimitNode info limitInfo)
        "Result" -> pure (ResultNode info)
        "Seq Scan" -> do
          seqScanInfo <- parseSeqScanNodeInfo object
          pure (SeqScanNode info seqScanInfo)
        "Sort" -> do
          sortInfo <- parseSortNodeInfo object
          pure (SortNode info sortInfo)
        typ -> fail ("Unknown node type: " ++ Text.unpack typ)

data NodeInfo = NodeInfo
  { actualLoops :: !Int,
    actualRows :: !Int,
    actualStartupTime :: !Double,
    actualTotalTime :: !Double,
    parallelAware :: !Bool,
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
  parallelAware <- Aeson.parseField @Bool object "Parallel Aware"
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
