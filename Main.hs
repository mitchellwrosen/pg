module Main (main) where

import Control.Applicative (asum, many)
import Control.Concurrent (threadDelay)
import Control.Exception (SomeAsyncException (..), SomeException, bracket, fromException, throwIO, try)
import Control.Monad (when)
import Data.Aeson qualified as Aeson
import Data.ByteString qualified as ByteString
import Data.Foldable (fold, for_)
import Data.Function ((&))
import Data.Functor (void)
import Data.List qualified as List
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Builder.Linear qualified as Text.Builder
import Data.Text.Encoding qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Data.Text.Read qualified as Text
import Hasql.Connection qualified as Hasql
import Hasql.Connection.Setting qualified
import Hasql.Connection.Setting.Connection qualified
import Hasql.Connection.Setting.Connection.Param qualified
import Hasql.Session qualified as Hasql
import Network.Socket qualified as Network
import Options.Applicative (optional)
import Options.Applicative qualified as Opt
import PgPlan (Analyze)
import PgPlanJson ()
import PgPlanPretty (prettyAnalyze)
import PgPostmasterPid (parsePostmasterPid)
import PgPrettyUtils (putPretty)
import PgQueries qualified
import PgTablePretty (prettyTable)
import PgUtils (rowsByKey)
import Prettyprinter qualified
import System.Directory qualified as Directory
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..), exitFailure, exitWith)
import System.IO qualified as IO
import System.Posix.ByteString qualified as Posix
import System.Process qualified as Process
import Text.ANSI qualified
import Text.Read (readMaybe)
import Prelude hiding (filter, lines, read)

main :: IO ()
main = do
  commandLineInterface
    [ Opt.showHelpOnEmpty,
      Opt.showHelpOnError
    ]
    [ Opt.progDescDoc $
        Just $
          fold
            [ Prettyprinter.pretty @Text "Postgres utility knife.",
              Prettyprinter.line,
              Prettyprinter.line,
              Prettyprinter.pretty @Text "The following environment variables may affect behavior:",
              Prettyprinter.line,
              Prettyprinter.line,
              Prettyprinter.indent 2 $
                fold
                  [ Prettyprinter.pretty @Text "PGDATABASE",
                    Prettyprinter.line,
                    Prettyprinter.pretty @Text "PGHOST",
                    Prettyprinter.line,
                    Prettyprinter.pretty @Text "PGPORT",
                    Prettyprinter.line,
                    Prettyprinter.pretty @Text "PGUSER"
                  ]
            ]
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
            [ Opt.progDesc "Dump a Postgres database to a file."
            ]
            "dump"
            ( pgDump
                <$> textOpt [Opt.metavar "DBNAME", Opt.short 'd']
            ),
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
                <*> many (textArg [Opt.metavar "PARAMETER"])
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
            [ Opt.progDesc "Show the logs of the Postgres cluster."
            ]
            "logs"
            (pure pgLogs),
          subcommand
            [ Opt.progDesc "Connect to a Postgres cluster."
            ]
            "repl"
            ( pgRepl
                <$> textOpt [Opt.metavar "DBNAME", Opt.short 'd']
                <*> textOpt [Opt.metavar "HOST", Opt.short 'h']
                <*> textOpt [Opt.metavar "PORT", Opt.short 'p']
                <*> textOpt [Opt.metavar "USERNAME", Opt.short 'u']
            ),
          subcommand
            [ Opt.progDesc "Print the syntax of Postgres DDL."
            ]
            "syntax"
            ( subcommands
                [ subcommand
                    [ Opt.progDesc "Create an index."
                    ]
                    "create-index"
                    ( pgSyntaxCreateIndex
                        <$> Opt.switch
                          ( Opt.help "Build index without preventing concurrent writes."
                              <> Opt.long "concurrently"
                          )
                        <*> textOpt
                          [ Opt.help "Index name, and do nothing if index with same name already exists (incompatible with --name).",
                            Opt.long "if-not-exists",
                            Opt.metavar "NAME"
                          ]
                        <*> many
                          ( Opt.strOption
                              ( Opt.help "Include non-key column in index."
                                  <> Opt.long "include"
                                  <> Opt.metavar "COLUMN"
                              )
                          )
                        <*> textOpt
                          [ Opt.help "Index name (incompatible with --if-not-exists).",
                            Opt.long "name",
                            Opt.metavar "NAME"
                          ]
                        <*> Opt.switch
                          ( Opt.help "Don't recurse creating indexes on partitions, if table is partitioned."
                              <> Opt.long "only"
                          )
                        <*> textOpt [Opt.help "Table name.", Opt.long "table", Opt.metavar "TABLE"]
                        <*> Opt.switch (Opt.help "Prevent duplicate values in index, allowing duplicate NULL values (incompatible with --unique-nulls-not-distinct)." <> Opt.long "unique")
                        <*> Opt.switch (Opt.help "Prevent duplicate values in index, disallowing duplicate NULL values (incompatible with --unique)." <> Opt.long "unique-nulls-not-distinct")
                        <*> textOpt
                          [ Opt.help "Index method (btree, hash, gist, spgist, bin, brin, or user-installed method)",
                            Opt.long "using",
                            Opt.metavar "METHOD"
                          ]
                    )
                ]
            ),
          subcommand
            [ Opt.progDesc "List tables in a Postgres database."
            ]
            "tables"
            ( pgTables
                <$> textOpt [Opt.metavar "DBNAME", Opt.short 'd']
                <*> textOpt [Opt.metavar "HOST", Opt.short 'h']
                <*> textOpt [Opt.metavar "PORT", Opt.short 'p']
                <*> textOpt [Opt.metavar "USERNAME", Opt.short 'u']
                <*> many (textArg [Opt.metavar "TABLE"])
            ),
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
  whenM (Directory.doesDirectoryExist (Text.unpack clusterDir)) do
    Text.putStrLn ("There's already a Postgres cluster at " <> clusterDir)
    exitFailure
  (out, err, code) <-
    process
      "initdb"
      ( fold
          [ ["-D", clusterDir],
            ["-E", "UTF8"],
            ["--locale=en_US.UTF-8"],
            ["-U", "postgres"]
          ]
      )
  when (code /= ExitSuccess) do
    Text.putStr out
    Text.putStr err
    exitWith code

pgDown :: IO ()
pgDown = do
  stateDir <- getStateDir
  let clusterDir = stateDir <> "/data"
  let postmasterFile = clusterDir <> "/postmaster.pid"
  whenNotM (Directory.doesFileExist (Text.unpack postmasterFile)) do
    Text.putStrLn ("There's no cluster running at " <> clusterDir)
    exitFailure
  (Text.lines <$> Text.readFile (Text.unpack postmasterFile)) >>= \case
    (readMaybe . Text.unpack -> Just pid) : _ -> Posix.signalProcess Posix.sigTERM pid
    _ -> Text.putStrLn ("Could not read PID from " <> postmasterFile)

pgDump :: Maybe Text -> IO ()
pgDump maybeDatabase = do
  dbname <-
    resolveValue (Or (Opt maybeDatabase) (TextEnv "PGDATABASE")) >>= \case
      Nothing -> do
        Text.putStrLn ("You must specify a database name with either `PGDATABASE` or `-d`.")
        exitFailure
      Just dbname -> pure dbname
  host <- getStateDir
  port <- resolveValue (Def (TextEnv "PGPORT") (pure "5432"))
  username <- resolveValue (Def (TextEnv "PGUSER") (pure "postgres"))
  (out, err, code) <-
    process
      "pg_dump"
      ( fold
          [ ["-d", dbname],
            ["-f", dbname <> ".pg_dump"],
            ["-F", "c"],
            ["-h", host],
            ["-p", port],
            ["-U", username]
          ]
      )
  when (code /= ExitSuccess) do
    Text.putStr out
    Text.putStr err
    exitWith code

pgExec :: Text -> IO ()
pgExec queryOrFilename = do
  dbname <- resolveValue (Def (TextEnv "PGDATABASE") (pure "postgres"))
  host <-
    resolveValue $
      Def
        (TextEnv "PGHOST")
        ( do
            stateDir <- getStateDir
            let clusterDir = stateDir <> "/data"
            whenNotM (Directory.doesFileExist (Text.unpack (clusterDir <> "/postmaster.pid"))) do
              Text.putStrLn ("There's no cluster running at " <> clusterDir)
              exitFailure
            pure stateDir
        )
  port <- resolveValue (Def (TextEnv "PGPORT") (pure "5432"))
  isFilename <- Directory.doesFileExist (Text.unpack queryOrFilename)
  username <- resolveValue (Def (TextEnv "PGUSER") (pure "postgres"))
  (out, err, code) <-
    process
      "psql"
      ( fold
          [ if isFilename then [] else ["-c", queryOrFilename],
            ["-d", dbname],
            if isFilename then ["-f", queryOrFilename] else [],
            ["-h", host],
            ["-p", port],
            ["-U", username]
          ]
      )
  Text.putStr out
  Text.putStr err
  exitWith code

pgExplain :: Maybe Text -> Text -> [Text] -> IO ()
pgExplain maybeDatabase queryOrFilename parameters = do
  dbname <- resolveValue (Def (Or (Opt maybeDatabase) (TextEnv "PGDATABASE")) (pure "postgres"))
  host <-
    resolveValue $
      Def
        (TextEnv "PGHOST")
        ( do
            stateDir <- getStateDir
            let clusterDir = stateDir <> "/data"
            whenNotM (Directory.doesFileExist (Text.unpack (clusterDir <> "/postmaster.pid"))) do
              Text.putStrLn ("There's no cluster running at " <> clusterDir)
              exitFailure
            pure stateDir
        )
  port <- resolveValue (Def (TextEnv "PGPORT") (pure "5432"))
  query <-
    Directory.doesFileExist (Text.unpack queryOrFilename) >>= \case
      False -> pure queryOrFilename
      True -> Text.readFile (Text.unpack queryOrFilename)
  username <- resolveValue (Def (TextEnv "PGUSER") (pure "postgres"))

  let commands =
        let explain = ("EXPLAIN (ANALYZE ON, FORMAT JSON) " <>)
         in fold
              [ -- '\x on' puts a "QUERY PLAN|" slug in the output, so turn it off
                ["--command=\\x off"],
                if null parameters
                  then [explain query]
                  else
                    [ "PREPARE query AS " <> query,
                      explain ("EXECUTE query(" <> Text.intercalate ", " parameters <> ")")
                    ]
              ]

  (out, err, code) <-
    process
      "psql"
      ( fold
          [ ["-A"],
            foldMap (\command -> ["-c", command]) commands,
            ["-d", dbname],
            ["-h", host],
            ["-p", port],
            ["-q"], -- This silences the "PREPARE" output, if we prepare
            ["-t"],
            ["-U", username]
          ]
      )
  if code == ExitSuccess
    then do
      Text.putStrLn out
      Text.putStrLn query
      case Aeson.eitherDecodeStrictText @[Analyze] out of
        Left parseError -> Text.putStrLn (Text.pack parseError)
        Right (analyze : _) -> putPretty (prettyAnalyze analyze)
        Right [] -> exitFailure
    else do
      Text.putStr out
      Text.putStr err
  exitWith code

pgLoad :: Maybe Text -> Text -> IO ()
pgLoad maybeDatabase file = do
  dbname <- resolveValue (Def (Or (Opt maybeDatabase) (TextEnv "PGDATABASE")) (pure "postgres"))
  host <- getStateDir
  port <- resolveValue (Def (TextEnv "PGPORT") (pure "5432"))
  username <- resolveValue (Def (TextEnv "PGUSER") (pure "postgres"))
  (out, err, code) <-
    process
      "pg_restore"
      ( fold
          [ ["-d", dbname],
            ["-h", host],
            ["-p", port],
            ["-U", username],
            [file]
          ]
      )
  when (code /= ExitSuccess) do
    Text.putStr out
    Text.putStr err
    exitWith code

pgLogs :: IO ()
pgLogs = do
  stateDir <- getStateDir
  let clusterDir = stateDir <> "/data"
  let logFile = stateDir <> "/log.txt"
  let postmasterFile = clusterDir <> "/postmaster.pid"
  whenNotM (Directory.doesFileExist (Text.unpack postmasterFile)) do
    Text.putStrLn ("There's no cluster running at " <> clusterDir)
    exitFailure
  Posix.touchFile (Text.encodeUtf8 logFile) -- in case we get here before postgres creates it
  void (foreground "tail" ["-f", logFile])

pgRepl :: Maybe Text -> Maybe Text -> Maybe Text -> Maybe Text -> IO ()
pgRepl maybeDatabase maybeHost maybePort maybeUsername = do
  dbname <- resolveValue (Def (Or (Opt maybeDatabase) (TextEnv "PGDATABASE")) (pure "postgres"))
  stateDir <- getStateDir
  host <-
    resolveValue $
      Def
        (Or (Opt maybeHost) (TextEnv "PGHOST"))
        ( do
            let clusterDir = stateDir <> "/data"
            whenNotM (Directory.doesFileExist (Text.unpack (clusterDir <> "/postmaster.pid"))) do
              Text.putStrLn ("There's no cluster running at " <> clusterDir)
              exitFailure
            pure stateDir
        )
  port <- resolveValue (Def (Or (Opt maybePort) (TextEnv "PGPORT")) (pure "5432"))
  username <- resolveValue (Def (Or (Opt maybeUsername) (TextEnv "PGUSER")) (pure "postgres"))
  code <-
    foreground
      "psql"
      ( fold
          [ ["-d", dbname],
            ["-h", host],
            ["-p", port],
            ["-P", "null=∅"],
            ["-U", username],
            [ "-v",
              let style x y = "%[%033[" <> Text.intercalate ";" x <> "m%]" <> y <> "%[%033[0m%]"
                  blue = "34"
                  bold = "1"
                  green = "32"
                  italic = "3"
                  magenta = "35"
                  yellow = "33"
               in fold
                    [ "PROMPT1=",
                      "╭ ",
                      if host == stateDir
                        then mempty
                        else
                          fold
                            [ style [italic, yellow] "host",
                              " ",
                              style [bold, yellow] "%M",
                              "\n│ "
                            ],
                      style [italic, green] "port",
                      " ",
                      style [bold, green] "%>",
                      "\n│ ",
                      style [italic, magenta] "user",
                      " ",
                      style [bold, magenta] "%n",
                      "\n│ ",
                      style [italic, blue] "database",
                      " ",
                      style [bold, blue] "%/",
                      "\n╰ ",
                      "%# " -- # (superuser) or > (user)
                    ]
            ],
            ["-v", "PROMPT2=%w "]
          ]
      )
  exitWith code

pgSyntaxCreateIndex :: Bool -> Maybe Text -> [Text] -> Maybe Text -> Bool -> Maybe Text -> Bool -> Bool -> Maybe Text -> IO ()
pgSyntaxCreateIndex concurrently maybeIfNotExists include maybeName only maybeTable unique uniqueNullsNotDistinct maybeUsing = do
  case (maybeIfNotExists, maybeName) of
    (Just _, Just _) -> do
      Text.putStrLn "--if-not-exists and --name are incompatible."
      exitFailure
    _ -> pure ()
  case (unique, uniqueNullsNotDistinct) of
    (True, True) -> do
      Text.putStrLn "--unique and --unique-nulls-not-distinct are incompatible."
      exitFailure
    _ -> pure ()
  Text.putStrLn . Text.Builder.runBuilder . fold $
    [ "CREATE ",
      if unique || uniqueNullsNotDistinct then "UNIQUE " else mempty,
      "INDEX ",
      if concurrently then "CONCURRENTLY " else mempty,
      maybe mempty (("IF NOT EXISTS " <>) . Text.Builder.fromText) maybeIfNotExists,
      maybe mempty ((<> " ") . Text.Builder.fromText) maybeName,
      "ON ",
      if only then "ONLY " else mempty,
      maybe "«table»" Text.Builder.fromText maybeTable,
      " ",
      maybe "USING btree " (\method -> "USING " <> Text.Builder.fromText method <> " ") maybeUsing,
      "(«column», «expression», ...) ",
      case include of
        [] -> mempty
        _ -> "INCLUDE (" <> fold (List.intersperse ", " (map Text.Builder.fromText include)) <> ") ",
      if unique then "NULLS DISTINCT " else if uniqueNullsNotDistinct then "NULLS NOT DISTINCT " else mempty
    ]

-- TODO: pg_class relpersistence, relispartition
pgTables :: Maybe Text -> Maybe Text -> Maybe Text -> Maybe Text -> [Text] -> IO ()
pgTables maybeDatabase maybeHost maybePort maybeUsername tableNamesFilter = do
  dbname <- resolveValue (Def (Or (Opt maybeDatabase) (TextEnv "PGDATABASE")) (pure "postgres"))
  host <-
    resolveValue $
      Def
        (Or (Opt maybeHost) (TextEnv "PGHOST"))
        ( do
            stateDir <- getStateDir
            let clusterDir = stateDir <> "/data"
            whenNotM (Directory.doesFileExist (Text.unpack (clusterDir <> "/postmaster.pid"))) do
              Text.putStrLn ("There's no cluster running at " <> clusterDir)
              exitFailure
            pure stateDir
        )
  port <- resolveValue (Def (Or (Opt maybePort) (TextEnv "PGPORT")) (pure "5432"))
  username <- resolveValue (Def (Or (Opt maybeUsername) (TextEnv "PGUSER")) (pure "postgres"))
  let settings =
        [ Hasql.Connection.Setting.connection
            ( Hasql.Connection.Setting.Connection.params
                [ Hasql.Connection.Setting.Connection.Param.dbname dbname,
                  Hasql.Connection.Setting.Connection.Param.host host,
                  Hasql.Connection.Setting.Connection.Param.port case Text.decimal port of
                    Right (port1, "") -> port1
                    _ -> 5432,
                  Hasql.Connection.Setting.Connection.Param.user username
                ]
            )
        ]
  result <-
    bracket
      (Hasql.acquire settings)
      \case
        Left _err -> pure ()
        Right connection -> Hasql.release connection
      \case
        Left err -> pure (Left err)
        Right connection ->
          let session = do
                tables <- Hasql.statement () PgQueries.readTables
                let tableOids = map (.oid) tables
                columns <- Hasql.statement () (PgQueries.readColumns tableOids)
                foreignKeyConstraints <- Hasql.statement () (PgQueries.readForeignKeyConstraints tableOids)
                incomingForeignKeyConstraints <- Hasql.statement () (PgQueries.readIncomingForeignKeyConstraints tableOids)
                checkConstraints <- Hasql.statement () (PgQueries.readCheckConstraints tableOids)
                indexes <- Hasql.statement () (PgQueries.readIndexes tableOids)
                pure (tables, columns, foreignKeyConstraints, incomingForeignKeyConstraints, checkConstraints, indexes)
           in Right <$> Hasql.run session connection
  result1 <-
    result & onLeft \connErr -> do
      Text.putStrLn (maybe "" Text.decodeUtf8 connErr)
      exitFailure
  (tables, columns, foreignKeyConstraints, incomingForeignKeyConstraints, checkConstraints, indexes) <-
    result1 & onLeft \queryErr -> do
      Text.putStrLn (Text.pack (show queryErr))
      exitFailure
  let getColumns = rowsByKey (.tableOid) columns
  let getForeignKeyConstraints = rowsByKey (.sourceTableOid) foreignKeyConstraints
  let getIncomingForeignKeyConstraints = rowsByKey (.targetTableOid) incomingForeignKeyConstraints
  let getCheckConstraints = rowsByKey (.tableOid) checkConstraints
  let getIndexes = rowsByKey (.tableOid) indexes
  let shouldPrintTable =
        case tableNamesFilter of
          [] -> \_ -> True
          names -> let names1 = Set.fromList names in (`Set.member` names1)
  for_ tables \table ->
    when (shouldPrintTable table.name) do
      putPretty $
        prettyTable
          table
          (getColumns table.oid)
          (getForeignKeyConstraints table.oid)
          (getIncomingForeignKeyConstraints table.oid)
          (getCheckConstraints table.oid)
          (getIndexes table.oid)

pgUp :: IO ()
pgUp = do
  stateDir <- getStateDir
  let clusterDir = stateDir <> "/data"
  let logFile = stateDir <> "/log.txt"
  let postmasterFile = clusterDir <> "/postmaster.pid"
  whenNotM (Directory.doesDirectoryExist (Text.unpack clusterDir)) do
    Text.putStrLn ("There's no cluster at " <> clusterDir)
    exitFailure
  whenM (Directory.doesFileExist (Text.unpack postmasterFile)) do
    Text.putStrLn ("There's already a cluster running at " <> clusterDir)
    exitFailure
  background
    "postgres"
    ( fold
        [ ["-D", clusterDir],
          -- disable fsync
          ["-F"],
          -- don't listen on IP
          ["-h", ""],
          -- unix socket directory
          ["-k", stateDir]
        ]
    )
    logFile
  bracket (Network.socket Network.AF_UNIX Network.Stream Network.defaultProtocol) Network.close \socket -> do
    let loop tried maybeUnixSocketAddr
          | tried >= 5 = do
              Posix.touchFile (Text.encodeUtf8 logFile) -- in case we get here before postgres creates it
              logs <- Text.readFile (Text.unpack logFile)
              Text.putStr logs
              exitFailure
          | otherwise = do
              threadDelay 25_000
              case maybeUnixSocketAddr of
                Nothing ->
                  try @SomeException (Text.readFile (Text.unpack postmasterFile)) >>= \case
                    Left exception -> do
                      assertSynchronousException exception
                      loop (tried + 1) Nothing
                    Right contents -> do
                      port <-
                        parsePostmasterPid contents & onNothing do
                          Text.putStrLn ("Internal error: could not parse contents of file " <> postmasterFile)
                          exitFailure
                      let unixSocketFile = stateDir <> "/.s.PGSQL." <> port
                      loop2 tried (Network.SockAddrUnix (Text.unpack unixSocketFile))
                Just unixSocketAddr -> loop2 tried unixSocketAddr
        loop2 tried unixSocketAddr =
          try @SomeException (Network.connect socket unixSocketAddr) & onLeftM \exception -> do
            assertSynchronousException exception
            loop (tried + 1) (Just unixSocketAddr)
    loop (0 :: Int) Nothing

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

data Val a where
  Def :: Val (Maybe a) -> IO a -> Val a
  Opt :: Maybe a -> Val (Maybe a)
  Or :: Val (Maybe a) -> Val (Maybe a) -> Val (Maybe a)
  TextEnv :: !Text -> Val (Maybe Text)

resolveValue :: Val a -> IO a
resolveValue = \case
  Def x y -> resolveValue x >>= maybe y pure
  Opt x -> pure x
  Or x y -> resolveValue x >>= maybe (resolveValue y) (pure . Just)
  TextEnv x -> fmap (Text.pack) <$> lookupEnv (Text.unpack x)

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

      printProcess name args

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

  printProcess name args

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

        printProcess name args

        bracket (Process.createProcess config) cleanup \(_, _, _, handle) -> do
          exitCode <- Process.waitForProcess handle
          out <- ByteString.hGetContents stdoutReadHandle
          err <- ByteString.hGetContents stderrReadHandle
          pure (Text.decodeUtf8 out, Text.decodeUtf8 err, exitCode)

printProcess :: Text -> [Text] -> IO ()
printProcess name args =
  Text.putStrLn (Text.ANSI.bold (Text.unwords (name : map quote args)))
  where
    quote arg
      | Text.null arg = "''"
      | Text.any (== ' ') arg = "'" <> Text.replace "'" "\\'" arg <> "'"
      | otherwise = arg

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

-- If in "$HOME, returns ""
-- If in "$HOME/foo/bar, returns "/foo/bar"
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
  pure (Text.pack stateDir <> "/" <> mangle currentDir)
  where
    mangle dir
      | Text.null dir = "_"
      | otherwise = Text.replace "/" "_" (Text.replace "_" "__" dir)

------------------------------------------------------------------------------------------------------------------------
-- Misc utils and missing prelude functions

assertSynchronousException :: SomeException -> IO ()
assertSynchronousException exception =
  case fromException @SomeAsyncException exception of
    Just _ -> throwIO exception
    Nothing -> pure ()

whenM :: (Monad m) => m Bool -> m () -> m ()
whenM mx my = do
  x <- mx
  when x my

whenNotM :: (Monad m) => m Bool -> m () -> m ()
whenNotM mx my = do
  x <- mx
  when (not x) my

onLeft :: (Applicative m) => (a -> m b) -> Either a b -> m b
onLeft mx =
  either mx pure

onLeftM :: (Monad m) => (a -> m b) -> m (Either a b) -> m b
onLeftM mx my =
  my >>= either mx pure

onNothing :: (Applicative m) => m a -> Maybe a -> m a
onNothing mx =
  maybe mx pure

onNothingM :: (Monad m) => m a -> m (Maybe a) -> m a
onNothingM mx my =
  my >>= \case
    Nothing -> mx
    Just x -> pure x
