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
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Data.Text.Read qualified as Text
import Data.Traversable (for)
import Hasql.Connection qualified as Hasql
import Hasql.Session qualified as Hasql
import Network.Socket qualified as Network
import Options.Applicative (optional)
import Options.Applicative qualified as Opt
import PgPlan (Analyze)
import PgPlanJson ()
import PgPlanPretty (prettyAnalyze)
import PgPostmasterPid (PostmasterPid (..), parsePostmasterPid)
import PgPrettyUtils (putPretty)
import PgQueries qualified
import PgTablePretty (prettyTable)
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
            (pure pgRepl),
          subcommand
            [ Opt.progDesc "List tables in a Postgres database."
            ]
            "tables"
            (pure pgTables),
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
      [ "--encoding=UTF8",
        "--locale=en_US.UTF-8",
        "--no-sync",
        "--pgdata=" <> clusterDir,
        "--username=postgres"
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
  whenNotM (Directory.doesFileExist (Text.unpack postmasterFile)) do
    Text.putStrLn ("There's no cluster running at " <> clusterDir)
    exitFailure
  (Text.lines <$> Text.readFile (Text.unpack postmasterFile)) >>= \case
    (readMaybe . Text.unpack -> Just pid) : _ -> Posix.signalProcess Posix.sigTERM pid
    _ -> Text.putStrLn ("Could not read PID from " <> postmasterFile)

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
  query <-
    Directory.doesFileExist (Text.unpack queryOrFilename) >>= \case
      False -> pure queryOrFilename
      True -> Text.readFile (Text.unpack queryOrFilename)
  username <- resolveValue (Def (TextEnv "PGUSER") (pure "postgres"))
  (out, err, code) <-
    process
      "psql"
      [ "--command=" <> query,
        "--dbname=" <> dbname,
        "--host=" <> host,
        "--port=" <> port,
        "--username=" <> username
      ]
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
          [ map ("--command=" <>) commands,
            [ "--dbname=" <> dbname,
              "--host=" <> host,
              "--no-align",
              "--port=" <> port,
              -- This silences the "PREPARE" output, if we prepare
              "--quiet",
              "--tuples-only",
              "--username=" <> username
            ]
          ]
      )
  if code == ExitSuccess
    then do
      Text.putStrLn out
      Text.putStrLn query
      case Aeson.eitherDecodeStrictText @[Analyze] out of
        Left parseError -> Text.putStrLn (Text.pack parseError)
        Right (head -> analyze) -> putPretty (prettyAnalyze analyze)
    else do
      Text.putStr out
      Text.putStr err
  exitWith code

pgLoad :: Maybe Text -> Text -> IO ()
pgLoad maybeDatabase file = do
  dbname <- resolveValue (Def (Opt maybeDatabase) (pure "postgres"))
  host <- getStateDir
  port <- resolveValue (Def (TextEnv "PGPORT") (pure "5432"))
  username <- resolveValue (Def (TextEnv "PGUSER") (pure "postgres"))
  (out, err, code) <-
    process
      "pg_restore"
      [ "--dbname=" <> dbname,
        "--host=" <> host,
        "--port=" <> port,
        "--username=" <> username,
        file
      ]
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

pgRepl :: IO ()
pgRepl = do
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
  username <- resolveValue (Def (TextEnv "PGUSER") (pure "postgres"))
  code <-
    foreground
      "psql"
      [ "--dbname=" <> dbname,
        "--host=" <> host,
        "--port=" <> port,
        "--pset=null=∅",
        let style x y = "%[%033[" <> Text.intercalate ";" x <> "m%]" <> y <> "%[%033[0m%]"
            blue = "34"
            bold = "1"
            green = "32"
            italic = "3"
            magenta = "35"
            yellow = "33"
         in fold
              [ "--set=PROMPT1=",
                "╭ ",
                style [italic, yellow] "host",
                " ",
                style [bold, yellow] "%M",
                "\n│ ",
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
              ],
        "--set=PROMPT2=%w ",
        "--username=" <> username
      ]
  exitWith code

pgTables :: IO ()
pgTables = do
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
  username <- resolveValue (Def (TextEnv "PGUSER") (pure "postgres"))
  let settings =
        Hasql.settings
          (Text.encodeUtf8 host)
          ( case Text.decimal port of
              Right (port1, "") -> port1
              _ -> 5432
          )
          (Text.encodeUtf8 username)
          ByteString.empty
          (Text.encodeUtf8 dbname)
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
                -- haha n+1 query, but it shouldn't matter
                columns <- for tables \(oid, _, _, _, _) -> Hasql.statement () (PgQueries.readColumns oid)
                pure (zip tables columns)
           in Right <$> Hasql.run session connection
  result1 <-
    result & onLeft \connErr -> do
      Text.putStrLn (maybe "" Text.decodeUtf8 connErr)
      exitFailure
  rows <-
    result1 & onLeft \queryErr -> do
      Text.putStrLn (Text.pack (show queryErr))
      exitFailure
  for_ rows \((_oid, schema, name, tuples, bytes), columns) ->
    putPretty (prettyTable schema name columns tuples bytes)

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
                      postmasterPid <-
                        parsePostmasterPid contents & onNothing do
                          Text.putStrLn ("Internal error: could not parse contents of file " <> postmasterFile)
                          exitFailure
                      let unixSocketFile = postmasterPid.socketDir <> "/.s.PGSQL." <> postmasterPid.port
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
