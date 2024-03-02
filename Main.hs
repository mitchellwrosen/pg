module Main (main) where

import Control.Applicative (asum)
import Control.Exception (bracket)
import Control.Monad (when)
import Data.Aeson qualified as Aeson
import Data.ByteString qualified as ByteString
import Data.Foldable (fold)
import Data.Function ((&))
import Data.Functor (void)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Options.Applicative (optional)
import Options.Applicative qualified as Opt
import PgPlan (Analyze)
import PgPlanJson ()
import PgPlanPretty (prettyAnalyze)
import Prettyprinter qualified
import Prettyprinter.Render.Terminal qualified
import System.Directory qualified as Directory
import System.Exit (ExitCode (..), exitFailure, exitWith)
import System.IO qualified as IO
import System.Posix.ByteString qualified as Posix
import System.Process qualified as Process
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
          [ "--command=EXPLAIN (ANALYZE ON, FORMAT JSON) " <> query,
            "--dbname=" <> fromMaybe "postgres" maybeDatabase,
            "--host=" <> stateDir,
            "--no-align",
            "--tuples-only"
          ]
      if code == ExitSuccess
        then do
          Text.putStrLn out
          Text.putStrLn query
          case Aeson.eitherDecodeStrictText @[Analyze] out of
            Left parseError -> Text.putStrLn (Text.pack parseError)
            Right (head -> analyze) ->
              analyze
                & prettyAnalyze
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
