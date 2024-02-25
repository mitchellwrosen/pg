module Main where

import Control.Applicative (asum)
import Control.Exception (bracket)
import Data.ByteString qualified as ByteString
import Data.Foldable (fold)
import Data.Functor (void)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Options.Applicative qualified as Opt
import System.Directory qualified as Directory
import System.Exit (ExitCode (..), exitWith)
import System.IO qualified as IO
import System.Process qualified as Process
import Prelude hiding (lines, read)

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
            [ Opt.progDesc "Postgres cluster commands."
            ]
            "cluster"
            ( subcommands
                [ subcommand
                    [ Opt.progDesc "Create a Postgres cluster."
                    ]
                    "create"
                    ( pgClusterCreate
                        <$> textArg [Opt.metavar "«name»"]
                    ),
                  subcommand
                    [ Opt.progDesc "Start a Postgres cluster."
                    ]
                    "start"
                    ( pgClusterStart
                        <$> textArg [Opt.metavar "«name»"]
                    )
                ]
            )
        ]
    ]

pgClusterCreate :: Text -> IO ()
pgClusterCreate name = do
  Directory.createDirectoryIfMissing False "_postgres_clusters"
  (out, err, code) <-
    process
      "initdb"
      [ "--encoding=UTF8",
        "--locale=en_US.UTF-8",
        "--no-sync",
        "--pgdata=_postgres_clusters/" <> name
      ]
  case code of
    ExitSuccess -> pure ()
    ExitFailure _ -> do
      Text.putStr out
      Text.putStr err
      exitWith code

pgClusterStart :: Text -> IO ()
pgClusterStart name = do
  cwd <- Directory.getCurrentDirectory
  code <-
    foreground
      "postgres"
      [ "-D",
        "_postgres_clusters/" <> name,
        -- disable fsync
        "-F",
        -- don't listen on IP
        "-h",
        "",
        -- unix socket directory
        "-k",
        Text.pack cwd <> "/_postgres_clusters/" <> name
      ]
  exitWith code

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

------------------------------------------------------------------------------------------------------------------------
-- Subprocess utils

foreground :: Text -> [Text] -> IO ExitCode
foreground name args = do
  withDevNull \devNull -> do
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
              Process.std_in = Process.UseHandle devNull,
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
