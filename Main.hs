module Main where

import Control.Applicative (asum)
import Control.Exception (bracket)
import Data.Foldable (fold)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import Options.Applicative qualified as Opt
import System.Directory qualified as Directory
import System.Exit (ExitCode (..), exitWith)
import System.IO qualified as IO
import System.Posix.IO.ByteString qualified as Posix
import System.Process qualified as Process
import Prelude hiding (lines)

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
            []
            "cluster"
            ( subcommands
                [ subcommand [] "create" $
                    pgClusterCreate
                      <$> textArg [Opt.metavar "«name»"]
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
      [ "--pgdata=_postgres_clusters/" <> name,
        "--encoding=UTF8"
      ]
  case code of
    ExitSuccess -> pure ()
    ExitFailure _ -> do
      Text.putStr (Text.unlines out)
      Text.putStr (Text.unlines err)
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

process :: Text -> [Text] -> IO ([Text], [Text], ExitCode)
process name args = do
  (stdoutReadFd, stdoutWriteFd) <- Posix.createPipe
  (stderrReadFd, stderrWriteFd) <- Posix.createPipe

  let setupFd fd = do
        handle <- Posix.fdToHandle fd
        IO.hSetEncoding handle IO.utf8
        IO.hSetBuffering handle IO.LineBuffering
        pure handle

  stdoutReadHandle <- setupFd stdoutReadFd
  stdoutWriteHandle <- setupFd stdoutWriteFd
  stderrReadHandle <- setupFd stderrReadFd
  stderrWriteHandle <- setupFd stderrWriteFd

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
            Process.std_in = Process.NoStream,
            Process.std_out = Process.UseHandle stdoutWriteHandle,
            Process.use_process_jobs = False
          }

  let cleanup (_, _, _, handle) = do
        Process.terminateProcess handle
        IO.hClose stdoutReadHandle
        IO.hClose stdoutWriteHandle
        IO.hClose stderrReadHandle
        IO.hClose stderrWriteHandle
        _ <- Process.waitForProcess handle
        pure ()

  bracket (Process.createProcess config) cleanup \(_, _, _, handle) -> do
    stdoutLines <- hGetLines stdoutReadHandle
    stderrLines <- hGetLines stderrReadHandle
    exitCode <- Process.waitForProcess handle
    pure (stdoutLines, stderrLines, exitCode)

hGetLines :: IO.Handle -> IO [Text]
hGetLines handle =
  go []
  where
    go lines =
      IO.hIsEOF handle >>= \case
        True -> pure (reverse lines)
        False -> do
          line <- Text.hGetLine handle
          go (line : lines)
