module Main where

import Control.Exception (bracket)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.IO.Utf8 qualified as Text
import System.Exit (ExitCode)
import System.IO qualified as IO
import System.Posix.IO.ByteString qualified as Posix
import System.Process qualified as Process
import Prelude hiding (lines)

main :: IO ()
main = do
  -- TODO check all required executables

  result <- process "ls" []
  print result

  pure ()

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
            Process.env = Just [],
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
