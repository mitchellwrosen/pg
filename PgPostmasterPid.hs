-- | @postmaster.pid@ file utilities.
module PgPostmasterPid
  ( PostmasterPid (..),
    parsePostmasterPid,
  )
where

import Data.Text (Text)
import Data.Text qualified as Text

data PostmasterPid = PostmasterPid
  { dataDir :: !Text,
    listenAddr :: !Text,
    pid :: !Text,
    port :: !Text,
    shmemId :: !Text,
    shmemKey :: !Text,
    socketDir :: !Text,
    startTime :: !Text,
    status :: !Text
  }
  deriving stock (Show)

parsePostmasterPid :: Text -> Maybe PostmasterPid
parsePostmasterPid text =
  case Text.lines text of
    [pid, dataDir, startTime, port, socketDir, listenAddr, Text.words -> [shmemKey, shmemId], status] ->
      Just
        PostmasterPid
          { dataDir,
            listenAddr,
            pid,
            port,
            shmemId,
            shmemKey,
            socketDir,
            startTime,
            status
          }
    _ -> Nothing
