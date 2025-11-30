-- | @postmaster.pid@ file utilities.
module PgPostmasterPid
  ( parsePostmasterPid,
  )
where

import Data.Text (Text)
import Data.Text qualified as Text

-- Turns out all we care about is the port, heh
parsePostmasterPid :: Text -> Maybe Text
parsePostmasterPid text =
  case Text.lines text of
    _pid : _dataDir : _startTime : port : _ -> Just port
    _ -> Nothing
