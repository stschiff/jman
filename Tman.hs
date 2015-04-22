module Tman (run, Task(..), LSFopt(..), SubmissionInfo(..), format, makedirs, getEnv, (</>), (<.>)) where

import Tman.Run (run)
import Tman.Task (Task(..), LSFopt(..), SubmissionInfo(..))
import Text.Format (format)
import System.Directory (createDirectoryIfMissing)
import System.Environment (getEnv)
import System.FilePath.Posix ((</>), (<.>))

makedirs :: FilePath -> IO ()
makedirs path = createDirectoryIfMissing True path

