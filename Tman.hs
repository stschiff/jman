module Tman (run,
             Task(..),
             LSFopt(..),
             SubmissionInfo(..),
             format,
             makedirs,
             getEnv,
             getHomeDirectory,
             findGroup,
             findTask,
             JobProject,
             (</>),
             (<.>)) where

import Tman.Run (run, findGroup, JobProject)
import Tman.Task (Task(..), LSFopt(..), SubmissionInfo(..), makedirs)
import Text.Format (format)
import System.Directory (getHomeDirectory)
import System.Environment (getEnv)
import System.FilePath.Posix ((</>), (<.>))

findTask :: JobProject -> String -> Maybe Task
findTask jobProject name =
    let allTasks = concatMap snd jobProject
        found = filter ((==name) . _tName) allTasks
    in  if length found > 0 then Just $ head found else Nothing