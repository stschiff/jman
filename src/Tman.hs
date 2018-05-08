{-# LANGUAGE OverloadedStrings #-}
module Tman (task,
             inputTasks,
             inputFiles,
             outputFiles,
             mem,
             nrThreads,
             minutes,
             partition,
             addTask,
             tman,
             TmanBlock,
             TaskSpec) where
    
import Tman.Task (TaskSpec(..))
import qualified Tman.Project as P

import Control.Error (runScript)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.State.Strict (execStateT, StateT, get, put)
import qualified Data.Map as M
import Data.Text (Text)
import Filesystem.Path (FilePath)
import Prelude hiding (FilePath)
import Turtle

task :: FilePath -> Text -> TaskSpec
task name command = TaskSpec (format fp name) [] [] [] command 100 1 60 "Short"

type Setter a = a -> TaskSpec -> TaskSpec
type TmanBlock = StateT P.Project IO ()

inputTasks :: Setter [FilePath]
inputTasks it taskSpec = taskSpec {_tsInputTasks = map (format fp) it}

inputFiles :: Setter [FilePath]
inputFiles i taskSpec = taskSpec {_tsInputFiles = map (format fp) i}

outputFiles :: Setter [FilePath]
outputFiles o' taskSpec = taskSpec {_tsOutputFiles = map (format fp) o'}

mem :: Setter Int
mem m taskSpec = taskSpec {_tsMem = m}

nrThreads :: Setter Int
nrThreads t taskSpec = taskSpec {_tsNrThreads = t}

minutes :: Setter Int
minutes m taskSpec = taskSpec {_tsMinutes = m}

partition :: Setter Text
partition p taskSpec = taskSpec {_tsPartition = p}

tman :: FilePath -> TmanBlock -> IO ()
tman logDir block = do
    Just projectFile <- need "TMAN_PROJECT_FILE"
    let project = P.Project "" logDir M.empty []
    project' <- execStateT block project
    runScript $ P.saveProject (fromText projectFile) project'

addTask :: TaskSpec -> TmanBlock
addTask taskSpec = do
    pr <- get
    pr' <- liftIO . runScript $ P.addTask pr True taskSpec
    put pr'

