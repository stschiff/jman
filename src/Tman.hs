{-# LANGUAGE OverloadedStrings #-}
module Tman (task,
             inputTasks,
             inputFiles,
             outputFiles,
             mem,
             nrThreads,
             hours,
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
task name command = TaskSpec (format fp name) [] [] [] command 100 1 12

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

hours :: Setter Int
hours h taskSpec = taskSpec {_tsHours = h}

tman :: FilePath -> TmanBlock -> IO ()
tman logDir block = do
    Just projectFile <- need "TMAN_PROJECT_FILE"
--     let projectFile = fromText $ format ("."%fp%".tman") scriptFile
--     ds <- datefile scriptFile
--     t <- testfile projectFile
--     update <- if (not t)
--         then
--             return True
--         else do
--             dp <- datefile projectFile
--             return $ ds > dp
--     when update $ do
    let project = P.Project "" logDir M.empty []
    project' <- execStateT block project
    runScript $ P.saveProject (fromText projectFile) project'
 --   args <- arguments 
 --   _ <- proc "tman" ("-p" : format s projectFile : args) empty
 --   return ()

addTask :: TaskSpec -> TmanBlock
addTask taskSpec = do
    pr <- get
    pr' <- liftIO . runScript $ P.addTask pr True taskSpec
    put pr'

