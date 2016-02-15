module Tman (task,
             inputTasks,
             inputFiles,
             outputFiles,
             mem,
             nrThreads,
             hours,
             loadProject,
             saveProject,
             addTask,
             TaskSpec,
             Project) where
    
import Tman.Task (TaskSpec(..))
import qualified Tman.Project as P

import Control.Error (runScript)
import Data.Text (Text)
import Filesystem.Path (FilePath)
import Prelude hiding (FilePath)
import Turtle.Format (format, fp)

task :: FilePath -> Text -> TaskSpec
task name command = TaskSpec (format fp name) [] [] [] command 100 1 12

type Setter a = a -> TaskSpec -> TaskSpec
type Project = P.Project

inputTasks :: Setter [FilePath]
inputTasks it taskSpec = taskSpec {_tsInputTasks = map (format fp) it}

inputFiles :: Setter [FilePath]
inputFiles i taskSpec = taskSpec {_tsInputFiles = map (format fp) i}

outputFiles :: Setter [FilePath]
outputFiles o taskSpec = taskSpec {_tsOutputFiles = map (format fp) o}

mem :: Setter Int
mem m taskSpec = taskSpec {_tsMem = m}

nrThreads :: Setter Int
nrThreads t taskSpec = taskSpec {_tsNrThreads = t}

hours :: Setter Int
hours h taskSpec = taskSpec {_tsHours = h}

loadProject :: IO Project
loadProject = runScript P.loadProject

addTask :: Project -> TaskSpec -> IO Project
addTask project = runScript . P.addTask project

saveProject :: Project -> IO ()
saveProject = runScript . P.saveProject