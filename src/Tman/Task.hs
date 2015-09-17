module Tman.Task (task,
                 printTask,
                 inputTasks,
                 inputFiles,
                 outputFiles,
                 mem,
                 nrThreads,
                 hours,
                 TaskSpec) where
    
import Tman.Internal.Task (TaskSpec(..), printTask)
import Prelude hiding (FilePath)
import Filesystem.Path (FilePath)
import Data.Text (Text)
import Turtle.Format (format, fp)

task :: FilePath -> Text -> TaskSpec
task name command = TaskSpec (format fp name) [] [] [] command 100 1 12

type Setter a = a -> TaskSpec -> TaskSpec

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
