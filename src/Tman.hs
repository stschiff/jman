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
             ProjectRef) where
    
import Tman.Task (TaskSpec(..))
import qualified Tman.Project as P

import Control.Error (runScript)
import Data.IORef (IORef, readIORef, newIORef, writeIORef)
import Data.Text (Text)
import Filesystem.Path (FilePath)
import Prelude hiding (FilePath)
import Turtle.Format (format, fp)

task :: FilePath -> Text -> TaskSpec
task name command = TaskSpec (format fp name) [] [] [] command 100 1 12

type Setter a = a -> TaskSpec -> TaskSpec
type ProjectRef = IORef P.Project

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

loadProject :: IO ProjectRef
loadProject = runScript P.loadProject >>= newIORef

addTask :: ProjectRef -> TaskSpec -> IO ()
addTask projectRef taskSpec = do
    p <- readIORef projectRef
    (runScript . P.addTask p) taskSpec >>= writeIORef projectRef

saveProject :: ProjectRef -> IO ()
saveProject p = readIORef p >>= runScript . P.saveProject