module TmanTask (task,
                 printTask,
                 inputFiles,
                 outputFiles,
                 mem,
                 nrThreads,
                 submissionQueue,
                 submissionGroup,
                 (&),
                 Task) where
    
import Task (Task(..), printTask)
import Prelude hiding (FilePath)
import Filesystem.Path (FilePath)
import Filesystem.Path.CurrentOS (encodeString)
import Data.Text (unpack, Text)

task :: FilePath -> Text -> Task
task name command = Task (encodeString name) [] [] (unpack command) 100 1 "normal" ""

type Setter a = a -> Task -> Task

inputFiles :: Setter [FilePath]
inputFiles i task = task {_tInputFiles = map encodeString i}

outputFiles :: Setter [FilePath]
outputFiles o task = task {_tOutputFiles = map encodeString o}

mem :: Setter Int
mem m task = task {_tMem = m}

nrThreads :: Setter Int
nrThreads t task = task {_tNrThreads = t}

submissionQueue :: Setter Text
submissionQueue q task = task {_tSubmissionQueue = unpack q}

submissionGroup :: Setter Text
submissionGroup g task = task {_tSubmissionGroup = unpack g}

(&) :: a -> (a -> b) -> b
a & f = f a
infixl 1 &
