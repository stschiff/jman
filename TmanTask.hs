module TmanTask (task,
                 printTask,
                 inputFiles,
                 outputFiles,
                 mem,
                 nrThreads,
                 submissionQueue,
                 submissionGroup) where
    
import Task (Task(..), printTask)

task :: String -> String -> Task
task name command = Task name [] [] command 100 1 "normal" ""

type Setter a = a -> Task -> Task

inputFiles :: Setter [FilePath]
inputFiles files task = task {_tInputFiles = files}

outputFiles :: Setter [FilePath]
outputFiles o task = task {_tOutputFiles = o}

mem :: Setter Int
mem m task = task {_tMem = m}

nrThreads :: Setter Int
nrThreads t task = task {_tNrThreads = t}

submissionQueue :: Setter String
submissionQueue q task = task {_tSubmissionQueue = q}

submissionGroup :: Setter String
submissionGroup g task = task {_tSubmissionGroup = g}
