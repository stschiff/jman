data Task = Task {
    _tName :: String,
    _tInputFiles :: [FilePath],
    _tOutputFiles :: [FilePath],
    _tLogFile :: FilePath,
    _tMemLimit :: Int,
    _tRunLimit :: Int,
    _tThreads :: Int,
    _tCommand :: String
}

data TaskStatus = ResultSuccess | ResultFailed String | ResultNotFinished | ResultNotRun

data SubmissionWrapper = SubmissionWrapper {
    
}

tSubmit :: Task -> SubmissionInfo -> IO ()
tSubmit task submissionInfo = undefined

tCheck :: Task -> IO TaskStatus
tCheck task = undefined

tClean :: Task -> IO ()
tClean task = undefined

tLog :: Task -> IO ()
tLog task = undefined