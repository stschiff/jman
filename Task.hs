module Task (
    Task(..),
    TaskStatus(..),
    SubmissionInfo(..),
    JobEnvLSFinfo(..),
    tSubmit,
    tCheck
) where

import Text.Format (format)
import System.Directory (doesFileExist)

data Task = Task {
    _tInputFiles :: [FilePath],
    _tOutputFiles :: [FilePath],
    _tLogFile :: FilePath,
    _tCommand :: String
}

data TaskStatus = ResultNoInput
                | ResultOutdated
                | ResultSuccess
                | ResultFailed
                | ResultNotFinished
                | ResultNotRun

data SubmissionInfo = JobEnvLSF JobEnvLSFinfo | JobEnvStandard

data JobEnvLSFinfo = JobEnvLSFinfo {
    _lsfName :: String,
    _lsfQueue :: String,
    _lsfMem :: Int,
    _lsfThreads :: Int,
    _lsfLog :: FilePath
}
    
tSubmit :: Task -> SubmissionInfo -> IO ()
tSubmit task (JobEnvLSF info) = do
    let rArg = format "select[mem>{0}] rusage[mem={0}] span[hosts=1]" [show $ _lsfMem info]
        mem = show $ _lsfMem info
        t = show $ _lsfThreads info
        cmd = intercalate " " ["tman_runner.sh", _tLogFile task, _tCommand task]
        args = ["-J", _lsfName info, "-q", _lsfQueue info, "-R", rArg, "-M", mem, "-n", t, "-oo", _lsfLog info, cmd]
    callProcess "bsub" args
tSubmit task JobEnvStandard = do
    jobId <- spawnCommand "tman_runner.sh" [_tLogFile task, _tCommand task]
    hPutStrLn stderr $ "job <" ++ show jobId ++ "> started"
    return ()

tCheck :: Task -> IO TaskStatus
tCheck task = do
    existing <- mapM doesFileExist $ _tInputFiles task
    if not $ and existing then
        return ResultNoInput
    else do
        if not $ doesFileExist (_tLogFile task) then
            return ResultNotRun
        else do
            l <- lines `fmap` readFile (_tLogFile task)
            let w = splitOn "\t" $ last l
            if head w /= "EXIT CODE" then
                return ResultNotFinished
            else
                if last w == "0" then do
                    inputModTimes <- mapM getModified $ _tInputFiles task
                    outputModTimes <- mapM getModified $ _tOutputFiles task
                    if maximum inputModTimes < minimum outputModTimes then
                        return ResultSuccess
                    else
                        return ResultOutdated
                else
                    return ResultFailed

tClean :: Task -> IO ()
tClean task = undefined

tLog :: Task -> IO ()
tLog task = undefined

tPrint :: Task -> IO ()
tPrint task = undefined