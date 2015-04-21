module Tman.Task (
    Task(..),
    TaskStatus(..),
    SubmissionInfo(..),
    tSubmit,
    tCheck,
    tPrint,
    tClean
) where

import Text.Format (format)
import System.Directory (doesFileExist, getModificationTime, removeFile)
import System.Process (callProcess, spawnProcess)
import System.IO (stderr, hPutStrLn)
import Control.Error (Script, scriptIO)
import Control.Monad (when)
import Control.Monad.Trans.Either (left)
import Data.List (intercalate)
import Data.List.Split (splitOn)

data Task = Task {
    _tInputFiles :: [FilePath],
    _tOutputFiles :: [FilePath],
    _tLogFile :: FilePath,
    _tCommand :: String,
    _tSubmissionInfo :: SubmissionInfo,
    _tName :: String
} deriving Show

data SubmissionInfo = StandardSubmission | LSFsubmission LSFopt deriving Show

data LSFopt = LSFopt {
    _lsfQueue :: String,
    _lsfMem :: Int,
    _lsfThreads :: Int,
    _lsfLog :: FilePath
} deriving Show

data TaskStatus = ResultNoInput
                | ResultOutdated
                | ResultSuccess
                | ResultFailed
                | ResultNotFinished
                | ResultNotRun
                deriving (Eq, Ord, Show)

tSubmit :: Task -> Script ()
tSubmit task = do
    check <- scriptIO $ tCheck task
    case check of
        ResultNoInput -> left ("missing inputs for task" ++ _tName task)
        ResultNotFinished -> left ("task " ++ _tName task ++ " not finished")
        ResultSuccess -> left ("task " ++ _tName task ++ " already successful")
        _ -> return ()
    logFileExists <- scriptIO $ doesFileExist (_tLogFile task)
    when logFileExists $ left ("task " ++ _tName task ++ ": log file already exists")
    case (_tSubmissionInfo task) of
        LSFsubmission (LSFopt q mem t l) -> do
            let rArg = format "select[mem>{0}] rusage[mem={0}] span[hosts=1]" [show $ mem]
                cmd = intercalate " " ["tman_runner.sh", _tLogFile task, _tCommand task]
                args = ["-J", _tName task, "-q", q, "-R", rArg, "-M", show mem, "-n", show t, "-oo", l, cmd]
            scriptIO $ callProcess "bsub" args
        StandardSubmission -> do
            _ <- scriptIO $ spawnProcess "tman_runner.sh" [_tLogFile task, _tCommand task]
            scriptIO (hPutStrLn stderr $ "job <" ++ _tName task ++ "> started")
            return ()

tCheck :: Task -> IO TaskStatus
tCheck task = do
    existing <- mapM doesFileExist $ _tInputFiles task
    if not $ and existing then
        return ResultNoInput
    else do
        logFileExists <- doesFileExist $ _tLogFile task
        if not logFileExists then
            return ResultNotRun
        else do
            l <- lines `fmap` readFile (_tLogFile task)
            let w = splitOn "\t" $ last l
            if head w /= "EXIT CODE" then
                return ResultNotFinished
            else
                if last w == "0" then do
                    existingOutputs <- mapM doesFileExist $ _tOutputFiles task
                    if not $ and existingOutputs then
                        return ResultFailed
                    else do
                        inputMod <- mapM getModificationTime $ _tInputFiles task
                        outputMod <- mapM getModificationTime $ _tOutputFiles task
                        if maximum inputMod < minimum outputMod then
                            return ResultSuccess
                        else
                            return ResultOutdated
                else
                    return ResultFailed

tClean :: Task -> Script ()
tClean task = do
    scriptIO $ mapM_ removeFileIfExists $ _tOutputFiles task
    scriptIO . removeFileIfExists $ _tLogFile task
  where
    removeFileIfExists f = do
        exists <- doesFileExist f
        when exists $ removeFile f

tLog :: Task -> Script ()
tLog task = undefined

tPrint :: Task -> Script ()
tPrint task = scriptIO . putStrLn $ _tCommand task