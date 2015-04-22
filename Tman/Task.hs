module Tman.Task (
    Task(..),
    TaskStatus(..),
    SubmissionInfo(..),
    LSFopt(..),
    tSubmit,
    tCheck,
    tInfo,
    tPrint,
    tClean,
    tMeta
) where

import Text.Format (format)
import System.Directory (doesFileExist, getModificationTime, removeFile)
import System.Process (callProcess, spawnProcess)
import System.IO (stderr, hPutStrLn)
import Control.Error (Script, scriptIO)
import System.Posix.Files (getFileStatus, fileSize)
import Control.Monad (when, filterM)
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

data TaskStatus = StatusMissingInput
                | StatusIncomplete
                | StatusOutdated
                | StatusComplete deriving (Eq, Ord, Show)

data TaskInfo = InfoNotStarted
              | InfoNotFinished
              | InfoFailed
              | InfoSuccess deriving (Eq, Ord, Show)

tSubmit :: Task -> Script ()
tSubmit task = do
    check <- tCheck task
    case check of
        StatusMissingInput -> left ("missing inputs for task" ++ _tName task)
        StatusComplete -> left ("task " ++ _tName task ++ " is already complete")
    info <- tInfo task
    when (info == InfoNotFinished) $ left ("task " ++ _tName task ++ " already running?")
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

tCheck :: Task -> Script TaskStatus
tCheck task = do
    iExisting <- scriptIO (mapM doesFileExist $ _tInputFiles task)
    iFileStatus <- scriptIO $ filterM doesFileExist (_tInputFiles task) >>= mapM getFileStatus
    if (not $ and iExisting) || or (map ((==0) . fileSize) iFileStatus) then
        return StatusMissingInput
    else do
        oExisting <- scriptIO (mapM doesFileExist $ _tOutputFiles task)
        oFileStatus <- scriptIO (filterM doesFileExist (_tOutputFiles task) >>= mapM getFileStatus)
        if (not $ and oExisting) || or (map ((==0) . fileSize) oFileStatus) then
            return StatusIncomplete
        else do
            inputMod <- scriptIO (mapM getModificationTime $ _tInputFiles task)
            outputMod <- scriptIO (mapM getModificationTime $ _tOutputFiles task)
            if maximum inputMod > minimum outputMod then
                return StatusOutdated
            else
                return StatusComplete

tInfo :: Task -> Script TaskInfo
tInfo task = do
    logFileExists <- scriptIO $ doesFileExist (_tLogFile task)
    if not logFileExists then
        return InfoNotStarted
    else do
        l <- scriptIO $ lines `fmap` readFile (_tLogFile task)
        let w = splitOn "\t" $ last l
        if head w /= "EXIT CODE" then
            return InfoNotFinished
        else
            if last w == "0" then do
                return InfoSuccess
            else
                return InfoFailed

tClean :: Task -> Script ()
tClean task = do
    scriptIO $ mapM_ removeFileIfExists $ _tOutputFiles task
    scriptIO . removeFileIfExists $ _tLogFile task
  where
    removeFileIfExists f = do
        exists <- doesFileExist f
        when exists $ removeFile f

tPrint :: Task -> String
tPrint task = _tCommand task

tMeta :: Task -> String
tMeta task = 
    let args = [_tName task, show $ _tInputFiles task, show $ _tOutputFiles task, _tLogFile task,
                show $ _tSubmissionInfo task]
    in  format "Name {0}\tIn {1}\tOut {2}\tLog {3}\tSubmission {4}" args