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
    tMeta,
    makedirs
) where

import Text.Format (format)
import System.Directory (doesFileExist, getModificationTime, removeFile, createDirectoryIfMissing)
import System.Process (callProcess, spawnProcess)
import System.IO (stderr, hPutStrLn, openFile, IOMode(..), hClose)
import Control.Error (Script, scriptIO)
import System.Posix.Files (getFileStatus, fileSize)
import Control.Monad (when, filterM)
import Control.Monad.Trans.Either (left)
import Data.List (intercalate)
import Data.List.Split (splitOn)
import System.FilePath.Posix ((</>), (<.>))

data Task = Task {
    _tInputFiles :: [FilePath],
    _tOutputFiles :: [FilePath],
    _tLogDir :: FilePath,
    _tCommand :: String,
    _tSubmissionInfo :: SubmissionInfo,
    _tName :: String
} deriving Show

logFileName :: Task -> FilePath
logFileName task = (_tLogDir task) </> (_tName task) <.> "log"

data SubmissionInfo = StandardSubmission | LSFsubmission LSFopt deriving Show

data LSFopt = LSFopt {
    _lsfQueue :: String,
    _lsfMem :: Int,
    _lsfThreads :: Int,
    _lsfGroup :: String
} deriving Show

data TaskStatus = StatusMissingInput
                | StatusIncomplete
                | StatusOutdated
                | StatusComplete deriving (Eq, Ord, Show)

data TaskInfo = InfoNoLogFile
              | InfoNotFinished
              | InfoFailed
              | InfoSuccess deriving (Eq, Ord, Show)

tSubmit :: Bool -> Bool -> Task -> Script ()
tSubmit force test task = do
    check <- tCheck task
    case check of
        StatusMissingInput -> left ("missing inputs for task" ++ _tName task)
        StatusComplete -> when (not force) $
            left ("task " ++ _tName task ++ " is already complete, use --force to run anyway")
        _ -> return ()
    info <- tInfo task
    when (info == InfoNotFinished && (not force)) $
        left ("task " ++ _tName task ++ " already running? Use --force to run anyway")
    let jobFileName = (_tLogDir task) </> (_tName task) <.> "job.sh"
    scriptIO . makedirs . _tLogDir $ task
    writeJobScript jobFileName (logFileName task) (_tCommand task)
    case (_tSubmissionInfo task) of
        LSFsubmission (LSFopt q mem t g) -> do
            let rArg = format "select[mem>{0}] rusage[mem={0}] span[hosts=1]" [show $ mem]
                cmd = ["bash", jobFileName]
                l = (_tLogDir task) </> (_tName task) <.> "bsub.log"
                args = ["-J", _tName task, "-q", q, "-R", rArg, "-M", show mem, "-n", show t, "-oo", l, "-G", g] ++ cmd
            if test then
                scriptIO $ putStrLn (intercalate " " $ wrapCmdArgs ("bsub":args))
            else
                scriptIO $ callProcess "bsub" args
        StandardSubmission -> do
            if test then
                scriptIO $ putStrLn ("bash " ++ jobFileName)
            else do
                _ <- scriptIO $ spawnProcess "bash" [jobFileName]
                scriptIO (hPutStrLn stderr $ "job <" ++ _tName task ++ "> started")
  where
    wrapCmdArgs args = [if ' ' `elem` a then "\"" ++ a ++ "\"" else a | a <- args]

makedirs :: FilePath -> IO ()
makedirs = createDirectoryIfMissing True

writeJobScript :: FilePath -> FilePath -> String -> Script ()
writeJobScript jobFileName logFileName command = do
    jobFile <- scriptIO $ openFile jobFileName WriteMode
    let l = ["printf \"STARTING\\t$(date)\\n\" > " ++ logFileName,
             "printf \"COMMAND\\t$CMD\\n\" >> " ++ logFileName,
             "printf \"OUTPUT\\n\" >> " ++ logFileName,
             command,
             "EXIT_CODE=$?",
             "printf \"FINISHED\\t$(date)\\n\" >> " ++ logFileName,
             "printf \"EXIT CODE\\t$EXIT_CODE\\n\" >> " ++ logFileName]
    scriptIO $ mapM_ (hPutStrLn jobFile) l
    scriptIO $ hClose jobFile
    
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
            if null inputMod || maximum inputMod <= minimum outputMod then
                return StatusComplete
            else
                return StatusOutdated

tInfo :: Task -> Script TaskInfo
tInfo task = do
    logFileExists <- scriptIO $ doesFileExist (logFileName task)
    if not logFileExists then
        return InfoNoLogFile
    else do
        l <- scriptIO $ lines `fmap` readFile (logFileName task)
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
    scriptIO . removeFileIfExists $ logFileName task
  where
    removeFileIfExists f = do
        exists <- doesFileExist f
        when exists $ removeFile f

tPrint :: Task -> String
tPrint task = _tCommand task

tMeta :: Task -> String
tMeta task = 
    let args = [_tName task, show $ _tInputFiles task, show $ _tOutputFiles task, _tLogDir task,
                show $ _tSubmissionInfo task]
    in  format "Name {0}\tIn {1}\tOut {2}\tLogDir {3}\tSubmission {4}" args
