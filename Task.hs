{-# LANGUAGE OverloadedStrings #-}

module Task (
    Task(..),
    TaskStatus(..),
    TaskInfo(..),
    tSubmit,
    tCheck,
    tInfo,
    tClean,
    makedirs,
    SubmissionType(..)
) where

import Text.Format (format)
import System.Directory (doesFileExist, getModificationTime, removeFile, createDirectoryIfMissing)
import System.Posix.Files (getFileStatus, fileSize, touchFile)
import System.FilePath.Posix ((</>), (<.>), takeDirectory)
import System.Process (callProcess, spawnProcess)
import System.IO (stderr, hPutStrLn, openFile, IOMode(..), hClose)
import Control.Error (Script, scriptIO)
import Control.Monad (when, filterM, mzero)
import Control.Monad.Trans.Either (left)
import Data.List (intercalate)
import Data.List.Split (splitOn)
import Control.Applicative ((<*>), (<$>))
import Data.Aeson (FromJSON, ToJSON, parseJSON, (.:), toJSON, Value(..), object, (.=))

data Task = Task {
    _tName :: String,
    _tInputFiles :: [FilePath],
    _tOutputFiles :: [FilePath],
    _tCommand :: String,
    _tMem :: Int,
    _tNrThreads :: Int,
    _tSubmissionQueue :: String,
    _tSubmissionGroup :: String
} deriving Show

instance FromJSON Task where
    parseJSON (Object v) = Task <$>
                           v .: "name" <*>
                           v .: "inputFiles" <*>
                           v .: "outputFiles" <*>
                           v .: "command" <*>
                           v .: "mem" <*>
                           v .: "nrThreads" <*>
                           v .: "submissionQueue" <*>
                           v .: "submissionGroup"
    parseJSON _         = mzero

instance ToJSON Task where
    toJSON (Task n i o c m t q g) =
        object ["name" .= n,
                "inputFiles" .= i,
                "outputFiles" .= o,
                "command" .= c,
                "mem" .= m,
                "nrThreads" .= t,
                "submissionQueue" .= q,
                "submissionGroup" .= g]

logFileName :: FilePath -> Task -> FilePath
logFileName projectDir task = projectDir </> _tName task <.> "log"

data TaskStatus = StatusMissingInput
                | StatusIncomplete
                | StatusOutdated
                | StatusComplete deriving (Eq, Ord, Show)

data TaskInfo = InfoNoLogFile
              | InfoNotFinished
              | InfoFailed
              | InfoSuccess deriving (Eq, Ord, Show)

data SubmissionType = StandardSubmission | LSFsubmission

tSubmit :: FilePath -> Bool -> SubmissionType -> Task -> Script ()
tSubmit projectDir test submissionType task = do
    check <- tCheck task
    when (check == StatusMissingInput) $ left ("missing inputs for task" ++ _tName task)
    info <- tInfo projectDir task
    when (info == InfoNotFinished) $ left ("task " ++ _tName task ++ " already running? Clean first")
    let jobFileName = projectDir </> _tName task <.> "job.sh"
        logFile = logFileName projectDir task
    scriptIO . makedirs . takeDirectory $ jobFileName
    writeJobScript jobFileName logFile (_tCommand task)
    case submissionType of
        LSFsubmission -> do
            let rArg = format "select[mem>{0}] rusage[mem={0}] span[hosts=1]" [show $ _tMem task]
                cmd = ["bash", jobFileName]
                l = projectDir </> (_tName task) <.> "bsub.log"
                args = ["-J", _tName task, "-q", _tSubmissionQueue task, "-R", rArg, "-M", show $ _tMem task,
                        "-n", show $ _tNrThreads task, "-oo", l, "-G", _tSubmissionGroup task] ++ cmd
            if test then
                scriptIO $ putStrLn (intercalate " " $ wrapCmdArgs ("bsub":args))
            else do
                scriptIO . touchFile $ logFile
                scriptIO $ callProcess "bsub" args
        StandardSubmission -> do
            if test then
                scriptIO $ putStrLn ("bash " ++ jobFileName)
            else do
                scriptIO . touchFile $ logFile
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
             format "printf \"COMMAND\\t{0}\\n\" >> {1}" [command, logFileName],
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

tInfo :: FilePath -> Task -> Script TaskInfo
tInfo projectDir task = do
    logFileExists <- scriptIO $ doesFileExist (logFileName projectDir task)
    if not logFileExists then
        return InfoNoLogFile
    else do
        l <- scriptIO $ lines `fmap` readFile (logFileName projectDir task)
        let w = splitOn "\t" $ last l
        if head w /= "EXIT CODE" then
            return InfoNotFinished
        else
            if last w == "0" then do
                return InfoSuccess
            else
                return InfoFailed

tClean :: FilePath -> Task -> Script ()
tClean projectDir task = do
    -- scriptIO $ mapM_ removeFileIfExists $ _tOutputFiles task
    scriptIO . removeFileIfExists $ logFileName projectDir task
  where
    removeFileIfExists f = do
        exists <- doesFileExist f
        when exists $ removeFile f
