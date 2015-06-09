{-# LANGUAGE OverloadedStrings #-}

module Task (
    Task(..),
    TaskStatus(..),
    TaskInfo(..),
    LSFInfo(..),
    tSubmit,
    recursiveCheckAll,
    tInfo,
    tLSFInfo,
    tClean,
    tLog,
    tLsfLog,
    tLsfKill,
    makedirs,
    SubmissionType(..),
    printTask
) where

import Text.Format (format)
import System.Directory (doesFileExist, getModificationTime, removeFile, createDirectoryIfMissing)
import System.Posix.Files (getFileStatus, fileSize, touchFile)
import System.FilePath.Posix ((</>), (<.>), takeDirectory)
import System.Process (callProcess, spawnProcess, readProcess)
import System.IO (stderr, hPutStrLn, openFile, IOMode(..), hClose)
import Control.Error (Script, scriptIO, tryAssert, err)
import Control.Monad (when, filterM, mzero)
import Data.List (intercalate, isInfixOf)
import qualified Data.ByteString.Lazy.Char8 as B
import Data.Aeson (FromJSON, ToJSON, parseJSON, (.:), toJSON, Value(..), object, (.=), encode)
import qualified Data.Map.Strict as M
import Data.Maybe (catMaybes)

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
                | StatusOutdatedR
                | StatusComplete deriving (Eq, Ord, Show)

data TaskInfo = InfoNoLogFile
              | InfoNotFinished
              | InfoFailed
              | InfoSuccess deriving (Eq, Ord, Show)

data LSFInfo = LSFInfoNoLogFile
              | LSFInfoNotFinished
              | LSFInfoMemFailed
              | LSFInfoRuntimeFailed
              | LSFInfoUnknownFailed
              | LSFInfoSuccess deriving (Eq, Ord, Show)

data SubmissionType = StandardSubmission | LSFsubmission | SequentialSubmission

tSubmit :: FilePath -> Bool -> SubmissionType -> Task -> Script ()
tSubmit projectDir test submissionType task = do
    let jobFileName = projectDir </> _tName task <.> "job.sh"
        logFile = logFileName projectDir task
    scriptIO . makedirs . takeDirectory $ jobFileName
    writeJobScript jobFileName logFile (_tCommand task)
    case submissionType of
        LSFsubmission -> do
            let rArg = format "select[mem>{0}] rusage[mem={0}] span[hosts=1]" [show $ _tMem task]
                cmd = ["bash", jobFileName]
                l = projectDir </> (_tName task) <.> "bsub.log"
                lsf_args = ["-J", _tName task, "-q", _tSubmissionQueue task, "-R", rArg, "-M", show $ _tMem task,
                        "-n", show $ _tNrThreads task, "-oo", l]
                group_args = if null $ _tSubmissionGroup task then [] else ["-G", _tSubmissionGroup task]
                args = lsf_args ++ group_args ++ cmd
            if test then
                scriptIO $ putStrLn (intercalate " " $ wrapCmdArgs ("bsub":args))
            else do
                scriptIO . touch $ logFile
                scriptIO $ callProcess "bsub" args
        StandardSubmission -> do
            if test then
                scriptIO $ putStrLn ("bash " ++ jobFileName)
            else do
                scriptIO . touch $ logFile
                _ <- scriptIO $ spawnProcess "bash" [jobFileName]
                scriptIO (hPutStrLn stderr $ "job <" ++ _tName task ++ "> started")
        SequentialSubmission -> do
            if test then
                scriptIO $ putStrLn ("bash " ++ jobFileName)
            else do
                scriptIO . touch $ logFile
                _ <- scriptIO $ callProcess "bash" [jobFileName]
                scriptIO (hPutStrLn stderr $ "job <" ++ _tName task ++ "> started")
  where
    wrapCmdArgs args = [if ' ' `elem` a then "\"" ++ a ++ "\"" else a | a <- args]
    touch path = do
        ex <- doesFileExist path
        if ex then touchFile path else writeFile path ""
            

makedirs :: FilePath -> IO ()
makedirs = createDirectoryIfMissing True

writeJobScript :: FilePath -> FilePath -> String -> Script ()
writeJobScript jobFileName logFileName command = do
    jobFile <- scriptIO $ openFile jobFileName WriteMode
    let l = ["printf \"STARTING\\t$(date)\\n\" > " ++ logFileName,
             format "printf \"COMMAND\\t{0}\\n\" >> {1}" [command, logFileName],
             "printf \"OUTPUT\\n\" >> " ++ logFileName,
             format "( {0} ) 2>> {1}" [command, logFileName],
             "EXIT_CODE=$?",
             "printf \"FINISHED\\t$(date)\\n\" >> " ++ logFileName,
             "printf \"EXIT CODE\\t$EXIT_CODE\\n\" >> " ++ logFileName]
    scriptIO $ mapM_ (hPutStrLn jobFile) l
    scriptIO $ hClose jobFile
    
tCheck :: Bool -> Task -> Script TaskStatus
tCheck verbose task = do
    iExisting <- scriptIO (mapM doesFileExist $ _tInputFiles task)
    when verbose $ scriptIO . err $ "checking task " ++ _tName task ++ "\n"
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

recursiveCheckAll :: Bool -> [Task] -> Script [TaskStatus]
recursiveCheckAll verbose tasks = do
    let outputFileTable = concatMap (\t -> [(f, t) | f <- _tOutputFiles t]) tasks
        fileTaskLookup = M.fromList outputFileTable
    tryAssert "Error: multiple tasks have same output file" $ M.size fileTaskLookup == length outputFileTable
    taskStatusLookup <- M.fromList . map (\(t, s) -> (_tName t, s)) . zip tasks <$> mapM (tCheck verbose) tasks
    return $ map (recursiveCheck fileTaskLookup taskStatusLookup) tasks

recursiveCheck :: M.Map FilePath Task -> M.Map String TaskStatus -> Task -> TaskStatus
recursiveCheck fileTaskLookup taskStatusLookup task =
    if rawStatus == StatusMissingInput then
        rawStatus
    else
        if any (/= StatusComplete) allInputStatus then
            StatusOutdatedR
        else
            rawStatus
  where
    rawStatus = taskStatusLookup M.! (_tName task)
    allInputStatus = map (recursiveCheck fileTaskLookup taskStatusLookup) allInputTasks
    allInputTasks = catMaybes . map (\f -> M.lookup f fileTaskLookup) . _tInputFiles $ task
            

tInfo :: FilePath -> Bool -> Task -> Script TaskInfo
tInfo projectDir verbose task = do
    when verbose $ scriptIO . err $ "checking task " ++ _tName task ++ "\n"
    logFileExists <- scriptIO $ doesFileExist (logFileName projectDir task)
    if not logFileExists then
        return InfoNoLogFile
    else do
        l <- scriptIO $ B.lines `fmap` B.readFile (logFileName projectDir task)
        if null l then return InfoFailed else do
            let w = B.split '\t' . last $ l
            if head w /= "EXIT CODE" then
                return InfoNotFinished
            else
                if last w == "0" then do
                    return InfoSuccess
                else
                    return InfoFailed

tLSFInfo :: FilePath -> Bool -> Task -> Script LSFInfo
tLSFInfo projectDir verbose task = do
    when verbose $ scriptIO . err $ "checking task " ++ _tName task ++ "\n"
    let lf = projectDir </> (_tName task) <.> "bsub.log"
    logFileExists <- scriptIO . doesFileExist $ lf
    if not logFileExists then
        return LSFInfoNoLogFile
    else do
        c <- scriptIO $ readProcess "tail" ["-100", lf] ""
        if null c then return LSFInfoNotFinished else
            if "Resource usage summary:" `isInfixOf` c then
                if "Successfully completed." `isInfixOf` c then return LSFInfoSuccess else
                    if "TERM_MEMLIMIT" `isInfixOf` c then return LSFInfoMemFailed else
                        if "TERM_RUNLIMIT" `isInfixOf` c then return LSFInfoRuntimeFailed else
                            return LSFInfoUnknownFailed
            else return LSFInfoNotFinished

tClean :: FilePath -> Task -> Script ()
tClean projectDir task = do
    scriptIO . removeFileIfExists $ logFileName projectDir task
    scriptIO . removeFileIfExists $ projectDir </> (_tName task) <.> "bsub.log"
  where
    removeFileIfExists f = do
        exists <- doesFileExist f
        when exists $ removeFile f

tLog :: FilePath -> Task -> Script ()
tLog projectDir task = scriptIO $ do
    ex <- doesFileExist fn
    putStrLn $ "Task: " ++ _tName task
    if not ex then
        putStrLn "Log file doesn't exist"
    else
        readFile fn >>= putStr
    putStrLn ""
  where
    fn = logFileName projectDir task

tLsfLog :: FilePath -> Task -> Script ()
tLsfLog projectDir task = scriptIO $ do
    ex <- doesFileExist fn
    putStrLn $ "Task: " ++ _tName task
    if not ex then
        putStrLn "Lsf Log file doesn't exist"
    else
        readFile fn >>= putStr
    putStrLn ""
  where
    fn = projectDir </> (_tName task) <.> "bsub.log"
    

tLsfKill :: Task -> Script ()
tLsfKill task = do
    _ <- scriptIO $ spawnProcess "bkill" ["-J", _tName task]
    return ()

printTask :: Task -> IO ()
printTask task = B.putStrLn . encode $ task