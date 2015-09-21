{-# LANGUAGE OverloadedStrings #-}

module Tman.Internal.Task (
    Task(..),
    TaskSpec(..),
    TaskStatus(..),
    TaskRunInfo(..),
    FailedReason(..),
    RunInfo(..),
    SubmissionSpec(..),
    tSubmit,
    tStatus,
    tRunInfo,
    tClean,
    tLog,
    printTask
) where

import Turtle.Prelude (testfile, datefile, rm, mktree, proc, du, touch, bytes, stdout, input, err)
import Turtle (UTCTime, empty, ExitCode(..), FilePath, (</>), (<.>), directory)
import Turtle.Format (format, fp, d, (%), s)
import Filesystem.Path.CurrentOS (encodeString)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Prelude hiding (FilePath)
import Control.Error (Script, scriptIO, tryAssert, throwE, tryHead, tryJust, tryRead)
import Control.Monad (when, mzero)
import qualified Data.ByteString.Lazy.Char8 as B
import Data.Aeson (FromJSON, ToJSON, parseJSON, (.:), toJSON, Value(..), object, (.=), encode)
import Data.Time (parseTimeM, defaultTimeLocale)

data TaskSpec = TaskSpec {
    _tsName :: T.Text,
    _tsInputTasks :: [T.Text],
    _tsInputFiles :: [T.Text],
    _tsOutputFiles :: [T.Text],
    _tsCommand :: T.Text,
    _tsMem :: Int,
    _tsNrThreads :: Int,
    _tsHours :: Int
} deriving Show

instance FromJSON TaskSpec where
    parseJSON (Object v) = TaskSpec <$>
                           v .: "name" <*>
                           v .: "inputTasks" <*>
                           v .: "inputFiles" <*>
                           v .: "outputFiles" <*>
                           v .: "command" <*>
                           v .: "mem" <*>
                           v .: "nrThreads" <*>
                           v .: "hours"
    parseJSON _         = mzero

instance ToJSON TaskSpec where
    toJSON (TaskSpec n it ifiles o c m t h) =
        object ["name" .= n,
                "inputTasks" .= it,
                "inputFiles" .= ifiles,
                "outputFiles" .= o,
                "command" .= c,
                "mem" .= m,
                "nrThreads" .= t,
                "hours" .= h]

data Task = Task {
    _tName :: FilePath,
    _tInputTasks :: [Task],
    _tInputFiles :: [FilePath],
    _tOutputFiles :: [FilePath],
    _tCommand :: T.Text,
    _tMem :: Int,
    _tNrThreads :: Int,
    _tHours :: Int
} deriving Show

printTask :: TaskSpec -> IO ()
printTask taskSpec = B.putStrLn . encode $ taskSpec

logFileName :: FilePath -> Task -> FilePath
logFileName projectDir task = projectDir </> _tName task <.> "log"

data TaskStatus = StatusIncompleteInputTask T.Text
                | StatusMissingInputFile T.Text
                | StatusIncomplete T.Text
                | StatusOutdated
                | StatusComplete deriving (Eq, Ord, Show)

data TaskRunInfo = InfoNoLogFile
                 | InfoNotFinished
                 | InfoFailed FailedReason
                 | InfoSuccess RunInfo deriving (Eq, Ord, Show)

data FailedReason = FailedMem | FailedRuntime | FailedUnknown deriving (Eq, Ord, Show)

data RunInfo = RunInfo {
    _runInfoBegin :: UTCTime,
    _runInfoEnd :: UTCTime,
    _runInfoMaxMem :: Double
} deriving (Eq, Show, Ord)

data SubmissionSpec = SequentialExecutionSubmission
                    | GnuParallelSubmission Int -- Number of jobs to be run parallel
                    | LSFsubmission T.Text T.Text -- group, queue

tSubmit :: FilePath -> Bool -> SubmissionSpec -> Task -> Script ()
tSubmit projectDir test submissionSpec task = do
    let jobFileName = projectDir </> _tName task <.> "job.sh"
        logFile = logFileName projectDir task
    mktree . directory $ jobFileName
    writeJobScript submissionSpec jobFileName (_tCommand task)
    case submissionSpec of
        LSFsubmission group queue -> do
            let m = _tMem task
                rArg = format ("select[mem>"%d%"] rusage[mem="%d%"] span[hosts=1]") m m
                cmd = ["bash", format fp jobFileName]
                lsf_args = ["-J", format fp $ _tName task, "-q", queue, "-R", rArg, "-M", format d $ _tMem task,
                        "-n", format d $ _tNrThreads task, "-oo", format fp $ logFileName projectDir task]
                group_args = ["-G", group]
                args = lsf_args ++ group_args ++ cmd
            if test then
                scriptIO $ T.putStrLn (T.intercalate " " $ wrapCmdArgs ("bsub":args))
            else do
                touch $ logFile
                exitCode <- proc "bsub" args empty
                case exitCode of
                    ExitFailure i -> throwE ("bsub command failed with exit code " ++ show i)
                    _ -> return ()
        SequentialExecutionSubmission -> throwE "sequential execution not yet implemented"
            -- if test then
            --     scriptIO $ putStrLn ("bash " ++ jobFileName)
            -- else do
            --     scriptIO . touch $ logFile
            --     _ <- scriptIO $ spawnProcess "bash" [jobFileName]
            --     scriptIO (hPutStrLn stderr $ "job <" ++ _tName task ++ "> started")
        GnuParallelSubmission _ -> throwE "Gnu Parallel not yet implemented"
  where
    wrapCmdArgs args = [if " " `T.isInfixOf` a then T.cons '\"' . flip T.snoc '\"' $ a else a | a <- args]
            
writeJobScript :: SubmissionSpec -> FilePath -> T.Text -> Script ()
writeJobScript submissionSpec jobFileName command = do
    let submissionName = case submissionSpec of
            LSFsubmission _ _ -> "LSF"
            SequentialExecutionSubmission -> "Sequential"
            GnuParallelSubmission _ -> "GNUparallel"
    let c = format ("printf \"SUBMISSION\\t"%s%"\n"%s) submissionName command
    scriptIO $ T.writeFile (encodeString jobFileName) c
    
tStatus :: Bool -> Bool -> Task -> Script TaskStatus
tStatus verbose full task = do
    inputTaskStatus <- mapM (tStatus verbose full) $ _tInputTasks task
    if any (/=StatusComplete) inputTaskStatus then
        if full then
            return . StatusIncompleteInputTask . T.intercalate "," $
                [format fp $ _tName t | (t, s') <- zip (_tInputTasks task) inputTaskStatus,
                                                   s' /= StatusComplete]
        else
            return . StatusIncompleteInputTask . format (d%" incomplete input Task(s)") . length $
                [format fp $ _tName t | (t, s') <- zip (_tInputTasks task) inputTaskStatus,
                                                   s' /= StatusComplete]
    else do
        iFileSize <- mapM getFileSize $ _tInputFiles task
        when verbose $ scriptIO . err $ format ("checking task "%fp%"\n") (_tName task)
        if any (==(0 :: Int)) iFileSize then
            if full then
                return . StatusMissingInputFile . T.intercalate "," $
                    [format fp f | (f, s') <- zip (_tInputFiles task) iFileSize, s' == 0]
            else 
                return . StatusMissingInputFile . format (d%" missing input file(s)") . length $ 
                    [format fp f | (f, s') <- zip (_tInputFiles task) iFileSize, s' == 0]
        else do
            oFileSize <- mapM getFileSize $ _tOutputFiles task
            if any (==(0 :: Int)) oFileSize then
                if full then 
                    return . StatusIncomplete . T.intercalate "," $
                        [format fp f | (f, s') <- zip (_tOutputFiles task) oFileSize, s' == 0]
                else
                    return . StatusIncomplete . format (d%" incomplete output file(s)") .length $ 
                        [format fp f | (f, s') <- zip (_tOutputFiles task) oFileSize, s' == 0]
            else do
                inputMod <- mapM datefile $ allInputFiles task
                outputMod <- mapM datefile $ _tOutputFiles task
                if null inputMod || maximum inputMod <= minimum outputMod then
                    return StatusComplete
                else
                    return StatusOutdated
  where
    getFileSize f = do
        iExists <- testfile f
        if iExists then do
            fs <- du f
            return $ bytes fs
        else
            return 0
    allInputFiles t = (concatMap _tOutputFiles $ _tInputTasks t) ++ _tInputFiles t

tRunInfo :: FilePath -> Bool -> Task -> Script TaskRunInfo
tRunInfo projectDir verbose task = do
    logFormat <- autodetectLogFormat projectDir task
    case logFormat of
        "LSF" -> tLSFrunInfo projectDir verbose task
        "Sequential" -> throwE "Sequential Log Format not implemented yet"
        "GNUparallel" -> throwE "Gnu Parallel Log Format not implemented yet"
        _ -> throwE "unknown format in log file"

autodetectLogFormat :: FilePath -> Task -> Script T.Text
autodetectLogFormat = undefined

tLSFrunInfo :: FilePath -> Bool -> Task -> Script TaskRunInfo
tLSFrunInfo projectDir verbose task = do
    when verbose $ scriptIO . err $ format ("getting LSF run info for task "%fp%"\n") (_tName task)
    logFileExists <- scriptIO . testfile $ logFileName projectDir task
    if not logFileExists then
        return InfoNoLogFile
    else do
        c <- scriptIO . T.readFile . encodeString . logFileName projectDir $ task
        let (_, infoPart) = T.breakOn "Sender: LSF System" c
        if T.null infoPart then return InfoNotFinished else
            if "Successfully completed." `T.isInfixOf` infoPart then
                parseLSFrunInfo infoPart >>= return . InfoSuccess
            else
                if "TERM_MEMLIMIT" `T.isInfixOf` infoPart then return $ InfoFailed FailedMem else
                    if "TERM_RUNLIMIT" `T.isInfixOf` infoPart then return $ InfoFailed FailedRuntime else
                        return $ InfoFailed FailedUnknown

parseLSFrunInfo :: T.Text -> Script RunInfo
parseLSFrunInfo content = do
    let l = T.lines content
    beginLine <- tryHead "cannot find starting time in LSF log output" . filter (T.isPrefixOf "Started at ") $ l
    endLine <- tryHead "cannot find end time in LSF log output" . filter (T.isPrefixOf "Results reported at ") $ l
    let beginTimeStr = T.drop (T.length "Started at ") beginLine
        endTimeStr = T.drop (T.length "Results reported at ") endLine
    maxMemLine <- tryHead "cannot find maximum memory in LSF log output" . filter (T.isPrefixOf "    Max Memory :             ") $ l
    maxMemStr <- tryHead "cannot read maximum memory in LSF log output" . T.words . T.drop (T.length "    Max Memory :             ") $ 
                    maxMemLine
    maxMem <- tryRead "cannot read maximum memory in LSF log output" . T.unpack $ maxMemStr
    beginTime <- tryJust "could not parse start time" . parseTimeM False defaultTimeLocale "%a %b %d %T %Y" . T.unpack $ beginTimeStr
    endTime <- tryJust "could not parse end time" . parseTimeM False defaultTimeLocale "%a %b %d %T %Y" . T.unpack $ endTimeStr
    return $ RunInfo beginTime endTime maxMem

tClean :: FilePath -> Task -> Script ()
tClean projectDir task = do
    let jobFile = projectDir </> _tName task <.> "job.sh"
        logFile = logFileName projectDir task
    rm' jobFile
    rm' logFile
  where
    rm' f = do
        b <- testfile f
        when b $ rm f

tLog :: FilePath -> Task -> Script ()
tLog projectDir task = do
    testfile fn >>= tryAssert "Log file doesn't exist"
    scriptIO . stdout . input $ fn
    scriptIO $ putStrLn ""
  where
    fn = logFileName projectDir task

-- tLsfKill :: Task -> Script ()
-- tLsfKill task = do
--     exitCode <- proc "bsub" ["-J", format fp $ _tName task] empty
--     case exitCode of
--         ExitFailure i -> throwE ("bkill command failed with exit code " ++ show i)
--         _ -> return ()
--     return ()
