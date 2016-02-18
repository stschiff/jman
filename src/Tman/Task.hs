{-# LANGUAGE OverloadedStrings #-}

module Tman.Task (
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
    tSlurmKill,
    tLsfKill
) where

import Control.Error (Script, scriptIO, tryAssert, throwE, headErr, tryRight, readErr, tryJust)
import Control.Monad (when, mzero)
import Data.Aeson (FromJSON, ToJSON, parseJSON, (.:), toJSON, Value(..), object, (.=))
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Filesystem.Path.CurrentOS (encodeString)
import Turtle (empty, ExitCode(..), FilePath, (</>), (<.>), directory)
import Turtle.Format (format, fp, d, (%), s)
import Turtle.Prelude (testfile, datefile, rm, mktree, proc, du, touch, bytes, stdout, input, err, echo, need)
import Prelude hiding (FilePath)
import System.IO (withFile, IOMode(..))
import qualified System.Process as P

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

logFileName :: FilePath -> Task -> FilePath
logFileName projectDir task = projectDir </> _tName task <.> "log"

data TaskStatus = StatusIncompleteInputTask T.Text
                | StatusMissingInputFile T.Text
                | StatusIncomplete T.Text
                | StatusOutdated
                | StatusComplete deriving (Eq, Ord, Show)

data TaskRunInfo = InfoNoLogFile
                 | InfoUnknownLogFormat
                 | InfoNotFinished
                 | InfoFailed FailedReason
                 | InfoSuccess RunInfo deriving (Eq, Ord, Show)

data FailedReason = FailedMem | FailedRuntime | FailedUnknown deriving (Eq, Ord, Show)

data RunInfo = RunInfo {
    _runInfoTime :: T.Text,
    _runInfoMaxMem :: Double
} deriving (Eq, Show, Ord)

data SubmissionSpec = SequentialExecutionSubmission
                    | LSFsubmission T.Text T.Text -- group, queue
                    | SlurmSubmission

tSubmit :: FilePath -> Bool -> SubmissionSpec -> Task -> Script ()
tSubmit projectDir test submissionSpec task = do
    let jobFileName = projectDir </> _tName task <.> "job.sh"
        logFile = logFileName projectDir task
    mktree . directory $ jobFileName
    writeJobScript submissionSpec jobFileName (_tCommand task)
    timeCmd <- need "TMAN_GTIME" >>= tryJust "Please set environment variable TMAN_GTIME to the absolute path to your Gnu Time installation"
    case submissionSpec of
        LSFsubmission group queue -> do
            let m = _tMem task
                rArg = format ("select[mem>"%d%"] rusage[mem="%d%"] span[hosts=1]") m m
                cmd = [timeCmd, "--verbose", "bash", format fp jobFileName]
                lsf_args = ["-J", format fp $ _tName task, "-R", rArg, "-M", format d $ _tMem task,
                        "-n", format d $ _tNrThreads task, "-oo", format fp $ logFileName projectDir task]
                group_args = if T.null group then [] else ["-G", group]
                queue_args = if T.null queue then [] else ["-q", queue]
                args = lsf_args ++ group_args ++ queue_args ++ cmd
            if test then
                echo (T.intercalate " " $ wrapCmdArgs ("bsub":args))
            else do
                touch logFile
                exitCode <- proc "bsub" args empty
                case exitCode of
                    ExitFailure i -> throwE ("bsub command failed with exit code " ++ show i)
                    _ -> return ()
        SequentialExecutionSubmission -> do
            if test then
                echo $ format("bash "%fp) jobFileName
            else do
                scriptIO . withFile (encodeString logFile) WriteMode $ \logFileH -> do
                    err $ format ("running job: "%fp) (_tName task)
                    let processRaw = P.proc (T.unpack timeCmd) ["--verbose", "bash", encodeString jobFileName]
                    (_, _, _, pHandle) <- P.createProcess (processRaw {P.std_err = P.UseHandle logFileH, P.std_out = P.UseHandle logFileH})
                    _ <- P.waitForProcess pHandle
                    return ()
        SlurmSubmission -> do
            let cmd = format (s%" --verbose bash "%fp) timeCmd jobFileName
                args = [format ("--job-name="%fp) (_tName task), format ("--mem="%d) (_tMem task), 
                            format ("--cpus-per-task="%d) (_tNrThreads task), format ("--output="%fp) (logFileName projectDir task),
                            format ("--wrap="%s) cmd]
            if test then
                echo (T.intercalate " " $ wrapCmdArgs ("sbatch":args))
            else do
                touch logFile
                exitCode <- proc "sbatch" args empty
                case exitCode of
                    ExitFailure i -> throwE ("sbatch command failed with exit code " ++ show i)
                    _ -> return ()
                
  where
    wrapCmdArgs args = [if " " `T.isInfixOf` a then T.cons '\"' . flip T.snoc '\"' $ a else a | a <- args]

writeJobScript :: SubmissionSpec -> FilePath -> T.Text -> Script ()
writeJobScript submissionSpec jobFileName command = do
    let submissionName = case submissionSpec of
            LSFsubmission _ _ -> "LSF"
            SequentialExecutionSubmission -> "Sequential"
            SlurmSubmission -> "Slurm"
    let c = format ("echo "%s%"\n"%s%"\n") submissionName command
    scriptIO $ T.writeFile (encodeString jobFileName) c

tStatus :: Task -> Script TaskStatus
tStatus task = do
    inputTaskStatus <- mapM tStatus (_tInputTasks task)
    if any (/=StatusComplete) inputTaskStatus then
        return . StatusIncompleteInputTask . T.intercalate "," $
            [format fp $ _tName t | (t, s') <- zip (_tInputTasks task) inputTaskStatus,
                                               s' /= StatusComplete]
    else do
        iFileSize <- mapM getFileSize $ _tInputFiles task
        if (0 :: Int) `elem` iFileSize then
            return . StatusMissingInputFile . T.intercalate "," $
                [format fp f | (f, s') <- zip (_tInputFiles task) iFileSize, s' == 0]
        else do
            oFileSize <- mapM getFileSize $ _tOutputFiles task
            if (0 :: Int) `elem` oFileSize then
                return . StatusIncomplete . T.intercalate "," $
                    [format fp f | (f, s') <- zip (_tOutputFiles task) oFileSize, s' == 0]
            else do
                inputMod <- mapM datefile $ allInputFiles task
                outputMod <- mapM datefile $ _tOutputFiles task
                if null inputMod || null outputMod || maximum inputMod <= minimum outputMod then
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
    allInputFiles t = concatMap _tOutputFiles (_tInputTasks t) ++ _tInputFiles t

tRunInfo :: FilePath -> Task -> Script TaskRunInfo
tRunInfo projectDir task = do
    let fn = logFileName projectDir task
    logFileExists <- scriptIO . testfile $ fn
    if not logFileExists then
        return InfoNoLogFile
    else do
        content <- scriptIO . T.readFile . encodeString $ fn
        let l = T.lines content
        if null l then return InfoNoLogFile else do
            let headerLine = head l
            case headerLine of
                "LSF" -> tryRight $ tLSFrunInfo content
                "Sequential" -> tryRight $ tStandardRunInfo content
                "Slurm" -> tryRight $ tSlurmRunInfo content
                _ -> return InfoUnknownLogFormat

tLSFrunInfo :: T.Text -> Either String TaskRunInfo
tLSFrunInfo c = do
    let (_, infoPart) = T.breakOn "Sender: LSF System" c
    if T.null infoPart then return InfoNotFinished else
        if "Successfully completed." `T.isInfixOf` infoPart then
            InfoSuccess `fmap` parseLogFile c
        else
            if "TERM_MEMLIMIT" `T.isInfixOf` infoPart then return $ InfoFailed FailedMem else
                if "TERM_RUNLIMIT" `T.isInfixOf` infoPart then return $ InfoFailed FailedRuntime else
                    return $ InfoFailed FailedUnknown

tSlurmRunInfo :: T.Text -> Either String TaskRunInfo
tSlurmRunInfo c = do
    if "Exceeded job memory limit" `T.isInfixOf` c then
        return $ InfoFailed FailedMem
    else
        tStandardRunInfo c

tStandardRunInfo :: T.Text -> Either String TaskRunInfo
tStandardRunInfo c = do
    let (_, infoPart) = T.breakOn "Command being timed" c
    if T.null infoPart then return InfoNotFinished else
        if "Exit status: 0" `T.isInfixOf` infoPart then
            InfoSuccess `fmap` parseLogFile infoPart
        else
            return $ InfoFailed FailedUnknown

parseLogFile :: T.Text -> Either String RunInfo
parseLogFile content = do
    let l = T.lines content
    maxMemLine <- headErr "cannot find maximum memory in GNU time log output" . filter (T.isPrefixOf "\tMaximum resident set size (kbytes): ") $ l
    maxMemStr <- headErr "cannot read maximum memory in GNU time log output" . T.words . T.drop
                 (T.length "\tMaximum resident set size (kbytes): ") $ maxMemLine
    maxMem <- readErr "cannot read maximum memory in GNU time log output" . T.unpack $ maxMemStr
    durationLine <- headErr "cannot find duration in GNU time log output" . filter (T.isPrefixOf "\tElapsed (wall clock) time (h:mm:ss or m:ss): ") $ l
    durationStr <- headErr "cannot read duration in GNU time log output" . T.words . T.drop
                 (T.length "\tElapsed (wall clock) time (h:mm:ss or m:ss): ") $ durationLine
    return $ RunInfo durationStr (maxMem / 1000)
    
tClean :: FilePath -> Bool -> Task -> Script ()
tClean projectDir force task = do
    let jobFile = projectDir </> _tName task <.> "job.sh"
        logFile = logFileName projectDir task
    if force then do
        rm' jobFile
        rm' logFile
    else do
        info <- tRunInfo projectDir task
        case info of
            InfoSuccess _ -> return ()
            _ -> do
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
    echo ""
  where
    fn = logFileName projectDir task

tLsfKill :: Task -> Script ()
tLsfKill task = do
    exitCode <- proc "bsub" ["-J", format fp $ _tName task] empty
    case exitCode of
        ExitFailure i -> throwE ("bkill command failed with exit code " ++ show i)
        _ -> return ()
    return ()

tSlurmKill :: Task -> Script ()
tSlurmKill task = do
    exitCode <- proc "scancel" ["-n", format fp $ _tName task] empty
    case exitCode of
        ExitFailure i -> throwE ("scancel command failed with exit code " ++ show i)
        _ -> return ()
    return ()
