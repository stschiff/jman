{-# LANGUAGE OverloadedStrings #-}

module Tman.Task (
    Task(..),
    TaskSpec(..),
    TaskStatus(..),
    TaskRunInfo(..),
    FailedReason(..),
    RunInfo(..),
    tSubmit,
    tStatus,
    tRunInfo,
    tClean,
    tLog
) where

import Control.Error (Script, scriptIO, tryAssert, throwE)
import Control.Monad (when, mzero)
import Data.Aeson (FromJSON, ToJSON, parseJSON, (.:), toJSON, Value(..),
    object, (.=))
import qualified Data.Text as T
import Prelude hiding (FilePath)
import Turtle

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
    toJSON (TaskSpec n it ifiles o' c m t h) =
        object ["name" .= n,
                "inputTasks" .= it,
                "inputFiles" .= ifiles,
                "outputFiles" .= o',
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

data FailedReason = FailedMem | FailedRuntime | FailedUnknown
    deriving (Eq, Ord, Show)

data RunInfo = RunInfo {
    _runInfoTime :: T.Text,
    _runInfoMaxMem :: Double
} deriving (Eq, Show, Ord)

tSubmit :: FilePath -> Bool -> Task -> Script ()
tSubmit projectDir test task = do
    let logFile = logFileName projectDir task
    mktree . directory $ logFile
    let args = [format ("--job-name="%fp) (_tName task), format ("--mem="%d) (_tMem task), 
            format ("--cpus-per-task="%d) (_tNrThreads task),
            format ("--output="%fp) logFile, format ("--wrap="%s) (_tCommand task)]
    if test then
        echo . unsafeTextToLine . T.intercalate " " $ wrapCmdArgs ("sbatch":args)
    else do
        touch logFile
        exitCode <- proc "sbatch" args empty
        case exitCode of
            ExitFailure i -> throwE $ format ("sbatch command failed with exit code "%d) i
            _ -> return ()
                
  where
    wrapCmdArgs args =
        [if " " `T.isInfixOf` a then T.cons '\"' . flip T.snoc '\"' $ a else a | a <- args]

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
                [format fp f' | (f', s') <- zip (_tInputFiles task) iFileSize, s' == 0]
        else do
            oFileSize <- mapM getFileSize $ _tOutputFiles task
            if (0 :: Int) `elem` oFileSize then
                return . StatusIncomplete . T.intercalate "," $
                    [format fp f' | (f', s') <- zip (_tOutputFiles task) oFileSize, s' == 0]
            else do
                inputMod <- mapM datefile $ allInputFiles task
                outputMod <- mapM datefile $ _tOutputFiles task
                if null inputMod || null outputMod || maximum inputMod <= minimum outputMod then
                    return StatusComplete
                else
                    return StatusOutdated
  where
    getFileSize f' = do
        iExists <- testfile f'
        if iExists then do
            fs <- du f'
            return $ bytes fs
        else
            return 0
    allInputFiles t = concatMap _tOutputFiles (_tInputTasks t) ++ _tInputFiles t

tRunInfo :: FilePath -> Task -> Script TaskRunInfo
tRunInfo = undefined 

tClean :: FilePath -> Task -> Script ()
tClean projectDir task = do
    let logFile = logFileName projectDir task
    rm' logFile
  where
    rm' f' = do
        b <- testfile f'
        when b $ rm f'

tLog :: FilePath -> Task -> Script ()
tLog projectDir task = do
    testfile fn >>= tryAssert "Log file doesn't exist"
    scriptIO . stdout . input $ fn
    echo ""
  where
    fn = logFileName projectDir task

