{-# LANGUAGE OverloadedStrings #-}

module Tman.Task (
    Task(..),
    TaskSpec(..),
    TaskStatus(..),
    TaskRunInfo(..),
    RunInfo(..),
    tSubmit,
    tStatus,
    tRunInfo,
    tClean,
    tLog
) where

import Control.Error (Script, scriptIO, tryAssert, throwE, tryJust, headErr, readErr, tryRight)
import Control.Monad (when, mzero)
import Data.Aeson (FromJSON, ToJSON, parseJSON, (.:), toJSON, Value(..),
    object, (.=))
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Filesystem.Path.CurrentOS (encodeString)
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

statsFileName :: FilePath -> Task -> FilePath
statsFileName projectDir task = projectDir </> _tName task <.> "stats.txt"

data TaskStatus = StatusIncompleteInputTask T.Text
                | StatusMissingInputFile T.Text
                | StatusIncomplete T.Text
                | StatusOutdated
                | StatusComplete deriving (Eq, Ord, Show)

data TaskRunInfo = InfoNoStatsFile
                 | InfoNotFinished
                 | InfoFailed
                 | InfoSuccess RunInfo deriving (Eq, Ord, Show)

data RunInfo = RunInfo {
    _runInfoTime :: T.Text,
    _runInfoMaxMem :: Double
} deriving (Eq, Show, Ord)

tSubmit :: FilePath -> Bool -> Task -> Script ()
tSubmit projectDir test task = do
    let jobFileName = projectDir </> _tName task <.> "job.sh"
        logFile = logFileName projectDir task
        statsFile = statsFileName projectDir task
    mktree . directory $ jobFileName
    writeJobScript jobFileName (_tCommand task)
    timeCmd <- need "TMAN_GTIME" >>= tryJust "Please set environment variable \
            \TMAN_GTIME to the absolute path to your Gnu Time installation"

    let cmd = format (s%" --verbose -o "%fp%" bash "%fp) timeCmd statsFile jobFileName
        args = [format ("--job-name="%fp) (_tName task), format ("--mem="%d) (_tMem task), 
            format ("--cpus-per-task="%d) (_tNrThreads task),
            format ("--output="%fp) logFile, format ("--wrap="%s) cmd]
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
    
writeJobScript :: FilePath -> T.Text -> Script ()
writeJobScript jobFileName command = do
    let c = format ("#!/usr/bin/env bash\n"%s%"\n") command
    scriptIO $ T.writeFile (T.unpack . format fp $ jobFileName) c
    
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
tRunInfo projectDir task = do
    let fn = statsFileName projectDir task
    statsFileExists <- scriptIO . testfile $ fn
    if not statsFileExists then
        return InfoNoStatsFile
    else do
        content <- scriptIO . T.readFile . encodeString $ fn
        if content == "" then return InfoNoStatsFile else do
                let (_, infoPart) = T.breakOn "Command being timed" content
                if T.null infoPart then return InfoNotFinished else
                    if "Exit status: 0" `T.isInfixOf` infoPart then
                        tryRight (InfoSuccess `fmap` parseStatsFile infoPart)
                    else
                        return InfoFailed

parseStatsFile :: T.Text -> Either T.Text RunInfo
parseStatsFile content = do
    let li = T.lines content
    maxMemLine <- headErr "cannot find maximum memory in GNU time log output" . filter (T.isPrefixOf "\tMaximum resident set size (kbytes): ") $ li
    maxMemStr <- headErr "cannot read maximum memory in GNU time log output" . T.words . T.drop
                 (T.length "\tMaximum resident set size (kbytes): ") $ maxMemLine
    maxMem <- readErr "cannot read maximum memory in GNU time log output" . T.unpack $ maxMemStr
    durationLine <- headErr "cannot find duration in GNU time log output" . filter (T.isPrefixOf "\tElapsed (wall clock) time (h:mm:ss or m:ss): ") $ li
    durationStr <- headErr "cannot read duration in GNU time log output" . T.words . T.drop
                 (T.length "\tElapsed (wall clock) time (h:mm:ss or m:ss): ") $ durationLine
    return $ RunInfo durationStr (maxMem / 1000)


tClean :: FilePath -> Task -> Script ()
tClean projectDir task = do
    let logFile = logFileName projectDir task
    rm' logFile
    let statsFile = statsFileName projectDir task
    rm' statsFile
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

