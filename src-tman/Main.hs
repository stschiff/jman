{-# LANGUAGE OverloadedStrings #-}
import Tman.Internal.Task (Task(..), tSubmit, tRunInfo, tStatus, tClean, tLog,
                           SubmissionSpec(..), TaskStatus(..), TaskRunInfo(..), RunInfo(..), 
                           tSlurmKill, tLsfKill)

import Tman.Internal.Project (Project(..), loadProject)

import Control.Error (runScript, Script, scriptIO, tryRight, throwE)
import Data.List (sortBy, groupBy)
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Time (diffUTCTime)
import Control.Monad (forM_)
import Data.Monoid ((<>))
import Filesystem.Path.CurrentOS (encodeString)
import qualified Options.Applicative as OP
import Prelude hiding (FilePath)
import System.FilePath.GlobPattern ((~~))
import Turtle (FilePath, fromText)
import Turtle.Format (format, s, d, fp, (%), w)
import Turtle.Prelude (err)

data Options = Options {
    _optGroupName :: String,
    _optAll :: Bool,
    _optCommand :: Command
}

data Command = CmdSubmit SubmitOpt
             | CmdList ListOpt
             | CmdPrint
             | CmdStatus StatusOpt
             | CmdClean Bool
             | CmdLog
             | CmdInfo
             | CmdKill String

data SubmitOpt = SubmitOpt {
    _suForce :: Bool,
    _suTest :: Bool,
    _suSubmissionType :: String,
    _suQueue :: String,
    _suGroup :: String,
    _suChunkSize :: Int,
    _suUnchecked :: Bool
}

data ListOpt = ListOpt {
    _liSummary :: Int,
    _liFull :: Bool
}

data StatusOpt = StatusOpt {
    _stSummary :: Int,
    _stInfo :: Bool,
    _stSkipSuccessful :: Bool,
    _stFull :: Bool
}

main :: IO ()
main = OP.execParser optParser >>= runWithOptions
  where
    optParser = OP.info (OP.helper <*> options) (OP.fullDesc <> OP.progDesc "task processing tool")

runWithOptions :: Options -> IO ()
runWithOptions (Options groupName allJobs cmdOpts) = runScript $ do
    scriptIO . err $ "loading project file"
    jobProject <- loadProject
    let logDir = _prLogDir jobProject
    tasks <- if allJobs then return $ _prTasks jobProject else tryRight $ selectTasks jobProject
    case cmdOpts of
        CmdSubmit (SubmitOpt force test submissionType queue group chunkSize unchecked) ->
            runSubmit logDir tasks force test submissionType queue group chunkSize unchecked
        CmdList (ListOpt summaryLevel full) -> runList tasks summaryLevel full
        CmdPrint -> scriptIO . mapM_ (T.putStrLn . _tCommand) $ tasks
        CmdStatus (StatusOpt summaryLevel info skipSuccessful full) ->
            runStatus logDir tasks summaryLevel info skipSuccessful full
        CmdClean force -> mapM_ (tClean logDir force) tasks
        CmdLog -> mapM_ (tLog $ _prLogDir jobProject) tasks
        CmdInfo -> runInfo logDir tasks
        CmdKill submissionType -> case submissionType of
            "slurm" -> mapM_ tSlurmKill tasks
            "lsf" -> mapM_ tLsfKill tasks
            _ -> throwE "unknown submission type"
  where
    selectTasks jobProject =
        let ret = filter ((~~ groupName) . encodeString . _tName) . _prTasks $ jobProject
        in  if null ret then Left "No Tasks found" else Right ret

runSubmit :: FilePath -> [Task] -> Bool -> Bool -> String -> String -> String -> Int -> Bool -> Script ()
runSubmit projectDir tasks force test submissionType queue group chunkSize unchecked = do
    status <- if unchecked then
            return . repeat $ StatusIncomplete ""
        else
            mapM tStatus tasks
    info <- if unchecked then
            return $ repeat InfoNoLogFile
        else
            mapM (tRunInfo projectDir) tasks
    submissionSpec <- case submissionType of
        "lsf" -> return $ LSFsubmission (T.pack group) (T.pack queue)
        "seq" -> return SequentialExecutionSubmission
        "slurm" -> return SlurmSubmission
        _ -> throwE "unknown submission type"
    forM_ (zip3 tasks status info) $ \(t, st, i) ->
        if i == InfoNotFinished then
            scriptIO . err $ format ("Job "%fp%": already running? skipping. Use --clean to reset") (_tName t)
        else
            case st of
                StatusIncompleteInputTask _ ->
                    scriptIO . err $ format ("Job "%fp%": incomplete input task(s), skipping") (_tName t)
                StatusMissingInputFile _ ->
                    scriptIO . err $ format ("Job "%fp%": missing input file(s), skipping") (_tName t)
                StatusComplete -> if force then
                        tSubmit projectDir test submissionSpec t
                    else
                        scriptIO . err $ format ("Job "%fp%": already complete, skipping (use --force to submit anyway)")
                                         (_tName t)
                _ -> tSubmit projectDir test submissionSpec t

runList :: [Task] -> Int -> Bool -> Script ()
runList tasks summaryLevel full = do
    if summaryLevel > 0 then do
        let groups = map (T.intercalate "/" . take summaryLevel . T.splitOn "/" . format fp . _tName) tasks
            entries = sortBy (\(e1, _) (e2, _) -> e1 `compare` e2) . M.toList .
                      foldl (\mm k -> M.insertWith (+) k 1 mm) M.empty $ groups
        scriptIO . mapM_ T.putStrLn $ [format ("Group "%s%": "%d%" job(s)") g num | (g, num) <- entries]
    else
        if full then do
            let headers = ["NAME", "MEMORY", "THREADS", "HOURS", "INPUTTASKS", "INPUTFILES",
                           "OUTPUTFILES"]
            scriptIO . T.putStrLn . T.intercalate "\t" $ headers
            scriptIO  . mapM_ (T.putStrLn . tMeta True) $ tasks
        else do
            let headers = ["NAME", "MEMORY", "THREADS", "HOURS"]
            scriptIO . T.putStrLn . T.intercalate "\t" $ headers
            scriptIO . mapM_ (T.putStrLn . tMeta False) $ tasks
  where
    tMeta True (Task n it ifiles o _ m t h) =
        format (fp%"\t"%d%"\t"%d%"\t"%d%"\t"%w%"\t"%w%"\t"%w) n m t h it ifiles o
    tMeta False (Task n _ _ _ _ m t h) =
        format (fp%"\t"%d%"\t"%d%"\t"%d) n m t h

runStatus :: FilePath -> [Task] -> Int -> Bool -> Bool -> Bool -> Script ()
runStatus logDir tasks summaryLevel withRunInfo skipSuccessful full = do
    fullStatusList <- do
        status <- mapM tStatus tasks
        info <- if withRunInfo then
                    mapM (fmap Just . tRunInfo logDir) tasks
                else
                    return [Nothing | _ <- tasks]
        return $ zip status info
    if summaryLevel > 0 then do
        let groups =
                map (T.intercalate "/" . take summaryLevel . T.splitOn "/" . format fp . _tName)
                tasks
            dict :: M.Map (T.Text, T.Text) Int
            dict = foldl (\mm k -> M.insertWith (+) k 1 mm) M.empty $
                   zip groups (map showFullStatus fullStatusList)

            entries :: [(T.Text, [(T.Text, Int)])]
            entries = map (\subList ->
                           (fst . fst . head $ subList, [(st, c) | ((_, st), c) <- subList])) .
                          groupBy (\((e1, _), _) ((e2, _), _) -> e1 == e2) .
                          sortBy (\((e1, _), _) ((e2, _), _) -> e1 `compare` e2) . M.toList $ dict

        scriptIO . mapM_ T.putStrLn $
            [format ("Group "%s%": "%s) g $
             T.intercalate ", " [format (s%"("%w%")") st c | (st, c) <- l] |
             (g, l) <- entries]
    else do
        let ll = map (\(t, l) -> format ("Job "%fp%": "%s) (_tName t) (showFullStatus l)) $
                 filter pred_ $ zip tasks fullStatusList
        scriptIO $ mapM_ T.putStrLn ll
  where
    pred_ = if skipSuccessful then
                (\(_, (st, i)) ->
                    case (st, i) of
                        (StatusComplete, Nothing) -> False
                        (StatusComplete, Just (InfoSuccess _)) -> False
                        (StatusComplete, Just InfoNoLogFile) -> False
                        (StatusComplete, Just InfoUnknownLogFormat) -> False
                        _ -> True)
            else
                const True
    showFullStatus (st, i) = format (s%s) (showSt st) (showI i)
    showSt StatusComplete = "complete"
    showSt StatusOutdated = "outdated"
    showSt (StatusIncomplete t) = if full then format ("incomplete output: "%s) t else "incomplete"
    showSt (StatusIncompleteInputTask t) = if full then format ("incomplete input task: "%s) t else "missingInputTask"
    showSt (StatusMissingInputFile t) = if full then format ("missing input file: "%s) t else "missingInputFile"
    showI (Just (InfoSuccess _)) = "+Success"
    showI (Just i) = format ("+"%w) i
    showI Nothing = ""

runInfo :: FilePath -> [Task] -> Script ()
runInfo logDir tasks = do
    scriptIO . putStrLn $ "JOB\tSTATUS\tDURATION [(h:mm:ss or m:ss)]\tMAX_MEM [Mb]"
    info <- mapM (tRunInfo logDir) tasks
    forM_ (zip tasks info) $ \(task, i) ->
        case i of
            InfoSuccess (RunInfo duration max_) -> do
                scriptIO . T.putStrLn $ format (fp%"\tSuccess\t"%s%"\t"%w) (_tName task) duration max_
            InfoFailed r -> scriptIO . T.putStrLn $ format (fp%"\t"%w) (_tName task) r
            _ -> scriptIO . T.putStrLn $ format (fp%"\t"%w) (_tName task) i

options :: OP.Parser Options
options = Options <$> parseGroupName <*> parseAll <*> parseCommand
  where
    readFP = (fromText . T.pack) `fmap` OP.str
    parseGroupName = OP.strOption $ OP.short 'j' <> OP.long "job" <> OP.metavar "<group_desc>" <>
                                    OP.help "Job or Jobgroup name" <> OP.value "" <> OP.showDefault
    parseAll = OP.switch $ OP.short 'a' <> OP.long "all" <> OP.help "select all jobs"

parseCommand :: OP.Parser Command
parseCommand = OP.subparser $
    OP.command "submit" (parseSubmit `withInfo` "submit jobs") <>
    OP.command "list" (parseList `withInfo` "list job info") <>
    OP.command "print" (parsePrint `withInfo` "print commands") <>
    OP.command "status" (parseStatus `withInfo` "print status for each job") <>
    OP.command "clean" (parseClean `withInfo` "remove job and log files") <>
    OP.command "log" (parseLog `withInfo` "print log file for a task") <>
    OP.command "info" (parseInfo `withInfo` "print run info for a task") <>
    OP.command "kill" (parseKill `withInfo` "kill job")

parseSubmit :: OP.Parser Command
parseSubmit = CmdSubmit <$> parseSubmitOpt
  where
    parseSubmitOpt = SubmitOpt <$> parseForce <*> parseTest <*>
                                   parseSubmissionType <*> parseQueue <*> parseSubGroup <*>
                                   parseChunkSize <*> parseUnchecked
    parseForce = OP.switch $ OP.short 'f' <> OP.long "force" <>
                 OP.help "force submission of completed tasks"
    parseTest = OP.switch $ OP.short 't' <> OP.long "test" <>
                            OP.help "only print submission commands, do not actually submit"
    parseSubmissionType = OP.strOption $ OP.short 's' <> OP.long "submissionType" <>
                          OP.value "seq" <> OP.showDefault <>
                          OP.help "type of submission [seq | lsf | slurm]"
    parseQueue = OP.strOption $ OP.short 'q' <> OP.long "submissionQueue" <> OP.value "" <>
                                OP.showDefault <>
                                OP.help "LSF submission Queue (only for lsf submissions)"
    parseSubGroup = OP.strOption $ OP.short 'g' <> OP.long "submissionGroup" <>
                    OP.value "" <> OP.help "LSF submission Group (only for lsf submissions)"
    parseChunkSize = OP.option OP.auto $ OP.short 'c' <> OP.long "chunkSize" <>
                     OP.value 2 <> OP.help "Chunk Size (only for Gnu Parallel submissions)"
    parseUnchecked = OP.switch $ OP.short 'u' <> OP.long "unchecked" <>
                     OP.help ("do not check any status, just submit (this is even stronger than " ++
                              "force and should be given with care)")

withInfo :: OP.Parser a -> String -> OP.ParserInfo a
withInfo opts desc = OP.info (OP.helper <*> opts) $ OP.progDesc desc

parseList :: OP.Parser Command
parseList = CmdList <$> parseListOpt
  where
    parseListOpt = ListOpt <$> parseSummary <*> parseFull
    parseFull = OP.switch $ OP.short 'f' <> OP.long "full" <> OP.help "show full list"

parseSummary :: OP.Parser Int
parseSummary = OP.option OP.auto $ OP.short 's' <> OP.long "summaryLevel" <> OP.value 0 <> OP.showDefault <>
                                     OP.metavar "<Level>" <>
                                     OP.help "summarize status for groups at given level, leave 0 for now grouping"

parsePrint :: OP.Parser Command
parsePrint = pure CmdPrint

parseStatus :: OP.Parser Command
parseStatus = CmdStatus <$> parseStatusOpt
  where
    parseStatusOpt = StatusOpt <$> parseSummary <*> parseWithRunInfo <*> parseSkipSuccessful <*>
                                   parseFull
    parseWithRunInfo = OP.switch $ OP.short 'i' <> OP.long "info" <> OP.help "show runInfo"
    parseSkipSuccessful = OP.switch $ OP.short 'S' <> OP.long "skipSuccessful" <> OP.help "skip complete tasks or tasks without a logfile, if -i and/or -l is used"
    parseFull = OP.switch $ OP.short 'f' <> OP.long "full" <> OP.help "full status output"

parseClean :: OP.Parser Command
parseClean = CmdClean <$> parseForce
  where
    parseForce = OP.switch $ OP.short 'f' <> OP.long "force" <> OP.help "clean even successfully run jobs"

parseLog :: OP.Parser Command
parseLog = pure CmdLog

parseInfo :: OP.Parser Command
parseInfo = pure CmdInfo

parseKill :: OP.Parser Command
parseKill = CmdKill <$> st
  where
    st = OP.strOption $ OP.short 's' <> OP.long "submissionType" <> OP.metavar "<slurm|lsf>" <>
                        OP.help "submission type, must be either slurm or lsf"