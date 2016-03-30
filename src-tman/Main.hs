{-# LANGUAGE OverloadedStrings #-}
import Tman.Task (Task(..), TaskSpec(..), tSubmit, tRunInfo, tStatus, tClean, tLog,
                           SubmissionSpec(..), TaskStatus(..), TaskRunInfo(..), RunInfo(..), 
                           tSlurmKill, tLsfKill)

import Tman.Project (Project(..), loadProject, saveProject, addTask)

import Control.Applicative ((<|>))
import Control.Error (runScript, Script, scriptIO, tryRight, throwE)
import Data.List (sortBy, groupBy)
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Control.Monad (forM_)
import Data.Monoid ((<>))
import qualified Options.Applicative as OP
import Prelude hiding (FilePath)
import System.FilePath.GlobPattern ((~~))
import Turtle (format, s, d, fp, (%), w, FilePath, err, fromText)

data Options = CmdSubmit SubmitOpt
             | CmdList ListOpt
             | CmdPrint PrintOpt
             | CmdStatus StatusOpt
             | CmdClean CleanOpt
             | CmdLog LogOpt
             | CmdInfo InfoOpt
             | CmdKill KillOpt
             | CmdInit InitOpt
             | CmdAdd AddOpt

data SubmitOpt = SubmitOpt JobSpec Bool Bool String String String Bool
-- jobSpec force test submissionType queue group unchecked

data JobSpec = JobPattern String | AllJobs

data ListOpt = ListOpt JobSpec Int Bool
-- jobSpec summary full

data PrintOpt = PrintOpt JobSpec

data StatusOpt = StatusOpt JobSpec Int Bool Bool Bool
-- jobSpec summary info skipSuccessful full

data CleanOpt = CleanOpt JobSpec Bool

data LogOpt = LogOpt JobSpec

data InfoOpt = InfoOpt JobSpec

data KillOpt = KillOpt JobSpec String

data InitOpt = InitOpt T.Text FilePath

data AddOpt = AddOpt TaskSpec

main :: IO ()
main = OP.execParser optParser >>= runWithOptions
  where
    optParser = OP.info (OP.helper <*> parseCommand)
                (OP.fullDesc <> OP.progDesc "tman: interactive task processing tool")

parseCommand :: OP.Parser Options
parseCommand = OP.subparser $
    OP.command "submit" (parseSubmit `withInfo` "submit jobs") <>
    OP.command "list"   (parseList `withInfo` "list job info") <>
    OP.command "print"  (parsePrint `withInfo` "print commands") <>
    OP.command "status" (parseStatus `withInfo` "print status for each job") <>
    OP.command "clean"  (parseClean `withInfo` "remove job and log files") <>
    OP.command "log"    (parseLog `withInfo` "print log file for a task") <>
    OP.command "info"   (parseInfo `withInfo` "print run info for a task") <>
    OP.command "kill"   (parseKill `withInfo` "kill job") <>
    OP.command "init"   (parseInit `withInfo` "initialize a new empty project") <>
    OP.command "add"    (parseAdd `withInfo` "add a new task to the project")

parseSubmit :: OP.Parser Options
parseSubmit = CmdSubmit <$> parseSubmitOpt
  where
    parseSubmitOpt = SubmitOpt <$> parseJobSpec <*> parseForce <*> parseTest <*>
                                   parseSubmissionType <*> parseQueue <*> parseSubGroup <*>
                                   parseUnchecked
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
    parseUnchecked = OP.switch $ OP.short 'u' <> OP.long "unchecked" <>
                     OP.help ("do not check any status, just submit (this is even stronger than " ++
                              "force and should be given with care)")

parseJobSpec :: OP.Parser JobSpec
parseJobSpec = parseGroupName <|> pure AllJobs
  where
    parseGroupName = OP.option (JobPattern <$> OP.str) $ OP.short 'j' <> OP.long "job" <>
                                    OP.metavar "<jobname_pattern>" <>
                                    OP.help "Job or Jobgroup name. Can contain * and ** to glob"

withInfo :: OP.Parser a -> String -> OP.ParserInfo a
withInfo opts desc = OP.info (OP.helper <*> opts) $ OP.progDesc desc

parseList :: OP.Parser Options
parseList = CmdList <$> parseListOpt
  where
    parseListOpt = ListOpt <$> parseJobSpec <*> parseSummary <*> parseFull
    parseFull = OP.switch $ OP.short 'f' <> OP.long "full" <> OP.help "show full list"

parseSummary :: OP.Parser Int
parseSummary = OP.option OP.auto $ OP.short 's' <> OP.long "summaryLevel" <> OP.value 0 <> 
                                   OP.showDefault <>
                                   OP.metavar "<Level>" <>
                                   OP.help "summarize status for groups at given level, leave 0 \
                                            \for now grouping"

parsePrint :: OP.Parser Options
parsePrint = CmdPrint <$> parsePrintOpt
  where
    parsePrintOpt = PrintOpt <$> parseJobSpec

parseStatus :: OP.Parser Options
parseStatus = CmdStatus <$> parseStatusOpt
  where
    parseStatusOpt = StatusOpt <$> parseJobSpec <*> parseSummary <*> parseWithRunInfo <*> 
                                   parseSkipSuccessful <*> parseFull
    parseWithRunInfo = OP.switch $ OP.short 'i' <> OP.long "info" <> OP.help "show runInfo"
    parseSkipSuccessful = OP.switch $ OP.short 'S' <> OP.long "skipSuccessful" <>
                                      OP.help "skip complete tasks or tasks without a logfile, \
                                               \if -i and/or -l is used"
    parseFull = OP.switch $ OP.short 'f' <> OP.long "full" <> OP.help "full status output"

parseClean :: OP.Parser Options
parseClean = CmdClean <$> parseCleanOpt
  where
    parseCleanOpt = CleanOpt <$> parseJobSpec <*> parseForce
    parseForce = OP.switch $ OP.short 'f' <> OP.long "force" <>
                             OP.help "clean even successfully run jobs"

parseLog :: OP.Parser Options
parseLog = CmdLog <$> parseLogOpt
  where
    parseLogOpt = LogOpt <$> parseJobSpec

parseInfo :: OP.Parser Options
parseInfo = CmdInfo <$> parseInfoOpt
  where
    parseInfoOpt = InfoOpt <$> parseJobSpec

parseKill :: OP.Parser Options
parseKill = CmdKill <$> parseKillOpt
  where
    parseKillOpt = KillOpt <$> parseJobSpec <*> st
    st = OP.strOption $ OP.short 's' <> OP.long "submissionType" <> OP.metavar "<slurm|lsf>" <>
                        OP.help "submission type, must be either slurm or lsf"

parseInit :: OP.Parser Options
parseInit = CmdInit <$> parseInitOpt
  where
    parseInitOpt = InitOpt <$> parseProjectName <*> parseProjectPath
    parseProjectName = OP.option (T.pack <$> OP.str) $ OP.short 'n' <> OP.long "projectName" <>
                       OP.metavar "<NAME>" <> OP.help "the name of the project"
    parseProjectPath = OP.option (fromText . T.pack <$> OP.str) $ OP.short 'p' <> OP.long "path" <>
                       OP.metavar "<PATH>" <> OP.help "the log- and job-script directory for \
                                                       \the project"

parseAdd :: OP.Parser Options
parseAdd = CmdAdd <$> parseAddOpt
  where
    parseAddOpt = AddOpt <$> parseTaskSpec
    parseTaskSpec = TaskSpec <$> parseName <*> parseInputTasks <*> parseInputFiles <*>
                                 parseOutputFiles <*> parseCmd <*>
                                 parseMem <*> parseThreads <*> parseHours
    
    parseName = OP.option (T.pack <$> OP.str) $ OP.short 'n' <> OP.long "name" <>
                                 OP.metavar "<NAME>" <> OP.help "name of the task"
    parseCmd = OP.option (T.pack <$> OP.str) $ OP.short 'c' <> OP.long "command" <>
                   OP.metavar "<CMD>" <> OP.help "full bash command line, wrapped in quotation \
                                                  \marks"
    parseInputTasks = OP.many (OP.option (T.pack <$> OP.str) $ OP.short 'I' <>
                      OP.long "inputTask" <> OP.metavar "<Task>" <>
                      OP.help "input task name, can be given multiple times")
    parseInputFiles = OP.many (OP.option (T.pack <$> OP.str) $ OP.long "inputFile" <>
                      OP.short 'i' <> OP.metavar "<File>" <>
                      OP.help "input file, can be given multiple times")
    parseOutputFiles = OP.many (OP.option (T.pack <$> OP.str) $ OP.long "output" <> OP.short 'o' <>
                       OP.metavar "<File>" <> OP.help "output file, can be given multiple times")
    parseMem = OP.option OP.auto $ OP.long "mem" <> OP.short 'm' <> OP.metavar "<Mb>" <>
                                         OP.value 100 <> OP.showDefault <>
                                         OP.help "maximum memory in Mb"
    parseThreads = OP.option OP.auto $ OP.long "threads" <> OP.short 't' <>
                   OP.metavar "<nrThreads>" <> OP.value 1 <> OP.showDefault <>
                   OP.help "nr of threads"
    parseHours = OP.option OP.auto $ OP.long "hours" <> OP.short 'h' <> OP.metavar "<Hours>" <>
                                    OP.value 10 <> OP.showDefault <> OP.help "run time in hours"


runWithOptions :: Options -> IO ()
runWithOptions options = runScript $ do
    case options of
        CmdSubmit opts -> runSubmit opts
        CmdList   opts -> runList   opts
        CmdPrint  opts -> runPrint  opts 
        CmdStatus opts -> runStatus opts
        CmdClean  opts -> runClean  opts
        CmdLog    opts -> runLog    opts
        CmdInfo   opts -> runInfo   opts
        CmdKill   opts -> runKill   opts
        CmdInit   opts -> runInit   opts
        CmdAdd    opts -> runAdd    opts

runSubmit :: SubmitOpt -> Script ()
runSubmit (SubmitOpt jobSpec force test submissionType queue group unchecked) = do
    project <- loadProject
    let projectDir = _prLogDir project
    tasks <- tryRight $ selectTasks project jobSpec
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
            scriptIO . err $ format ("Job "%fp%": already running? skipping. Use --clean to reset") 
                             (_tName t)
        else
            case st of
                StatusIncompleteInputTask _ ->
                    scriptIO . err $ format ("Job "%fp%": incomplete input task(s), skipping") 
                                     (_tName t)
                StatusMissingInputFile _ ->
                    scriptIO . err $ format ("Job "%fp%": missing input file(s), skipping")
                                     (_tName t)
                StatusComplete -> if force then
                        tSubmit projectDir test submissionSpec t
                    else
                        scriptIO . err $ format ("Job "%fp%": already complete, skipping (use \
                                                 \ --force to submit anyway)") (_tName t)
                _ -> tSubmit projectDir test submissionSpec t

selectTasks :: Project -> JobSpec -> Either String [Task]
selectTasks jobProject jobSpec =
    case jobSpec of
        AllJobs -> return . M.elems . _prTasks $ jobProject
        JobPattern pat -> do
            let ret = filter ((~~ pat) . T.unpack . format fp . _tName) . M.elems . _prTasks $ 
                      jobProject
            if null ret then Left "No Tasks found" else Right ret

runList :: ListOpt -> Script ()
runList (ListOpt jobSpec summaryLevel full) = do
    project <- loadProject
    tasks <- tryRight $ selectTasks project jobSpec
    if summaryLevel > 0 then do
        let groups = map (T.intercalate "/" . take summaryLevel . T.splitOn "/" . format fp . 
                          _tName) tasks
            entries :: [(T.Text, Int)]
            entries = sortBy (\(e1, _) (e2, _) -> e1 `compare` e2) . M.toList .
                      foldl (\mm k -> M.insertWith (+) k 1 mm) M.empty $ groups
        scriptIO . mapM_ T.putStrLn $
            [format ("Group "%s%": "%d%" job(s)") g num | (g, num) <- entries]
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
        format (fp%"\t"%d%"\t"%d%"\t"%d%"\t"%w%"\t"%w%"\t"%w) n m t h (map _tName it) ifiles o
    tMeta False (Task n _ _ _ _ m t h) =
        format (fp%"\t"%d%"\t"%d%"\t"%d) n m t h

runPrint :: PrintOpt -> Script ()
runPrint (PrintOpt jobSpec) = do
    project <- loadProject
    tasks <- tryRight $ selectTasks project jobSpec    
    scriptIO . mapM_ (T.putStrLn . _tCommand) $ tasks

runStatus :: StatusOpt -> Script ()
runStatus (StatusOpt jobSpec summaryLevel withRunInfo skipSuccessful full) = do
    project <- loadProject
    let logDir = _prLogDir project
    tasks <- tryRight $ selectTasks project jobSpec
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
    showSt (StatusIncompleteInputTask t) =
        if full then format ("incomplete input task: "%s) t else "missingInputTask"
    showSt (StatusMissingInputFile t) =
        if full then format ("missing input file: "%s) t else "missingInputFile"
    showI (Just (InfoSuccess _)) = "+Success"
    showI (Just i) = format ("+"%w) i
    showI Nothing = ""

runClean :: CleanOpt -> Script ()
runClean (CleanOpt jobSpec force) = do
    project <- loadProject
    let logDir = _prLogDir project
    tasks <- tryRight $ selectTasks project jobSpec
    mapM_ (tClean logDir force) tasks

runLog :: LogOpt -> Script ()
runLog (LogOpt jobSpec) = do
    project <- loadProject
    let logDir = _prLogDir project
    tasks <- tryRight $ selectTasks project jobSpec
    mapM_ (tLog logDir) tasks

runInfo :: InfoOpt -> Script ()
runInfo (InfoOpt jobSpec) = do
    project <- loadProject
    let logDir = _prLogDir project
    tasks <- tryRight $ selectTasks project jobSpec
    scriptIO . putStrLn $ "JOB\tSTATUS\tDURATION [(h:mm:ss or m:ss)]\tMAX_MEM [Mb]"
    info <- mapM (tRunInfo logDir) tasks
    forM_ (zip tasks info) $ \(task, i) ->
        case i of
            InfoSuccess (RunInfo duration max_) -> do
                scriptIO . T.putStrLn $ format (fp%"\tSuccess\t"%s%"\t"%w) (_tName task) duration 
                                        max_
            InfoFailed r -> scriptIO . T.putStrLn $ format (fp%"\t"%w) (_tName task) r
            _ -> scriptIO . T.putStrLn $ format (fp%"\t"%w) (_tName task) i

runKill :: KillOpt -> Script ()
runKill (KillOpt jobSpec submissionType) = do
    project <- loadProject
    tasks <- tryRight $ selectTasks project jobSpec
    case submissionType of
        "slurm" -> mapM_ tSlurmKill tasks
        "lsf" -> mapM_ tLsfKill tasks
        _ -> throwE "unknown submission type"

runInit :: InitOpt -> Script ()
runInit (InitOpt name path) = do
    let proj = Project name path M.empty []
    saveProject proj

runAdd :: AddOpt -> Script ()
runAdd (AddOpt taskSpec) = do
    project <- loadProject
    project' <- addTask project taskSpec
    saveProject project'
