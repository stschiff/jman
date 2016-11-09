{-# LANGUAGE OverloadedStrings #-}
import Tman.Task (Task(..), TaskSpec(..), tSubmit, tRunInfo, tStatus, tClean,
    tLog, SubmissionSpec(..), TaskStatus(..), TaskRunInfo(..), RunInfo(..), 
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
import Turtle (format, s, d, fp, (%), w, FilePath, err, fromText, need)

data Options = Options FilePath SubCommand

data SubCommand = CmdSubmit SubmitOpt
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
main = do
    projectFileEnv <- need "TMAN_PROJECT_FILE"
    OP.execParser (optParser projectFileEnv) >>= runWithOptions
  where
    optParser projectFileEnv = OP.info (OP.helper <*> parseOptions projectFileEnv)
                (OP.fullDesc <> OP.progDesc "tman: interactive task processing \
                \tool")

parseOptions :: Maybe T.Text -> OP.Parser Options
parseOptions projectFileEnv = Options <$> parseProjectFile <*> parseSubCommand
  where
    parseProjectFile = case projectFileEnv of
        Just projectFile -> OP.option (fromText . T.pack <$> OP.str)
            (OP.short 'p' <> OP.metavar "<PROJECT FILE>" <>
            OP.value (fromText projectFile) <> OP.help "the project file. Can \
            \also be set via environment variable TMAN_PROJECT_FILE")
        Nothing -> OP.option (fromText . T.pack <$> OP.str)
            (OP.short 'p' <> OP.metavar "<PROJECT FILE>" <>
            OP.help "the project file. Can also be set via environment \
            \variable TMAN_PROJECT_FILE")
    parseSubCommand = OP.subparser $
        OP.command "submit" (parseSubmit `withInfo` "submit jobs") <>
        OP.command "list"   (parseList `withInfo` "list job info") <>
        OP.command "print"  (parsePrint `withInfo` "print commands") <>
        OP.command "status" (parseStatus `withInfo`
            "print status for each job") <>
        OP.command "clean"  (parseClean `withInfo`
            "remove job and log files") <>
        OP.command "log"    (parseLog `withInfo`
            "print log file for a task") <>
        OP.command "info"   (parseInfo `withInfo`
            "print run info for a task") <>
        OP.command "kill"   (parseKill `withInfo` "kill job") <>
        OP.command "init"   (parseInit `withInfo`
            "initialize a new empty project") <>
        OP.command "add"    (parseAdd `withInfo`
            "add a new task to the project") 

parseSubmit :: OP.Parser SubCommand
parseSubmit = CmdSubmit <$> parseSubmitOpt
  where
    parseSubmitOpt = SubmitOpt <$> parseJobSpec <*> parseForce <*> parseTest <*>
        parseSubmissionType <*> parseQueue <*> parseSubGroup <*> parseUnchecked
    parseForce = OP.switch $ OP.short 'f' <> OP.long "force" <>
                 OP.help "force submission of completed tasks"
    parseTest = OP.switch $ OP.short 't' <> OP.long "test" <>
        OP.help "only print submission commands, do not actually submit"
    parseSubmissionType = OP.strOption $ OP.short 's' <>
        OP.long "submissionType" <> OP.value "seq" <> OP.showDefault <>
        OP.help "type of submission [seq | lsf | slurm]"
    parseQueue = OP.strOption $ OP.short 'q' <> OP.long "submissionQueue" <>
        OP.value "" <> OP.showDefault <>
        OP.help "LSF submission Queue (only for lsf submissions)"
    parseSubGroup = OP.strOption $ OP.short 'g' <> OP.long "submissionGroup" <>
        OP.value "" <> OP.help "LSF submission Group (only for lsf submissions)"
    parseUnchecked = OP.switch $ OP.short 'u' <> OP.long "unchecked" <>
         OP.help ("do not check any status, just submit (this is even stronger \
         \than force and should be given with care)")

parseJobSpec :: OP.Parser JobSpec
parseJobSpec = parseGroupName <|> pure AllJobs
  where
    parseGroupName = OP.option (JobPattern <$> OP.str) $ OP.short 'j' <>
        OP.long "job" <> OP.metavar "<jobname_pattern>" <>
        OP.help "Job or Jobgroup name. Can contain * and ** to glob"

withInfo :: OP.Parser a -> String -> OP.ParserInfo a
withInfo opts desc = OP.info (OP.helper <*> opts) $ OP.progDesc desc

parseList :: OP.Parser SubCommand
parseList = CmdList <$> parseListOpt
  where
    parseListOpt = ListOpt <$> parseJobSpec <*> parseSummary <*> parseFull
    parseFull = OP.switch $ OP.short 'f' <> OP.long "full" <>
        OP.help "show full list"

parseSummary :: OP.Parser Int
parseSummary = OP.option OP.auto $ OP.short 's' <> OP.long "summaryLevel" <>
    OP.value 0 <> OP.showDefault <> OP.metavar "<Level>" <>
    OP.help "summarize status for groups at given level, leave 0 for now \
    \grouping"

parsePrint :: OP.Parser SubCommand
parsePrint = CmdPrint <$> parsePrintOpt
  where
    parsePrintOpt = PrintOpt <$> parseJobSpec

parseStatus :: OP.Parser SubCommand
parseStatus = CmdStatus <$> parseStatusOpt
  where
    parseStatusOpt = StatusOpt <$> parseJobSpec <*> parseSummary <*>
        parseWithRunInfo <*> parseSkipSuccessful <*> parseFull
    parseWithRunInfo = OP.switch $ OP.short 'i' <> OP.long "info" <>
        OP.help "show runInfo"
    parseSkipSuccessful = OP.switch $ OP.short 'S' <>
        OP.long "skipSuccessful" <>
        OP.help "skip complete tasks or tasks without a logfile, if -i and/or \
        \-l is used"
    parseFull = OP.switch $ OP.short 'f' <> OP.long "full" <>
        OP.help "full status output"

parseClean :: OP.Parser SubCommand
parseClean = CmdClean <$> parseCleanOpt
  where
    parseCleanOpt = CleanOpt <$> parseJobSpec <*> parseForce
    parseForce = OP.switch $ OP.short 'f' <> OP.long "force" <>
                             OP.help "clean even successfully run jobs"

parseLog :: OP.Parser SubCommand
parseLog = CmdLog <$> parseLogOpt
  where
    parseLogOpt = LogOpt <$> parseJobSpec

parseInfo :: OP.Parser SubCommand
parseInfo = CmdInfo <$> parseInfoOpt
  where
    parseInfoOpt = InfoOpt <$> parseJobSpec

parseKill :: OP.Parser SubCommand
parseKill = CmdKill <$> parseKillOpt
  where
    parseKillOpt = KillOpt <$> parseJobSpec <*> st
    st = OP.strOption $ OP.short 's' <> OP.long "submissionType" <>
        OP.metavar "<slurm|lsf>" <>
        OP.help "submission type, must be either slurm or lsf"

parseInit :: OP.Parser SubCommand
parseInit = CmdInit <$> parseInitOpt
  where
    parseInitOpt = InitOpt <$> parseProjectName <*> parseProjectPath
    parseProjectName = OP.option (T.pack <$> OP.str) $ OP.short 'n' <>
        OP.long "projectName" <> OP.value "" <> OP.metavar "<NAME>" <>
        OP.help "the name of the project"
    parseProjectPath = OP.option (fromText . T.pack <$> OP.str) $
        OP.short 'p' <> OP.long "path" <> OP.metavar "<PATH>" <>
        OP.help "the log- and job-script directory for the project"

parseAdd :: OP.Parser SubCommand
parseAdd = CmdAdd <$>  parseAddOpt
  where
    parseAddOpt = AddOpt <$> parseTaskSpec
    parseTaskSpec =
        (\n its ofs c m t h ifs -> TaskSpec n its ifs ofs c m t h) <$>
        parseName <*> parseInputTasks <*> parseOutputFiles <*> parseCmd <*>
        parseMem <*> parseThreads <*> parseHours <*> parseInputFiles 
    
    parseName = OP.option (T.pack <$> OP.str) $ OP.short 'n' <>
        OP.long "name" <> OP.metavar "<NAME>" <> OP.help "name of the task"
    parseCmd = OP.option (T.pack <$> OP.str) $ OP.short 'c' <>
        OP.long "command" <> OP.metavar "<CMD>" <>
        OP.help "full bash command line, wrapped in quotation marks"
    parseInputTasks = OP.many (OP.option (T.pack <$> OP.str) $ OP.short 'I' <>
                      OP.long "inputTask" <> OP.metavar "<Task>" <>
                      OP.help "input task name, can be given multiple times")
    parseInputFiles = OP.many (OP.argument (T.pack <$> OP.str) $
        OP.metavar "INPUTFILE1 INPUTFILE2 ..." <>
        OP.help "input file, can be given multiple times")
    parseOutputFiles = OP.many (OP.option (T.pack <$> OP.str) $
        OP.long "output" <> OP.short 'o' <> OP.metavar "<File>" <>
        OP.help "output file, can be given multiple times")
    parseMem = OP.option OP.auto $ OP.long "mem" <> OP.short 'm' <> 
        OP.metavar "<Mb>" <> OP.value 100 <> OP.showDefault <>
        OP.help "maximum memory in Mb"
    parseThreads = OP.option OP.auto $ OP.long "threads" <> OP.short 't' <>
                   OP.metavar "<nrThreads>" <> OP.value 1 <> OP.showDefault <>
                   OP.help "nr of threads"
    parseHours = OP.option OP.auto $ OP.long "hours" <> OP.short 'h' <>
        OP.metavar "<Hours>" <> OP.value 10 <> OP.showDefault <>
        OP.help "run time in hours"

runWithOptions :: Options -> IO ()
runWithOptions (Options fn subCommand) = runScript $ do
    case subCommand of
        CmdSubmit opts -> runSubmit fn opts
        CmdList   opts -> runList   fn opts
        CmdPrint  opts -> runPrint  fn opts 
        CmdStatus opts -> runStatus fn opts
        CmdClean  opts -> runClean  fn opts
        CmdLog    opts -> runLog    fn opts
        CmdInfo   opts -> runInfo   fn opts
        CmdKill   opts -> runKill   fn opts
        CmdInit   opts -> runInit   fn opts
        CmdAdd    opts -> runAdd    fn opts

runSubmit :: FilePath -> SubmitOpt -> Script ()
runSubmit fn (SubmitOpt jobSpec force test submissionType queue group
        unchecked) = do
    project <- loadProject fn
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
            scriptIO . err $ format
                ("Job "%fp%": already running? skipping. Use --clean to reset") 
                (_tName t)
        else
            case st of
                StatusIncompleteInputTask _ ->
                    scriptIO . err $ format
                        ("Job "%fp%": incomplete input task(s), skipping")
                        (_tName t)
                StatusMissingInputFile _ ->
                    scriptIO . err $ format
                        ("Job "%fp%": missing input file(s), skipping")
                        (_tName t)
                StatusComplete -> if force then
                        tSubmit projectDir test submissionSpec t
                    else
                        scriptIO . err $ format ("Job "%fp%
                            ": already complete, skipping (use --force to \
                            \submit anyway)") (_tName t)
                _ -> tSubmit projectDir test submissionSpec t

selectTasks :: Project -> JobSpec -> Either String [Task]
selectTasks jobProject jobSpec =
    case jobSpec of
        AllJobs -> return . M.elems . _prTasks $ jobProject
        JobPattern pat -> do
            let ret = filter ((~~ pat) . T.unpack . format fp . _tName) .
                    M.elems . _prTasks $ jobProject
            if null ret then Left "No Tasks found" else Right ret

runList :: FilePath -> ListOpt -> Script ()
runList fn (ListOpt jobSpec summaryLevel full) = do
    project <- loadProject fn
    tasks <- tryRight $ selectTasks project jobSpec
    if summaryLevel > 0 then do
        let groups = map (T.intercalate "/" . take summaryLevel .
                T.splitOn "/" . format fp .  _tName) tasks
            entries :: [(T.Text, Int)]
            entries = sortBy (\(e1, _) (e2, _) -> e1 `compare` e2) . M.toList .
                      foldl (\mm k -> M.insertWith (+) k 1 mm) M.empty $ groups
        scriptIO . mapM_ T.putStrLn $
            [format ("Group "%s%": "%d%" job(s)") g num | (g, num) <- entries]
    else
        if full then do
            let headers = ["NAME", "MEMORY", "THREADS", "HOURS", "INPUTTASKS",
                    "INPUTFILES", "OUTPUTFILES"]
            scriptIO . T.putStrLn . T.intercalate "\t" $ headers
            scriptIO  . mapM_ (T.putStrLn . tMeta True) $ tasks
        else do
            let headers = ["NAME", "MEMORY", "THREADS", "HOURS"]
            scriptIO . T.putStrLn . T.intercalate "\t" $ headers
            scriptIO . mapM_ (T.putStrLn . tMeta False) $ tasks
  where
    tMeta True (Task n it ifiles o _ m t h) =
        format (fp%"\t"%d%"\t"%d%"\t"%d%"\t"%w%"\t"%w%"\t"%w) n m t h
        (map _tName it) ifiles o
    tMeta False (Task n _ _ _ _ m t h) =
        format (fp%"\t"%d%"\t"%d%"\t"%d) n m t h

runPrint :: FilePath -> PrintOpt -> Script ()
runPrint fn (PrintOpt jobSpec) = do
    project <- loadProject fn
    tasks <- tryRight $ selectTasks project jobSpec    
    scriptIO . mapM_ (T.putStrLn . _tCommand) $ tasks

runStatus :: FilePath -> StatusOpt -> Script ()
runStatus fn (StatusOpt jobSpec summaryLevel withRunInfo skipSuccessful
        full) = do
    project <- loadProject fn
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
        let groups = map (T.intercalate "/" . take summaryLevel .
                T.splitOn "/" .  format fp . _tName) tasks
            dict :: M.Map (T.Text, T.Text) Int
            dict = foldl (\mm k -> M.insertWith (+) k 1 mm) M.empty $
                   zip groups (map showFullStatus fullStatusList)

            entries :: [(T.Text, [(T.Text, Int)])]
            entries = map (\subList -> (fst . fst . head $ subList,
                [(st, c) | ((_, st), c) <- subList])) . 
                groupBy (\((e1, _), _) ((e2, _), _) -> e1 == e2) .
                sortBy (\((e1, _), _) ((e2, _), _) -> e1 `compare` e2) .
                M.toList $ dict

        scriptIO . mapM_ T.putStrLn $
            [format ("Group "%s%": "%s) g $
             T.intercalate ", " [format (s%"("%w%")") st c | (st, c) <- l] |
             (g, l) <- entries]
    else do
        let ll = map (\(t, l) -> format ("Job "%fp%": "%s) (_tName t)
                (showFullStatus l)) $ filter pred_ $ zip tasks fullStatusList
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
    showSt (StatusIncomplete t) =
        if full then format ("incomplete output: "%s) t else "incomplete"
    showSt (StatusIncompleteInputTask t) =
        if full
        then format ("incomplete input task: "%s) t
        else "missingInputTask"
    showSt (StatusMissingInputFile t) =
        if full
        then format ("missing input file: "%s) t
        else "missingInputFile"
    showI (Just (InfoSuccess _)) = "+Success"
    showI (Just i) = format ("+"%w) i
    showI Nothing = ""

runClean :: FilePath -> CleanOpt -> Script ()
runClean fn (CleanOpt jobSpec force) = do
    project <- loadProject fn
    let logDir = _prLogDir project
    tasks <- tryRight $ selectTasks project jobSpec
    mapM_ (tClean logDir force) tasks

runLog :: FilePath -> LogOpt -> Script ()
runLog fn (LogOpt jobSpec) = do
    project <- loadProject fn
    let logDir = _prLogDir project
    tasks <- tryRight $ selectTasks project jobSpec
    mapM_ (tLog logDir) tasks

runInfo :: FilePath -> InfoOpt -> Script ()
runInfo fn (InfoOpt jobSpec) = do
    project <- loadProject fn
    let logDir = _prLogDir project
    tasks <- tryRight $ selectTasks project jobSpec
    scriptIO . putStrLn $
        "JOB\tSTATUS\tDURATION [(h:mm:ss or m:ss)]\tMAX_MEM [Mb]"
    info <- mapM (tRunInfo logDir) tasks
    forM_ (zip tasks info) $ \(task, i) ->
        case i of
            InfoSuccess (RunInfo duration max_) -> do
                scriptIO . T.putStrLn $ format
                    (fp%"\tSuccess\t"%s%"\t"%w) (_tName task) duration max_
            InfoFailed r -> scriptIO . T.putStrLn $
                format (fp%"\t"%w) (_tName task) r
            _ -> scriptIO . T.putStrLn $ format (fp%"\t"%w) (_tName task) i

runKill :: FilePath -> KillOpt -> Script ()
runKill fn (KillOpt jobSpec submissionType) = do
    project <- loadProject fn
    tasks <- tryRight $ selectTasks project jobSpec
    case submissionType of
        "slurm" -> mapM_ tSlurmKill tasks
        "lsf" -> mapM_ tLsfKill tasks
        _ -> throwE "unknown submission type"

runInit :: FilePath -> InitOpt -> Script ()
runInit fn (InitOpt name path) = do
    ignoreFlag <- need "TMAN_DO_NOT_UPDATE_PROJECT"
    case ignoreFlag of
        Just "True" -> return ()
        _ -> do
            let proj = Project name path M.empty []
            saveProject fn proj

runAdd :: FilePath -> AddOpt -> Script ()
runAdd fn (AddOpt taskSpec) = do
    ignoreFlag <- need "TMAN_DO_NOT_UPDATE_PROJECT"
    case ignoreFlag of
        Just "True" -> return ()
        _ -> do
            project <- loadProject fn
            project' <- addTask project True taskSpec
            saveProject fn project'

