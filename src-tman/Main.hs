{-# LANGUAGE OverloadedStrings #-}
import Tman.Task (Task(..), TaskSpec(..), tSubmit, tStatus, tClean, tLog, TaskStatus(..))

import Tman.Project (Project(..), loadProject, saveProject, addTask)

import Control.Applicative ((<|>))
import Control.Error (runScript, Script, scriptIO, tryRight)
import Data.List (sortBy, groupBy)
import qualified Data.Map as M
import qualified Data.Text.IO as T
import Control.Monad (forM_)
import Data.Monoid ((<>))
import qualified Data.Text as T
import qualified Options.Applicative as OP
import Prelude hiding (FilePath)
import System.FilePath.GlobPattern ((~~))
import Turtle hiding (sortBy)

data Options = Options FilePath SubCommand

data SubCommand = CmdSubmit SubmitOpt
                | CmdList ListOpt
                | CmdPrint PrintOpt
                | CmdStatus StatusOpt
                | CmdClean CleanOpt
                | CmdLog LogOpt
                | CmdInit InitOpt
                | CmdAdd AddOpt

data SubmitOpt = SubmitOpt JobSpec Bool Bool String String Bool
-- jobSpec force test queue group unchecked

data JobSpec = JobPattern String | AllJobs

data ListOpt = ListOpt JobSpec Int Bool
-- jobSpec summary full

data PrintOpt = PrintOpt JobSpec

data StatusOpt = StatusOpt JobSpec Int Bool Bool
-- jobSpec summary skipSuccessful full

data CleanOpt = CleanOpt JobSpec

data LogOpt = LogOpt JobSpec

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
        OP.command "status" (parseStatus `withInfo` "print status for each job") <>
        OP.command "clean"  (parseClean `withInfo` "remove job and log files") <>
        OP.command "log"    (parseLog `withInfo` "print log file for a task") <>
        OP.command "init"   (parseInit `withInfo` "initialize a new empty project") <>
        OP.command "add"    (parseAdd `withInfo` "add a new task to the project") 

parseSubmit :: OP.Parser SubCommand
parseSubmit = CmdSubmit <$> parseSubmitOpt
  where
    parseSubmitOpt = SubmitOpt <$> parseJobSpec <*> parseForce <*> parseTest <*>
        parseQueue <*> parseSubGroup <*> parseUnchecked
    parseForce = OP.switch $ OP.short 'f' <> OP.long "force" <>
                 OP.help "force submission of completed tasks"
    parseTest = OP.switch $ OP.short 't' <> OP.long "test" <>
        OP.help "only print submission commands, do not actually submit"
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
    parseStatusOpt = StatusOpt <$> parseJobSpec <*> parseSummary <*> parseSkipSuccessful <*> 
        parseFull
    parseSkipSuccessful = OP.switch $ OP.short 'S' <>
        OP.long "skipSuccessful" <>
        OP.help "skip complete tasks or tasks without a logfile, if -i and/or \
        \-l is used"
    parseFull = OP.switch $ OP.short 'f' <> OP.long "full" <>
        OP.help "full status output"

parseClean :: OP.Parser SubCommand
parseClean = CmdClean <$> parseCleanOpt
  where
    parseCleanOpt = CleanOpt <$> parseJobSpec

parseLog :: OP.Parser SubCommand
parseLog = CmdLog <$> parseLogOpt
  where
    parseLogOpt = LogOpt <$> parseJobSpec

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
        CmdInit   opts -> runInit   fn opts
        CmdAdd    opts -> runAdd    fn opts

runSubmit :: FilePath -> SubmitOpt -> Script ()
runSubmit fn (SubmitOpt jobSpec force test _ _ unchecked) = do
    project <- loadProject fn
    let projectDir = _prLogDir project
    tasks <- tryRight $ selectTasks project jobSpec
    status <- if unchecked then
            return . repeat $ StatusIncomplete ""
        else
            mapM tStatus tasks
    forM_ (zip tasks status) $ \(t, st) ->
        case st of
            StatusIncompleteInputTask _ ->
                scriptIO . err . unsafeTextToLine $
                    format ("Job "%fp%": incomplete input task(s), skipping") (_tName t)
            StatusMissingInputFile _ ->
                scriptIO . err . unsafeTextToLine $
                    format ("Job "%fp%": missing input file(s), skipping") (_tName t)
            StatusComplete -> if force then
                    tSubmit projectDir test t
                else
                    scriptIO . err . unsafeTextToLine $
                        format ("Job "%fp%": already complete, skipping (use --force to \
                        \submit anyway)") (_tName t)
            _ -> tSubmit projectDir test t

selectTasks :: Project -> JobSpec -> Either T.Text [Task]
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
            [format ("Group "%s%": "%d%" job(s)") g' num | (g', num) <- entries]
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
    tMeta True (Task n it ifiles o' _ m t h) =
        format (fp%"\t"%d%"\t"%d%"\t"%d%"\t"%w%"\t"%w%"\t"%w) n m t h
        (map _tName it) ifiles o'
    tMeta False (Task n _ _ _ _ m t h) =
        format (fp%"\t"%d%"\t"%d%"\t"%d) n m t h

runPrint :: FilePath -> PrintOpt -> Script ()
runPrint fn (PrintOpt jobSpec) = do
    project <- loadProject fn
    tasks <- tryRight $ selectTasks project jobSpec    
    scriptIO . mapM_ (T.putStrLn . _tCommand) $ tasks

runStatus :: FilePath -> StatusOpt -> Script ()
runStatus fn (StatusOpt jobSpec summaryLevel skipSuccessful full) = do
    project <- loadProject fn
    tasks <- tryRight $ selectTasks project jobSpec
    status <- mapM tStatus tasks
    if summaryLevel > 0 then do
        let groups = map (T.intercalate "/" . take summaryLevel . T.splitOn "/" .  format fp . 
                _tName) tasks
            dict :: M.Map (T.Text, T.Text) Int
            dict = foldl (\mm k -> M.insertWith (+) k 1 mm) M.empty $ zip groups (map showSt status)

            entries :: [(T.Text, [(T.Text, Int)])]
            entries = map (\subList -> (fst . fst . head $ subList,
                    [(st, c) | ((_, st), c) <- subList])) . 
                groupBy (\((e1, _), _) ((e2, _), _) -> e1 == e2) .
                sortBy (\((e1, _), _) ((e2, _), _) -> e1 `compare` e2) .
                M.toList $ dict

        scriptIO . mapM_ T.putStrLn $
            [format ("Group "%s%": "%s) g' $
             T.intercalate ", " [format (s%"("%w%")") st c | (st, c) <- l'] |
             (g', l') <- entries]
    else do
        let ll = map (\(t, l') -> format ("Job "%fp%": "%s) (_tName t) (showSt l')) $
                filter pred_ $ zip tasks status
        scriptIO $ mapM_ T.putStrLn ll
  where
    pred_ = if skipSuccessful then
                (\(_, st) ->
                    case st of
                        StatusComplete -> False
                        _ -> True)
            else
                const True
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

runClean :: FilePath -> CleanOpt -> Script ()
runClean fn (CleanOpt jobSpec) = do
    project <- loadProject fn
    let logDir = _prLogDir project
    tasks <- tryRight $ selectTasks project jobSpec
    mapM_ (tClean logDir) tasks

runLog :: FilePath -> LogOpt -> Script ()
runLog fn (LogOpt jobSpec) = do
    project <- loadProject fn
    let logDir = _prLogDir project
    tasks <- tryRight $ selectTasks project jobSpec
    mapM_ (tLog logDir) tasks

runInit :: FilePath -> InitOpt -> Script ()
runInit fn (InitOpt name path) = do
    let proj = Project name path M.empty []
    saveProject fn proj

runAdd :: FilePath -> AddOpt -> Script ()
runAdd fn (AddOpt taskSpec) = do
    project <- loadProject fn
    project' <- addTask project True taskSpec
    saveProject fn project'

