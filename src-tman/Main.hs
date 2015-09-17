{-# LANGUAGE OverloadedStrings #-}
import Tman.Internal.Task (Task(..), tSubmit, tRunInfo, tStatus, tClean, tLog, SubmissionSpec(..), TaskStatus(..), TaskRunInfo(..))
import Tman.Internal.Project (Project(..), loadProject)
import Control.Error (runScript, Script, scriptIO, tryRight, throwE)
import Turtle.Prelude (err)
import Filesystem.Path.CurrentOS (encodeString)
import Turtle (FilePath, fromText)
import Prelude hiding (FilePath)
import Turtle.Format (format, s, d, fp, (%), w)
import qualified Options.Applicative as OP
import Data.Monoid ((<>))
import qualified Data.Map as M
import Data.List (sortBy, groupBy)
import Control.Monad (forM_)
import System.FilePath.GlobPattern ((~~))
import qualified Data.Text as T
import qualified Data.Text.IO as T

data Options = Options FilePath Command
data Command = CmdSubmit SubmitOpt
             | CmdList ListOpt
             | CmdPrint PrintOpt
             | CmdStatus StatusOpt
             | CmdClean CleanOpt
             | CmdLog LogOpt

data SubmitOpt = SubmitOpt {
    _suGroupName :: String,
    _suForce :: Bool,
    _suTest :: Bool,
    _suSubmissionType :: String,
    _suQueue :: String,
    _suGroup :: String,
    _suChunkSize :: Int,
    _suUnchecked :: Bool
}

data ListOpt = ListOpt {
    _liGroupName :: String,
    _liSummary :: Int,
    _liFull :: Bool
}

data PrintOpt = PrintOpt {
    _prGroupName :: String
}

data StatusOpt = StatusOpt {
    _stGroupName :: String,
    _stSummary :: Int,
    _stInfo :: Bool,
    _stSkipSuccessful :: Bool,
    _stVerbose :: Bool
}

data CleanOpt = CleanOpt {
    _clGroupName :: String
}

data LogOpt = LogOpt {
    _loGroupName :: String
}

main :: IO ()
main = OP.execParser optParser >>= runWithOptions
  where
    optParser = OP.info (OP.helper <*> options) (OP.fullDesc <> OP.progDesc "task processing tool")

runWithOptions :: Options -> IO ()
runWithOptions (Options projectFileName cmdOpts) = runScript $ do
    scriptIO . err $ format ("loading project file "%fp) projectFileName
    jobProject <- loadProject projectFileName
    case cmdOpts of
        CmdSubmit opts -> runSubmit jobProject opts
        CmdList opts -> runList jobProject opts
        CmdPrint opts -> runPrint jobProject opts
        CmdStatus opts -> runStatus jobProject opts
        CmdClean opts -> runClean jobProject opts
        CmdLog opts -> runLog jobProject opts

runSubmit :: Project -> SubmitOpt -> Script ()
runSubmit jobProject
          (SubmitOpt groupName force test submissionType queue group chunkSize unchecked) = do
    tasks <- tryRight $ selectTasks groupName jobProject
    let projectDir = _prLogDir jobProject
    status <- if unchecked then
            return . repeat $ StatusIncomplete ""
        else
            mapM (tStatus False) tasks
    info <- if unchecked then
            return $ repeat InfoNoLogFile
        else
            mapM (tRunInfo projectDir False) tasks
    submissionSpec <- case submissionType of
        "lsf" -> return $ LSFsubmission (T.pack queue) (T.pack group)
        "seq" -> return SequentialExecutionSubmission
        "par" -> return $ GnuParallelSubmission chunkSize
        _ -> throwE "unknown submission type"
    forM_ (zip3 tasks status info) $ \(t, st, i) ->
        if i == InfoNotFinished then
            throwE . T.unpack $ format ("Job "%fp%": already running? skipping. Use --clean to "%
                                        "reset") (_tName t)
        else
            case st of
                StatusIncompleteInputTask msg ->
                    throwE' $ format ("Job "%fp%": incomplete input task(s): "%s) (_tName t) msg
                StatusMissingInputFile msg ->
                    throwE' $ format ("Job "%fp%": missing input file(s): "%s) (_tName t) msg
                StatusOutdated -> throwE' $ format ("Job "%fp%": outdated input, skipping") (_tName t)
                StatusComplete -> if force then
                        tSubmit projectDir test submissionSpec t
                    else
                        throwE' $ format ("Job "%fp%": already complete, skipping (use --force to "%
                                          "submit anyway)") (_tName t)
                _ -> tSubmit projectDir test submissionSpec t

throwE' :: T.Text -> Script ()
throwE' = throwE . T.unpack

runList :: Project -> ListOpt -> Script ()
runList jobProject opts = do
    tasks <- tryRight $ selectTasks (_liGroupName opts) jobProject
    let summaryLevel = _liSummary opts
    if summaryLevel > 0 then do
        let groups = map (T.intercalate "/" . take summaryLevel . T.splitOn "/" . format fp . _tName) tasks
            entries = sortBy (\(e1, _) (e2, _) -> e1 `compare` e2) . M.toList .
                      foldl (\mm k -> M.insertWith (+) k 1 mm) M.empty $ groups
        scriptIO . mapM_ T.putStrLn $ [format ("Group "%s%": "%d%" job(s)") g num | (g, num) <- entries]
    else do
        let indices = if (_liFull opts) then [0..6] else [0..4]
            headers = ["NAME", "MEMORY", "THREADS", "SUBMISSION-QUEUE", "SUBMISSION-GROUP", 
                                                  "INPUTFILES", "OUTPUTFILES"]
        scriptIO . T.putStrLn . T.intercalate "\t" . map (headers!!) $ indices
        scriptIO . mapM_ T.putStrLn . map (tMeta indices) $ tasks
  where
    tMeta _ (Task n it ifiles o _ m t h) =
        format (fp%"\t"%w%"\t"%w%"\t"%w%"\t"%d%"\t"%d%"\t"%d) n it ifiles o m t h
        
runPrint :: Project -> PrintOpt -> Script ()
runPrint jobProject opts = do
    tasks <- tryRight $ selectTasks (_prGroupName opts) jobProject
    scriptIO (mapM_ T.putStrLn . map _tCommand $ tasks)

runStatus :: Project -> StatusOpt -> Script ()
runStatus jobProject opts = do
    tasks <- tryRight $ selectTasks (_stGroupName opts) jobProject
    let verbose = _stVerbose opts
    fullStatusList <- do 
        status <- mapM (tStatus verbose) tasks
        info <- if _stInfo opts then
                    mapM (fmap Just . tRunInfo (_prLogDir jobProject) verbose) tasks
                else
                    return [Nothing | _ <- tasks]
        return $ zip status info
    let summaryLevel = _stSummary opts
    if summaryLevel > 0 then do
        let groups = map (T.intercalate "/" . take summaryLevel . T.splitOn "/" . format fp . _tName) 
                         tasks

            dict :: M.Map (T.Text, (TaskStatus, Maybe TaskRunInfo)) Int
            dict = foldl (\mm k -> M.insertWith (+) k 1 mm) M.empty $ zip groups fullStatusList

            entries :: [(T.Text, [((TaskStatus, Maybe TaskRunInfo), Int)])]
            entries = map (\subList ->
                           (fst . fst . head $ subList, [(st, c) | ((_, st), c) <- subList])) .
                          groupBy (\((e1, _), _) ((e2, _), _) -> e1 == e2) .
                          sortBy (\((e1, _), _) ((e2, _), _) -> e1 `compare` e2) . M.toList $ dict

        scriptIO . mapM_ T.putStrLn $ 
            [format ("Group "%s%": "%s) g $
             T.intercalate ", " [format (s%"("%w%")") (showFullStatus st) c | (st, c) <- l] |
             (g, l) <- entries]
    else do
        let ll = map (\(t, l) -> format ("Job "%fp%": "%s) (_tName t) (showFullStatus l)) $
                 filter pred_ $ zip tasks fullStatusList
        scriptIO $ mapM_ T.putStrLn ll
  where
    pred_ = if _stSkipSuccessful opts then
                (\(_, (st, i)) ->
                    case (st, i) of
                        (StatusComplete, Nothing) -> False
                        (StatusComplete, Just (InfoSuccess _)) -> False
                        (StatusComplete, Just (InfoNoLogFile)) -> False
                        _ -> True)
            else
                const True
    showFullStatus (st, i) = format (w%s) st (show' i)
    show' (Just i) = format ("+"%w) i
    show' Nothing = ""

selectTasks :: String -> Project -> Either String [Task]
selectTasks group jobProject =
    let ret = if null group then
            (_prTasks jobProject)
        else
            -- filter (startswith groupParts . splitOn "/" . _tName) $ _prTasks jobProject
            filter ((~~ group) . encodeString . _tName) . _prTasks $ jobProject
     in  if null ret then Left "No Tasks found" else Right ret
  -- where
  --   groupParts = splitOn "/" group

runClean :: Project -> CleanOpt -> Script ()
runClean jobProject (CleanOpt groupName) = do
    tasks <- tryRight $ selectTasks groupName jobProject
    infos <- mapM (tRunInfo (_prLogDir jobProject) False) tasks
    forM_ (zip tasks infos) $ \(task, info) -> do
        if info == InfoNotFinished then
            tClean (_prLogDir jobProject) task
        else
            scriptIO . err $ format ("skipping task "%fp) (_tName task)

runLog :: Project -> LogOpt -> Script ()
runLog jobProject (LogOpt groupName) = do
    tasks <- tryRight $ selectTasks groupName jobProject
    mapM_ (tLog $ _prLogDir jobProject) tasks

options :: OP.Parser Options
options = Options <$> parseProjectFileName <*> parseCommand
  where
    parseProjectFileName = OP.option readFP (OP.short 'p' <> OP.long "projectFile" <>
                                             OP.value "tman.project" <>
                                             OP.showDefault <> OP.metavar "<Project_file>" <>
                                             OP.help "Project file to work with")
    readFP = OP.str >>= return . fromText . T.pack

parseCommand :: OP.Parser Command
parseCommand = OP.subparser $
    OP.command "submit" (parseSubmit `withInfo` "submit jobs") <>
    OP.command "list" (parseList `withInfo` "list job info") <>
    OP.command "print" (parsePrint `withInfo` "print commands") <>
    OP.command "status" (parseStatus `withInfo` "print status for each job") <>
    OP.command "clean" (parseClean `withInfo` "clean output and log files") <>
    OP.command "log" (parseLog `withInfo` "print log file for a task")

parseSubmit :: OP.Parser Command
parseSubmit = CmdSubmit <$> parseSubmitOpt
  where
    parseSubmitOpt = SubmitOpt <$> parseGroupName <*> parseForce <*> parseTest <*> 
                                   parseSubmissionType <*> parseQueue <*> parseSubGroup <*> 
                                   parseChunkSize <*> parseUnchecked
    parseForce = OP.switch $ OP.short 'f' <> OP.long "force" <>
                 OP.help "force submission of completed tasks"
    parseTest = OP.switch $ OP.short 't' <> OP.long "test" <>
                            OP.help "only print submission commands, do not actually submit"
    parseSubmissionType = OP.strOption $ OP.short 's' <> OP.long "submissionType" <>
                          OP.value "standard" <> OP.showDefault <>
                          OP.help "type of submission [standard | lsf]"
    parseQueue = OP.strOption $ OP.short 'q' <> OP.long "submissionQueue" <> OP.value "normal" <>
                                OP.showDefault <> 
                                OP.help "LSF submission Queue (only for lsf submissions)"
    parseSubGroup = OP.strOption $ OP.short 'g' <> OP.long "submissionGroup" <>
                    OP.help "LSF submission Group (only for lsf submissions)"
    parseChunkSize = OP.option OP.auto $ OP.short 'c' <> OP.long "chunkSize" <>
                     OP.help "Chunk Size (only for Gnu Parallel submissions)"
    parseUnchecked = OP.switch $ OP.short 'u' <> OP.long "unchecked" <>
                     OP.help ("do not check any status, just submit (this is even stronger than " ++
                              "force and should be given with care)")

parseGroupName :: OP.Parser String
parseGroupName = OP.strArgument $ OP.metavar "<group_desc>" <> OP.help "Job group name" <> OP.value "" <> OP.showDefault

withInfo :: OP.Parser a -> String -> OP.ParserInfo a
withInfo opts desc = OP.info (OP.helper <*> opts) $ OP.progDesc desc

parseList :: OP.Parser Command
parseList = CmdList <$> parseListOpt
  where
    parseListOpt = ListOpt <$> parseGroupName <*> parseSummary <*> parseFull
    parseFull = OP.switch $ OP.short 'f' <> OP.long "full" <> OP.help "show full list"

parseSummary :: OP.Parser Int
parseSummary = OP.option OP.auto $ OP.short 's' <> OP.long "summaryLevel" <> OP.value 0 <> OP.showDefault <> 
                                     OP.metavar "<Level>" <>
                                     OP.help "summarize status for groups at given level, leave 0 for now grouping"

parsePrint :: OP.Parser Command
parsePrint = CmdPrint <$> parsePrintOpt
  where
    parsePrintOpt = PrintOpt <$> parseGroupName

parseStatus :: OP.Parser Command
parseStatus = CmdStatus <$> parseStatusOpt
  where
    parseStatusOpt = StatusOpt <$> parseGroupName <*> parseSummary <*> parseInfo <*> 
                     parseSkipSuccessful <*> parseVerbose
    parseInfo = OP.switch $ OP.short 'i' <> OP.long "info" <> OP.help "show runInfo"
    parseSkipSuccessful = OP.switch $ OP.short 'S' <> OP.long "skipSuccessful" <> OP.help "skip complete tasks or tasks without a logfile, if -i and/or -l is used"
    parseVerbose = OP.switch $ OP.short 'v' <> OP.long "verbose" <> OP.help "verbose output"

parseClean :: OP.Parser Command
parseClean = CmdClean <$> parseCleanOpt
  where
    parseCleanOpt = CleanOpt <$> parseGroupName

parseLog :: OP.Parser Command
parseLog = CmdLog <$> parseLogOpt
  where
    parseLogOpt = LogOpt <$> parseGroupName
