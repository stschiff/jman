module Tman.Run (run) where

import Tman.Task (Task(..), tSubmit, tCheck, tInfo, tPrint, tClean, tMeta)
import Control.Error (runScript, Script, scriptIO)
import Control.Error.Safe (tryAssert)
import Control.Applicative ((<$>), (<*>))
import qualified Options.Applicative as OP
import Data.Monoid ((<>))
import Data.List (nub)
import qualified Data.Map as M
import Control.Monad.Trans.Either (hoistEither, left)
import Text.Format (format)

data Options = Options Command
data Command = CmdSubmit SubmitOpt | CmdList ListOpt | CmdClean CleanOpt

data SubmitOpt = SubmitOpt {
    _suGroupName :: String
}

data ListOpt = ListOpt {
    _liWhat :: String,
    _liGroupName :: String,
    _liSummary :: Bool
}

data CleanOpt = CleanOpt {
    _clGroupName :: String
}

type JobProject = [(String, [Task])]

run :: JobProject -> IO ()
run jobProject =
    runWithOptions jobProject =<< OP.execParser (parseOptions `withInfo` "tman: A utility for running Data processing jobs")

runWithOptions :: JobProject -> Options -> IO ()
runWithOptions jobProject (Options cmdOpts) = runScript $ do
    tryAssert "job names must be unique" $ checkUniqueJobNames jobProject
    tryAssert "job group names must be unique" $ checkUniqueJobGroupNames jobProject
    case cmdOpts of
        CmdSubmit opts -> runSubmit jobProject opts
        CmdList opts -> runList jobProject opts
        CmdClean opts -> runClean jobProject opts

checkUniqueJobNames :: JobProject -> Bool
checkUniqueJobNames jobProject =
    let allTasks = map _tName . concatMap snd $ jobProject
    in  (length $ nub allTasks) == length allTasks

checkUniqueJobGroupNames :: JobProject -> Bool
checkUniqueJobGroupNames jobProject =
    let allGroups = map fst jobProject
    in  (length $ nub allGroups) == length allGroups

runSubmit :: JobProject -> SubmitOpt -> Script ()
runSubmit jobProject (SubmitOpt groupName) = do
    tasks <- hoistEither $ findGroup jobProject groupName
    mapM_ tSubmit tasks

runList :: JobProject -> ListOpt -> Script ()
runList jobProject opt = do
    tasks <- if (_liGroupName opt) == "" then
            return $ concatMap snd jobProject
        else 
            hoistEither $ findGroup jobProject (_liGroupName opt)
    case (_liWhat opt) of
        "cmd" -> scriptIO (mapM_ putStrLn . map tPrint $ tasks)
        "status" -> do
            status <- mapM tCheck tasks
            info <- mapM tInfo tasks
            if (_liSummary opt) then do
                statusDicts <- mapM (getStatusDict . snd) jobProject
                infoDicts <- mapM (getInfoDict . snd) jobProject
                let l = zipWith3 (\g s i -> format "Job Group {0}: {1} {2}" [fst g, show s, show i]) jobProject statusDicts infoDicts
                scriptIO $ mapM_ putStrLn l
            else do
                let l = zipWith3 (\t s i -> format "Job {0}: {1}, {2}" [_tName t, show s, show i]) tasks status info
                scriptIO $ mapM_ putStrLn l
        "meta" -> do
            scriptIO (mapM_ putStrLn . map tMeta $ tasks)
        _ -> left "unknown list feature"
  where
    getStatusDict tasks = do
        status <- mapM tCheck tasks
        return $ foldl (\m s -> M.insertWith (+) s 1 m) M.empty status
    getInfoDict tasks = do
        info <- mapM tInfo tasks
        return $ foldl (\m s -> M.insertWith (+) s 1 m) M.empty info

findGroup :: JobProject -> String -> Either String [Task]
findGroup jobProject name = do
    let found = filter ((==name) . fst) jobProject
    if length found == 0 then
        Left $ "Job group not found"
    else
        return . snd . head $ found

runClean :: JobProject -> CleanOpt -> Script ()
runClean jobProject (CleanOpt groupName) = do
    tasks <- hoistEither $ findGroup jobProject groupName
    mapM_ tClean tasks

parseOptions :: OP.Parser Options
parseOptions = Options <$> parseCommand

parseCommand :: OP.Parser Command
parseCommand = OP.subparser $
    OP.command "submit" (parseSubmit `withInfo` "submit jobs") <>
    OP.command "list" (parseList `withInfo` "list job info") <>
    OP.command "clean" (parseClean `withInfo` "clean output and log files")

parseSubmit :: OP.Parser Command
parseSubmit = CmdSubmit <$> parseSubmitOpt
  where
    parseSubmitOpt = SubmitOpt <$> parseGroupName

parseGroupName :: OP.Parser String
parseGroupName = OP.option OP.str $ OP.short 'g' <> OP.long "jobGroup" <> OP.metavar "<group_desc>" <> OP.value ""
                                                 <> OP.help "Job group name"

withInfo :: OP.Parser a -> String -> OP.ParserInfo a
withInfo opts desc = OP.info (OP.helper <*> opts) $ OP.progDesc desc

parseList :: OP.Parser Command
parseList = CmdList <$> parseListOpt
  where
    parseListOpt = ListOpt <$> parseWhat <*> parseGroupName <*> parseSummary
    parseWhat = OP.argument OP.str $ OP.metavar "<what>" <> OP.help "What to show [cmd, status, meta]"
    parseSummary = OP.switch $ OP.short 's' <> OP.long "summary" <> OP.help "for ListCheck: show only summary"

parseClean :: OP.Parser Command
parseClean = CmdClean <$> parseCleanOpt
  where
    parseCleanOpt = CleanOpt <$> parseGroupName

    