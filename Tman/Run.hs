module Tman.Run (run) where

import Tman.Task (Task(..), tSubmit, tCheck, tPrint, tClean)
import Control.Error (runScript, Script, scriptIO)
import Control.Applicative ((<$>), (<*>))
import qualified Options.Applicative as OP
import Data.Monoid ((<>))
import Data.List (nub)
import qualified Data.Map as M
import Control.Monad.Trans.Either (hoistEither)

data Options = Options Command
data Command = CmdSubmit SubmitOpt | CmdCheck CheckOpt | CmdPrint PrintOpt | CmdClean CleanOpt

data SubmitOpt = SubmitOpt {
    _suGroupName :: String
}

data CheckOpt = CheckOpt {
    _chGroupName :: String
}

data PrintOpt = PrintOpt {
    _prGroupName :: String
}

data CleanOpt = CleanOpt {
    _clGroupName :: String
}

type JobProject = [(String, [Task])]

run :: JobProject -> IO ()
run jobProject =
    runWithOptions jobProject =<< OP.execParser (parseOptions `withInfo` "tman: A utility for running Data processing jobs")

runWithOptions :: JobProject -> Options -> IO ()
runWithOptions jobProject (Options cmdOpts) = runScript $
    case cmdOpts of
        CmdSubmit opts -> runSubmit jobProject opts
        CmdCheck opts -> runCheck jobProject opts
        CmdPrint opts -> runPrint jobProject opts
        CmdClean opts -> runClean jobProject opts

parseOptions :: OP.Parser Options
parseOptions = Options <$> parseCommand

parseCommand :: OP.Parser Command
parseCommand = OP.subparser $
    OP.command "submit" (parseSubmit `withInfo` "submit jobs") <>
    OP.command "check" (parseCheck `withInfo` "check job status") <>
    OP.command "print" (parsePrint `withInfo` "print commands") <>
    OP.command "clean" (parseClean `withInfo` "clean output and log files")

parseSubmit :: OP.Parser Command
parseSubmit = CmdSubmit <$> parseSubmitOpt
  where
    parseSubmitOpt = SubmitOpt <$> parseGroupName

parseGroupName :: OP.Parser String
parseGroupName = OP.option OP.str $ OP.short 'g' <> OP.long "jobGroup" <> OP.metavar "<group_desc>" <> OP.value ""
                                                 <> OP.help "Job group name"

parseCheck :: OP.Parser Command
parseCheck = CmdCheck <$> parseCheckOpt
  where
    parseCheckOpt = CheckOpt <$> parseGroupName

parsePrint :: OP.Parser Command
parsePrint = CmdPrint <$> parsePrintOpt
  where
    parsePrintOpt = PrintOpt <$> parseGroupName

parseClean :: OP.Parser Command
parseClean = CmdClean <$> parseCleanOpt
  where
    parseCleanOpt = CleanOpt <$> parseGroupName

withInfo :: OP.Parser a -> String -> OP.ParserInfo a
withInfo opts desc = OP.info (OP.helper <*> opts) $ OP.progDesc desc

runSubmit :: JobProject -> SubmitOpt -> Script ()
runSubmit jobProject (SubmitOpt groupName) = do
    tasks <- hoistEither $ findGroup jobProject groupName
    mapM_ tSubmit tasks

runCheck :: JobProject -> CheckOpt -> Script ()
runCheck jobProject (CheckOpt groupName) =
    if groupName == "" then do
        statusDicts <- mapM (getStatusDict . snd) jobProject
        let l = zipWith (\g s -> "Job Group " ++ fst g ++ ": " ++ show s) jobProject statusDicts
        scriptIO $ mapM_ putStrLn l
    else do
        tasks <- hoistEither $ findGroup jobProject groupName
        status <- scriptIO $ mapM tCheck tasks
        let l = zipWith (\t s -> "Job " ++ _tName t ++ ": " ++ show s) tasks status
        scriptIO $ mapM_ putStrLn l
  where
    getStatusDict tasks = do
        status <- scriptIO $ mapM tCheck tasks
        return $ foldl (\m s -> M.insertWith (+) s 1 m) M.empty status

runPrint :: JobProject -> PrintOpt -> Script ()
runPrint jobProject (PrintOpt groupName) = do
    tasks <- hoistEither $ findGroup jobProject groupName
    mapM_ tPrint tasks

findGroup :: JobProject -> String -> Either String [Task]
findGroup jobProject name = do
    let found = filter ((==name) . fst) jobProject
    if length found == 0 then
        Left $ "Job group not found"
    else
        if length found > 1 then
            Left $ "jobGroup names must be unique"
        else
            return . snd . head $ found

runClean :: JobProject -> CleanOpt -> Script ()
runClean jobProject (CleanOpt groupName) = do
    tasks <- hoistEither $ findGroup jobProject groupName
    mapM_ tClean tasks
    