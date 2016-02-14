{-# LANGUAGE OverloadedStrings #-}

module Tman.Internal.Project (Project(..), ProjectSpec(..), loadProject, saveProject,
                              addTask, removeTask) where
import Tman.Internal.Task (TaskSpec(..), Task(..))

import Control.Applicative ((<|>))
import Control.Monad (mzero, foldM)
import Control.Error (Script, scriptIO, tryRight, atErr, throwE, exceptT, tryJust)
import Data.Aeson (Value(..), (.:), parseJSON, toJSON, FromJSON, ToJSON, (.=), object,
                   eitherDecode, encode)
import qualified Data.Attoparsec.Text as A
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Prelude hiding (FilePath)
import System.IO (openFile, IOMode(..))
import Turtle (FilePath, fromText, format, s, fp, (%), testfile, testdir, (</>), err)

data ProjectSpec = ProjectSpec {
    _prsName :: T.Text,
    _prsLogDir :: T.Text,
    _prsTasks :: [TaskSpec]
}

instance FromJSON ProjectSpec where
    parseJSON (Object v) = ProjectSpec <$> v .: "name" <*> v .: "logDir" <*> v .: "tasks"
    parseJSON _          = mzero

instance ToJSON ProjectSpec where
    toJSON (ProjectSpec name logDir tasks) =
        object ["name" .= name, "logDir" .= logDir, "tasks" .= tasks]

data Project = Project {
    _prName :: T.Text,
    _prLogDir :: FilePath,
    _prTasks :: M.Map FilePath Task,
    _prTaskOrder :: [FilePath]
}

data InterpolationString = TextChunk T.Text
                         | InputFileChunk Int
                         | InputTaskChunk Int Int
                         | OutputFileChunk Int
                         | EscapeChunk
                         deriving (Show)

loadProject :: Script Project
loadProject = do
    fn <- findProjectFile "."
    c <- scriptIO . B.readFile . T.unpack . format fp $ fn
    let eitherProjectSpec = eitherDecode c :: Either String ProjectSpec
    projectSpec <- tryRight eitherProjectSpec
    makeProject projectSpec

findProjectFile :: FilePath -> Script FilePath
findProjectFile path = do
    td <- testdir path
    if td then do
        let fn = path </> "tman.project"
        tf <- testfile fn
        if tf then return fn else findProjectFile (path </> "..")
    else
        throwE "did not find tman.project file. Please run tman init to create one"
        
        
makeProject :: ProjectSpec -> Script Project
makeProject (ProjectSpec name logDir taskSpecs) = do
    let emptyProject = Project name (fromText logDir) M.empty []
    foldM addTask emptyProject taskSpecs

addTask :: Project -> TaskSpec -> Script Project
addTask project@(Project _ _ tasks taskOrder) (TaskSpec n it ifiles ofiles c m t h) = do
    -- if (fromText n) `M.member` tasks then
    --     scriptIO . err $ format ("updating task "%s) n
    -- else
    --     scriptIO . err $ format ("adding task "%s) n
    inputTasks <- mapM (tryToFindTask tasks . fromText) it
    let newTask =
            Task (fromText n) inputTasks (map fromText ifiles) (map fromText ofiles) c m t h
    cmd <- tryRight . interpolateCommand $ newTask
    let newTasks = M.insert (fromText n) newTask {_tCommand = cmd} tasks
    return $ project {_prTasks = newTasks, _prTaskOrder = taskOrder ++ [fromText n]}
  where
    tryToFindTask m' tn = tryJust ("unknown task " ++ show tn) $ M.lookup tn m'

removeTask :: Project -> FilePath -> Script Project
removeTask project@(Project _ _ tasks taskOrder) name =
    if name `M.member` tasks then do
        scriptIO . err $ format ("deleting task "%fp) name
        let newTasks = M.delete name tasks
        return $ project {_prTasks = newTasks, _prTaskOrder = filter (/=name) taskOrder}
    else do
        scriptIO . err $ format ("unknown task "%fp) name
        return project

saveProject :: Project -> Script ()
saveProject project = do
    projectFile <- scriptIO . exceptT (const (return "tman.project")) return $ findProjectFile "."
    h <- scriptIO . flip openFile WriteMode . T.unpack . format fp $ projectFile 
    let projectSpec = makeProjectSpec project
    scriptIO . B.hPutStrLn h . encode $ projectSpec
 
makeProjectSpec :: Project -> ProjectSpec
makeProjectSpec (Project name logDir tasks taskOrder) =
    ProjectSpec name (format fp logDir) taskSpecs
  where
    taskSpecs = do
        Task n it ifiles ofiles cmd mem threads hours <- map ((M.!) tasks) taskOrder
        let itText = map (format fp . _tName) it
            ifilesText = map (format fp) ifiles
            ofilesText = map (format fp) ofiles
            nText = format fp n
        return $ TaskSpec nText itText ifilesText ofilesText cmd mem threads hours

interpolateCommand :: Task -> Either String T.Text
interpolateCommand (Task n it ifiles ofiles cmd _ _ _) = do
    let chunksParse = A.parseOnly (parser <* A.endOfInput) cmd
    chunks <- case chunksParse of
        Left _ -> Left $ T.unpack (format ("error in task "%fp%": problematic tags in command: "%s) n cmd)
        Right a -> Right a
    textChunks <- mapM chunkToText chunks
    return $ T.concat textChunks
  where
    chunkToText c = case c of
        TextChunk t -> return t
        InputTaskChunk tIndex fIndex -> do
            let e1 = format ("Error in task "%fp%": input task index in command too high: "%s)
                            n cmd
            inputTask <- atErr (T.unpack e1) it tIndex
            let e2 = format ("Error in task "%fp%": input task file index in command too high: "%s)
                            n cmd
            inputFile <- atErr (T.unpack e2) (_tOutputFiles inputTask) fIndex
            return $ format fp inputFile
        InputFileChunk fIndex -> do
            let e = format ("Error in task "%fp%": input file index in command too high: "%s)
                           n cmd
            inputFile <- atErr (T.unpack e) ifiles fIndex
            return $ format fp inputFile
        OutputFileChunk fIndex -> do
            let e = format ("Error in task "%fp%": output file index in command too high: "%s)
                           n cmd
            outputFile <- atErr (T.unpack e) ofiles fIndex
            return $ format fp outputFile
        EscapeChunk -> return "%"

parser :: A.Parser [InterpolationString]
parser = A.many1 parseChunk

parseChunk :: A.Parser InterpolationString
parseChunk = parseInputFile <|> parseInputTask <|> parseOutputFile <|> parseText <|> parseEscape

parseInputFile :: A.Parser InterpolationString
parseInputFile = do
    _ <- A.char '%'
    _ <- A.char 'i'
    num <- A.decimal
    _ <- A.char '%'
    return $ InputFileChunk num

parseInputTask :: A.Parser InterpolationString
parseInputTask = do
    _ <- A.char '%'
    _ <- A.char 't'
    num1 <- A.decimal
    _ <- A.char '.'
    num2 <- A.decimal
    _ <- A.char '%'
    return $ InputTaskChunk num1 num2

parseOutputFile :: A.Parser InterpolationString
parseOutputFile = do
    _ <- A.char '%'
    _ <- A.char 'o'
    num <- A.decimal
    _ <- A.char '%'
    return $ OutputFileChunk num

parseText :: A.Parser InterpolationString
parseText = TextChunk <$> A.takeWhile1 (/='%')

parseEscape :: A.Parser InterpolationString
parseEscape = do
    _ <- A.string "%%"
    return EscapeChunk
