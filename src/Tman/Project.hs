{-# LANGUAGE OverloadedStrings #-}

module Tman.Project (Project(..), ProjectSpec(..), loadProject, saveProject,
                              addTask, removeTask) where
import Tman.Task (TaskSpec(..), Task(..))

import Control.Applicative ((<|>))
import Control.Monad (mzero, foldM, forM_, when)
import Control.Error (Script, scriptIO, tryRight, atErr, throwE, tryJust, tryAssert)
import Data.Aeson (Value(..), (.:), parseJSON, toJSON, FromJSON, ToJSON, (.=), object,
                   eitherDecode, encode)
import qualified Data.Attoparsec.Text as A
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Prelude hiding (FilePath)
import System.IO (IOMode(..), withFile)
import Turtle

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
                         | InputFileChunk [RangeSpec]
                         | InputTaskChunk Int Int
                         | OutputFileChunk Int
                         | EscapeChunk
                         deriving (Show)

data RangeSpec = SingletonRange Int | FromToRange Int Int deriving (Show)

loadProject :: FilePath -> Script Project
loadProject fn = do
    c <- scriptIO . B.readFile . T.unpack . format fp $ fn
    let eitherProjectSpec = eitherDecode c :: Either String ProjectSpec
    projectSpec <- case eitherProjectSpec of
        Left e' -> throwE (T.pack e')
        Right spec -> return spec
    makeProject projectSpec

makeProject :: ProjectSpec -> Script Project
makeProject (ProjectSpec name logDir taskSpecs) = do
    let emptyProject = Project name (fromText logDir) M.empty []
    foldM readTask emptyProject taskSpecs
  where
    readTask project@(Project _ _ tasks taskOrder) (TaskSpec n it ifiles ofiles c m t h) = do
        inputTasks <- mapM (tryToFindTask tasks . fromText) it
        let newTask =
                Task (fromText n) inputTasks (map fromText ifiles) (map fromText ofiles) c m t h
            newTasks = M.insert (fromText n) newTask tasks
            newTaskOrder = taskOrder ++ [fromText n] 
        return $ project {_prTasks = newTasks, _prTaskOrder = newTaskOrder}

tryToFindTask :: M.Map FilePath Task -> FilePath -> Script Task
tryToFindTask m' tn = tryJust (format ("unknown task "%fp) tn) $ M.lookup tn m'

addTask :: Project -> Bool -> TaskSpec -> Script Project
addTask project@(Project _ _ tasks taskOrder) verbose
        (TaskSpec n it ifiles ofiles c m t h) = do
    inputTasks <- mapM (tryToFindTask tasks . fromText) it
    let newTask =
            Task (fromText n) inputTasks (map fromText ifiles) (map fromText ofiles) c m t h
    cmd <- tryRight . interpolateCommand $ newTask
    let newTasks = M.insert (fromText n) newTask {_tCommand = cmd} tasks
    newTaskOrder <- if (fromText n) `M.member` tasks && verbose then do
        scriptIO . err . unsafeTextToLine $ format ("updating task "%s) n
        return taskOrder
    else do
        let allOutFiles = concatMap _tOutputFiles tasks
        forM_ (map fromText ofiles) $ \ofile -> 
            tryAssert (format ("duplicated output file "%fp) ofile) $
                (not . flip elem allOutFiles) ofile
        when verbose $ 
            scriptIO . err . unsafeTextToLine $ format ("adding task "%s) n
        return $ taskOrder ++ [fromText n]
    return $ project {_prTasks = newTasks, _prTaskOrder = newTaskOrder}

removeTask :: Project -> FilePath -> Script Project
removeTask project@(Project _ _ tasks taskOrder) name =
    if name `M.member` tasks then do
        scriptIO . err . unsafeTextToLine $ format ("deleting task "%fp) name
        let newTasks = M.delete name tasks
        return $ project {_prTasks = newTasks, _prTaskOrder = filter (/=name) taskOrder}
    else do
        scriptIO . err . unsafeTextToLine $ format ("unknown task "%fp) name
        return project

saveProject :: FilePath -> Project -> Script ()
saveProject fn project = do
    let projectFileS = T.unpack . format fp $ fn
    scriptIO . withFile projectFileS WriteMode $ \h -> do
        B.hPutStrLn h . encode $ makeProjectSpec project
 
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

interpolateCommand :: Task -> Either T.Text T.Text
interpolateCommand (Task n it ifiles ofiles cmd _ _ _) = do
    let chunksParse = A.parseOnly (parser <* A.endOfInput) cmd
    chunks <- case chunksParse of
        Left _ -> Left $ format ("error in task "%fp%": problematic tags in command: "%s) n cmd
        Right a -> Right a
    textChunks <- mapM chunkToText chunks
    return $ T.concat textChunks
  where
    chunkToText c = case c of
        TextChunk t -> return t
        InputTaskChunk tIndex fIndex -> do
            let e1 = format ("Error in task "%fp%": input task index in command too high: "%s)
                            n cmd
            inputTask <- atErr e1 it (tIndex - 1)
            let e2 = format ("Error in task "%fp%": input task file index in command too high: "%s)
                            n cmd
            inputFile <- atErr e2 (_tOutputFiles inputTask) (fIndex - 1)
            return $ format fp inputFile
        InputFileChunk rangeSpecs -> do
            let fileIndices = concat $ do
                    rangeSpec <- rangeSpecs
                    case rangeSpec of
                        SingletonRange num -> return [num]
                        FromToRange from to -> return [from..to]
            let e' = format ("Error in task "%fp%": input file index in command too high: "%s)
                           n cmd
            inputFiles <- mapM (atErr e' ifiles . (\a -> a - 1)) fileIndices
            return . T.intercalate " " . map (format fp) $ inputFiles
        OutputFileChunk fIndex -> do
            let e' = format ("Error in task "%fp%": output file index in command too high: "%s)
                           n cmd
            outputFile <- atErr e' ofiles (fIndex - 1)
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
    rangeSpecs <- parseRangeSpec `A.sepBy1` (A.char ',')
    _ <- A.char '%'
    return $ InputFileChunk rangeSpecs

parseRangeSpec :: A.Parser RangeSpec
parseRangeSpec = parseFromToRange <|> parseSingletonRange
  where
    parseSingletonRange = SingletonRange <$> A.decimal
    parseFromToRange = FromToRange <$> A.decimal <* A.char '-' <*> A.decimal

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
