{-# LANGUAGE OverloadedStrings #-}

module Tman.Internal.Project (Project(..), ProjectSpec(..), loadProject) where
import Tman.Internal.Task (TaskSpec(..), Task(..))

import Control.Applicative ((<|>))
import Control.Monad (mzero, foldM, when)
import Control.Error (Script, scriptIO, tryRight, justErr, atErr, throwE)
import Data.Aeson (Value(..), (.:), parseJSON, toJSON, FromJSON, ToJSON, (.=), object, eitherDecode)
import qualified Data.Attoparsec.Text as A
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Filesystem.Path.CurrentOS (encodeString)
import Prelude hiding (FilePath)
import Turtle (FilePath, fromText, format, s, fp, (%), testfile, (</>))

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
    _prTasks :: [Task]
}

data InterpolationString = TextChunk T.Text
                         | InputFileChunk Int
                         | InputTaskChunk Int Int
                         | OutputFileChunk Int
                         | EscapeChunk
                         deriving (Show)

loadProject :: Script Project
loadProject = do
    fn <- findProjectFile
    c <- scriptIO . B.readFile . T.unpack . format fp $ fn
    let eitherProjectSpec = eitherDecode c :: Either String ProjectSpec
    projectSpec <- tryRight eitherProjectSpec
    tryRight . makeProject $ projectSpec
  where
    findProjectFile = go "."
    go path = do
        fn <- testfile (path </> "tman.project")
        if fn then return path else
            if path == "/" then
                throwE "did not find tman.project file. Please run tman init to create one"
            else
                go ".."
        
makeProject :: ProjectSpec -> Either String Project
makeProject (ProjectSpec name logDir taskSpecs) = do
    taskMap <- foldM insert M.empty taskSpecs
    tasks <- mapM (tryToFindTask taskMap . _tsName) taskSpecs
    return $ Project name (fromText logDir) tasks
  where
    insert taskMap (TaskSpec n it ifiles ofiles c m t h) = do
        when (n `M.member` taskMap) $ Left ("duplicate task " ++ show n)
        inputTasks <- mapM (tryToFindTask taskMap) it
        let newTask =
                Task (fromText n) inputTasks (map fromText ifiles) (map fromText ofiles) c m t h
        cmd <- interpolateCommand newTask
        Right $ M.insert n newTask {_tCommand = cmd} taskMap
    tryToFindTask m tn = justErr ("unknown task " ++ show tn) $ M.lookup tn m

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
