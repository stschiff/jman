{-# LANGUAGE OverloadedStrings #-}

module Tman.Internal.Project (Project(..), ProjectSpec(..), loadProject) where

import Data.Aeson (Value(..), (.:), parseJSON, toJSON, FromJSON, ToJSON, (.=), object, eitherDecode)
import Tman.Internal.Task (TaskSpec(..), Task(..))
import Control.Monad (mzero, foldM, when)
import Control.Error (Script, scriptIO, tryRight, justErr)
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Text as T
import Turtle (FilePath, fromText)
import Prelude hiding (FilePath)
import qualified Data.Map.Strict as M
import Filesystem.Path.CurrentOS (encodeString)

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

loadProject :: FilePath -> Script Project
loadProject projectFileName = do
    c <- scriptIO $ B.readFile (encodeString projectFileName)
    let eitherProjectSpec = eitherDecode c :: Either String ProjectSpec
    projectSpec <- tryRight eitherProjectSpec
    tryRight . makeProject $ projectSpec

makeProject :: ProjectSpec -> Either String Project
makeProject (ProjectSpec name logDir taskSpecs) = do
    taskMap <- foldM insert M.empty taskSpecs
    tasks <- mapM (tryToFindTask taskMap . _tsName) taskSpecs
    return $ Project name (fromText logDir) tasks
  where
    insert taskMap (TaskSpec n it ifiles ofiles c m t h) = do
        when (n `M.member` taskMap) $ Left ("duplicate task " ++ show n)
        inputTasks <- mapM (tryToFindTask taskMap) it
        let newTask = Task (fromText n) inputTasks (map fromText ifiles) (map fromText ofiles) c m t h
        Right $ M.insert n newTask taskMap
    tryToFindTask m tn = justErr ("unknown task " ++ show tn) $ M.lookup tn m
        
