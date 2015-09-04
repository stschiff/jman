{-# LANGUAGE OverloadedStrings #-}

module System.Tman.Internal.Project (Project(..), loadProject, checkUniqueJobNames) where

import Data.Aeson (Value(..), (.:), parseJSON, toJSON, FromJSON, ToJSON, (.=), object, eitherDecode)
import System.Tman.Internal.Task (Task(..))
import Control.Monad (mzero)
import Control.Error (Script, scriptIO, tryRight)
import qualified Data.ByteString.Lazy.Char8 as B
import Data.List (nub)

data Project = Project {
    _prName :: String,
    _prLogDir :: FilePath,
    _prTasks :: [Task]
}

instance FromJSON Project where
    parseJSON (Object v) = Project <$> v .: "name" <*> v .: "logDir" <*> v .: "tasks"
    parseJSON _          = mzero

instance ToJSON Project where
    toJSON (Project name logDir tasks) =
        object ["name" .= name, "logDir" .= logDir, "tasks" .= tasks]

loadProject :: FilePath -> Script Project
loadProject projectFileName = do
    c <- scriptIO $ B.readFile projectFileName
    let eitherProject = eitherDecode c :: Either String Project
    tryRight eitherProject

checkUniqueJobNames :: Project -> Bool
checkUniqueJobNames project =
    let names = map _tName $ _prTasks project
    in  (length $ nub names) == length names
