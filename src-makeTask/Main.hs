import qualified Options.Applicative as OP
import Control.Error (runScript, scriptIO, tryRight)
import Tman.Internal.Task (TaskSpec(..), printTask)
import Control.Applicative (many)
import Data.Monoid ((<>))
import qualified Data.Text as T

data Options = Options {
    _optName :: String,
    _optCommand :: String,
    _optInputTasks :: [FilePath],
    _optInputFiles :: [FilePath],
    _optOutputFiles :: [FilePath],
    _optMem :: Int,
    _optThreads :: Int,
    _optHours :: Int
}

main :: IO ()
main = OP.execParser optParser >>= runWithOptions
  where
    optParser = OP.info (OP.helper <*> options)
                (OP.fullDesc <> OP.progDesc "A utility to generate a JSON encoded task for the tman program")

options :: OP.Parser Options
options = Options <$> OP.strArgument (OP.metavar "<NAME>" <>
                                      OP.help "full hierarchical name of the command")
                  <*> OP.strArgument (OP.metavar "<CMD>" <> OP.help "full bash command line")
                  <*> many (OP.strOption (OP.short 'I' <> OP.long "inputTask" <>
                            OP.metavar "<Task>" <> OP.help "input task name, can be given multiple times"))
                  <*> many (OP.strOption (OP.long "inputFile" <> OP.short 'i' <>
                            OP.metavar "<File>" <> OP.help "input file, can be given multiple times"))
                  <*> many (OP.strOption (OP.long "output" <> OP.short 'o' <>
                            OP.metavar "<File>" <> OP.help "output file, can be given multiple times"))
                  <*> OP.option OP.auto (OP.long "mem" <> OP.short 'm' <> OP.metavar "<Mb>" <>
                                         OP.value 100 <> OP.showDefault <>
                                         OP.help "maximum memory in Mb")
                  <*> OP.option OP.auto (OP.long "threads" <> OP.short 't' <> OP.metavar "<nrThreads>" <>
                                         OP.value 1 <> OP.showDefault <>
                                         OP.help "nr of threads")
                  <*> OP.option OP.auto (OP.long "hours" <> OP.short 'h' <> OP.metavar "<Hours>" <>
                                    OP.value 10 <> OP.showDefault <>
                                    OP.help "run time in hours")

runWithOptions :: Options -> IO ()
runWithOptions opts = runScript $ do
    task <- tryRight $ makeTask opts
    scriptIO . printTask $ task

makeTask :: Options -> Either String TaskSpec
makeTask (Options n c it ifiles ofiles m t h) =
    let name = T.pack n
        inputTasks = map T.pack it
        inputFiles = map T.pack ifiles
        outputFiles = map T.pack ofiles
        command = T.pack c  
    in Right $ TaskSpec name inputTasks inputFiles outputFiles command m t h
