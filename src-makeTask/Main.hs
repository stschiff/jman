import qualified Options.Applicative as OP
import Control.Error (runScript, scriptIO, tryRight)
import System.Tman.Internal.Task (Task(..), printTask)
import Control.Applicative (many)
import Data.Monoid ((<>))

data Options = Options {
    _optName :: String,
    _optCommand :: String,
    _optInputFiles :: [FilePath],
    _optOutputFiles :: [FilePath],
    _optMem :: Int,
    _optThreads :: Int,
    _optFarmQueue :: String,
    _optFarmGroup :: String
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
                  <*> many (OP.strOption (OP.long "input" <> OP.short 'i' <>
                            OP.metavar "<File>" <> OP.help "input file, can be given multiple times"))
                  <*> many (OP.strOption (OP.long "output" <> OP.short 'o' <>
                            OP.metavar "<File>" <> OP.help "output file, can be given multiple times"))
                  <*> OP.option OP.auto (OP.long "mem" <> OP.short 'm' <> OP.metavar "<Mb>" <>
                                         OP.value 100 <> OP.showDefault <>
                                         OP.help "maximum memory in Mb (only for LSF submission)")
                  <*> OP.option OP.auto (OP.long "threads" <> OP.short 't' <> OP.metavar "<nrThreads>" <>
                                         OP.value 1 <> OP.showDefault <>
                                         OP.help "nr of threads (only for LSF submission)")
                  <*> OP.strOption (OP.long "queue" <> OP.short 'q' <> OP.metavar "<Queue>" <>
                                    OP.value "normal" <> OP.showDefault <>
                                    OP.help "queue name (only for LSF submission)")
                  <*> OP.strOption (OP.long "group" <> OP.short 'g' <> OP.metavar "<Farm-Group>" <>
                                    OP.value "" <> OP.showDefault <>
                                    OP.help ("Farm group, can be left blank to use default group"))

runWithOptions :: Options -> IO ()
runWithOptions opts = runScript $ do
    task <- tryRight $ makeTask opts
    scriptIO . printTask $ task

makeTask :: Options -> Either String Task
makeTask (Options n c i o m t q g) =  return $ Task n i o c m t q g
