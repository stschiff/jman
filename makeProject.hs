import qualified Data.ByteString.Lazy.Char8 as B
import Options.Applicative as OP
import Control.Error.Script (runScript, scriptIO)
import Control.Monad.Trans.Either (hoistEither)
import Task (Task(..))
import Project (Project(..))
import Data.Aeson (encode, eitherDecode)

data Options = Options {
    _optName :: String,
    _optLogDir :: FilePath
}

main :: IO ()
main = execParser optParser >>= runWithOptions
  where
    optParser = OP.info (OP.helper <*> options)
                (OP.fullDesc <> OP.progDesc ("utility to combine tasks generated by makeTask, " ++
                                             "reading task line by line from stdin"))

options :: OP.Parser Options
options = Options <$> OP.strArgument (OP.metavar "<PROJECT_NAME>" <> OP.help "Name of the project")
                  <*> OP.strArgument (OP.metavar "<PATH for log files>" <> OP.help "Path to write logging files to")

runWithOptions :: Options -> IO ()
runWithOptions (Options name logDir) = runScript $ do
    l <- scriptIO $ B.lines <$> B.getContents
    let tasks' = map eitherDecode l :: [Either String Task]
    tasks <- hoistEither $ sequence tasks'
    let proj = Project name logDir tasks
    scriptIO . B.putStrLn . encode $ proj
