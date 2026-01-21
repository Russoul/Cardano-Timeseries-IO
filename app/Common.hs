{-# OPTIONS_GHC -Wno-deprecations #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{- HLINT ignore "Use print" -}

module Common where

import           Cardano.Timeseries.Import.PlainCBOR
import           Cardano.Timeseries.Interp              (interp)
import           Cardano.Timeseries.Interp.Value        (Value)
import           Cardano.Timeseries.Store
import           Cardano.Timeseries.Store.Flat          (Flat,
                                                         Point (instant, name))
import           Cardano.Timeseries.Store.Flat.Parser   (point, double)
import qualified Cardano.Timeseries.Surface.Expr.Parser as Surface.Parser

import           Cardano.Logging.Resources              (ResourceStats,
                                                         Resources (..),
                                                         readResourceStats)
import           Cardano.Timeseries.Elab                (elab, initialSt)
import           Cardano.Timeseries.Interp.Config       (Config (..))
import           Cardano.Timeseries.Interp.Types        (Error)
import           Cardano.Timeseries.Store.Tree          (Tree, fromFlat)
import           Control.DeepSeq                        (force)
import           Control.Monad                          (forever)
import           Control.Monad.Except                   (runExceptT)
import           Control.Monad.State.Strict             (evalState, runState)
import           Data.Foldable                          (for_, traverse_)
import qualified Data.Map                               as Map
import           Data.Text                              (Text, pack, unpack)
import qualified Data.Text                              as Text
import qualified Data.Text.IO                           as Text
import           Data.Word                              (Word64)
import           GHC.List                               (foldl')
import           System.Environment                     (getArgs)
import           System.Exit                            (die)
import           System.FilePath                        (takeExtension)
import           System.IO                              (hFlush, stdout, readFile')
import           Text.Megaparsec                        hiding (count)
import           Text.Megaparsec.Char                   (space, space1, newline)
import qualified Cardano.Timeseries.Store.Flat.Parser as Flat.Parser
import Text.Megaparsec.Char.Lexer (scientific)
import Data.Void (Void)
import Data.Functor (void)

interpConfig :: Config
interpConfig = Config {defaultRangeSamplingRateMillis = 15 * 1000}

printStore :: Flat Double -> IO ()
printStore = traverse_ print

printQueryResult :: Either Error Value -> IO ()
printQueryResult (Left err) = Text.putStrLn ("Error: " <> err)
printQueryResult (Right ok) = print ok

printStats :: ResourceStats -> IO ()
printStats stats =
  putStrLn $ "Alloc: " <> show ((fromIntegral (rAlloc stats) :: Double) / 1024 / 1024) <> "MB\n"
          <> "Live: " <> show ((fromIntegral (rLive stats) :: Double) / 1024 / 1024) <> "MB\n"
          <> "Heap: " <> show ((fromIntegral (rHeap stats) :: Double) / 1024 / 1024) <> "MB\n"
          <> "RSS: " <> show ((fromIntegral (rRSS stats) :: Double) / 1024 / 1024) <> "MB"

interactive :: Store s Double => s -> IO ()
interactive store = forever $ do
 Just stats <- readResourceStats
 putStrLn "----------"
 printStats stats
 putStrLn $ "Number of store entries: " <> show (count store)
 putStrLn "----------"
 putStr "> "
 hFlush stdout
 queryString <- Text.getLine
 case parse (Surface.Parser.expr <* space <* eof) "input" queryString of
   Left err -> putStrLn (errorBundlePretty err)
   Right surfaceQuery -> do
     -- putStrLn ("Surface expr: " <> show surfaceQuery)
     case evalState (runExceptT (elab surfaceQuery)) initialSt of
       Left err   -> Text.putStrLn err
       Right query -> do
         Text.putStrLn (Text.show query)
         printQueryResult (evalState (runExceptT $ interp interpConfig store mempty query 0) 0)

readStore :: String -> IO (Tree Double)
readStore path | takeExtension path == ".cbor" = do
  content <- readFileSnapshots path
  putStrLn "Read the snapshots CBOR file!"
  let store = {-# SCC "XXX" #-} force $ fromFlat $ snapshotsToFlatStore content
  putStrLn "Created a store from CBOR!"
  pure store
readStore path | takeExtension path == ".txt" = do
  content <- Text.lines <$> Text.readFile path
  case traverse (parse (Flat.Parser.point double <* whitespace <* eof) "input") content of
    Left err -> die (errorBundlePretty err)
    Right store -> pure $ fromFlat store
readStore path = die $ "Unknown extension: " <> takeExtension path

type Parser = Parsec Void Text

whitespace :: Parser ()
whitespace = skipMany (try space1 <|> void newline)

repl :: IO ()
repl = do
  [snapshotsFile] <- getArgs
  store <- readStore snapshotsFile
  putStrLn "Metrics:"
  for_ (Map.keys store) $ \k ->
    Text.putStrLn ("  — " <> k <> "[" <> showMaybe (earliest store k) <> "ms; " <> showMaybe (latest store k) <> "ms]")
  interactive store where
   showMaybe :: Show a => Maybe a -> Text
   showMaybe Nothing  = "N/A"
   showMaybe (Just x) = Text.show x

file :: IO ()
file = do
 queryString <- Text.getContents
 case parse (Surface.Parser.expr <* space <* eof) "input" queryString of
   Left err -> putStrLn (errorBundlePretty err)
   Right query -> do
     putStrLn ("Expr: " <> show query)
     putStrLn "-----------"
     case evalState (runExceptT (elab query)) initialSt of
       Left err   -> die (unpack err)
       Right expr -> putStrLn (show expr)
