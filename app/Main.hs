{-# OPTIONS_GHC -Wno-deprecations #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{- HLINT ignore "Use print" -}

module Main where

import           Cardano.Timeseries.Import.PlainCBOR
import           Cardano.Timeseries.Interp              (interp)
import           Cardano.Timeseries.Interp.Value        (Value)
import           Cardano.Timeseries.Query.Expr.Parser   (expr)
import           Cardano.Timeseries.Store
import           Cardano.Timeseries.Store.Flat          (Flat,
                                                         Point (instant, name))
import           Cardano.Timeseries.Store.Flat.Parser   (points)
import qualified Cardano.Timeseries.Surface.Expr.Parser as Surface.Parser

import           Cardano.Logging.Resources              (ResourceStats,
                                                         Resources (..),
                                                         readResourceStats)
import           Cardano.Timeseries.Query.Types         (Error)
import           Cardano.Timeseries.Store.Tree          (fromFlat)
import           Cardano.Timeseries.Surface.Elab        (elab, initialSt)
import           Control.DeepSeq                        (force)
import           Control.Monad                          (forever)
import           Control.Monad.Except                   (runExceptT)
import           Control.Monad.State.Strict             (evalState, runState)
import           Data.Foldable                          (for_, traverse_)
import qualified Data.Map                               as Map
import           Data.Text                              (pack, unpack)
import           GHC.List                               (foldl')
import           System.Exit                            (die)
import           System.IO                              (hFlush, stdout)
import           Text.Megaparsec                        hiding (count)
import           Text.Megaparsec.Char                   (space)

snapshotsFile :: String
snapshotsFile = "data/preprod_2bp_max1764622920.cbor"

printStore :: Flat Double -> IO ()
printStore = traverse_ print

printQueryResult :: Either Error Value -> IO ()
printQueryResult (Left err) = putStrLn ("Error: " <> err)
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
 queryString <- getLine
 case parse (expr <* space <* eof) "input" (pack queryString) of
   Left err -> putStrLn (errorBundlePretty err)
   Right query -> do
     putStrLn ("Expr: " <> show query)
     printQueryResult (evalState (runExceptT $ interp store mempty query 0) 0)

main1 :: IO ()
main1 = do
  content <- readFileSnapshots snapshotsFile
  putStrLn "Read the snapshots file!"
  let store = {-# SCC "XXX" #-} force $ fromFlat $ snapshotsToFlatStore content
  putStrLn "Created a store!"
  putStrLn "Metrics:"
  for_ (Map.keys store) (\k -> putStrLn ("  — " <> k))
  interactive store

main2 :: IO ()
main2 = do
 queryString <- getLine
 case parse (Surface.Parser.expr <* space <* eof) "input" (pack queryString) of
   Left err -> putStrLn (errorBundlePretty err)
   Right query -> do
     putStrLn ("Expr: " <> show query)

main :: IO ()
main = do
 queryString <- getLine
 case parse (Surface.Parser.expr <* space <* eof) "input" (pack queryString) of
   Left err -> putStrLn (errorBundlePretty err)
   Right query -> do
     putStrLn ("Expr: " <> show query)
     case evalState (runExceptT (elab query)) initialSt of
       Left err   -> die (unpack err)
       Right expr -> putStrLn (show expr)

