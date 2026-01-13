module Cardano.Timeseries.Data.SnocList(SnocList(..), find, toList) where
import Cardano.Timeseries.Util (toMaybe)
import Control.Applicative ((<|>))

data SnocList a = Lin | Snoc (SnocList a) a deriving (Show, Eq, Ord, Functor, Foldable, Traversable)

find :: (a -> Bool) -> SnocList a -> Maybe a
find f Lin = Nothing
find f (Snoc xs x) = toMaybe (f x) x <|> find f xs

toList :: SnocList a -> [a]
toList snoc = go snoc [] where
  go :: SnocList a -> [a] -> [a]
  go Lin acc = acc
  go (Snoc xs x) acc = go xs (x : acc)
