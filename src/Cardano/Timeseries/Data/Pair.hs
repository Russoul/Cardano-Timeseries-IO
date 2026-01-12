{-# LANGUAGE Strict #-}

module Cardano.Timeseries.Data.Pair(Pair(..)) where

data Pair a b = Pair a b deriving (Show, Eq, Ord)
