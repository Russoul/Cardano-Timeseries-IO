{-# LANGUAGE Strict #-}

module Cardano.Timeseries.Data.Triple(Triple(..)) where

data Triple a b c = Triple a b c deriving (Show, Eq, Ord)
