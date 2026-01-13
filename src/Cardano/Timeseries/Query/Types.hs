module Cardano.Timeseries.Query.Types(Error, QueryM, HoleIdentifier) where
import           Control.Monad.Except       (ExceptT)
import           Control.Monad.State.Strict (State)

type HoleIdentifier = Int
type Error = String
type QueryM a = ExceptT Error (State Int) a
