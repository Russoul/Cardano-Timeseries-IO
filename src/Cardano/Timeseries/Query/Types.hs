module Cardano.Timeseries.Query.Types(Error, QueryM) where
import Control.Monad.Except (ExceptT)
import Control.Monad.State.Strict (State)

type Error = String
type QueryM a = ExceptT Error (State Int) a
