module Cardano.Timeseries.Interp.Types where
import           Control.Monad.Except       (ExceptT)
import           Control.Monad.State.Strict (State)
import Data.Text (Text)

type Error = Text
type QueryM a = ExceptT Error (State Int) a
