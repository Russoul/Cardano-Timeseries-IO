module Cardano.Timeseries.Surface.Expr.Head(Head(..)) where
import           Cardano.Timeseries.Domain.Identifier (Identifier)
import           Cardano.Timeseries.Domain.Types      (Label, Labelled)
import           Data.Set                             (Set)

data Head = Fst
          | Snd
          | FilterByLabel (Set (Labelled String))
          | Min
          | Max
          | Avg
          | Filter
          | Join
          | Map
          | Abs
          | Increase
          | Rate
          | AvgOverTime
          | SumOverTime
          | QuantileOverTime
          | Unless
          | QuantileBy (Set Label)
          | Earliest Identifier
          | Latest Identifier
          | ToScalar
