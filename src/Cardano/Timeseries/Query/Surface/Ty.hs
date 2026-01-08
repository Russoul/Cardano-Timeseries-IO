module Cardano.Timeseries.Query.Surface.Ty(Ty(..)) where
import Data.Text (Text)

type HoleIdentifier = Text

data Ty = InstantVector Ty
        | RangeVector Ty
        | Scalar
        | Bool
        | Pair Ty Ty
        | Timestamp
        | Duration
        | Hole HoleIdentifier
