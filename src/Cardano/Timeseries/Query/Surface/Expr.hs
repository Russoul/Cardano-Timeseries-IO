module Cardano.Timeseries.Query.Surface.Expr(Expr(..), mkRange, mkApp) where

import           Cardano.Timeseries.Domain.Identifier (Identifier)
import           Cardano.Timeseries.Domain.Types      (Label, Labelled)
import           Data.Set                             (Set)
import Data.Text (Text)

-- ---- not ----
-- not < atom
-- ---- range ----
-- range < atom
-- ---- app ----
-- app < atom
-- app < not
-- app < range
-- ---- add ----
-- add < atom
-- add < not
-- add < range
-- add < app
-- ---- mul ----
-- mul < atom
-- mul < not
-- mul < range
-- mul < app
-- mul < add
-- ---- comp ----
-- comp < atom
-- comp < not
-- comp < range
-- comp < app
-- comp < add
-- comp < mul
-- ---- and ----
-- and < atom
-- and < not
-- and < range
-- and < app
-- and < add
-- and < mul
-- and < comp
-- ---- or ----
-- or < atom
-- or < not
-- or < range
-- or < app
-- or < add
-- or < mul
-- or < comp
-- or < and
-- ---- universe ----
-- universe < atom
-- universe < not
-- universe < range
-- universe < app
-- universe < add
-- universe < mul
-- universe < comp
-- universe < and
-- universe < or

-- t{atom} ::= (t{≥ universe}, t{≥ universe})
--           | x
--           | epoch
--           | now
--           | true
--           | false
--           | <int>ms
--           | <int>s
--           | <int>min
--           | <int>h
--           | (t{≥ universe})
--           | <float>
--           | "<string>"
-- t{not} ::= !t{> not}
-- t{range} ::= t{> range} [̅t̅{̅≥̅ ̅u̅n̅i̅v̅e̅r̅s̅e̅}̅;̅ ̅t̅{̅≥̅ ̅u̅n̅i̅v̅e̅r̅s̅e̅}̅]̅ ̅|̅ ̅t̅[̅t̅{̅≥̅ ̅u̅n̅i̅v̅e̅r̅s̅e̅}̅;̅ ̅t̅{̅≥̅ ̅u̅n̅i̅v̅e̅r̅s̅e̅}̅ ̅:̅ ̅t̅{̅≥̅ ̅u̅n̅i̅v̅e̅r̅s̅e̅}̅]̅
-- t{app} ::= fst t{> app} | snd t{> app} | filter_by_label {s = s, ..., s = s} t{> app}
--            | min t{> app} | max t{> app} | avg t{> app} | filter t{> app} t{> app}
--            | join t{> app} t{> app}
--            | map t{> app} t{> app}
--            | abs t{> app}
--            | increase t{> app}
--            | rate t{> app}
--            | avg_over_time t{> app}
--            | sum_over_time t{> app}
--            | quantile_over_time t{> app} t{> app}
--            | unless t{> app} t{> app}
--            | quantile_by (s, ..., s) t{> app} t{> app}
--            | earliest x
--            | latest x
--            | to_scalar t{> app}
--            | t{> app} t̅{̅>̅ ̅a̅p̅p̅}̅
-- t{mul} ::= t{> mul} *̅|̅/̅ ̅t̅{̅>̅ ̅m̅u̅l̅}̅
-- t{add} ::= t{> add} +̅|̅-̅ ̅t̅{̅>̅ ̅a̅d̅d̅}̅
-- t{comp} ::= t{> comp} == t{> comp} | t{> comp} != t{> comp} | t{> comp} < t{> comp} | t{> comp} <= t{> comp}
--            | t{> comp} > t{> comp} | t{> comp} >= t{> comp}
-- t{and} ::= t{> and} &̅&̅ ̅t̅{̅>̅ ̅a̅n̅d̅}̅
-- t{or}  ::= t{> or} |̅|̅ ̅t̅{̅>̅ ̅o̅r̅}̅
-- t{universe} ::= let x = t{> universe} in t{≥ universe} | \x -> t{≥ universe}


data Expr =
    Let Identifier Expr Expr
  | Lambda Identifier Expr
  | Fst Expr
  | Snd Expr
  | MkPair Expr Expr
  | Eq Expr Expr
  | NotEq Expr Expr
  | Lt Expr Expr
  | Lte Expr Expr
  | Gt Expr Expr
  | Gte Expr Expr
  | Add Expr Expr
  | Sub Expr Expr
  | Mul Expr Expr
  | Div Expr Expr
  | Not Expr
  | Or Expr Expr
  | And Expr Expr
  | Milliseconds Int
  | Seconds Int
  | Minutes Int
  | Hours Int
  | Epoch
  | Now
  | Range Expr Expr Expr (Maybe Expr)
  | FilterByLabel (Set (Labelled String)) Expr
  | Max Expr
  | Min Expr
  | Avg Expr
  | Filter Expr Expr
  | Join Expr Expr
  | Map Expr Expr
  | Abs Expr
  | Increase Expr
  | Rate Expr
  | AvgOverTime Expr
  | SumOverTime Expr
  | QuantileOverTime Expr Expr
  | Unless Expr Expr
  | QuantileBy (Set Label) Expr Expr
  | Earliest Identifier
  | Latest Identifier
  | ToScalar Expr
  | Variable Identifier
  | Str String
  | Number Double
  | Truth
  | Falsity
  | App Expr Expr deriving (Show)

mkRange :: Expr -> (Expr, Expr, Maybe Expr) -> Expr
mkRange t (a, b, c) = Range t a b c

mkApp :: Expr -> [Expr] -> Expr
mkApp = foldl' App
