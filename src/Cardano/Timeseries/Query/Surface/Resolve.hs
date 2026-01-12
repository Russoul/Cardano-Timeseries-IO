{-# LANGUAGE QuasiQuotes  #-}

module Cardano.Timeseries.Query.Surface.Resolve(eval) where
import           Cardano.Timeseries.Query.Expr          (Expr(..))
import           Cardano.Timeseries.Query.Surface.Types (Defs, Def (ExprHoleInst))
import Data.String.Interpolate (i)
import qualified Data.Map.Strict as Map

-- | Resolve all holes in the expression.
--   Assumes that all holes that occur in the expression have been solved.
eval :: Defs -> Expr -> Expr
eval defs (Number f) = Number f
eval defs (Str s) = Str s
eval defs (Builtin f) = Builtin f
eval defs (Application f e) = Application (eval defs f) (fmap (eval defs) e)
eval defs (Lambda x f) = Lambda x (eval defs f)
eval defs (Let x rhs e) = Let x (eval defs rhs) (eval defs e)
eval defs (MkPair a b) = MkPair (eval defs a) (eval defs b)
eval defs (Variable x) = Variable x
eval defs (Hole idx) =
  case Map.lookup idx defs of
    Just (ExprHoleInst rhs _) -> eval defs rhs
    _ -> error [i| Invalid hole: #{show idx} |]
