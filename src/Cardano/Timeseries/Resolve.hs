module Cardano.Timeseries.Resolve(resolveTy, resolveBinding, resolveContext, resolveExpr') where
import           Cardano.Timeseries.Query.Expr (Expr (..))
import qualified Cardano.Timeseries.Query.Expr as Expr
import           Cardano.Timeseries.Typing
import qualified Cardano.Timeseries.Typing     as Ty
import qualified Data.Map.Strict               as Map
import           Data.String.Interpolate       (i)


-- | Computes the head-normal form of `Ty` w.r.t. hole resolution (i.e. unfolds holes recursively up to the head expression).
resolveTy :: Defs -> Ty -> Ty
resolveTy defs (Ty.Hole x) =
  case Map.lookup x defs of
    Just (TyHoleInst rhs) -> resolveTy defs rhs
    Just _  -> Ty.Hole x
    Nothing -> error $ "[INTERNAL ERROR] Can't find hole in Σ: " <> show x
resolveTy defs (InstantVector typ) = InstantVector (resolveTy defs typ)
resolveTy defs (RangeVector typ) = RangeVector (resolveTy defs typ)
resolveTy defs (Fun typ typ') = Fun (resolveTy defs typ) (resolveTy defs typ')
resolveTy defs (Pair typ typ') = Pair (resolveTy defs typ) (resolveTy defs typ')
resolveTy defs Scalar = Scalar
resolveTy defs Timestamp = Timestamp
resolveTy defs Duration = Duration
resolveTy defs Bool = Bool

-- | Computes the head-normal form of `Binding` w.r.t. hole resolution
--   (i.e. unfolds holes recursively up to the head expression in type of the binding).
resolveBinding :: Defs -> Binding -> Binding
resolveBinding defs (LetBinding x rhs typ) =
  LetBinding x rhs (resolveTy defs typ)
resolveBinding defs (LambdaBinding x typ) =
  LambdaBinding x (resolveTy defs typ)

-- | Computes the head-normal form of `Context` w.r.t. hole resolution
--   (i.e. unfolds holes recursively up to the head expression in every type of the context).
resolveContext :: Defs -> Context -> Context
resolveContext defs = fmap (resolveBinding defs)


-- | Computes the normal form of `Expr` w.r.t. hole resolution (i.e. resolves *all* holes in the expression).
resolveExpr' :: Defs -> Expr -> Expr
resolveExpr' defs (Number f) = Number f
resolveExpr' defs (Str s) = Str s
resolveExpr' defs (Builtin f) = Builtin f
resolveExpr' defs (Application f e) = Application (resolveExpr' defs f) (fmap (resolveExpr' defs) e)
resolveExpr' defs (Lambda x f) = Lambda x (resolveExpr' defs f)
resolveExpr' defs (Let x rhs e) = Let x (resolveExpr' defs rhs) (resolveExpr' defs e)
resolveExpr' defs (MkPair a b) = MkPair (resolveExpr' defs a) (resolveExpr' defs b)
resolveExpr' defs (Variable x) = Variable x
resolveExpr' defs (Expr.Hole idx) =
  case Map.lookup idx defs of
    Just (ExprHoleInst rhs _) -> resolveExpr' defs rhs
    _                         -> Expr.Hole idx
