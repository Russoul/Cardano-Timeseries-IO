module Cardano.Timeseries.Query.Surface.Types(
  Ty(..),
  Binding(..),
  Context,
  identifier,
  Def(..),
  Defs,
  instantiateTy,
  instantiateExpr,
  ty) where
import           Cardano.Timeseries.Data.SnocList
import           Cardano.Timeseries.Domain.Identifier (Identifier)
import qualified Cardano.Timeseries.Query.Expr        as Semantic
import           Cardano.Timeseries.Query.Types       (HoleIdentifier)
import           Data.Map.Strict                      (Map)
import qualified Data.Map.Strict                      as Map
import           Data.Text                            (Text)

data Ty = InstantVector Ty
        | RangeVector Ty
        | Scalar
        | Bool
        | Pair Ty Ty
        | Timestamp
        | Duration
        | Fun Ty Ty
        | Hole HoleIdentifier deriving (Show, Eq)

data Binding = LetBinding Identifier Semantic.Expr Ty
             | LambdaBinding Identifier Ty deriving (Show)

identifier :: Binding -> Identifier
identifier (LetBinding x _ _)  = x
identifier (LambdaBinding x _) = x

ty :: Binding -> Ty
ty (LetBinding _ _ typ)  = typ
ty (LambdaBinding _ typ) = typ

-- | Γ
type Context = SnocList Binding

-- | (? type) | (? ≔ T type) | (? : T) | (? ≔ t : T)
data Def = TyHoleDecl | TyHoleInst Ty | ExprHoleDecl Ty | ExprHoleInst Semantic.Expr Ty

-- | Σ
type Defs = Map HoleIdentifier Def

-- | Assumes that the given `Defs` contains a `TyHoleDecl` of the given `HoleIdentifier`.
instantiateTy :: HoleIdentifier -> Ty -> Defs -> Defs
instantiateTy x rhs defs =
  case Map.lookup x defs of
    Just TyHoleDecl -> Map.insert x (TyHoleInst rhs) defs
    _ -> error $ "[INTERNAL ERROR] Incorrect or missing Def for type hole identifier: " <> show x

-- | Assumes that the given `Defs` contains a `ExprHoleDecl` of the given `HoleIdentifier`.
--   Types of the hole and the provided `Semantic.Expr` must be compatible.
instantiateExpr :: HoleIdentifier -> Semantic.Expr -> Defs -> Defs
instantiateExpr x rhs defs =
  case Map.lookup x defs of
    Just (ExprHoleDecl typ) -> Map.insert x (ExprHoleInst rhs typ) defs
    _ -> error $ "[INTERNAL ERROR] Incorrect or missing Def for expr hole identifier: " <> show x
