module Cardano.Timeseries.Typing(
  Ty(..),
  Binding(..),
  Context,
  identifier,
  Def(..),
  Defs,
  instantiateTy,
  instantiateExpr,
  prettyTy,
  prettyBinding,
  prettyContext,
  TyPrec(..),
  ty) where
import           Cardano.Timeseries.Data.SnocList
import           Cardano.Timeseries.Domain.Identifier (Identifier (..))
import           Cardano.Timeseries.Query.Expr        (HoleIdentifier)
import qualified Cardano.Timeseries.Query.Expr        as Semantic
import           Data.Map.Strict                      (Map)
import qualified Data.Map.Strict                      as Map
import           Data.Text                            (Text)
import qualified Data.Text                            as Text

-- | Typing of a query expression.
data Ty = InstantVector Ty
        | RangeVector Ty
        | Scalar
        | Bool
        | Pair Ty Ty
        | Timestamp
        | Duration
        | Fun Ty Ty
        | Hole HoleIdentifier deriving (Show, Eq)

data TyPrec = Loose | FunCodomain | FunDomain | Tight deriving (Show, Eq, Ord)

conditionalParens :: Bool -> Text -> Text
conditionalParens True t  = "(" <> t <> ")"
conditionalParens False t = t

prettyTy :: TyPrec -> Ty -> Text
prettyTy prec (InstantVector typ) = conditionalParens (prec == Tight) $
  "InstantVector " <> prettyTy Tight typ
prettyTy prec (RangeVector typ) = conditionalParens (prec == Tight) $
  "RangeVector " <> prettyTy Tight typ
prettyTy prec (Pair typ typ') =
  "(" <> prettyTy Loose typ <> ", " <> prettyTy Loose typ' <> ")"
prettyTy prec (Fun typ typ') = conditionalParens (prec > FunCodomain) $
  prettyTy FunDomain typ <> " -> " <> prettyTy FunCodomain typ'
prettyTy _ Scalar = "Scalar"
prettyTy _ Bool = "Bool"
prettyTy _ Timestamp = "Timestamp"
prettyTy _ Duration = "Duration"
prettyTy _ (Hole idx) = "?" <> Text.show idx

-- | A context entry of a typing context.
data Binding = LetBinding Identifier Semantic.Expr Ty
             | LambdaBinding Identifier Ty deriving (Show)

prettyIdentifier ::  Identifier -> Text
prettyIdentifier (User x)    = x
prettyIdentifier (Machine i) = "$" <> Text.show i

prettyBinding :: Binding -> Text
prettyBinding (LetBinding x rhs typ) = "(" <> prettyIdentifier x <> " ≔ " <> "..." <> " : " <> prettyTy Loose typ <> ")"
prettyBinding (LambdaBinding x typ) = "(" <> prettyIdentifier x <> " : " <> prettyTy Loose typ <> ")"

identifier :: Binding -> Identifier
identifier (LetBinding x _ _)  = x
identifier (LambdaBinding x _) = x

ty :: Binding -> Ty
ty (LetBinding _ _ typ)  = typ
ty (LambdaBinding _ typ) = typ

-- | Γ
--   A typing context of a query expression.
type Context = SnocList Binding

prettyContext :: Context -> Text
prettyContext Lin = "()"
prettyContext ctx = Text.intercalate " " (fmap prettyBinding (toList ctx))

-- | (? type) | (? ≔ T type) | (? : T) | (? ≔ t : T)
--   Definition of a type- or expression- level hole.
data Def = TyHoleDecl | TyHoleInst Ty | ExprHoleDecl Ty | ExprHoleInst Semantic.Expr Ty

-- | Σ
--   A collection of hole definitions `Def` indexed by `HoleIdentifier`.
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
