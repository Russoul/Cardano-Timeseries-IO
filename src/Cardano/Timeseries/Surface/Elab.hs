{-# LANGUAGE QuasiQuotes  #-}
{-# LANGUAGE ViewPatterns #-}
module Cardano.Timeseries.Surface.Elab(initialSt, St(..), ElabM, elab) where
import           Cardano.Timeseries.Data.Pair                (Pair (..))
import           Cardano.Timeseries.Data.SnocList
import           Cardano.Timeseries.Domain.Identifier        (Identifier)
import           Cardano.Timeseries.Domain.Types             (Labelled)
import           Cardano.Timeseries.Query.BinaryArithmeticOp
import qualified Cardano.Timeseries.Query.BinaryArithmeticOp as BinaryArithmeticOp
import           Cardano.Timeseries.Query.BinaryRelation     (BinaryRelation,
                                                              prettyBinaryRelation)
import qualified Cardano.Timeseries.Query.BinaryRelation     as BinaryRelation
import qualified Cardano.Timeseries.Query.Expr               as Semantic
import           Cardano.Timeseries.Query.Types              (HoleIdentifier)
import           Cardano.Timeseries.Resolve
import           Cardano.Timeseries.Surface.Expr             (LabelConstraint (..),
                                                              Loc, getLoc)
import qualified Cardano.Timeseries.Surface.Expr             as Surface
import           Cardano.Timeseries.Surface.Unify            (UnificationProblem (..),
                                                              UnifyM)
import qualified Cardano.Timeseries.Surface.Unify            as Unify
import           Cardano.Timeseries.Typing                   (Binding (..),
                                                              Context, Def (..),
                                                              Defs,
                                                              Ty (Bool, Duration, Fun, Hole, InstantVector, RangeVector, Scalar, Timestamp),
                                                              TyPrec (Loose),
                                                              instantiateExpr,
                                                              prettyContext,
                                                              prettyTy)
import qualified Cardano.Timeseries.Typing                   as Ty
import qualified Cardano.Timeseries.Typing                   as Types
import           Control.Monad                               (when)
import           Control.Monad.Except                        (ExceptT,
                                                              liftEither,
                                                              runExceptT,
                                                              throwError)
import           Control.Monad.State.Strict                  (State, get,
                                                              modify, put,
                                                              runState, state)
import           Data.List.NonEmpty                          (NonEmpty (..))
import qualified Data.List.NonEmpty                          as NonEmpty
import qualified Data.Map.Strict                             as Map
import qualified Data.Set                                    as Set
import           Data.String.Interpolate                     (i)
import           Data.Text                                   (Text, pack)
import qualified Data.Text                                   as Text
import           Debug.Trace                                 (traceM)
import           Text.Megaparsec                             (sourcePosPretty)

-- | Γ ⊦ s ~> ?x : A
data GeneralElabProblem = GeneralElabProblem {
  gpgamma   :: Context,
  gpsurface :: Surface.Expr,
  gphole    :: HoleIdentifier,
  gpholeTy  :: Ty
} deriving (Show)

prettyGeneralElabProblem :: GeneralElabProblem -> Text
prettyGeneralElabProblem (GeneralElabProblem gam sur hole holeTy) =
  [i|#{prettyContext gam} ⊦ #{prettyTy Loose holeTy}
      @ #{sourcePosPretty (getLoc sur)}|]

evalGeneralElabProblem :: Defs -> GeneralElabProblem -> GeneralElabProblem
evalGeneralElabProblem defs (GeneralElabProblem gam tm hole holeTy) =
  GeneralElabProblem (resolveContext defs gam) tm hole (resolveTy defs holeTy)

-- | Γ ⊦ ((t : T) R (t : T)) ~> ? : T
data BinaryRelationElabProblem = BinaryRelationElabProblem {
  brpgamma  :: Context,
  brploc    :: Loc,
  brplhs    :: Semantic.Expr,
  brplhsTy  :: Ty,
  brprel    :: BinaryRelation.BinaryRelation,
  brprhs    :: Semantic.Expr,
  brprhsTy  :: Ty,
  brphole   :: HoleIdentifier,
  brpholeTy :: Ty
} deriving (Show)

prettyBinaryRelationElabProblem :: BinaryRelationElabProblem -> Text
prettyBinaryRelationElabProblem (BinaryRelationElabProblem gam loc lhs lhsTy rel rhs rhsTy hole holeTy) =
  [i|#{prettyContext gam} ⊦ #{prettyTy Loose lhsTy} #{prettyBinaryRelation rel} #{prettyTy Loose rhsTy} : #{prettyTy Loose holeTy}
      @ #{sourcePosPretty loc}|]

evalBinaryRelationElabProblem :: Defs -> BinaryRelationElabProblem -> BinaryRelationElabProblem
evalBinaryRelationElabProblem defs (BinaryRelationElabProblem gam loc lhs lhsTy rel rhs rhsTy hole holeTy) =
  BinaryRelationElabProblem
    (resolveContext defs gam)
    loc
    lhs
    (resolveTy defs lhsTy)
    rel
    rhs
    (resolveTy defs rhsTy)
    hole
    (resolveTy defs holeTy)

-- | Γ ⊦ (t R t) ~> ? : t
data BinaryArithmeticOpElabProblem = BinaryArithmeticOpElabProblem {
  baopgamma  :: Context,
  baoploc    :: Loc,
  baoplhs    :: Semantic.Expr,
  baoplhsTy  :: Ty,
  baopop     :: BinaryArithmeticOp.BinaryArithmeticOp,
  baoprhs    :: Semantic.Expr,
  baoprhsTy  :: Ty,
  baophole   :: HoleIdentifier,
  baopholeTy :: Ty
} deriving (Show)

prettyBinaryArithmeticOpElabProblem :: BinaryArithmeticOpElabProblem -> Text
prettyBinaryArithmeticOpElabProblem (BinaryArithmeticOpElabProblem gam loc lhs lhsTy op rhs rhsTy hole holeTy) =
  [i|#{prettyContext gam} ⊦ #{prettyTy Loose lhsTy} #{prettyOp op} #{prettyTy Loose rhsTy} : #{prettyTy Loose holeTy}
      @ #{sourcePosPretty loc}|]

evalBinaryArithmethicOpElabProblem :: Defs -> BinaryArithmeticOpElabProblem -> BinaryArithmeticOpElabProblem
evalBinaryArithmethicOpElabProblem defs (BinaryArithmeticOpElabProblem gam loc lhs lhsTy op rhs rhsTy hole holeTy) =
  BinaryArithmeticOpElabProblem
    (resolveContext defs gam)
    loc
    lhs
    (resolveTy defs lhsTy)
    op
    rhs
    (resolveTy defs rhsTy)
    hole
    (resolveTy defs holeTy)

-- | Γ ⊦ to_scalar (t : T) ~> ?
data ToScalarElabProblem = ToScalarElabProblem {
  tsepgamma :: Context,
  tseploc   :: Loc,
  tsepexpr  :: Semantic.Expr,
  tsepty    :: Ty,
  tsephole  :: HoleIdentifier
} deriving (Show)

prettyToScalarElabProblem :: ToScalarElabProblem -> Text
prettyToScalarElabProblem (ToScalarElabProblem gam loc expr ty hole) =
  [i|#{prettyContext gam} ⊦ to_scalar #{prettyTy Loose ty}
      @ #{sourcePosPretty loc}|]

evalToScalarElabProblem :: Defs -> ToScalarElabProblem -> ToScalarElabProblem
evalToScalarElabProblem defs (ToScalarElabProblem gam loc expr exprTy hole) =
  ToScalarElabProblem
    (resolveContext defs gam)
    loc
    expr
    (resolveTy defs exprTy)
    hole

data ElabProblem = General GeneralElabProblem
                 | BinaryRelation BinaryRelationElabProblem
                 | BinaryArithmeticOp BinaryArithmeticOpElabProblem
                 | ToScalar ToScalarElabProblem deriving (Show)

evalElabProblem :: Defs -> ElabProblem -> ElabProblem
evalElabProblem defs (General p) = General (evalGeneralElabProblem defs p)
evalElabProblem defs (BinaryRelation p) = BinaryRelation (evalBinaryRelationElabProblem defs p)
evalElabProblem defs (BinaryArithmeticOp p) = BinaryArithmeticOp (evalBinaryArithmethicOpElabProblem defs p)
evalElabProblem defs (ToScalar p) = ToScalar (evalToScalarElabProblem defs p)

prettyElabProblem :: ElabProblem -> Text
prettyElabProblem (General p)            = prettyGeneralElabProblem p
prettyElabProblem (BinaryRelation p)     = prettyBinaryRelationElabProblem p
prettyElabProblem (BinaryArithmeticOp p) = prettyBinaryArithmeticOpElabProblem p
prettyElabProblem (ToScalar p)           = prettyToScalarElabProblem p

data St = St {
  defs               :: Defs,
  nextHoleIdentifier :: HoleIdentifier
}

initialSt :: St
initialSt = St mempty 0

updateDefs :: (Defs -> Defs) -> St -> St
updateDefs f (St ds x) = St (f ds) x

getDefs :: St -> Defs
getDefs = defs

setDefs :: Defs -> St -> St
setDefs v = updateDefs (const v)

updateNextHoleIdentifier :: (HoleIdentifier -> HoleIdentifier) -> St -> St
updateNextHoleIdentifier f (St ds x) = St ds (f x)

setNextHoleIdentifier :: HoleIdentifier -> St -> St
setNextHoleIdentifier v = updateNextHoleIdentifier (const v)

runUnifyM :: UnifyM a -> ElabM a
runUnifyM f = do
  st <- get
  let ds = getDefs st
  let !(!r, Unify.St ds') = runState (runExceptT f) (Unify.St ds)
  put (setDefs ds' st)
  liftEither r


type Error = Text

type ElabM a = ExceptT Error (State St) a

freshHoleIdentifier :: ElabM HoleIdentifier
freshHoleIdentifier = do
  x <- nextHoleIdentifier <$> get
  modify (updateNextHoleIdentifier (+ 1))
  pure x

freshTyHole :: ElabM HoleIdentifier
freshTyHole = do
  x <- freshHoleIdentifier
  modify (updateDefs (Map.insert x TyHoleDecl))
  pure x

freshExprHole :: Ty -> ElabM HoleIdentifier
freshExprHole typ = do
  x <- freshHoleIdentifier
  modify (updateDefs (Map.insert x (ExprHoleDecl typ)))
  pure x

mbBinaryRelation :: Surface.Expr -> Maybe (Loc, Surface.Expr, BinaryRelation.BinaryRelation, Surface.Expr)
mbBinaryRelation (Surface.Lte l a b)   = Just (l, a, BinaryRelation.Lte, b)
mbBinaryRelation (Surface.Lt l a b)    = Just (l, a, BinaryRelation.Lt, b)
mbBinaryRelation (Surface.Gte l a b)   = Just (l, a, BinaryRelation.Gte, b)
mbBinaryRelation (Surface.Gt l a b)    = Just (l, a, BinaryRelation.Gt, b)
mbBinaryRelation (Surface.Eq l a b)    = Just (l, a, BinaryRelation.Eq, b)
mbBinaryRelation (Surface.NotEq l a b) = Just (l, a, BinaryRelation.NotEq, b)
mbBinaryRelation _                     = Nothing

mbBinaryArithmeticOp :: Surface.Expr -> Maybe (Loc, Surface.Expr, BinaryArithmeticOp.BinaryArithmeticOp, Surface.Expr)
mbBinaryArithmeticOp (Surface.Add l a b) = Just (l, a, BinaryArithmeticOp.Add, b)
mbBinaryArithmeticOp (Surface.Sub l a b) = Just (l, a, BinaryArithmeticOp.Sub, b)
mbBinaryArithmeticOp (Surface.Mul l a b) = Just (l, a, BinaryArithmeticOp.Mul, b)
mbBinaryArithmeticOp (Surface.Div l a b) = Just (l, a, BinaryArithmeticOp.Div, b)
mbBinaryArithmeticOp _                   = Nothing

checkFresh :: Context -> Identifier -> ElabM ()
checkFresh Lin _ = pure ()
checkFresh (Snoc rest b) v | Types.identifier b == v = throwError $ pack $ "Reused variable name: " <> show (Types.identifier b)
checkFresh (Snoc rest b) v = checkFresh rest v

-- | Γ ⊦ to_scalar (t : T) ~> ?
-- Assumes that `Ty` is normal w.r.t. hole substitution.
solveToScalarElabProblem :: Context
                         -> Loc
                         -> Semantic.Expr
                         -> Ty
                         -> HoleIdentifier
                         -> ElabM (Maybe ([UnificationProblem], [ElabProblem]))
solveToScalarElabProblem gam loc expr Scalar hole = do
  modify $ updateDefs $ instantiateExpr hole expr
  pure $ Just ([], [])
solveToScalarElabProblem gam loc expr Bool hole = do
  modify $ updateDefs $ instantiateExpr hole (Semantic.Application (Semantic.Builtin Semantic.BoolToScalar) (expr :| []))
  pure $ Just ([], [])
solveToScalarElabProblem gam loc expr Duration hole = do
  modify $ updateDefs $ instantiateExpr hole (Semantic.Application (Semantic.Builtin Semantic.DurationToScalar) (expr :| []))
  pure $ Just ([], [])
solveToScalarElabProblem gam loc expr Timestamp hole = do
  modify $ updateDefs $ instantiateExpr hole (Semantic.Application (Semantic.Builtin Semantic.TimestampToScalar) (expr :| []))
  pure $ Just ([], [])
solveToScalarElabProblem gam loc expr (Hole _) hole = pure Nothing
solveToScalarElabProblem gam loc expr badType hole = throwError
  [i| to_scalar can't be applied to an expression of type #{prettyTy Loose badType} @ #{sourcePosPretty loc} |]

-- | Σ Γ ⊦ InstantVector Scalar `rel` Scalar ~> ? : InstantVector Scalar
-- | Σ Γ ⊦ Scalar `rel` Scalar ~> ? : Bool
solveCanonicalBinaryRelationElabProblem :: Context
                                        -> Loc
                                        -> Semantic.Expr
                                        -> Ty
                                        -> BinaryRelation
                                        -> Semantic.Expr
                                        -> Ty
                                        -> HoleIdentifier
                                        -> Ty
                                        -> ElabM (Maybe ([UnificationProblem], [ElabProblem]))
solveCanonicalBinaryRelationElabProblem gam loc lhs (InstantVector Scalar) rel rhs Scalar hole (InstantVector Scalar) = do
  modify $ updateDefs $
    instantiateExpr hole
      (Semantic.Application (Semantic.Builtin (BinaryRelation.embedInstantVectorScalar rel)) (NonEmpty.fromList [lhs, rhs]))
  pure $ Just ([], [])
solveCanonicalBinaryRelationElabProblem gam loc lhs Scalar rel rhs Scalar hole Bool = do
  modify $ updateDefs $
    instantiateExpr hole
      (Semantic.Application (Semantic.Builtin (BinaryRelation.embedScalar rel)) (NonEmpty.fromList [lhs, rhs]))
  pure $ Just ([], [])
solveCanonicalBinaryRelationElabProblem _ _ _ _ _ _ _ _ _ = pure Nothing

solveNoncanonicalBinaryRelationElabProblem :: Context
                                           -> Loc
                                           -> Semantic.Expr
                                           -> Ty
                                           -> BinaryRelation
                                           -> Semantic.Expr
                                           -> Ty
                                           -> HoleIdentifier
                                           -> Ty
                                           -> ElabM (Maybe ([UnificationProblem], [ElabProblem]))
solveNoncanonicalBinaryRelationElabProblem gam loc lhs (InstantVector Scalar) rel rhs Scalar hole typ = do
  pure $ Just ([UnificationProblem loc typ (InstantVector Scalar)],
    [BinaryRelation $
      BinaryRelationElabProblem gam loc lhs (InstantVector Scalar)
        rel rhs Scalar hole (InstantVector Scalar)])
solveNoncanonicalBinaryRelationElabProblem gam loc lhs Scalar rel rhs (InstantVector Scalar) hole typ = do
  pure $ Just ([UnificationProblem loc typ (InstantVector Scalar)],
    [BinaryRelation $
      BinaryRelationElabProblem gam loc rhs (InstantVector Scalar)
        (BinaryRelation.swapInstantVectorScalar rel) lhs Scalar hole (InstantVector Scalar)])
solveNoncanonicalBinaryRelationElabProblem gam loc lhs Scalar rel rhs Scalar hole typ = do
  pure $ Just ([UnificationProblem loc typ Bool],
        [BinaryRelation $ BinaryRelationElabProblem gam loc lhs Scalar rel rhs Scalar hole Bool])
solveNoncanonicalBinaryRelationElabProblem gam loc lhs lhsTy rel rhs rhsTy hole Bool = do
  pure $ Just ([UnificationProblem loc lhsTy Scalar, UnificationProblem loc rhsTy Scalar],
        [BinaryRelation $ BinaryRelationElabProblem gam loc lhs Scalar rel rhs Scalar hole Bool])
solveNoncanonicalBinaryRelationElabProblem gam loc lhs lshTy rel rhs rhsTy hole holeTy = pure Nothing

-- | Σ Γ ⊦ (a : A) `rel` (b : B) ~> ?x : C
--   Assumes that all given `Ty` are normal w.r.t. hole substitution.
-- FIXME: Incomplete
solveBinaryRelationElabProblem :: Context
                               -> Loc
                               -> Semantic.Expr
                               -> Ty
                               -> BinaryRelation
                               -> Semantic.Expr
                               -> Ty
                               -> HoleIdentifier
                               -> Ty
                               -> ElabM (Maybe ([UnificationProblem], [ElabProblem]))
solveBinaryRelationElabProblem gam loc lhs lhsTy rel rhs rhsTy hole holeTy =
  solveCanonicalBinaryRelationElabProblem gam loc lhs lhsTy rel rhs rhsTy hole holeTy >>= \case
    Nothing -> solveNoncanonicalBinaryRelationElabProblem gam loc lhs lhsTy rel rhs rhsTy hole holeTy
    Just ok -> pure (Just ok)

-- | Σ Γ ⊦ Timestamp + Duration ~> ? : Timestamp
-- | Σ Γ ⊦ Timestamp - Duration ~> ? : Timestamp
-- | Σ Γ ⊦ Duration + Duration ~> ? : Duration
-- | Σ Γ ⊦ Scalar `op` Scalar ~> ? : Scalar
-- | Σ Γ ⊦ InstantVector Scalar `op` Scalar ~> ? : InstantVector Scalar
solveCanonicalBinaryArithmeticOpElabProblem :: Context
                                            -> Loc
                                            -> Semantic.Expr
                                            -> Ty
                                            -> BinaryArithmeticOp
                                            -> Semantic.Expr
                                            -> Ty
                                            -> HoleIdentifier
                                            -> Ty
                                            -> ElabM (Maybe ([UnificationProblem], [ElabProblem]))
solveCanonicalBinaryArithmeticOpElabProblem gam loc lhs Timestamp BinaryArithmeticOp.Add rhs Duration hole Timestamp = do
  modify $ updateDefs $
    instantiateExpr hole
      (Semantic.Application (Semantic.Builtin Semantic.FastForward) (NonEmpty.fromList [lhs, rhs]))
  pure $ Just ([], [])
solveCanonicalBinaryArithmeticOpElabProblem gam loc lhs Duration BinaryArithmeticOp.Add rhs Duration hole Duration = do
  modify $ updateDefs $
    instantiateExpr hole
      (Semantic.Application (Semantic.Builtin Semantic.AddDuration) (NonEmpty.fromList [lhs, rhs]))
  pure $ Just ([], [])
solveCanonicalBinaryArithmeticOpElabProblem gam loc lhs Timestamp BinaryArithmeticOp.Sub rhs Duration hole Timestamp = do
  modify $ updateDefs $
    instantiateExpr hole
      (Semantic.Application (Semantic.Builtin Semantic.Rewind) (NonEmpty.fromList [lhs, rhs]))
  pure $ Just ([], [])
solveCanonicalBinaryArithmeticOpElabProblem gam loc lhs Scalar op rhs Scalar hole Scalar = do
  modify $ updateDefs $
    instantiateExpr hole
      (Semantic.Application (Semantic.Builtin (BinaryArithmeticOp.embedScalar op)) (NonEmpty.fromList [lhs, rhs]))
  pure $ Just ([], [])
solveCanonicalBinaryArithmeticOpElabProblem gam loc lhs (InstantVector Scalar)
  op rhs Scalar hole (InstantVector Scalar) = do
  modify $ updateDefs $
    instantiateExpr hole
      (Semantic.Application (Semantic.Builtin (BinaryArithmeticOp.embedInstantVectorScalar op)) (NonEmpty.fromList [lhs, rhs]))
  pure $ Just ([], [])
solveCanonicalBinaryArithmeticOpElabProblem _ _ _ _ _ _ _ _ _ = pure Nothing

solveNoncanonicalBinaryArithmeticOpElabProblem ::
                                      Context
                                   -> Loc
                                   -> Semantic.Expr
                                   -> Ty
                                   -> BinaryArithmeticOp
                                   -> Semantic.Expr
                                   -> Ty
                                   -> HoleIdentifier
                                   -> Ty
                                   -> ElabM (Maybe ([UnificationProblem], [ElabProblem]))
solveNoncanonicalBinaryArithmeticOpElabProblem gam loc lhs Duration BinaryArithmeticOp.Add rhs Timestamp hole typ = do
  pure $ Just ([UnificationProblem loc typ Timestamp], [BinaryArithmeticOp $
    BinaryArithmeticOpElabProblem gam loc rhs Timestamp BinaryArithmeticOp.Add lhs Duration hole Timestamp])
solveNoncanonicalBinaryArithmeticOpElabProblem gam loc lhs Timestamp BinaryArithmeticOp.Add rhs rhsTy hole typ = do
  pure $ Just ([UnificationProblem loc rhsTy Duration, UnificationProblem loc typ Timestamp],
               [BinaryArithmeticOp $ BinaryArithmeticOpElabProblem gam loc lhs Timestamp BinaryArithmeticOp.Add rhs Duration hole Timestamp])
solveNoncanonicalBinaryArithmeticOpElabProblem gam loc lhs lhsTy BinaryArithmeticOp.Add rhs Timestamp hole typ = do
  pure $ Just ([UnificationProblem loc lhsTy Duration, UnificationProblem loc typ Timestamp],
               [BinaryArithmeticOp $ BinaryArithmeticOpElabProblem gam loc lhs Duration BinaryArithmeticOp.Add rhs Timestamp hole Timestamp])
solveNoncanonicalBinaryArithmeticOpElabProblem gam loc lhs Timestamp BinaryArithmeticOp.Sub rhs rhsTy hole typ = do
  pure $ Just ([UnificationProblem loc rhsTy Duration, UnificationProblem loc typ Timestamp],
               [BinaryArithmeticOp $ BinaryArithmeticOpElabProblem gam loc lhs Timestamp BinaryArithmeticOp.Sub rhs Duration hole Timestamp])
solveNoncanonicalBinaryArithmeticOpElabProblem gam loc lhs lhsTy@(InstantVector _) op rhs rhsTy@(InstantVector _) hole Scalar =
  throwError
    [i| Incompatibility: (_ : #{prettyTy Loose lhsTy}) #{prettyOp op} (_ : #{prettyTy Loose rhsTy}) |]
solveNoncanonicalBinaryArithmeticOpElabProblem gam loc lhs Scalar op rhs (InstantVector Scalar) hole typ = do
  pure (Just ([UnificationProblem loc typ (InstantVector Scalar)], [BinaryArithmeticOp $
    BinaryArithmeticOpElabProblem gam loc rhs (InstantVector Scalar) op lhs Scalar hole (InstantVector Scalar)]))
solveNoncanonicalBinaryArithmeticOpElabProblem gam loc lhs lhsTy op rhs rhsTy hole Scalar = do
  pure $ Just ([UnificationProblem loc lhsTy Scalar, UnificationProblem loc lhsTy Scalar],
        [BinaryArithmeticOp $ BinaryArithmeticOpElabProblem gam loc lhs Scalar op rhs Scalar hole Scalar])
solveNoncanonicalBinaryArithmeticOpElabProblem gam loc lhs Scalar op rhs Scalar hole holeTy = do
  pure $ Just ([UnificationProblem loc holeTy Scalar],
        [BinaryArithmeticOp $ BinaryArithmeticOpElabProblem gam loc lhs Scalar op rhs Scalar hole Scalar])
solveNoncanonicalBinaryArithmeticOpElabProblem gam loc lhs lhsTy op rhs rhsTy hole holeTy
  | lhsTy == InstantVector Scalar || rhsTy == InstantVector Scalar =
  pure $ Just
    (
     [UnificationProblem loc holeTy (InstantVector Scalar)],
     [BinaryArithmeticOp $
       BinaryArithmeticOpElabProblem
         gam
         loc
         lhs
         lhsTy
         op
         rhs
         rhsTy
         hole
         (InstantVector Scalar)
     ]
    )
solveNoncanonicalBinaryArithmeticOpElabProblem gam loc lhs lhsTy op rhs rhsTy hole holeTy = pure Nothing

-- | Σ Γ ⊦ (a : A) `op` (b : B) ~> ?x : C
--   Assumes that all given `Ty` are normal w.r.t. hole substitution.
-- FIXME: Incomplete
solveBinaryArithmeticOpElabProblem ::
     Context
  -> Loc
  -> Semantic.Expr
  -> Ty
  -> BinaryArithmeticOp
  -> Semantic.Expr
  -> Ty
  -> HoleIdentifier
  -> Ty
  -> ElabM (Maybe ([UnificationProblem], [ElabProblem]))
solveBinaryArithmeticOpElabProblem gam loc lhs lhsTy op rhs rhsTy hole holeTy =
  solveCanonicalBinaryArithmeticOpElabProblem gam loc lhs lhsTy op rhs rhsTy hole holeTy >>= \case
    Nothing -> solveNoncanonicalBinaryArithmeticOpElabProblem gam loc lhs lhsTy op rhs rhsTy hole holeTy
    Just ok -> pure (Just ok)

-- | Σ Γ ⊦ s ~> ?x : A
--   Assumes that the given `Ty` is normal w.r.t. hole substitution.
solveGeneralElabProblem :: Context -> Surface.Expr -> HoleIdentifier -> Ty -> ElabM ([UnificationProblem], [ElabProblem])
solveGeneralElabProblem gam (mbBinaryRelation -> Just (l, a, r, b)) x typ = do
  expectedA <- freshTyHole
  expectedB <- freshTyHole
  ah <- freshExprHole (Hole expectedA)
  bh <- freshExprHole (Hole expectedB)
  let e1 = General $ GeneralElabProblem gam a ah (Hole expectedA)
  let e2 = General $ GeneralElabProblem gam b bh (Hole expectedB)
  let e3 = BinaryRelation $
        BinaryRelationElabProblem
          gam
          l
          (Semantic.Hole ah)
          (Hole expectedA)
          r
          (Semantic.Hole bh)
          (Hole expectedB)
          x
          typ
  pure ([], [e1, e2, e3])
solveGeneralElabProblem gam (Surface.Number l f) x typ = do
  let u = UnificationProblem l typ Scalar
  modify (updateDefs $ instantiateExpr x (Semantic.Number f))
  pure ([u], [])
solveGeneralElabProblem gam (Surface.Truth l) x typ = do
  let u = UnificationProblem l typ Bool
  modify (updateDefs $ instantiateExpr x (Semantic.Builtin Semantic.True))
  pure ([u], [])
solveGeneralElabProblem gam (Surface.Falsity l) x typ = do
  let u = UnificationProblem l typ Bool
  modify (updateDefs $ instantiateExpr x (Semantic.Builtin Semantic.False))
  pure ([u], [])
solveGeneralElabProblem gam (Surface.Epoch l) x typ = do
  let u = UnificationProblem l typ Timestamp
  modify (updateDefs $ instantiateExpr x (Semantic.Builtin Semantic.Epoch))
  pure ([u], [])
solveGeneralElabProblem gam (Surface.Now l) x typ = do
  let u = UnificationProblem l typ Timestamp
  modify (updateDefs $ instantiateExpr x (Semantic.Builtin Semantic.Now))
  pure ([u], [])
solveGeneralElabProblem gam (Surface.Milliseconds l n) x typ = do
  let u = UnificationProblem l typ Duration
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Milliseconds)
        (NonEmpty.fromList [Semantic.Number (fromIntegral n)])
  pure ([u], [])
solveGeneralElabProblem gam (Surface.Seconds l n) x typ = do
  let u = UnificationProblem l typ Duration
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Seconds)
        (NonEmpty.fromList [Semantic.Number (fromIntegral n)])
  pure ([u], [])
solveGeneralElabProblem gam (Surface.Minutes l n) x typ = do
  let u = UnificationProblem l typ Duration
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Minutes)
        (NonEmpty.fromList [Semantic.Number (fromIntegral n)])
  pure ([u], [])
solveGeneralElabProblem gam (Surface.Hours l n) x typ = do
  let u = UnificationProblem l typ Duration
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Hours)
        (NonEmpty.fromList [Semantic.Number (fromIntegral n)])
  pure ([u], [])
solveGeneralElabProblem gam (Surface.Or l a b) x typ = do
  let u = UnificationProblem l typ Bool
  ax <- freshExprHole Bool
  ay <- freshExprHole Bool
  let e1 = General $ GeneralElabProblem gam a ax Bool
  let e2 = General $ GeneralElabProblem gam b ay Bool
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Or)
        (NonEmpty.fromList [Semantic.Hole ax, Semantic.Hole ay])
  pure ([u], [e1, e2])
solveGeneralElabProblem gam (Surface.And l a b) x typ = do
  let u = UnificationProblem l typ Bool
  ax <- freshExprHole Bool
  ay <- freshExprHole Bool
  let e1 = General $ GeneralElabProblem gam a ax Bool
  let e2 = General $ GeneralElabProblem gam b ay Bool
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.And)
        (NonEmpty.fromList [Semantic.Hole ax, Semantic.Hole ay])
  pure ([u], [e1, e2])
solveGeneralElabProblem gam (Surface.Not l a) x typ = do
  let u = UnificationProblem l typ Bool
  x' <- freshExprHole Bool
  let e1 = General $ GeneralElabProblem gam a x' Bool
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Not)
        (NonEmpty.fromList [Semantic.Hole x'])
  pure ([u], [e1])
solveGeneralElabProblem gam (Surface.Abs l a) x typ = do
  let u = UnificationProblem l typ Scalar
  x' <- freshExprHole Scalar
  let e1 = General $ GeneralElabProblem gam a x' Scalar
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Abs)
        (NonEmpty.fromList [Semantic.Hole x'])
  pure ([u], [e1])
solveGeneralElabProblem gam (Surface.Round l a) x typ = do
  let u = UnificationProblem l typ Scalar
  x' <- freshExprHole Scalar
  let e1 = General $ GeneralElabProblem gam a x' Scalar
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.RoundScalar)
        (NonEmpty.fromList [Semantic.Hole x'])
  pure ([u], [e1])
solveGeneralElabProblem gam (Surface.Increase l a) x typ = do
  let u = UnificationProblem l typ (InstantVector Scalar)
  x' <- freshExprHole (RangeVector Scalar)
  let e1 = General $ GeneralElabProblem gam a x' (RangeVector Scalar)
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Increase)
        (NonEmpty.fromList [Semantic.Hole x'])
  pure ([u], [e1])
solveGeneralElabProblem gam (Surface.Rate l a) x typ = do
  let u = UnificationProblem l typ (InstantVector Scalar)
  x' <- freshExprHole (RangeVector Scalar)
  let e1 = General $ GeneralElabProblem gam a x' (RangeVector Scalar)
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Rate)
        (NonEmpty.fromList [Semantic.Hole x'])
  pure ([u], [e1])
solveGeneralElabProblem gam (Surface.Max l a) x typ = do
  let u = UnificationProblem l typ Scalar
  x' <- freshExprHole (InstantVector Scalar)
  let e1 = General $ GeneralElabProblem gam a x' (InstantVector Scalar)
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Max)
        (NonEmpty.fromList [Semantic.Hole x'])
  pure ([u], [e1])
solveGeneralElabProblem gam (Surface.Min l v) x typ = do
  let u = UnificationProblem l typ Scalar
  vh <- freshExprHole (InstantVector Scalar)
  let e1 = General $ GeneralElabProblem gam v vh (InstantVector Scalar)
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Min)
        (NonEmpty.fromList [Semantic.Hole vh])
  pure ([u], [e1])
solveGeneralElabProblem gam (Surface.Avg l v) x typ = do
  let u = UnificationProblem l typ Scalar
  vh <- freshExprHole (InstantVector Scalar)
  let e1 = General $ GeneralElabProblem gam v vh (InstantVector Scalar)
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Avg)
        (NonEmpty.fromList [Semantic.Hole vh])
  pure ([u], [e1])
solveGeneralElabProblem gam (Surface.AvgOverTime l r) x typ = do
  let u = UnificationProblem l typ (InstantVector Scalar)
  rh <- freshExprHole (RangeVector Scalar)
  let e1 = General $ GeneralElabProblem gam r rh (RangeVector Scalar)
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.AvgOverTime)
        (NonEmpty.fromList [Semantic.Hole rh])
  pure ([u], [e1])
solveGeneralElabProblem gam (Surface.SumOverTime l r) x typ = do
  let u = UnificationProblem l typ (InstantVector Scalar)
  rh <- freshExprHole (RangeVector Scalar)
  let e1 = General $ GeneralElabProblem gam r rh (RangeVector Scalar)
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.AvgOverTime)
        (NonEmpty.fromList [Semantic.Hole rh])
  pure ([u], [e1])
solveGeneralElabProblem gam (Surface.QuantileOverTime l k r) x typ = do
  let u = UnificationProblem l typ (InstantVector Scalar)
  rh <- freshExprHole (RangeVector Scalar)
  let e1 = General $ GeneralElabProblem gam r rh (RangeVector Scalar)
  kh <- freshExprHole Scalar
  let e2 = General $ GeneralElabProblem gam k kh Scalar
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.QuantileOverTime)
        (NonEmpty.fromList [Semantic.Hole kh, Semantic.Hole rh])
  pure ([u], [e1])
solveGeneralElabProblem gam (Surface.QuantileBy loc ls k v) x typ = do
  let u = UnificationProblem loc typ (InstantVector Scalar)
  vh <- freshExprHole (InstantVector Scalar)
  let e1 = General $ GeneralElabProblem gam v vh (InstantVector Scalar)
  kh <- freshExprHole Scalar
  let e2 = General $ GeneralElabProblem gam k kh Scalar
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.QuantileBy)
        (NonEmpty.fromList ([Semantic.Hole kh, Semantic.Hole vh] ++ fmap Semantic.Str (Set.toList ls)))
  pure ([u], [e1, e2])
solveGeneralElabProblem gam (Surface.Range l expr t0 t1 Nothing) x typ = do
  tyh <- freshTyHole
  let u = UnificationProblem l typ (RangeVector (Hole tyh))
  exprh <- freshExprHole (Fun Timestamp (InstantVector (Hole tyh)))
  t0h <- freshExprHole Timestamp
  t1h <- freshExprHole Timestamp
  let e1 = General $ GeneralElabProblem gam expr exprh (Fun Timestamp (InstantVector (Hole tyh)))
  let e2 = General $ GeneralElabProblem gam t0 t0h Timestamp
  let e3 = General $ GeneralElabProblem gam t1 t1h Timestamp
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Range)
        (NonEmpty.fromList [Semantic.Hole exprh, Semantic.Hole t0h, Semantic.Hole t1h])
  pure ([u], [e1, e2, e3])
solveGeneralElabProblem gam (Surface.Range l expr t0 t1 (Just step)) x typ = do
  tyh <- freshTyHole
  let u = UnificationProblem l typ (RangeVector (Hole tyh))
  exprh <- freshExprHole (Fun Timestamp (InstantVector (Hole tyh)))
  t0h <- freshExprHole Timestamp
  t1h <- freshExprHole Timestamp
  steph <- freshExprHole Duration
  let e1 = General $ GeneralElabProblem gam expr exprh (Fun Timestamp (InstantVector (Hole tyh)))
  let e2 = General $ GeneralElabProblem gam t0 t0h Timestamp
  let e3 = General $ GeneralElabProblem gam t1 t1h Timestamp
  let e4 = General $ GeneralElabProblem gam step steph Duration
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Range)
        (NonEmpty.fromList [Semantic.Hole exprh, Semantic.Hole t0h, Semantic.Hole t1h, Semantic.Hole steph])
  pure ([u], [e1, e2, e3, e4])
solveGeneralElabProblem gam (Surface.Fst l t) x typ = do
  tyah <- freshTyHole
  tybh <- freshTyHole
  let u = UnificationProblem l typ (Hole tyah)
  th <- freshExprHole (Ty.Pair (Hole tyah) (Hole tybh))
  let e = General $ GeneralElabProblem gam t th (Ty.Pair (Hole tyah) (Hole tybh))
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Fst)
        (NonEmpty.fromList [Semantic.Hole th])
  pure ([u], [e])
solveGeneralElabProblem gam (Surface.Snd l t) x typ = do
  tyah <- freshTyHole
  tybh <- freshTyHole
  let u = UnificationProblem l typ (Hole tybh)
  th <- freshExprHole (Ty.Pair (Hole tyah) (Hole tybh))
  let e = General $ GeneralElabProblem gam t th (Ty.Pair (Hole tyah) (Hole tybh))
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Application
        (Semantic.Builtin Semantic.Snd)
        (NonEmpty.fromList [Semantic.Hole th])
  pure ([u], [e])
solveGeneralElabProblem gam (Surface.MkPair l a b) x typ = do
  tyah <- freshTyHole
  tybh <- freshTyHole
  let u = UnificationProblem l typ (Ty.Pair (Hole tyah) (Hole tybh))
  ah <- freshExprHole (Hole tyah)
  bh <- freshExprHole (Hole tybh)
  let e1 = General $ GeneralElabProblem gam a ah (Hole tyah)
  let e2 = General $ GeneralElabProblem gam b bh (Hole tybh)
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.MkPair
        (Semantic.Hole ah)
        (Semantic.Hole bh)
  pure ([u], [e1, e2])
solveGeneralElabProblem gam (Surface.Lambda l v scope) x typ = do
  tyah <- freshTyHole
  tybh <- freshTyHole
  checkFresh gam v
  let u = UnificationProblem l typ (Fun (Hole tyah) (Hole tybh))
  scopeh <- freshExprHole (Hole tybh)
  let e = General $ GeneralElabProblem (Snoc gam (LambdaBinding v (Hole tyah))) scope scopeh (Hole tybh)
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Lambda
        v
        (Semantic.Hole scopeh)
  pure ([u], [e])
solveGeneralElabProblem gam (Surface.Let l v rhs scope) x typ = do
  tyah <- freshTyHole
  tybh <- freshTyHole
  checkFresh gam v
  let u = UnificationProblem l typ (Hole tybh)
  rhsh <- freshExprHole (Hole tyah)
  scopeh <- freshExprHole (Hole tybh)
  let e1 = General $ GeneralElabProblem gam rhs rhsh (Hole tyah)
  let e2 = General $ GeneralElabProblem (Snoc gam (LetBinding v (Semantic.Hole rhsh) (Hole tyah))) scope scopeh (Hole tybh)
  modify $ updateDefs $
    instantiateExpr x $
      Semantic.Let
        v
        (Semantic.Hole rhsh)
        (Semantic.Hole scopeh)
  pure ([u], [e1, e2])
solveGeneralElabProblem gam (mbBinaryArithmeticOp -> Just (loc, left, op, right)) x typ = do
  tyah <- freshTyHole
  tybh <- freshTyHole
  lefth <- freshExprHole typ
  righth <- freshExprHole typ
  let e1 = General $ GeneralElabProblem gam left lefth (Hole tyah)
  let e2 = General $ GeneralElabProblem gam right righth (Hole tybh)
  let e3 = BinaryArithmeticOp $
       BinaryArithmeticOpElabProblem gam loc (Semantic.Hole lefth) (Hole tyah) op (Semantic.Hole righth) (Hole tybh) x typ
  pure ([], [e1, e2, e3])
solveGeneralElabProblem gam (Surface.Variable l v) x typ | Just b <- find (\b -> Types.identifier b == v) gam = do
  modify $ updateDefs $ instantiateExpr x $ Semantic.Variable v
  pure ([UnificationProblem l typ (Types.ty b)], [])
-- Assumes that all variables into the store (= metrics) have type (Timestamp -> Scalar)
solveGeneralElabProblem gam (Surface.Variable l v) x typ = do
  modify $ updateDefs $ instantiateExpr x $ Semantic.Variable v
  pure ([UnificationProblem l typ (Fun Timestamp (InstantVector Scalar))], [])
solveGeneralElabProblem gam (Surface.Filter l f v) h hty = do
  argTy <- freshTyHole
  fh <- freshExprHole (Fun (Hole argTy) Bool)
  vh <- freshExprHole (InstantVector (Hole argTy))
  modify $ updateDefs $ instantiateExpr h $ Semantic.Application (Semantic.Builtin Semantic.Filter)
    (NonEmpty.fromList [Semantic.Hole fh, Semantic.Hole vh])
  pure ([UnificationProblem l hty (InstantVector (Hole argTy))],
        [General $ GeneralElabProblem gam f fh (Fun (Hole argTy) Bool),
         General $ GeneralElabProblem gam v vh (InstantVector (Hole argTy))
        ]
       )
solveGeneralElabProblem gam (Surface.FilterByLabel loc ls v) h hty = do
  argTy <- freshTyHole
  vh <- freshExprHole (InstantVector (Hole argTy))
  modify $ updateDefs $ instantiateExpr h $ Semantic.Application (Semantic.Builtin Semantic.FilterByLabel)
    (NonEmpty.fromList (Semantic.Hole vh : elabLabelConstraints (Set.toList ls)))
  pure ([UnificationProblem loc hty (InstantVector (Hole argTy))],
        [General $ GeneralElabProblem gam v vh (InstantVector (Hole argTy))]
       )
  where
    elabLabelConstraint :: LabelConstraint -> Semantic.Expr
    elabLabelConstraint (LabelConstraintEq (x, v)) =
      Semantic.MkPair (Semantic.Str x) (Semantic.Str v)
    elabLabelConstraint (LabelConstraintNotEq (x, v)) =
      Semantic.MkPair
        (Semantic.Str x)
        (Semantic.Application (Semantic.Builtin Semantic.Inv) (Semantic.Str v :| []))

    elabLabelConstraints :: [LabelConstraint] -> [Semantic.Expr]
    elabLabelConstraints = fmap elabLabelConstraint
solveGeneralElabProblem gam (Surface.Map l f v) h hty = do
  aTy <- freshTyHole
  bTy <- freshTyHole
  fh <- freshExprHole (Fun (Hole aTy) (Hole bTy))
  vh <- freshExprHole (InstantVector (Hole aTy))
  modify $ updateDefs $ instantiateExpr h $ Semantic.Application (Semantic.Builtin Semantic.Map)
    (NonEmpty.fromList [Semantic.Hole fh, Semantic.Hole vh])
  pure ([UnificationProblem l hty (InstantVector (Hole bTy))],
        [General $ GeneralElabProblem gam f fh (Fun (Hole aTy) (Hole bTy)),
         General $ GeneralElabProblem gam v vh (InstantVector (Hole aTy))
        ]
       )
solveGeneralElabProblem gam (Surface.Join l v u) h hty = do
  aTy <- freshTyHole
  bTy <- freshTyHole
  vh <- freshExprHole (InstantVector (Hole aTy))
  uh <- freshExprHole (InstantVector (Hole bTy))
  modify $ updateDefs $ instantiateExpr h $ Semantic.Application (Semantic.Builtin Semantic.Join)
    (NonEmpty.fromList [Semantic.Hole vh, Semantic.Hole uh])
  pure ([UnificationProblem l hty (InstantVector (Ty.Pair (Hole aTy) (Hole bTy)))],
        [General $ GeneralElabProblem gam v vh (InstantVector (Hole aTy)),
         General $ GeneralElabProblem gam u uh (InstantVector (Hole bTy))
        ]
       )
solveGeneralElabProblem gam (Surface.Unless l v u) h hty = do
  aTy <- freshTyHole
  bTy <- freshTyHole
  vh <- freshExprHole (InstantVector (Hole aTy))
  uh <- freshExprHole (InstantVector (Hole bTy))
  modify $ updateDefs $ instantiateExpr h $ Semantic.Application (Semantic.Builtin Semantic.Unless)
    (NonEmpty.fromList [Semantic.Hole vh, Semantic.Hole uh])
  pure ([UnificationProblem l hty (InstantVector (Hole aTy))],
        [General $ GeneralElabProblem gam v vh (InstantVector (Hole aTy)),
         General $ GeneralElabProblem gam u uh (InstantVector (Hole bTy))
        ]
       )
solveGeneralElabProblem gam (Surface.App l f e) h hty = do
  aTy <- freshTyHole
  bTy <- freshTyHole
  fh <- freshExprHole (Fun (Hole aTy) (Hole bTy))
  eh <- freshExprHole (Hole aTy)
  modify $ updateDefs $ instantiateExpr h $ Semantic.Application (Semantic.Hole fh)
    (NonEmpty.fromList [Semantic.Hole eh])
  pure ([UnificationProblem l hty (Hole bTy)],
        [General $ GeneralElabProblem gam f fh (Fun (Hole aTy) (Hole bTy)),
         General $ GeneralElabProblem gam e eh (Hole aTy)
        ]
       )
solveGeneralElabProblem gam (Surface.ToScalar l t) h hty = do
  tTy <- freshTyHole
  th <- freshExprHole (Hole tTy)
  pure ([UnificationProblem l hty Scalar],
        [
         General $ GeneralElabProblem gam t th (Hole tTy)
        ,
         ToScalar $ ToScalarElabProblem gam l (Semantic.Hole th) (Hole tTy) h
        ])
solveGeneralElabProblem gam s x typ = throwError [i| Do not know how to elaborate: #{show s} |]

solveElabProblem :: ElabProblem -> ElabM (Maybe ([UnificationProblem], [ElabProblem]))
solveElabProblem (General (GeneralElabProblem gam s h typ)) = do
  defs <- defs <$> get
  let typ' = resolveTy defs typ
  let gam' = resolveContext defs gam
  Just <$> solveGeneralElabProblem gam' s h typ'
solveElabProblem (BinaryArithmeticOp (BinaryArithmeticOpElabProblem gam loc lhs lhsTy op rhs rhsTy hole holeTy)) = do
  defs <- defs <$> get
  let gam' = resolveContext defs gam
  let lhsTy' = resolveTy defs lhsTy
  let rhsTy' = resolveTy defs rhsTy
  let holeTy' = resolveTy defs holeTy
  solveBinaryArithmeticOpElabProblem gam' loc lhs lhsTy' op rhs rhsTy' hole holeTy'
solveElabProblem (BinaryRelation (BinaryRelationElabProblem gam loc lhs lhsTy rel rhs rhsTy hole holeTy)) = do
  defs <- defs <$> get
  let gam' = resolveContext defs gam
  let lhsTy' = resolveTy defs lhsTy
  let rhsTy' = resolveTy defs rhsTy
  let holeTy' = resolveTy defs holeTy
  solveBinaryRelationElabProblem gam' loc lhs lhsTy' rel rhs rhsTy' hole holeTy'
solveElabProblem (ToScalar (ToScalarElabProblem gam loc t tTy hole)) = do
  defs <- defs <$> get
  let gam' = resolveContext defs gam
  let tTy' = resolveTy defs tTy
  solveToScalarElabProblem gam' loc t tTy' hole

solveH :: Bool -> SnocList ElabProblem -> [ElabProblem] -> ElabM (Bool, SnocList ElabProblem)
solveH progress stuck [] = pure (progress, stuck)
solveH progress stuck (p : ps) = do
  solveElabProblem p >>= \case
    Nothing -> solveH progress (Snoc stuck p) ps
    Just (us, es) -> runUnifyM (Unify.solve us) >> solveH True stuck (es ++ ps)

solve :: [ElabProblem] -> ElabM ()
solve [] = pure ()
solve problems =
  solveH False Lin problems >>= \case
    (True, stuck) -> solve (toList stuck)
    (False, stuck) -> do
      defs <- defs <$> get
      let toShow = fmap (prettyElabProblem . evalElabProblem defs) (toList stuck)
      throwError $ "Can't solve elaboration problems:\n" <>
        Text.intercalate "\n" toShow

elab :: Surface.Expr -> ElabM Semantic.Expr
elab expr = do
  typ <- freshTyHole
  t <- freshExprHole (Hole typ)
  solve [General $ GeneralElabProblem Lin expr t (Hole typ)]
  ds <- getDefs <$> get
  pure $ resolveExpr' ds (Semantic.Hole t)
