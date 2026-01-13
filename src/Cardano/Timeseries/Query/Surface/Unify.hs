{- HLINT ignore "Use newtype instead of data" -}
module Cardano.Timeseries.Query.Surface.Unify(UnificationProblem(..), St(..), UnifyM, eval, evalBinding, evalContext, unify, solve) where
import           Cardano.Timeseries.Query.Surface.Types (Def (..), Defs,
                                                         Ty (..), instantiateTy, Context, Binding (..))
import           Cardano.Timeseries.Query.Types         (HoleIdentifier)
import           Control.Monad                          (join)
import           Control.Monad.Except                   (ExceptT, throwError)
import           Control.Monad.State.Strict             (State, get, lift,
                                                         state)
import qualified Data.Map.Strict                        as Map
import           Data.Text                              (Text, pack)

-- | A = B type
data UnificationProblem = UnificationProblem {
  lhs :: Ty,
  rhs :: Ty
}

data St = St {
  defs :: Defs
}

updateDefs :: Defs -> St -> St
updateDefs defs (St _) = St defs

type Error = Text

type UnifyM a = ExceptT Error (State St) a

unifyHole :: HoleIdentifier -> Ty -> UnifyM ()
unifyHole x rhs = do
  occurs x rhs >>= \case
    True -> error $ "[INTERNAL ERROR] Occurs check failed for " <> show x <> " and " <> show rhs
    False -> state $ \st ->
     ((), updateDefs (instantiateTy x rhs (defs st)) st)

-- | Assume the types are head-neutral (i.e. resolved holes have been substituted in if the hole is the outer expression)
unifyNu :: Ty -> Ty -> UnifyM [UnificationProblem]
unifyNu (Fun a b) (Fun a' b') = pure [UnificationProblem a a', UnificationProblem b b']
unifyNu (InstantVector a) (InstantVector a') = pure [UnificationProblem a a']
unifyNu (RangeVector a) (RangeVector a') = pure [UnificationProblem a a']
unifyNu Scalar Scalar = pure []
unifyNu Bool Bool = pure []
unifyNu Timestamp Timestamp = pure []
unifyNu Duration Duration = pure []
unifyNu (Pair a b) (Pair a' b') = pure [UnificationProblem a a', UnificationProblem b b']
unifyNu (Hole x) (Hole y) | x == y = pure []
unifyNu (Hole x) ty = [] <$ unifyHole x ty
unifyNu ty (Hole y) = [] <$ unifyHole y ty
unifyNu lhs rhs =
  -- TODO: pretty-printing of lhs and rhs
  throwError $ pack $ "Can't solve unification constraint: " <> show lhs <> " = " <> show rhs

-- | Check if the given hole identifier occurs in the given type.
occurs :: HoleIdentifier -> Ty -> UnifyM Bool
occurs x ty = occursNu x =<< eval ty

-- | `Ty` is assumed to be normal w.r.t. hole substitution.
occursNu :: HoleIdentifier -> Ty -> UnifyM Bool
occursNu x (InstantVector ty) = occursNu x ty
occursNu x (RangeVector ty)   = occursNu x ty
occursNu x (Fun ty ty')       = (||) <$> occursNu x ty <*> occursNu x ty'
occursNu x (Pair ty ty')      = (||) <$> occursNu x ty <*> occursNu x ty'
occursNu x Scalar             = pure False
occursNu x Bool               = pure False
occursNu x Timestamp          = pure False
occursNu x Duration           = pure False
occursNu x (Hole x')          = pure (x == x')

-- | Computes the normal form of `Ty` (unfolds head holes).
eval :: Ty -> UnifyM Ty
eval (Hole x) = do
  ds <- defs <$> get
  case Map.lookup x ds of
    Just (TyHoleInst rhs) -> eval rhs
    Just _  -> pure $ Hole x
    Nothing -> error $ "[INTERNAL ERROR] Can't find hole in Σ: " <> show x
eval (InstantVector ty) = InstantVector <$> eval ty
eval (RangeVector ty) = RangeVector <$> eval ty
eval (Fun ty ty') = Fun <$> eval ty <*> eval ty'
eval (Pair ty ty') = Pair <$> eval ty <*> eval ty'
eval Scalar = pure Scalar
eval Timestamp = pure Timestamp
eval Duration = pure Duration
eval Bool = pure Bool

evalBinding :: Binding -> UnifyM Binding
evalBinding (LetBinding x rhs typ) =
  LetBinding x rhs <$> eval typ
evalBinding (LambdaBinding x typ) =
  LambdaBinding x <$> eval typ

evalContext :: Context -> UnifyM Context
evalContext = traverse evalBinding

unify :: Ty -> Ty -> UnifyM [UnificationProblem]
unify lhs rhs = join $ unifyNu <$> eval lhs <*> eval rhs

solve :: [UnificationProblem] -> UnifyM ()
solve [] = pure ()
solve (UnificationProblem lhs rhs : rest) = do
  new <- unify lhs rhs
  solve (new ++ rest)
