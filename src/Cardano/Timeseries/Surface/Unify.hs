{- HLINT ignore "Use newtype instead of data" -}
module Cardano.Timeseries.Surface.Unify(UnificationProblem(..), St(..), UnifyM, unify, solve) where
import           Cardano.Timeseries.Query.Types    (HoleIdentifier)
import           Cardano.Timeseries.Typing         (Binding (..), Context,
                                                    Def (..), Defs, Ty (..),
                                                    instantiateTy)
import           Cardano.Timeseries.Resolve
import           Control.Monad                     (join)
import           Control.Monad.Except              (ExceptT, throwError)
import           Control.Monad.State.Strict        (State, get, lift, state)
import qualified Data.Map.Strict                   as Map
import           Data.Text                         (Text, pack)

-- | A = B type
--   An equation between two query types containing holes.
--   Unification is an algorithm of finding unique solutions to such equations.
data UnificationProblem = UnificationProblem {
  lhs :: Ty,
  rhs :: Ty
}

data St = St {
  defs :: Defs
}

type Error = Text

type UnifyM a = ExceptT Error (State St) a

updateDefs :: Defs -> St -> St
updateDefs defs (St _) = St defs

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
occurs x ty = do
  ds <- defs <$> get
  occursNu x (resolveTy ds ty)

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

unify :: Ty -> Ty -> UnifyM [UnificationProblem]
unify lhs rhs = do
  ds <- defs <$> get
  unifyNu (resolveTy ds lhs) (resolveTy ds rhs)

-- | Solve the list of unification problems, instantiating holes in the process.
--   If a problem doesn't have a (unique) solution, throw an error.
solve :: [UnificationProblem] -> UnifyM ()
solve [] = pure ()
solve (UnificationProblem lhs rhs : rest) = do
  new <- unify lhs rhs
  solve (new ++ rest)
