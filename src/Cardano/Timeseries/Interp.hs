{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-typed-holes #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ViewPatterns         #-}
module Cardano.Timeseries.Interp(interp) where
import           Cardano.Timeseries.Domain.Identifier    (Identifier (..))
import           Cardano.Timeseries.Domain.Instant       (Instant (Instant),
                                                          InstantVector, share)
import qualified Cardano.Timeseries.Domain.Instant       as Domain
import qualified Cardano.Timeseries.Domain.Instant       as Instant
import           Cardano.Timeseries.Domain.Interval
import           Cardano.Timeseries.Domain.Timeseries    (Timeseries (Timeseries),
                                                          TimeseriesVector,
                                                          eachNewest,
                                                          eachOldest,
                                                          superseries,
                                                          transpose)
import qualified Cardano.Timeseries.Domain.Timeseries    as Timeseries
import           Cardano.Timeseries.Domain.Types         (Label, Labelled,
                                                          MetricIdentifier,
                                                          SeriesIdentifier,
                                                          Timestamp)
import           Cardano.Timeseries.Interp.Value         as Value
import           Cardano.Timeseries.Query.BinaryRelation (BinaryRelation,
                                                          embedScalar,
                                                          mbBinaryRelationInstantVector,
                                                          mbBinaryRelationScalar)
import qualified Cardano.Timeseries.Query.BinaryRelation as BinaryRelation
import           Cardano.Timeseries.Query.Expr           as Expr
import           Cardano.Timeseries.Store                (Store (metrics),
                                                          earliest, latest)
import qualified Cardano.Timeseries.Store                as Store
import           Cardano.Timeseries.Util                 (maybeToEither,
                                                          safeToDouble,
                                                          safeToWord64)

import           Control.Monad                           (filterM, (<=<))
import           Control.Monad.Except                    (ExceptT, liftEither,
                                                          throwError)
import           Control.Monad.State                     (get, put)
import           Control.Monad.State.Strict              (State)
import           Control.Monad.Trans                     (lift)
import           Data.List                               (find)
import           Data.List.NonEmpty                      (fromList, toList)
import           Data.Map.Strict                         (Map)
import qualified Data.Map.Strict                         as Map
import           Data.Maybe                              (fromJust)
import           Data.Set                                (Set, isSubsetOf,
                                                          member)
import qualified Data.Set                                as Set
import           Data.Word                               (Word64)
import           GHC.Base                                (NonEmpty ((:|)))

import           Cardano.Timeseries.Interp.Expect
import           Cardano.Timeseries.Interp.Statistics
import           Cardano.Timeseries.Query.Types          (Error, QueryM)
import           Data.Foldable                           (for_, traverse_)
import           Data.Function                           (on)
import           Data.List                               (groupBy)
import qualified Data.List                               as List
import           Statistics.Function                     (minMax)
import           Statistics.Quantile                     (cadpw, quantile)
import           Statistics.Sample                       (mean)
import qualified Cardano.Timeseries.Query.BinaryArithmeticOp as BinaryArithmeticOp

interpJoin :: (a -> b -> c) -> InstantVector a -> InstantVector b -> Either Error (InstantVector c)
interpJoin _ [] _ = Right []
interpJoin f (inst@(Domain.Instant ls t v) : xs) other = do
  Domain.Instant _ _ v' <- maybeToEither ("No matching label: " <> show ls) $ find (share inst) other
  rest <- interpJoin f xs other
  Right (Domain.Instant ls t (f v v') : rest)

interpRange :: FunctionValue -> Interval -> Word64 -> QueryM (TimeseriesVector Value)
interpRange f Interval{..} rate = transpose <$> sample start end where

  sample :: Timestamp -> Timestamp -> QueryM [InstantVector Value]
  sample t max | t > max = pure []
  sample t max = (:) <$> (expectInstantVector <=< f) (Value.Timestamp t) <*> sample (t + rate) max

interpVariable :: Store s Double => s -> MetricIdentifier -> Value -> QueryM Value
interpVariable store x t = do
  t <- expectTimestamp t
  pure (Value.InstantVector (fmap (fmap Value.Scalar) (Store.evaluate store x t)))

interpLabel :: Expr -> QueryM Label
interpLabel (Expr.Str x) = pure x
interpLabel _            = throwError "Unexpected expression: expected a label"

interpLabelInst :: Expr -> QueryM (Labelled (Bool, String))
interpLabelInst (Expr.MkPair (Expr.Str k) (Expr.Str v)) = pure (k, (Prelude.True, v))
interpLabelInst (Expr.MkPair (Expr.Str k) (Expr.Application (Expr.Builtin Expr.Inv) (Expr.Str v :| []))) =
  pure (k, (Prelude.False, v))
interpLabelInst _ = throwError "Unexpected expression: expected a label instance"

interpLabelInsts :: [Expr] -> QueryM (Set (Labelled String), Set (Labelled String))
interpLabelInsts ls = do
  (sub, notsub) <- List.partition cond <$> traverse interpLabelInst ls
  pure (Set.fromList $ fmap extract sub, Set.fromList $ fmap extract notsub) where
    cond (_, (b, _)) = b
    extract (x, (_, y)) = (x, y)

interpLabels :: [Expr] -> QueryM (Set Label)
interpLabels ls = Set.fromList <$> traverse interpLabel ls

interpQuantileBy :: Set Label -> Double -> InstantVector Double -> Timestamp -> QueryM (InstantVector Double)
interpQuantileBy ls k vs now =
  let groups = groupBy (on (==) (superseries ls . Instant.labels)) vs
      quantiles = fmap (\g -> (superseries ls (Instant.labels (head g)), quantile cadpw (floor (k * 100)) 100 (Instant.toVector g)))
                       groups in
  pure $ fmap (\(idx, v) -> Instant idx now v) quantiles

interpFilter :: FunctionValue -> InstantVector Value -> QueryM (InstantVector Value)
interpFilter f = filterM pred where
  pred :: Instant Value -> QueryM Bool
  pred inst = (expectBoolean <=< f) (Instant.value inst)

interpMap :: FunctionValue -> InstantVector Value -> QueryM (InstantVector Value)
interpMap f = traverse (traverse f)

interpRate :: TimeseriesVector Double -> QueryM (InstantVector Double)
interpRate v = do
  min <- liftEither $ maybeToEither "Can't compute rate" (eachOldest v)
  max <- liftEither $ maybeToEither "Can't compute rate" (eachNewest v)
  pure $ zipWith compute min max where

  compute :: Instant Double -> Instant Double -> Instant Double
  compute min max =
    let v = (Instant.value max - Instant.value min) / fromIntegral (Instant.timestamp max - Instant.timestamp min) in
    Instant (Instant.labels min) (Instant.timestamp max) v

interpIncrease :: TimeseriesVector Double -> QueryM (InstantVector Double)
interpIncrease v = liftEither $ do
  min <- maybeToEither "Can't compute rate" (eachOldest v)
  max <- maybeToEither "Can't compute rate" (eachNewest v)
  Right $ zipWith compute min max where

  compute :: Instant Double -> Instant Double -> Instant Double
  compute min max =
    let v = Instant.value max - Instant.value min in
    Instant (Instant.labels min) (Instant.timestamp max) v

-- | (v `R` s) ≡ filter (\x -> x `R` s) v
-- | where v : InstantVector Scalar
-- |       s : Scalar
interpFilterBinaryRelation :: Store s Double
                           => s
                           -> Map Identifier Value
                           -> Expr
                           -> BinaryRelation
                           -> Expr
                           -> Timestamp
                           -> QueryM Value
interpFilterBinaryRelation store env v rel k now = do
  nextVarIdx <- lift get
  lift (put (1 + nextVarIdx))
  interp store env
    (Application
      (Builtin Filter)
      (fromList
        [
          Lambda
            (Machine nextVarIdx)
            (Application (Builtin (embedScalar rel)) (fromList [Variable (Machine nextVarIdx), k]))
        ,
          v
        ]
      )
    )
    now

interp :: Store s Double => s -> Map Identifier Value -> Expr -> Timestamp -> QueryM Value
interp _ env (Expr.Number x) _ = do
  pure (Value.Scalar x)
interp store env (Expr.Variable x) _ =
  case Map.lookup x env of
    Just v -> pure v
    Nothing ->
      case x of
        User x | member x (metrics store) ->
          pure $ Value.Function (interpVariable store x)
        _ ->
          throwError ("Undefined variable: " <> show x)
interp store env (Application (Builtin Expr.Earliest) (Expr.Variable x :| [])) _ =
  case x of
    User (earliest store -> Just x) ->
      pure $ Value.Timestamp x
    _ ->
      throwError ("Undefined variable: " <> show x)
interp store env (Application (Builtin Expr.Latest) (Expr.Variable x :| [])) _ =
  case x of
    User (latest store -> Just x) ->
      pure $ Value.Timestamp x
    _ ->
      throwError ("Undefined variable: " <> show x)
interp _ env (Builtin Now) now = pure (Timestamp (fromIntegral now))
interp _ env (Builtin Epoch) now = pure (Timestamp 0)
interp store env (Lambda x body) now = pure $ Value.Function $ \v ->
  interp store (Map.insert x v env) body now
interp store env (Let x rhs body) now = do
  v <- interp store env rhs now
  interp store (Map.insert x v env) body now
interp store env (Application (Builtin FastForward) (toList -> [t, d])) now = do
  t <- interp store env t now >>= expectTimestamp
  d <- interp store env d now >>= expectDuration
  pure (Value.Timestamp (t + d))
interp store env (Application (Builtin FilterByLabel) (s :| rest)) now = do
  s <- interp store env s now >>= expectInstantVector
  (mustBe, mustNotBe) <- interpLabelInsts rest
  pure $
    Value.InstantVector $
     flip filter s $ \i ->
       (&&)
         (mustBe `isSubsetOf` Instant.labels i)
         (Set.null (mustNotBe `Set.intersection` Instant.labels i))
interp store env (Application (Builtin Unless) (u :| [v])) now = do
  u <- interp store env u now >>= expectInstantVector
  v <- interp store env v now >>= expectInstantVector
  let vls = Set.fromList (map Instant.labels v)
  pure (Value.InstantVector (filter (\i -> not (member (Instant.labels i) vls)) u))
interp store env (Application (Builtin Filter) (toList -> [f, t])) now = do
  f <- interp store env f now >>= expectFunction
  t <- interp store env t now >>= expectInstantVector
  Value.InstantVector <$> interpFilter f t
interp store env (Application (Builtin Join) (toList -> [a, b])) now = do
  a <- interp store env a now >>= expectInstantVector
  b <- interp store env b now >>= expectInstantVector
  Value.InstantVector <$> liftEither (interpJoin Value.Pair a b)
interp store env (Application (Builtin Map) (toList -> [f, x])) now = do
  f <- interp store env f now >>= expectFunction
  x <- interp store env x now >>= expectInstantVector
  Value.InstantVector <$> interpMap f x
interp store env (Application (Builtin Range) (toList -> [s, a, b])) now = do
  s <- interp store env s now >>= expectFunction
  a <- interp store env a now >>= expectTimestamp
  b <- interp store env b now >>= expectTimestamp
  RangeVector <$> interpRange s (Interval a b) (15 * 1000)
interp store env (Application (Builtin Range) (toList -> [s, a, b, r])) now = do
  s <- interp store env s now >>= expectFunction
  a <- interp store env a now >>= expectTimestamp
  b <- interp store env b now >>= expectTimestamp
  r <- interp store env r now >>= expectDuration
  RangeVector <$> interpRange s (Interval a b) r
interp store env (Application (Builtin Rewind) (toList -> [t, d])) now = do
  t <- interp store env t now >>= expectTimestamp
  d <- interp store env d now >>= expectDuration
  pure (Timestamp (t - d))
interp store env (Application (Builtin BoolToScalar) (t :| [])) now = do
  t <- interp store env t now >>= expectBoolean
  pure (Scalar (if t then 1 else 0))
interp store env (Application (Builtin InstantVectorToScalar) (t :| [])) now = do
  t <- interp store env t now >>= expectInstantVectorBool
  pure (Value.InstantVector (fmap (\x -> Value.Scalar (if x then 1.0 else 0.0)) <$> t))
interp store env (Application (Builtin TimestampToScalar) (t :| [])) now = do
  t <- interp store env t now >>= expectTimestamp
  pure (Scalar (fromIntegral t))
interp store env (Application (Builtin DurationToScalar) (t :| [])) now = do
  t <- interp store env t now >>= expectDuration
  pure (Scalar (fromIntegral t))
interp _ env (Application (Builtin Milliseconds) (Expr.Number t :| [])) _ =
  Duration <$> expectWord64 t
interp _ env (Application (Builtin Seconds) (Expr.Number t :| [])) _ =
  Duration . (1000 *) <$> expectWord64 t
interp _ env (Application (Builtin Minutes) (Expr.Number t :| [])) _ =
  Duration . (60 * 1000 *) <$> expectWord64 t
interp _ env (Application (Builtin Hours) (Expr.Number t :| [])) _ =
  Duration . (60 * 60 * 1000 *) <$> expectWord64 t
interp store env (Application (Builtin (BinaryArithmeticOp.mbBinaryArithmeticOpInstantVectorScalar -> Just op))
                 (toList -> [a, b])) now = do
  va <- interp store env a now >>= expectInstantVectorScalar
  vb <- interp store env b now >>= expectInstantVectorScalar
  v <- liftEither (interpJoin (BinaryArithmeticOp.materializeScalar op) va vb)
  pure (Value.InstantVector (fmap (fmap Value.Scalar) v))
interp store env (Application (Builtin Quantile) (toList -> [k, expr])) now = do
  k <- interp store env k now >>= expectScalar
  v <- interp store env expr now >>= expectInstantVectorScalar
  pure $ Value.Scalar $ quantile cadpw (floor (k * 100)) 100 (Instant.toVector v)
interp store env (Application (Builtin QuantileBy) (toList -> k : expr : ls)) now = do
  k <- interp store env k now >>= expectScalar
  v <- interp store env expr now >>= expectInstantVectorScalar
  ls <- interpLabels ls
  Value.InstantVector . fmap (fmap Value.Scalar) <$> interpQuantileBy ls k v now
interp store env (Application (Builtin QuantileOverTime) (toList -> [k, expr])) now = do
  k <- interp store env k now >>= expectScalar
  v <- interp store env expr now >>= expectRangeVectorScalar
  pure $ Value.InstantVector (fmap Value.Scalar <$> quantileRangeVector k v)
interp store env (Application (Builtin Rate) (r :| [])) now = do
  r <- interp store env r now >>= expectRangeVectorScalar
  -- TODO: PromQL's rate() performs linear regression to extrapolate the samples to the bounds
  r <- interpRate r
  pure (Value.InstantVector (fmap (fmap Value.Scalar) r))
interp store env (Application (Builtin Increase) (r :| [])) now = do
  r <- interp store env r now >>= expectRangeVectorScalar
  -- TODO: PromQL's increase() performs linear regression to extrapolate the samples to the bounds
  r <- interpIncrease r
  pure (Value.InstantVector (fmap (fmap Value.Scalar) r))
interp store env (Application (Builtin Avg) (expr :| [])) now = do
  v <- interp store env expr now >>= expectInstantVectorScalar
  pure $ Value.Scalar $ mean (Instant.toVector v)
interp store env (Application (Builtin Max) (expr :| [])) now = do
  v <- interp store env expr now >>= expectInstantVectorScalar
  pure $ Value.Scalar $ snd $ minMax (Instant.toVector v)
interp store env (Application (Builtin Min) (expr :| [])) now = do
  v <- interp store env expr now >>= expectInstantVectorScalar
  pure $ Value.Scalar $ fst $ minMax (Instant.toVector v)
interp store env (Application (Builtin AvgOverTime) (expr :| [])) now = do
  v <- interp store env expr now >>= expectRangeVectorScalar
  pure $ Value.InstantVector (fmap Value.Scalar <$> avgOverTime now v)
interp store env (Application (Builtin SumOverTime) (expr :| [])) now = do
  v <- interp store env expr now >>= expectRangeVectorScalar
  pure $ Value.InstantVector (fmap Value.Scalar <$> sumOverTime now v)
interp store env (MkPair a b) now = do
  va <- interp store env a now
  vb <- interp store env b now
  pure $ Value.Pair va vb
interp store env (Application (Builtin Fst) (t :| [])) now = do
  (a, _) <- interp store env t now >>= expectPair
  pure a
interp store env (Application (Builtin Snd) (t :| [])) now = do
  (_, b) <- interp store env t now >>= expectPair
  pure b
interp store env (Builtin Expr.True) now = do
  pure Truth
interp store env (Builtin Expr.False) now = do
  pure Falsity
interp store env (Application (Builtin Expr.And) (toList -> [a, b])) now = do
  va <- interp store env a now >>= expectBoolean
  vb <- interp store env b now >>= expectBoolean
  pure (fromBool (va && vb))
interp store env (Application (Builtin Expr.Or) (toList -> [a, b])) now = do
  va <- interp store env a now >>= expectBoolean
  vb <- interp store env b now >>= expectBoolean
  pure (fromBool (va || vb))
interp store env (Application (Builtin Expr.Not) (t :| [])) now = do
  vt <- interp store env t now >>= expectBoolean
  pure (fromBool (not vt))
interp store env (Application (Builtin Expr.EqBool) (toList -> [a, b])) now = do
  va <- interp store env a now >>= expectBoolean
  vb <- interp store env b now >>= expectBoolean
  pure (fromBool (va == vb))
interp store env (Application (Builtin (mbBinaryRelationScalar -> Just rel)) (toList -> [a, b])) now = do
  va <- interp store env a now >>= expectScalar
  vb <- interp store env b now >>= expectScalar
  pure (fromBool (BinaryRelation.materializeScalar rel va vb))
interp store env (Application (Builtin (BinaryArithmeticOp.mbBinaryArithmeticOpScalar -> Just op))
                   (toList -> [a, b])) now = do
  va <- interp store env a now >>= expectScalar
  vb <- interp store env b now >>= expectScalar
  pure (Value.Scalar (BinaryArithmeticOp.materializeScalar op va vb))
interp store env (Application (Builtin Expr.Abs) (x :| [])) now = do
  x <- interp store env x now >>= expectScalar
  pure (Value.Scalar (abs x))
interp store env (Application (Builtin Expr.RoundScalar) (x :| [])) now = do
  x <- interp store env x now >>= expectScalar
  pure (Value.Scalar (fromIntegral (round x :: Int)))
interp store env (Application f (e :| [])) now = do
  f <- interp store env f now >>= expectFunction
  e <- interp store env e now
  f e
interp store env (Application (Builtin Expr.AddDuration) (toList -> [a, b])) now = do
  a <- interp store env a now >>= expectDuration
  b <- interp store env b now >>= expectDuration
  pure (Value.Duration (a + b))
interp store env (Application (Builtin (mbBinaryRelationInstantVector -> Just rel)) (toList -> [v, k])) now =
  interpFilterBinaryRelation store env v rel k now
interp _ _ expr _ = throwError $ "Can't interpret expression: " <> show expr
