{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-typed-holes #-}
{-# LANGUAGE OverloadedRecordDot  #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ViewPatterns         #-}
module Cardano.Timeseries.Interp(interp) where
import           Cardano.Timeseries.Domain.Identifier        (Identifier (..))
import           Cardano.Timeseries.Domain.Instant           (Instant (Instant),
                                                              InstantVector,
                                                              share)
import qualified Cardano.Timeseries.Domain.Instant           as Domain
import qualified Cardano.Timeseries.Domain.Instant           as Instant
import           Cardano.Timeseries.Domain.Interval
import           Cardano.Timeseries.Domain.Timeseries        (Timeseries (Timeseries),
                                                              TimeseriesVector,
                                                              eachNewest,
                                                              eachOldest,
                                                              superseries,
                                                              transpose)
import qualified Cardano.Timeseries.Domain.Timeseries        as Timeseries
import           Cardano.Timeseries.Domain.Types             (Label, Labelled,
                                                              MetricIdentifier,
                                                              SeriesIdentifier,
                                                              Timestamp)
import           Cardano.Timeseries.Interp.Value             as Value
import           Cardano.Timeseries.Query.BinaryRelation     (BinaryRelation,
                                                              embedScalar,
                                                              mbBinaryRelationInstantVector,
                                                              mbBinaryRelationScalar)
import qualified Cardano.Timeseries.Query.BinaryRelation     as BinaryRelation
import           Cardano.Timeseries.Query.Expr               as Expr
import           Cardano.Timeseries.Store                    (Store (metrics),
                                                              earliest, latest)
import qualified Cardano.Timeseries.Store                    as Store
import           Cardano.Timeseries.Util                     (maybeToEither,
                                                              safeToDouble,
                                                              safeToWord64)

import           Control.Monad                               (filterM, (<=<))
import           Control.Monad.Except                        (ExceptT,
                                                              liftEither,
                                                              throwError)
import           Control.Monad.State                         (get, put)
import           Control.Monad.State.Strict                  (State)
import           Control.Monad.Trans                         (lift)
import           Data.List.NonEmpty                          (fromList, toList)
import           Data.Map.Strict                             (Map)
import qualified Data.Map.Strict                             as Map
import           Data.Maybe                                  (fromJust,
                                                              fromMaybe)
import           Data.Set                                    (Set, isSubsetOf,
                                                              member)
import qualified Data.Set                                    as Set
import           Data.Word                                   (Word64)
import           GHC.Base                                    (NonEmpty ((:|)))

import           Cardano.Timeseries.Interp.Expect
import           Cardano.Timeseries.Interp.Statistics
import qualified Cardano.Timeseries.Query.BinaryArithmeticOp as BinaryArithmeticOp
import           Cardano.Timeseries.Query.Types              (Error, QueryM)
import           Data.Foldable                               (for_, traverse_)
import           Data.Function                               (on)
import           Data.List                                   (find, groupBy)
import qualified Data.List                                   as List
import           Data.Text                                   (Text)
import           Statistics.Function                         (minMax)
import           Statistics.Quantile                         (cadpw, quantile)
import           Statistics.Sample                           (mean)

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

interpLabelInst :: LabelConstraint -> Labelled (Bool, Text)
interpLabelInst (LabelConstraintEq (k, v))    = (k, (Prelude.True, v))
interpLabelInst (LabelConstraintNotEq (k, v)) = (k, (Prelude.False, v))

interpLabelInsts :: Set LabelConstraint -> (Set (Labelled Text), Set (Labelled Text))
interpLabelInsts ls =
  let (sub, notsub) = List.partition cond $ fmap interpLabelInst (Set.toList ls) in
  (Set.fromList $ fmap extract sub, Set.fromList $ fmap extract notsub) where
    cond (_, (b, _)) = b
    extract (x, (_, y)) = (x, y)

interpVariable :: Store s Double => s -> MetricIdentifier -> Value -> QueryM Value
interpVariable store x t = do
  t <- expectTimestamp t
  pure (Value.InstantVector (fmap (fmap Value.Scalar) (Store.evaluate store x t)))

interpQuantileBy :: Set Label -> Double -> InstantVector Double -> Timestamp -> QueryM (InstantVector Double)
interpQuantileBy ls k vs now =
  let groups = groupBy (on (==) (superseries ls . (.labels))) vs
      quantiles = fmap (\g -> (superseries ls (head g).labels, quantile cadpw (floor (k * 100)) 100 (Instant.toVector g)))
                       groups in
  pure $ fmap (\(idx, v) -> Instant idx now v) quantiles

interpFilter :: FunctionValue -> InstantVector Value -> QueryM (InstantVector Value)
interpFilter f = filterM pred where
  pred :: Instant Value -> QueryM Bool
  pred inst = (expectBoolean <=< f) inst.value

interpMap :: FunctionValue -> InstantVector Value -> QueryM (InstantVector Value)
interpMap f = traverse (traverse f)

interpRate :: TimeseriesVector Double -> QueryM (InstantVector Double)
interpRate v = do
  min <- liftEither $ maybeToEither "Can't compute rate" (eachOldest v)
  max <- liftEither $ maybeToEither "Can't compute rate" (eachNewest v)
  pure $ zipWith compute min max where

  compute :: Instant Double -> Instant Double -> Instant Double
  compute min max =
    let v = (max.value - min.value) / fromIntegral (max.timestamp - min.timestamp) in
    Instant min.labels max.timestamp v

interpIncrease :: TimeseriesVector Double -> QueryM (InstantVector Double)
interpIncrease v = liftEither $ do
  min <- maybeToEither "Can't compute rate" (eachOldest v)
  max <- maybeToEither "Can't compute rate" (eachNewest v)
  Right $ zipWith compute min max where

  compute :: Instant Double -> Instant Double -> Instant Double
  compute min max =
    let v = max.value - min.value in
    Instant min.labels max.timestamp v

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
    (Filter
      (
        Lambda
          (Machine nextVarIdx)
          (embedScalar rel (Variable (Machine nextVarIdx)) k)
      )
      v
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
interp _ env Now now = pure (Timestamp (fromIntegral now))
interp _ env Epoch now = pure (Timestamp 0)
interp store env (Lambda x body) now = pure $ Value.Function $ \v ->
  interp store (Map.insert x v env) body now
interp store env (Let x rhs body) now = do
  v <- interp store env rhs now
  interp store (Map.insert x v env) body now
interp store env (FastForward t d) now = do
  t <- interp store env t now >>= expectTimestamp
  d <- interp store env d now >>= expectDuration
  pure (Value.Timestamp (t + d))
interp store env (FilterByLabel cs s) now = do
  s <- interp store env s now >>= expectInstantVector
  let (mustBe, mustNotBe) = interpLabelInsts cs
  pure $
    Value.InstantVector $
     flip filter s $ \i ->
       (&&)
         (mustBe `isSubsetOf` i.labels)
         (Set.null (mustNotBe `Set.intersection` i.labels))
interp store env (Unless u v) now = do
  u <- interp store env u now >>= expectInstantVector
  v <- interp store env v now >>= expectInstantVector
  let vls = Set.fromList (map (.labels) v)
  pure (Value.InstantVector (filter (\i -> not (member i.labels vls)) u))
interp store env (Filter f t) now = do
  f <- interp store env f now >>= expectFunction
  t <- interp store env t now >>= expectInstantVector
  Value.InstantVector <$> interpFilter f t
interp store env (Join a b) now = do
  a <- interp store env a now >>= expectInstantVector
  b <- interp store env b now >>= expectInstantVector
  Value.InstantVector <$> liftEither (interpJoin Value.Pair a b)
interp store env (Map f x) now = do
  f <- interp store env f now >>= expectFunction
  x <- interp store env x now >>= expectInstantVector
  Value.InstantVector <$> interpMap f x
interp store env (Range s a b r) now = do
  s <- interp store env s now >>= expectFunction
  a <- interp store env a now >>= expectTimestamp
  b <- interp store env b now >>= expectTimestamp
  r <- traverse (\r -> interp store env r now >>= expectDuration) r
                                           -- TODO:        vvvvvvvvv make configurable
  RangeVector <$> interpRange s (Interval a b) (fromMaybe (15 * 1000) r)
interp store env (Rewind t d) now = do
  t <- interp store env t now >>= expectTimestamp
  d <- interp store env d now >>= expectDuration
  pure (Timestamp (t - d))
interp store env (BoolToScalar t) now = do
  t <- interp store env t now >>= expectBoolean
  pure (Scalar (if t then 1 else 0))
interp store env (InstantVectorToScalar t) now = do
  t <- interp store env t now >>= expectInstantVectorBool
  pure (Value.InstantVector (fmap (\x -> Value.Scalar (if x then 1.0 else 0.0)) <$> t))
interp store env (TimestampToScalar t) now = do
  t <- interp store env t now >>= expectTimestamp
  pure (Scalar (fromIntegral t))
interp store env (DurationToScalar t) now = do
  t <- interp store env t now >>= expectDuration
  pure (Scalar (fromIntegral t))
interp _ env (Milliseconds t) _ = pure $ Duration t
interp _ env (Seconds t) _ = pure $ Duration (1000 * t)
interp _ env (Minutes t) _ = pure $ Duration (60 * 1000 * t)
interp _ env (Hours t) _ = pure $ Duration (60 * 60 * 1000 * t)
interp store env (BinaryArithmeticOp.mbBinaryArithmeticOpInstantVectorScalar -> Just (a, op, b)) now = do
  va <- interp store env a now >>= expectInstantVectorScalar
  vb <- interp store env b now >>= expectInstantVectorScalar
  v <- liftEither (interpJoin (BinaryArithmeticOp.materializeScalar op) va vb)
  pure (Value.InstantVector (fmap (fmap Value.Scalar) v))
interp store env (Quantile k expr) now = do
  k <- interp store env k now >>= expectScalar
  v <- interp store env expr now >>= expectInstantVectorScalar
  pure $ Value.Scalar $ quantile cadpw (floor (k * 100)) 100 (Instant.toVector v)
interp store env (QuantileBy ls k expr) now = do
  k <- interp store env k now >>= expectScalar
  v <- interp store env expr now >>= expectInstantVectorScalar
  Value.InstantVector . fmap (fmap Value.Scalar) <$> interpQuantileBy ls k v now
interp store env (QuantileOverTime k expr) now = do
  k <- interp store env k now >>= expectScalar
  v <- interp store env expr now >>= expectRangeVectorScalar
  pure $ Value.InstantVector (fmap Value.Scalar <$> quantileRangeVector k v)
interp store env (Rate r) now = do
  r <- interp store env r now >>= expectRangeVectorScalar
  -- TODO: PromQL's rate() performs linear regression to extrapolate the samples to the bounds
  r <- interpRate r
  pure (Value.InstantVector (fmap (fmap Value.Scalar) r))
interp store env (Increase r) now = do
  r <- interp store env r now >>= expectRangeVectorScalar
  -- TODO: PromQL's increase() performs linear regression to extrapolate the samples to the bounds
  r <- interpIncrease r
  pure (Value.InstantVector (fmap (fmap Value.Scalar) r))
interp store env (Avg expr) now = do
  v <- interp store env expr now >>= expectInstantVectorScalar
  pure $ Value.Scalar $ mean (Instant.toVector v)
interp store env (Max expr) now = do
  v <- interp store env expr now >>= expectInstantVectorScalar
  pure $ Value.Scalar $ snd $ minMax (Instant.toVector v)
interp store env (Min expr) now = do
  v <- interp store env expr now >>= expectInstantVectorScalar
  pure $ Value.Scalar $ fst $ minMax (Instant.toVector v)
interp store env (AvgOverTime expr) now = do
  v <- interp store env expr now >>= expectRangeVectorScalar
  pure $ Value.InstantVector (fmap Value.Scalar <$> avgOverTime now v)
interp store env (SumOverTime expr) now = do
  v <- interp store env expr now >>= expectRangeVectorScalar
  pure $ Value.InstantVector (fmap Value.Scalar <$> sumOverTime now v)
interp store env (MkPair a b) now = do
  va <- interp store env a now
  vb <- interp store env b now
  pure $ Value.Pair va vb
interp store env (Fst t) now = do
  (a, _) <- interp store env t now >>= expectPair
  pure a
interp store env (Snd t) now = do
  (_, b) <- interp store env t now >>= expectPair
  pure b
interp store env Expr.True now = do
  pure Truth
interp store env Expr.False now = do
  pure Falsity
interp store env (Expr.And a b) now = do
  va <- interp store env a now >>= expectBoolean
  vb <- interp store env b now >>= expectBoolean
  pure (fromBool (va && vb))
interp store env (Expr.Or a b) now = do
  va <- interp store env a now >>= expectBoolean
  vb <- interp store env b now >>= expectBoolean
  pure (fromBool (va || vb))
interp store env (Expr.Not t) now = do
  vt <- interp store env t now >>= expectBoolean
  pure (fromBool (not vt))
interp store env (Expr.EqBool a b) now = do
  va <- interp store env a now >>= expectBoolean
  vb <- interp store env b now >>= expectBoolean
  pure (fromBool (va == vb))
interp store env (Expr.NotEqBool a b) now = do
  va <- interp store env a now >>= expectBoolean
  vb <- interp store env b now >>= expectBoolean
  pure (fromBool (va /= vb))
interp store env (mbBinaryRelationScalar -> Just (a, rel, b)) now = do
  va <- interp store env a now >>= expectScalar
  vb <- interp store env b now >>= expectScalar
  pure (fromBool (BinaryRelation.materializeScalar rel va vb))
interp store env (BinaryArithmeticOp.mbBinaryArithmeticOpScalar -> Just (a, op, b)) now = do
  va <- interp store env a now >>= expectScalar
  vb <- interp store env b now >>= expectScalar
  pure (Value.Scalar (BinaryArithmeticOp.materializeScalar op va vb))
interp store env (Expr.Abs x) now = do
  x <- interp store env x now >>= expectScalar
  pure (Value.Scalar (abs x))
interp store env (Expr.RoundScalar x) now = do
  x <- interp store env x now >>= expectScalar
  pure (Value.Scalar (fromIntegral (round x :: Int)))
interp store env (Application f e) now = do
  f <- interp store env f now >>= expectFunction
  e <- interp store env e now
  f e
interp store env (Expr.AddDuration a b) now = do
  a <- interp store env a now >>= expectDuration
  b <- interp store env b now >>= expectDuration
  pure (Value.Duration (a + b))
interp store env (mbBinaryRelationInstantVector -> Just (v, rel, k)) now =
  interpFilterBinaryRelation store env v rel k now
interp _ _ expr _ = throwError $ "Can't interpret expression: " <> show expr
