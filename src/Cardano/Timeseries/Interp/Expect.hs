{-# OPTIONS_GHC -Wno-name-shadowing #-}
module Cardano.Timeseries.Interp.Expect where
import           Cardano.Timeseries.Domain.Instant    (Instant (Instant),
                                                       InstantVector)
import           Cardano.Timeseries.Domain.Timeseries (Timeseries,
                                                       TimeseriesVector)
import           Cardano.Timeseries.Interp.Value      as Value
import           Cardano.Timeseries.Util              (maybeToEither,
                                                       safeToDouble,
                                                       safeToWord64)

import           Control.Monad.Except                 (liftEither, throwError)
import           Data.Word                            (Word64)

import           Cardano.Timeseries.Interp.Types      (Error, QueryM)
import qualified Data.Text                            as Text

expectInstantVector :: Value -> QueryM (InstantVector Value)
expectInstantVector (Value.InstantVector v) = pure v
expectInstantVector _ = throwError "Unexpected expression type: expected an instant vector"

expectRangeVector :: Value -> QueryM (TimeseriesVector Value)
expectRangeVector (Value.RangeVector v) = pure v
expectRangeVector _ = throwError "Unexpected expression type: expected a range vector"

expectTimeseriesScalar :: Timeseries Value -> QueryM (Timeseries Double)
expectTimeseriesScalar = traverse expectScalar

expectRangeVectorScalar :: Value -> QueryM (TimeseriesVector Double)
expectRangeVectorScalar v = expectRangeVector v >>= traverse expectTimeseriesScalar

expectInstantScalar :: Instant Value -> QueryM (Instant Double)
expectInstantScalar = traverse expectScalar

expectInstantBool :: Instant Value -> QueryM (Instant Bool)
expectInstantBool = traverse expectBool

expectInstantVectorScalar :: Value -> QueryM (InstantVector Double)
expectInstantVectorScalar v = expectInstantVector v >>= traverse expectInstantScalar

expectInstantVectorBool :: Value -> QueryM (InstantVector Bool)
expectInstantVectorBool v = expectInstantVector v >>= traverse expectInstantBool

expectPair :: Value -> QueryM (Value, Value)
expectPair (Value.Pair a b) = pure (a, b)
expectPair _ = throwError "Unexpected expression type: expected a pair"

expectScalar :: Value -> QueryM Double
expectScalar (Value.Scalar x) = pure x
expectScalar _ = throwError "Unexpected expression type: expected a scalar"

expectBool :: Value -> QueryM Bool
expectBool Value.Truth = pure Prelude.True
expectBool Value.Falsity = pure Prelude.False
expectBool _ = throwError "Unexpected expression type: expected a bool"

expectBoolean :: Value -> QueryM Bool
expectBoolean Truth = pure Prelude.True
expectBoolean Falsity = pure Prelude.False
expectBoolean _ = throwError "Unexpected expression type: expected a boolean"

expectDuration :: Value -> QueryM Word64
expectDuration (Value.Duration x) = pure x
expectDuration e = throwError "Unexpected expression type: expected a duration"

expectTimestamp :: Value -> QueryM Word64
expectTimestamp (Value.Timestamp x) = pure x
expectTimestamp e = throwError "Unexpected expression type: expected a timestamp"

expectFunction :: Value -> QueryM FunctionValue
expectFunction (Value.Function f) = pure f
expectFunction e = throwError "Unexpected expression type: expected a function"

expectWord64 :: Double -> QueryM Word64
expectWord64 x = if isWhole x then pure (truncate x) else throwError ("Expected a whole number, got: " <> Text.show x) where
  isWhole :: Double -> Bool
  isWhole x = snd (properFraction x :: (Integer, Double)) == 0
