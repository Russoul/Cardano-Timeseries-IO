module Cardano.Timeseries.Query.BinaryArithmeticOp(BinaryArithmeticOp(..),
  embedScalar, embedInstantVectorScalar, prettyOp,
  mbBinaryArithmeticOpScalar, mbBinaryArithmeticOpInstantVectorScalar, materializeScalar) where
import Cardano.Timeseries.Query.Expr(Function(..))
import Data.Text (Text)

data BinaryArithmeticOp = Add | Sub | Mul | Div deriving (Show, Eq, Ord)

prettyOp :: BinaryArithmeticOp -> Text
prettyOp Add = "+"
prettyOp Sub = "-"
prettyOp Mul = "*"
prettyOp Div = "/"

embedScalar :: BinaryArithmeticOp -> Function
embedScalar Add = AddScalar
embedScalar Sub = SubScalar
embedScalar Mul = MulScalar
embedScalar Div = DivScalar

embedInstantVectorScalar :: BinaryArithmeticOp -> Function
embedInstantVectorScalar Add = AddInstantVectorScalar
embedInstantVectorScalar Sub = SubInstantVectorScalar
embedInstantVectorScalar Mul = MulInstantVectorScalar
embedInstantVectorScalar Div = DivInstantVectorScalar

mbBinaryArithmeticOpInstantVectorScalar :: Function -> Maybe BinaryArithmeticOp
mbBinaryArithmeticOpInstantVectorScalar AddInstantVectorScalar = Just Add
mbBinaryArithmeticOpInstantVectorScalar SubInstantVectorScalar = Just Sub
mbBinaryArithmeticOpInstantVectorScalar MulInstantVectorScalar = Just Mul
mbBinaryArithmeticOpInstantVectorScalar DivInstantVectorScalar = Just Div
mbBinaryArithmeticOpInstantVectorScalar _ = Nothing

mbBinaryArithmeticOpScalar :: Function -> Maybe BinaryArithmeticOp
mbBinaryArithmeticOpScalar AddScalar = Just Add
mbBinaryArithmeticOpScalar SubScalar = Just Sub
mbBinaryArithmeticOpScalar MulScalar = Just Mul
mbBinaryArithmeticOpScalar DivScalar = Just Div
mbBinaryArithmeticOpScalar _ = Nothing

materializeScalar :: BinaryArithmeticOp -> Double -> Double -> Double
materializeScalar Add = (+)
materializeScalar Sub = (-)
materializeScalar Mul = (*)
materializeScalar Div = (/)
