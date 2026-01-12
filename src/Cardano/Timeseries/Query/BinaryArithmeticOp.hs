module Cardano.Timeseries.Query.BinaryArithmeticOp(BinaryArithmeticOp(..), embedScalar, prettyOp) where
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
