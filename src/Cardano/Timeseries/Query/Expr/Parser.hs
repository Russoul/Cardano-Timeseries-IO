{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use <$>" #-}

module Cardano.Timeseries.Query.Expr.Parser(expr) where

import           Cardano.Timeseries.Domain.Identifier (Identifier (User))
import           Cardano.Timeseries.Query.Expr

import           Control.Monad                        (guard)
import           Data.Char                            (isAlpha, isAlphaNum)
import           Data.Functor                         (void)
import           Data.List.NonEmpty                   (NonEmpty ((:|)),
                                                       fromList)
import           Data.Scientific                      (toRealFloat)
import           Data.Text                            (Text)
import           Data.Void                            (Void)
import           GHC.Unicode                          (isControl)
import           Text.Megaparsec
import           Text.Megaparsec.Char                 (char, space, space1,
                                                       string)
import           Text.Megaparsec.Char.Lexer           (scientific, signed)
import           Text.Megaparsec.Debug                (dbg)

type Parser = Parsec Void Text

keywords :: [String]
keywords = ["let", "in"]

variableIdentifier :: Parser String
variableIdentifier = (:) <$> firstChar <*> many nextChar <?> "identifier" where
  firstChar :: Parser Char
  firstChar = satisfy (\x -> isAlpha x || x == '_')

  nextChar :: Parser Char
  nextChar = satisfy (\x -> isAlphaNum x || x == '_')

number :: Parser Expr
number = Number . toRealFloat <$> signed (pure ()) scientific <?> "number"

escapedVariable :: Parser String
escapedVariable = char '`' *> manyTill one (char '`') where
  one :: Parser Char
  one = satisfy (\x -> not (isControl x) && (x /= '`') && (x /= '\n') && (x /= '\r'))

literalVariable :: Parser String
literalVariable = do
  x <- variableIdentifier
  guard (x `notElem` keywords)
  pure x

variable :: Parser Expr
variable = Variable . User <$> (literalVariable <|> escapedVariable)

str :: Parser Expr
str = Str <$> (char '\"' *> many one) <* char '\"' where
  one :: Parser Char
  one = satisfy (\x -> not (isControl x) && (x /= '"') && (x /= '\n') && (x /= '\r'))

function :: Parser Function
function =
           AddDuration              <$ string "add_duration"
       <|> Minutes                  <$ string "minutes"
       <|> Milliseconds             <$ string "milliseconds"
       <|> Seconds                  <$ string "seconds"
       <|> Hours                    <$ string "hours"
       <|> AvgOverTime              <$ string "avg_over_time"
       <|> Avg                      <$ string "avg"
       <|> SumOverTime              <$ string "sum_over_time"
       <|> Max                      <$ string "max"
       <|> Min                      <$ string "min"
       <|> Fst                      <$ string "fst"
       <|> Snd                      <$ string "snd"
       <|> EqBool                   <$ string "eq_bool"
       <|> EqScalar                 <$ string "eq_scalar"
       <|> NotEqScalar              <$ string "not_eq_scalar"
       <|> LtScalar                 <$ string "lt_scalar"
       <|> LteScalar                <$ string "lte_scalar"
       <|> GtScalar                 <$ string "gt_scalar"
       <|> GteScalar                <$ string "gte_scalar"
       <|> AddScalar                <$ string "add_scalar"
       <|> MulScalar                <$ string "mul_scalar"
       <|> SubScalar                <$ string "sub_scalar"
       <|> DivScalar                <$ string "div_scalar"
       <|> RoundScalar              <$ string "round_scalar"
       <|> Abs                      <$ string "abs"
       <|> QuantileOverTime         <$ string "quantile_over_time"
       <|> QuantileBy               <$ string "quantile_by"
       <|> Quantile                 <$ string "quantile"
       <|> Rate                     <$ string "rate"
       <|> Increase                 <$ string "increase"
       <|> Now                      <$ string "now"
       <|> BoolToScalar             <$ string "bool_to_scalar"
       <|> InstantVectorToScalar    <$ string "instant_vector_to_scalar"
       <|> TimestampToScalar        <$ string "timestamp_to_scalar"
       <|> DurationToScalar         <$ string "duration_to_scalar"
       <|> Range                    <$ string "range"
       <|> FilterByLabel            <$ string "filter_by_label"
       <|> Inv                      <$ string "inv"
       <|> Filter                   <$ string "filter"
       <|> Join                     <$ string "join"
       <|> Unless                   <$ string "unless"
       <|> Map                      <$ string "map"
       <|> Epoch                    <$ string "epoch"
       <|> FastForward              <$ string "fast_forward"
       <|> AddInstantVectorScalar   <$ string "add_instant_vector_scalar"
       <|> MulInstantVectorScalar   <$ string "mul_instant_vector_scalar"
       <|> SubInstantVectorScalar   <$ string "sub_instant_vector_scalar"
       <|> DivInstantVectorScalar   <$ string "div_instant_vector_scalar"
       <|> And                      <$ string "and"
       <|> Or                       <$ string "or"
       <|> EqInstantVectorScalar    <$ string "eq_instant_vector_scalar"
       <|> NotEqInstantVectorScalar <$ string "not_eq_instant_vector_scalar"
       <|> LtInstantVectorScalar    <$ string "lt_instant_vector_scalar"
       <|> LteInstantVectorScalar   <$ string "lte_instant_vector_scalar"
       <|> GtInstantVectorScalar    <$ string "gt_instant_vector_scalar"
       <|> GteInstantVectorScalar   <$ string "gte_instant_vector_scalar"
       <|> Earliest                 <$ string "earliest"
       <|> Latest                   <$ string "latest"

builtin :: Parser Expr
builtin = Builtin <$> function

application :: Parser Expr
application = do
  f <- expr1
  args <- many (try (space1 *> expr1))
  pure $ case args of
    []     -> f
    x : xs -> Application f (x :| xs)

lambda :: Parser Expr
lambda = do
  void $ string "\\"
  space
  x <- variableIdentifier
  space
  void $ string "->"
  space
  body <- expr
  pure $ Lambda (User x) body

let_ :: Parser Expr
let_ = do
  void $ string "let"
  space
  x <- variableIdentifier
  space
  void $ string "="
  space
  rhs <- expr
  space
  void $ string "in"
  space
  body <- expr
  pure $ Let (User x) rhs body

continueTight :: Expr -> Parser Expr
continueTight a = a <$ string ")"

continuePair :: Expr -> Parser Expr
continuePair a = do
  void $ string ","
  space
  b <- expr
  space
  void $ string ")"
  pure (MkPair a b)

tightOrPair :: Parser Expr
tightOrPair = do
  void $ string "("
  space
  a <- expr
  space
  try (continuePair a) <|> continueTight a

expr1 :: Parser Expr
expr1 = number <|> builtin <|> variable <|> str <|> tightOrPair

expr :: Parser Expr
expr = let_ <|> lambda <|> application <?> "expression"
