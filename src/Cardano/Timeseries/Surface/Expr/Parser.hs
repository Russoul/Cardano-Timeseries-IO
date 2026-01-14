{-# OPTIONS_GHC -Wno-name-shadowing #-}
{- HLINT ignore "Use <$>" -}
module Cardano.Timeseries.Surface.Expr.Parser(expr) where

import           Cardano.Timeseries.Domain.Identifier (Identifier (User))
import           Cardano.Timeseries.Surface.Expr
import           Control.Applicative                  hiding (many, some)

import           Cardano.Timeseries.Domain.Types      (Label, Labelled)
import           Cardano.Timeseries.Surface.Expr.Head (Head)
import qualified Cardano.Timeseries.Surface.Expr.Head as Head
import           Control.Monad                        (guard)
import           Data.Char                            (isAlpha, isAlphaNum)
import           Data.Functor                         (void)
import           Data.List                            (foldl')
import           Data.List.NonEmpty                   (NonEmpty ((:|)),
                                                       fromList)
import           Data.Scientific                      (toRealFloat)
import           Data.Set                             (Set)
import qualified Data.Set                             as Set
import           Data.Text                            (Text, pack)
import           Data.Void                            (Void)
import           GHC.Unicode                          (isControl)
import           Prelude                              hiding (head)
import           Text.Megaparsec
import           Text.Megaparsec.Char                 (char, space, space1,
                                                       string)
import           Text.Megaparsec.Char.Lexer           (decimal, scientific,
                                                       signed)

type Parser = Parsec Void Text

keywords :: [String]
keywords = ["let", "in", "true", "false", "epoch", "now"]

unescapedVariableIdentifier :: Parser String
unescapedVariableIdentifier = (:) <$> firstChar <*> many nextChar <?> "identifier" where
  firstChar :: Parser Char
  firstChar = satisfy (\x -> isAlpha x || x == '_')

  nextChar :: Parser Char
  nextChar = satisfy (\x -> isAlphaNum x || x == '_')

number :: Parser Expr
number =
  Number <$> getSourcePos <*> (toRealFloat <$> signed (pure ()) scientific <?> "number")

escapedVariableIdentifier :: Parser String
escapedVariableIdentifier = char '`' *> manyTill one (char '`') where
  one :: Parser Char
  one = satisfy (\x -> not (isControl x) && (x /= '`') && (x /= '\n') && (x /= '\r'))

literalVariableIdentifier :: Parser String
literalVariableIdentifier = do
  x <- unescapedVariableIdentifier
  guard (x `notElem` keywords)
  pure x

variableIdentifier :: Parser Identifier
variableIdentifier = User <$> (literalVariableIdentifier <|> escapedVariableIdentifier)

variable :: Parser Expr
variable = Variable <$> getSourcePos <*> variableIdentifier

text :: Parser String
text = (char '\"' *> many one) <* char '\"' where
  one :: Parser Char
  one = satisfy (\x -> not (isControl x) && (x /= '"') && (x /= '\n') && (x /= '\r'))

str :: Parser Expr
str = Str <$> getSourcePos <*> text

milliseconds :: Parser Expr
milliseconds = Milliseconds <$> getSourcePos <*> decimal <* string "ms" <* notFollowedBy (satisfy isAlpha)

seconds :: Parser Expr
seconds = Seconds <$> getSourcePos <*> decimal <* string "s" <* notFollowedBy (satisfy isAlpha)

minutes :: Parser Expr
minutes = Minutes <$> getSourcePos <*> decimal <* string "min" <* notFollowedBy (satisfy isAlpha)

hours :: Parser Expr
hours = Hours <$> getSourcePos <*> decimal <* string "h" <* notFollowedBy (satisfy isAlpha)

now :: Parser Expr
now = Now <$> getSourcePos <* string "now"

epoch :: Parser Expr
epoch = Now <$> getSourcePos <* string "epoch"

true :: Parser Expr
true = Truth <$> getSourcePos <* string "true"

false :: Parser Expr
false = Falsity <$> getSourcePos <* string "false"

continueTight :: Expr -> Parser Expr
continueTight a = a <$ string ")"

continuePair :: Loc -> Expr -> Parser Expr
continuePair l a = do
  void $ string ","
  space
  b <- exprUniverse
  space
  void $ string ")"
  pure (MkPair l a b)

tightOrPair :: Parser Expr
tightOrPair = do
  l <- getSourcePos
  void $ string "("
  space
  a <- exprUniverse
  space
  try (continuePair l a) <|> continueTight a

exprAtom :: Parser Expr
exprAtom = tightOrPair
       <|> epoch
       <|> true
       <|> false
       <|> now
       <|> variable
       <|> try milliseconds
       <|> try seconds
       <|> try minutes
       <|> try hours
       <|> number
       <|> str

exprNot :: Parser Expr
exprNot = Not <$> getSourcePos <*> (string "!" *> space *> exprAtom)

range :: Parser (Expr, Expr, Maybe Expr)
range = do
  void $ string "["
  space
  a <- exprUniverse
  space
  void $ string ";"
  space
  b <- exprUniverse
  c <- optional $ do
    space
    void $ string ":"
    space
    c <- exprUniverse
    space
    pure c
  void $ string "]"
  pure (a, b, c)

exprRange :: Parser Expr
exprRange = do
  l <- getSourcePos
  head <- exprAtom
  ext <- many (try (space *> range))
  pure $ foldl' (mkRange l) head ext

labelledString :: Parser (Labelled String)
labelledString = do
  x <- text
  space
  void $ string "="
  space
  v <- text
  pure (x, v)

setLabelledString :: Parser (Set (Labelled String))
setLabelledString = do
  void $ string "{"
  space
  list <- sepBy labelledString (space *> string "," <* space)
  space
  void $ string "}"
  pure $ Set.fromList list

setLabel :: Parser (Set Label)
setLabel = do
  void $ string "("
  space
  list <- sepBy text (space *> string "," <* space)
  space
  void $ string ")"
  pure $ Set.fromList list

head :: Parser Head
head =
        Head.Fst <$ string "fst"
    <|> Head.Snd <$ string "snd"
    <|> Head.FilterByLabel <$> (string "filter_by_label" *> space1 *> setLabelledString)
    <|> Head.Min <$ string "min"
    <|> Head.Max <$ string "max"
    <|> Head.Avg <$ string "avg"
    <|> Head.Filter <$ string "filter"
    <|> Head.Join <$ string "join"
    <|> Head.Map <$ string "map"
    <|> Head.Abs <$ string "abs"
    <|> Head.Increase <$ string "increase"
    <|> Head.Rate <$ string "rate"
    <|> Head.AvgOverTime <$ string "avg_over_time"
    <|> Head.SumOverTime <$ string "sum_over_time"
    <|> Head.QuantileOverTime <$ string "quantile_over_time"
    <|> Head.Unless <$ string "unless"
    <|> Head.QuantileBy <$> (string "quantile_by" *> space1 *> setLabel)
    <|> Head.Earliest <$> (string "earliest" *> space1 *> variableIdentifier)
    <|> Head.Latest <$> (string "latest" *> space1 *> variableIdentifier)
    <|> Head.ToScalar <$ string "to_scalar"

applyBuiltin :: Loc -> Head -> [Expr] -> Parser Expr
applyBuiltin l Head.Fst [t] = pure $ Fst l t
applyBuiltin l Head.Fst args = fail "Wrong number of arguments for `fst`"
applyBuiltin l Head.Snd [t] = pure $ Snd l t
applyBuiltin l Head.Snd args = fail "Wrong number of arguments for `snd`"
applyBuiltin l (Head.FilterByLabel ls) [t] = pure $ FilterByLabel l ls t
applyBuiltin l (Head.FilterByLabel ls) args = fail "Wrong number of arguments for `filter_by_label`"
applyBuiltin l Head.Min [t] = pure $ Min l t
applyBuiltin l Head.Min args = fail "Wrong number of arguments for `min`"
applyBuiltin l Head.Max [t] = pure $ Max l t
applyBuiltin l Head.Max args = fail "Wrong number of arguments for `max`"
applyBuiltin l Head.Avg [t] = pure $ Avg l t
applyBuiltin l Head.Avg args = fail "Wrong number of arguments for `avg`"
applyBuiltin l Head.Filter [f, xs] = pure $ Filter l f xs
applyBuiltin l Head.Filter args = fail "Wrong number of arguments for `filter`"
applyBuiltin l Head.Join [xs, ys] = pure $ Join l xs ys
applyBuiltin l Head.Join args = fail "Wrong number of arguments for `join`"
applyBuiltin l Head.Map [f, xs] = pure $ Map l f xs
applyBuiltin l Head.Map args = fail "Wrong number of arguments for `map`"
applyBuiltin l Head.Abs [t] = pure $ Abs l t
applyBuiltin l Head.Abs args = fail "Wrong number of arguments for `abs`"
applyBuiltin l Head.Increase [xs] = pure $ Increase l xs
applyBuiltin l Head.Increase args = fail "Wrong number of arguments for `increase`"
applyBuiltin l Head.Rate [xs] = pure $ Rate l xs
applyBuiltin l Head.Rate args = fail "Wrong number of arguments for `rate`"
applyBuiltin l Head.AvgOverTime [xs] = pure $ AvgOverTime l xs
applyBuiltin l Head.AvgOverTime args = fail "Wrong number of arguments for `avg_over_time`"
applyBuiltin l Head.SumOverTime [xs] = pure $ SumOverTime l xs
applyBuiltin l Head.SumOverTime args = fail "Wrong number of arguments for `sum_over_time`"
applyBuiltin l Head.QuantileOverTime [k, xs] = pure $ QuantileOverTime l k xs
applyBuiltin l Head.QuantileOverTime args = fail "Wrong number of arguments for `quantile_over_time`"
applyBuiltin l Head.Unless [xs, ys] = pure $ Unless l xs ys
applyBuiltin l Head.Unless args = fail "Wrong number of arguments for `unless`"
applyBuiltin l (Head.QuantileBy ls) [k, xs] = pure $ QuantileBy l ls k xs
applyBuiltin l (Head.QuantileBy ls) args = fail "Wrong number of arguments for `quantile_by`"
applyBuiltin l (Head.Earliest x) [] = pure $ Earliest l x
applyBuiltin l (Head.Earliest _) args = fail "Wrong number of arguments for `earliest`"
applyBuiltin l (Head.Latest x) [] = pure $ Latest l x
applyBuiltin l (Head.Latest _) args = fail "Wrong number of arguments for `latest`"
applyBuiltin l Head.ToScalar [t] = pure $ ToScalar l t
applyBuiltin l Head.ToScalar args = fail "Wrong number of arguments for `to_scalar`"

apply :: Loc -> Either Head Expr -> [Expr] -> Parser Expr
apply l (Left t)  = applyBuiltin l t
apply l (Right t) = pure . mkApp l t

exprAppArg :: Parser Expr
exprAppArg = try exprNot <|> exprRange

exprApp :: Parser Expr
exprApp = do
  l <- getSourcePos
  h <- Left <$> head <|> Right <$> exprAppArg
  args <- many (try (space1 *> exprAppArg))
  apply l h args

data Mul = Asterisk | Slash

mul :: Parser Mul
mul = Asterisk <$ try (string "*") <|> Slash <$ string "/"

applyMul :: Loc -> Expr -> (Mul, Expr) -> Expr
applyMul l x (Asterisk, y) = Mul l x y
applyMul l x (Slash, y)    = Div l x y

applyListMul :: Loc -> Expr -> [(Mul, Expr)] -> Expr
applyListMul l = foldl' (applyMul l)

exprMul :: Parser Expr
exprMul = do
  l <- getSourcePos
  h <- exprApp
  args <- many ((,) <$> try (space *> mul) <*> (space *> exprApp))
  pure $ applyListMul l h args

data Add = Plus | Minus deriving (Show)

add :: Parser Add
add = Plus <$ try (string "+") <|> Minus <$ string "-"

applyAdd :: Loc -> Expr -> (Add, Expr) -> Expr
applyAdd l x (Plus, y)  = Add l x y
applyAdd l x (Minus, y) = Sub l x y

applyListAdd :: Loc -> Expr -> [(Add, Expr)] -> Expr
applyListAdd l = foldl' (applyAdd l)

exprAdd :: Parser Expr
exprAdd = do
  l <- getSourcePos
  h <- exprMul
  args <- many ((,) <$> try (space *> add) <*> (space *> exprMul))
  pure $ applyListAdd l h args

data Comp = EqSign | NotEqSign | LtSign | LteSign | GtSign | GteSign

comp :: Parser Comp
comp = EqSign <$ string "=="
   <|> NotEqSign <$ string "!="
   <|> LtSign <$ string "<"
   <|> LteSign <$ string "<="
   <|> GtSign <$ string ">"
   <|> GteSign <$ string ">="

applyComp :: Loc -> Expr -> (Comp, Expr) -> Expr
applyComp l a (EqSign, b)    = Eq l a b
applyComp l a (NotEqSign, b) = NotEq l a b
applyComp l a (LtSign, b)    = Lt l a b
applyComp l a (LteSign, b)   = Lte l a b
applyComp l a (GtSign, b)    = Gt l a b
applyComp l a (GteSign, b)   = Gte l a b

applyListComp :: Loc -> Expr -> [(Comp, Expr)] -> Expr
applyListComp l = foldl' (applyComp l)

exprComp :: Parser Expr
exprComp = do
  l <- getSourcePos
  h <- exprAdd
  args <- many ((,) <$> try (space *> comp) <*> (space *> exprAdd))
  pure $ applyListComp l h args

applyAnd :: Loc -> Expr -> [Expr] -> Expr
applyAnd l = foldl' (And l)

exprAnd :: Parser Expr
exprAnd = do
  l <- getSourcePos
  h <- exprComp
  args <- many (try (space *> string "&&") *> space *> exprComp)
  pure $ applyAnd l h args

applyOr :: Loc -> Expr -> [Expr] -> Expr
applyOr l = foldl' (Or l)

exprOr :: Parser Expr
exprOr = do
  l <- getSourcePos
  h <- exprAnd
  args <- many (try (space *> string "||") *> space *> exprAnd)
  pure $ applyOr l h args

exprLet :: Parser Expr
exprLet = do
  l <- getSourcePos
  void $ string "let"
  space1
  x <- variableIdentifier
  space
  void $ string "="
  space
  rhs <- exprOr
  space
  void $ string "in"
  space1
  body <- exprUniverse
  pure $ Let l x rhs body

exprLambda :: Parser Expr
exprLambda = do
  l <- getSourcePos
  void $ string "\\"
  space
  x <- variableIdentifier
  space
  void $ string "->"
  space
  body <- exprUniverse
  pure $ Lambda l x body

exprUniverse :: Parser Expr
exprUniverse = try exprLet <|> (try exprLambda <|> exprOr)

expr :: Parser Expr
expr = exprUniverse
