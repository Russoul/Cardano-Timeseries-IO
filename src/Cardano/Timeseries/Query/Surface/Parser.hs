{-# OPTIONS_GHC -Wno-name-shadowing #-}
{- HLINT ignore "Use <$>" -}
module Cardano.Timeseries.Query.Surface.Parser(expr) where

import           Cardano.Timeseries.Domain.Identifier  (Identifier (User))
import           Cardano.Timeseries.Query.Surface.Expr
import           Control.Applicative                   hiding (many, some)

import           Cardano.Timeseries.Domain.Types       (Label, Labelled)
import           Cardano.Timeseries.Query.Surface.Head (Head)
import qualified Cardano.Timeseries.Query.Surface.Head as Head
import           Control.Monad                         (guard)
import           Data.Char                             (isAlpha, isAlphaNum)
import           Data.Functor                          (void)
import           Data.List.NonEmpty                    (NonEmpty ((:|)),
                                                        fromList)
import           Data.Scientific                       (toRealFloat)
import           Data.Set                              (Set)
import qualified Data.Set                              as Set
import           Data.Text                             (Text, pack)
import           Data.Void                             (Void)
import           GHC.Unicode                           (isControl)
import           Prelude                               hiding (head)
import           Text.Megaparsec
import           Text.Megaparsec.Char                  (char, space, space1,
                                                        string)
import           Text.Megaparsec.Char.Lexer            (decimal, scientific,
                                                        signed)

type Parser = Parsec Void Text

keywords :: [String]
keywords = ["let", "in"]

unescapedVariableIdentifier :: Parser String
unescapedVariableIdentifier = (:) <$> firstChar <*> many nextChar <?> "identifier" where
  firstChar :: Parser Char
  firstChar = satisfy (\x -> isAlpha x || x == '_')

  nextChar :: Parser Char
  nextChar = satisfy (\x -> isAlphaNum x || x == '_')

number :: Parser Expr
number = Number . toRealFloat <$> signed (pure ()) scientific <?> "number"

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
variable = Variable <$> variableIdentifier

text :: Parser String
text = (char '\"' *> many one) <* char '\"' where
  one :: Parser Char
  one = satisfy (\x -> not (isControl x) && (x /= '"') && (x /= '\n') && (x /= '\r'))

str :: Parser Expr
str = Str <$> text

milliseconds :: Parser Expr
milliseconds = Milliseconds <$> decimal <* string "ms" <* notFollowedBy (satisfy isAlpha)

seconds :: Parser Expr
seconds = Seconds <$> decimal <* string "s" <* notFollowedBy (satisfy isAlpha)

minutes :: Parser Expr
minutes = Minutes <$> decimal <* string "min" <* notFollowedBy (satisfy isAlpha)

hours :: Parser Expr
hours = Hours <$> decimal <* string "h" <* notFollowedBy (satisfy isAlpha)

now :: Parser Expr
now = Now <$ string "now"

epoch :: Parser Expr
epoch = Now <$ string "epoch"

continueTight :: Expr -> Parser Expr
continueTight a = a <$ string ")"

continuePair :: Expr -> Parser Expr
continuePair a = do
  void $ string ","
  space
  b <- exprUniverse
  space
  void $ string ")"
  pure (MkPair a b)

tightOrPair :: Parser Expr
tightOrPair = do
  void $ string "("
  space
  a <- exprUniverse
  space
  try (continuePair a) <|> continueTight a

exprAtom :: Parser Expr
exprAtom = tightOrPair
       <|> variable
       <|> epoch
       <|> now
       <|> try milliseconds
       <|> try seconds
       <|> try minutes
       <|> try hours
       <|> number
       <|> str

exprNot :: Parser Expr
exprNot = Not <$> (string "!" *> space *> exprAtom)

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
  head <- exprAtom
  ext <- many (try (space *> range))
  pure $ foldl' mkRange head ext

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

applyBuiltin :: Head -> [Expr] -> Parser Expr
applyBuiltin Head.Fst [t] = pure $ Fst t
applyBuiltin Head.Fst args = fail "Wrong number of arguments for `fst`"
applyBuiltin Head.Snd [t] = pure $ Snd t
applyBuiltin Head.Snd args = fail "Wrong number of arguments for `snd`"
applyBuiltin (Head.FilterByLabel ls) [t] = pure $ FilterByLabel ls t
applyBuiltin (Head.FilterByLabel ls) args = fail "Wrong number of arguments for `filter_by_label`"
applyBuiltin Head.Min [t] = pure $ Min t
applyBuiltin Head.Min args = fail "Wrong number of arguments for `min`"
applyBuiltin Head.Max [t] = pure $ Max t
applyBuiltin Head.Max args = fail "Wrong number of arguments for `max`"
applyBuiltin Head.Avg [t] = pure $ Avg t
applyBuiltin Head.Avg args = fail "Wrong number of arguments for `avg`"
applyBuiltin Head.Filter [f, xs] = pure $ Filter f xs
applyBuiltin Head.Filter args = fail "Wrong number of arguments for `filter`"
applyBuiltin Head.Join [xs, ys] = pure $ Join xs ys
applyBuiltin Head.Join args = fail "Wrong number of arguments for `join`"
applyBuiltin Head.Map [f, xs] = pure $ Map f xs
applyBuiltin Head.Map args = fail "Wrong number of arguments for `map`"
applyBuiltin Head.Abs [t] = pure $ Abs t
applyBuiltin Head.Abs args = fail "Wrong number of arguments for `abs`"
applyBuiltin Head.Increase [xs] = pure $ Increase xs
applyBuiltin Head.Increase args = fail "Wrong number of arguments for `increase`"
applyBuiltin Head.Rate [xs] = pure $ Rate xs
applyBuiltin Head.Rate args = fail "Wrong number of arguments for `rate`"
applyBuiltin Head.AvgOverTime [xs] = pure $ AvgOverTime xs
applyBuiltin Head.AvgOverTime args = fail "Wrong number of arguments for `avg_over_time`"
applyBuiltin Head.SumOverTime [xs] = pure $ SumOverTime xs
applyBuiltin Head.SumOverTime args = fail "Wrong number of arguments for `sum_over_time`"
applyBuiltin Head.QuantileOverTime [k, xs] = pure $ QuantileOverTime k xs
applyBuiltin Head.QuantileOverTime args = fail "Wrong number of arguments for `quantile_over_time`"
applyBuiltin Head.Unless [xs, ys] = pure $ Unless xs ys
applyBuiltin Head.Unless args = fail "Wrong number of arguments for `unless`"
applyBuiltin (Head.QuantileBy ls) [k, xs] = pure $ QuantileBy ls k xs
applyBuiltin (Head.QuantileBy ls) args = fail "Wrong number of arguments for `quantile_by`"
applyBuiltin (Head.Earliest x) [] = pure $ Earliest x
applyBuiltin (Head.Earliest _) args = fail "Wrong number of arguments for `earliest`"
applyBuiltin (Head.Latest x) [] = pure $ Latest x
applyBuiltin (Head.Latest _) args = fail "Wrong number of arguments for `latest`"

apply :: Either Head Expr -> [Expr] -> Parser Expr
apply (Left t)  = applyBuiltin t
apply (Right t) = pure . mkApp t

exprAppArg :: Parser Expr
exprAppArg = try exprNot <|> exprRange

exprApp :: Parser Expr
exprApp = do
  h <- Left <$> head <|> Right <$> exprAppArg
  args <- many (try (space1 *> exprAppArg))
  apply h args

data Mul = Asterisk | Slash

mul :: Parser Mul
mul = Asterisk <$ try (string "*") <|> Slash <$ string "/"

applyMul :: Expr -> (Mul, Expr) -> Expr
applyMul x (Asterisk, y) = Mul x y
applyMul x (Slash, y)    = Div x y

applyListMul :: Expr -> [(Mul, Expr)] -> Expr
applyListMul = foldl' applyMul

exprMul :: Parser Expr
exprMul = do
  h <- exprApp
  args <- many ((,) <$> try (space *> mul) <*> (space *> exprApp))
  pure $ applyListMul h args

data Add = Plus | Minus deriving (Show)

add :: Parser Add
add = Plus <$ try (string "+") <|> Minus <$ string "-"

applyAdd :: Expr -> (Add, Expr) -> Expr
applyAdd x (Plus, y)  = Add x y
applyAdd x (Minus, y) = Sub x y

applyListAdd :: Expr -> [(Add, Expr)] -> Expr
applyListAdd = foldl' applyAdd

exprAdd :: Parser Expr
exprAdd = do
  h <- exprMul
  args <- many ((,) <$> try (space *> add) <*> (space *> exprMul))
  pure $ applyListAdd h args

data Comp = EqSign | NotEqSign | LtSign | LteSign | GtSign | GteSign

comp :: Parser Comp
comp = EqSign <$ string "=="
   <|> NotEqSign <$ string "!="
   <|> LtSign <$ string "<"
   <|> LteSign <$ string "<="
   <|> GtSign <$ string ">"
   <|> GteSign <$ string ">="

applyComp :: Expr -> (Comp, Expr) -> Expr
applyComp a (EqSign, b)    = Eq a b
applyComp a (NotEqSign, b) = NotEq a b
applyComp a (LtSign, b)    = Lt a b
applyComp a (LteSign, b)   = Lte a b
applyComp a (GtSign, b)    = Gt a b
applyComp a (GteSign, b)   = Gte a b

applyListComp :: Expr -> [(Comp, Expr)] -> Expr
applyListComp = foldl' applyComp

exprComp :: Parser Expr
exprComp = do
  h <- exprAdd
  args <- many ((,) <$> try (space *> comp) <*> (space *> exprAdd))
  pure $ applyListComp h args

applyAnd :: Expr -> [Expr] -> Expr
applyAnd = foldl' And

exprAnd :: Parser Expr
exprAnd = do
  h <- exprComp
  args <- many (try (space *> string "&&") *> space *> exprComp)
  pure $ applyAnd h args

applyOr :: Expr -> [Expr] -> Expr
applyOr = foldl' Or

exprOr :: Parser Expr
exprOr = do
  h <- exprAnd
  args <- many (try (space *> string "||") *> space *> exprAnd)
  pure $ applyOr h args

exprLet :: Parser Expr
exprLet = do
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
  pure $ Let x rhs body

exprLambda :: Parser Expr
exprLambda = do
  void $ string "\\"
  space
  x <- variableIdentifier
  space
  void $ string "->"
  space
  body <- exprUniverse
  pure $ Lambda x body

exprUniverse :: Parser Expr
exprUniverse = try exprLet <|> (try exprLambda <|> exprOr)

expr :: Parser Expr
expr = exprUniverse
