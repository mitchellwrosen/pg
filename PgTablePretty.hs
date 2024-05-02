module PgTablePretty
  ( prettyTable,
  )
where

import Data.Foldable (fold)
import Data.Function ((&))
import Data.Int (Int16, Int64)
import Data.List qualified as List
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Vector (Vector)
import Data.Vector qualified as Vector
import PgPrettyUtils (prettyBytes, prettyInt)
import PgQueries (ColumnRow (..), ForeignKeyConstraintRow (..), GeneratedAsIdentity (..), IndexRow (..), OnDelete (..))
import Prettyprinter hiding (column)
import Prettyprinter.Render.Terminal (AnsiStyle, Color (..), bold, color, colorDull, italicized)
import Queue (Queue)
import Queue qualified
import Witch (unsafeInto)

prettyColumn :: ColumnRow -> [ForeignKeyConstraintRow] -> [Doc AnsiStyle]
prettyColumn row {-(name, typ, generatedAsIdentity, nullable, maybeDefault)-} foreignKeyConstraints =
  [ fold
      [ annotate (color Green <> bold) (pretty row.name),
        " : ",
        annotate
          (color Yellow <> italicized)
          (pretty (aliasType row.type_) <> prettyIf row.nullable "?"),
        case (row.default_, row.generatedAsIdentity) of
          (Just default_, _) -> " = " <> annotate (color Magenta) (pretty default_)
          (_, GeneratedAlwaysAsIdentity) -> " = " <> annotate (color Magenta) "«autoincrement»"
          (_, GeneratedByDefaultAsIdentity) -> " = " <> annotate (color Magenta) "«autoincrement»"
          (_, NotGeneratedAsIdentity) -> mempty,
        case foreignKeyConstraints of
          [c] -> prettyForeignKeyConstraint False c
          _ -> mempty
      ]
  ]
    ++ case foreignKeyConstraints of
      [_] -> [] -- rendered it inline above
      _ -> map (\c -> " " <> prettyForeignKeyConstraint False c) foreignKeyConstraints
  where
    -- prettify the types a bit
    aliasType :: Text -> Text
    aliasType = \case
      "bigint" -> "int8"
      "bigserial" -> "serial8"
      "bit varying" -> "varbit"
      "boolean" -> "bool"
      "character varying" -> "varchar"
      "character" -> "char"
      "double precision" -> "float8"
      "integer" -> "int4"
      "numeric" -> "decimal"
      "real" -> "float4"
      "serial" -> "serial4"
      "time with time zone" -> "timetz"
      "time without time zone" -> "time"
      "timestamp with time zone" -> "timestamptz"
      "timestamp without time zone" -> "timestamp"
      t -> t

prettyForeignKeyConstraint :: Bool -> ForeignKeyConstraintRow -> Doc AnsiStyle
prettyForeignKeyConstraint showSource row =
  fold
    [ prettyIf showSource (prettyConstraintSource row.columnNames),
      " → ",
      annotate (colorDull Green) (pretty row.targetTableName <> "."),
      prettyConstraintSource row.targetColumnNames,
      case row.onDelete of
        OnDeleteCascade -> annotate italicized " on delete cascade"
        OnDeleteNoAction -> mempty
        OnDeleteRestrict -> mempty
        OnDeleteSetDefault -> annotate italicized " on delete set default"
        OnDeleteSetNull -> annotate italicized " on delete set null"
    ]
  where
    prettyConstraintSource :: [Text] -> Doc AnsiStyle
    prettyConstraintSource =
      annotate (colorDull Green) . pretty . Text.intercalate ","

prettyTable ::
  Text ->
  Text ->
  Maybe Text ->
  Vector ColumnRow ->
  [ForeignKeyConstraintRow] ->
  Float ->
  Int64 ->
  [IndexRow] ->
  Doc AnsiStyle
prettyTable schema tableName maybeType columns foreignKeyConstraints tuples bytes indexes =
  fold
    [ "╭─",
      line,
      "│" <> annotate bold (prettyIf (schema /= "public") (pretty schema <> ".") <> pretty tableName),
      case maybeType of
        Nothing -> mempty
        Just typ -> " :: " <> annotate (color Yellow <> italicized) (pretty typ),
      prettyIf (tuples /= -1) $
        fold
          [ " | ",
            annotate (colorDull Cyan) case round tuples of
              1 -> "1 row"
              tuples1 -> prettyInt tuples1 <> " rows",
            " | ",
            annotate (color Green) (prettyBytes (unsafeInto @Int bytes))
          ],
      line,
      "│",
      columns & foldMap \column ->
        prettyIf (not column.dropped) $
          foldMap
            (\doc -> line <> "│" <> doc)
            (prettyColumn column (maybe [] Queue.toList (Map.lookup column.name foreignKeyConstraints1))),
      foldMap
        (\c -> line <> "│" <> prettyForeignKeyConstraint True c)
        ( -- Filter out single-column foreign keys because we show them with the column, not at the bottom
          foreignKeyConstraints & filter \constraint ->
            case constraint.columnNames of
              [_] -> False
              _ -> True
        ),
      line,
      prettyIf (not (null indexes)) ("│" <> line),
      indexes & foldMap \index ->
        let columnIndexToPrettyColumn i =
              let column = columns Vector.! fromIntegral @Int16 @Int (i - 1)
               in annotate (color Green) (pretty column.name)
         in fold
              [ "│",
                let things =
                      index.columnIndexes
                        & take (fromIntegral @Int16 @Int index.numKeyColumns)
                        & map
                          ( \i ->
                              if i == 0
                                then Nothing
                                else Just (columnIndexToPrettyColumn i)
                          )
                        -- Splitting expressions on ", " isn't perfect, because an expression can of course have a
                        -- comma+space in it (e.g. a string literal), but that seems unlikely, so I don't want to invest
                        -- more effort in this at the moment. I could not find a way to return the expressions in an array
                        -- rather than a text - it seems our two options are do-or-do-not pass the column to pg_get_expr.
                        & fillHoles
                          "«bug»"
                          ( maybe
                              []
                              (map (annotate (color Magenta) . pretty) . Text.splitOn ", ")
                              index.expressions
                          )
                 in fold (punctuate ", " things),
                prettyIf index.isUnique " unique",
                case index.predicate of
                  Nothing -> mempty
                  Just predicate -> " where " <> annotate (color Magenta) (pretty predicate),
                case drop (fromIntegral @Int16 @Int index.numKeyColumns) index.columnIndexes of
                  [] -> mempty
                  is -> " → " <> fold (punctuate ", " (map columnIndexToPrettyColumn is)),
                line
              ],
      "╰─"
    ]
  where
    foreignKeyConstraints1 :: Map Text (Queue ForeignKeyConstraintRow)
    foreignKeyConstraints1 =
      List.foldl'
        ( \acc row ->
            case row.columnNames of
              [name] -> Map.alter (Just . maybe (Queue.singleton row) (Queue.enqueue row)) name acc
              _ -> acc
        )
        Map.empty
        foreignKeyConstraints

prettyIf :: Bool -> Doc a -> Doc a
prettyIf True x = x
prettyIf False _ = mempty

-- `fillHoles def dirt xs` plugs each `Nothing` in `xs` with a value from `dirt`, and if/when `dirt` runs out, starts
-- plugging with `def` instead.
--
-- >>> fillHoles 0 [1,2,3] [Nothing, Nothing, Just 10, Nothing, Nothing, Nothing]
-- [1,2,10,3,0,0]
fillHoles :: forall a. a -> [a] -> [Maybe a] -> [a]
fillHoles def =
  go Queue.empty
  where
    go :: Queue a -> [a] -> [Maybe a] -> [a]
    go acc _ [] = Queue.toList acc
    go acc ys (Just x : xs) = go (Queue.enqueue x acc) ys xs
    go acc (y : ys) (Nothing : xs) = go (Queue.enqueue y acc) ys xs
    go acc [] (Nothing : xs) = go (Queue.enqueue def acc) (repeat def) xs
