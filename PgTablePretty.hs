module PgTablePretty
  ( prettyTable,
  )
where

import Data.Foldable (fold)
import Data.Function ((&))
import Data.Int (Int64)
import Data.List qualified as List
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import PgPrettyUtils (prettyBytes, prettyInt)
import PgQueries (ForeignKeyConstraintRow (..), GeneratedAsIdentity (..), OnDelete (..))
import Prettyprinter
import Prettyprinter.Render.Terminal (AnsiStyle, Color (..), bold, color, colorDull, italicized)
import Queue (Queue)
import Queue qualified
import Witch (unsafeInto)

prettyColumn ::
  (Text, Text, GeneratedAsIdentity, Bool, Maybe Text) ->
  [ForeignKeyConstraintRow] ->
  [Doc AnsiStyle]
prettyColumn (name, typ, generatedAsIdentity, nullable, maybeDefault) foreignKeyConstraints =
  [ fold
      [ annotate (color Green <> bold) (pretty name),
        " : ",
        annotate
          (color Yellow <> italicized)
          (pretty (aliasType typ) <> (if nullable then "?" else mempty)),
        case (maybeDefault, generatedAsIdentity) of
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
    [ if showSource then prettyConstraintSource row.columnNames else mempty,
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
  [(Text, Text, GeneratedAsIdentity, Bool, Maybe Text)] ->
  [ForeignKeyConstraintRow] ->
  Float ->
  Int64 ->
  Doc AnsiStyle
prettyTable schema tableName maybeType columns foreignKeyConstraints tuples bytes =
  fold
    [ "╭─",
      line,
      "│" <> annotate bold ((if schema /= "public" then (pretty schema <> ".") else mempty) <> pretty tableName),
      case maybeType of
        Nothing -> mempty
        Just typ -> " :: " <> annotate (color Yellow <> italicized) (pretty typ),
      foldMap
        ( \col@(name, _, _, _, _) ->
            foldMap
              (\doc -> line <> "│  " <> doc)
              ( prettyColumn
                  col
                  ( maybe
                      []
                      Queue.toList
                      (Map.lookup name foreignKeyConstraints1)
                  )
              )
        )
        columns,
      foldMap
        (\c -> line <> "│  " <> prettyForeignKeyConstraint True c)
        ( -- Filter out single-column foreign keys because we show them with the column, not at the bottom
          foreignKeyConstraints & filter \constraint ->
            case constraint.columnNames of
              [_] -> False
              _ -> True
        ),
      if tuples /= -1
        then
          fold
            [ line,
              "│",
              annotate (colorDull Cyan) case round tuples of
                1 -> "1 row"
                tuples1 -> prettyInt tuples1 <> " rows",
              " ",
              annotate (color Green) ("(" <> prettyBytes (unsafeInto @Int bytes) <> ")")
            ]
        else mempty,
      line,
      "╰─"
    ]
  where
    foreignKeyConstraints1 :: Map Text (Queue ForeignKeyConstraintRow)
    foreignKeyConstraints1 =
      List.foldl'
        ( \acc row ->
            case row.columnNames of
              [name] ->
                Map.alter
                  ( Just . \case
                      Nothing -> Queue.singleton row
                      Just rows -> Queue.enqueue row rows
                  )
                  name
                  acc
              _ -> acc
        )
        Map.empty
        foreignKeyConstraints
