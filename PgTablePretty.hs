module PgTablePretty
  ( prettyTable,
  )
where

import Data.Foldable (fold)
import Data.Function ((&))
import Data.Int (Int16)
import Data.List qualified as List
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Vector (Vector)
import Data.Vector qualified as Vector
import PgPrettyUtils (prettyBytes, prettyInt)
import PgQueries
  ( CheckConstraintRow (..),
    ColumnRow (..),
    ForeignKeyConstraintRow (..),
    GeneratedAsIdentity (..),
    IndexRow (..),
    OnDelete (..),
    OnUpdate (..),
    TableRow (..),
  )
import PgUtils (rowsByKey)
import Prettyprinter hiding (column)
import Prettyprinter.Render.Terminal (AnsiStyle, Color (..), bold, color, colorDull, italicized)
import Queue (Queue)
import Queue qualified
import Witch (unsafeInto)

prettyColumn :: ColumnRow -> [CheckConstraintRow] -> [ForeignKeyConstraintRow] -> [Doc AnsiStyle]
prettyColumn row checkConstraints foreignKeyConstraints
  | row.dropped = [annotate (color Black <> bold) "«dropped»"]
  | otherwise =
      [ fold
          [ annotate (color Green <> bold) (pretty row.name),
            " : ",
            annotate
              (color Yellow <> italicized)
              (pretty (aliasType row.type_) <> prettyIf row.nullable "?"),
            prettyIf (not (null checkConstraints)) $
              fold
                [ " ∣ ",
                  checkConstraints
                    & map (prettyCheckConstraintDefinition . (.definition))
                    & punctuate ", "
                    & fold
                ],
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

prettyCheckConstraintDefinition :: Text -> Doc AnsiStyle
prettyCheckConstraintDefinition =
  annotate (color Magenta) . pretty . Text.dropEnd 1 . Text.drop 7

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
        OnDeleteSetNull -> annotate italicized " on delete set null",
      case row.onUpdate of
        OnUpdateCascade -> annotate italicized " on update cascade"
        OnUpdateNoAction -> mempty
        OnUpdateRestrict -> mempty
        OnUpdateSetDefault -> annotate italicized " on update set default"
        OnUpdateSetNull -> annotate italicized " on update set null"
    ]
  where
    prettyConstraintSource :: [Text] -> Doc AnsiStyle
    prettyConstraintSource =
      annotate (colorDull Green) . pretty . Text.intercalate ","

prettyTable ::
  TableRow ->
  Vector ColumnRow ->
  [ForeignKeyConstraintRow] ->
  [CheckConstraintRow] ->
  [IndexRow] ->
  Doc AnsiStyle
prettyTable table columns foreignKeyConstraints checkConstraints indexes =
  fold
    [ "╭─",
      line,
      "│",
      if table.visible then annotate italicized ("(" <> pretty table.schema <> "." <> ")") else annotate bold (pretty table.schema) <> ".",
      annotate bold (pretty table.name),
      case table.type_ of
        Nothing -> mempty
        Just type_ -> " :: " <> annotate (color Yellow <> italicized) (pretty type_),
      prettyIf table.unlogged (" " <> annotate italicized "unlogged"),
      prettyIf (table.tuples /= -1) $
        fold
          [ " | ",
            annotate (colorDull Cyan) case round table.tuples of
              1 -> "1 row"
              tuples1 -> prettyInt tuples1 <> " rows",
            " | ",
            annotate (color Green) (prettyBytes (unsafeInto @Int table.bytes))
          ],
      line,
      "│",
      columns & foldMap \column ->
        prettyColumn column (getOneColumnCheckConstraints column.num) (getForeignKeyConstraints column.name) & foldMap \doc ->
          line <> "│" <> doc,
      foldMap
        (\c -> line <> "│" <> prettyForeignKeyConstraint True c)
        ( -- Filter out single-column foreign keys because we show them with the column, not at the bottom
          foreignKeyConstraints & filter \constraint ->
            case constraint.columnNames of
              [_] -> False
              _ -> True
        ),
      line,
      twoColumnCheckConstraints & foldMap \constraint ->
        "│" <> prettyCheckConstraintDefinition constraint.definition <> line,
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
    getForeignKeyConstraints :: Text -> [ForeignKeyConstraintRow]
    getForeignKeyConstraints =
      rowsByKey (getOne . (.columnNames)) (filter (one . (.columnNames)) foreignKeyConstraints)

    getOneColumnCheckConstraints :: Int16 -> [CheckConstraintRow]
    getOneColumnCheckConstraints =
      rowsByKey (getOne . (.columns)) (filter (one . (.columns)) checkConstraints)

    twoColumnCheckConstraints :: [CheckConstraintRow]
    twoColumnCheckConstraints =
      filter (two . (.columns)) checkConstraints
      where
        two (_ : _ : _) = True
        two _ = False

    one [_] = True
    one _ = False

    getOne [x] = x
    getOne _ = undefined

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
