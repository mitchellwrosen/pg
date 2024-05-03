module PgTablePretty
  ( prettyTable,
  )
where

import Data.Foldable (fold)
import Data.Function ((&))
import Data.Int (Int16)
import Data.Maybe (fromJust)
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
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
import PgUtils (rowsByMaybeKey)
import Prettyprinter hiding (column)
import Prettyprinter.Render.Terminal (AnsiStyle, Color (..), bold, color, colorDull, italicized, underlined)
import Queue (Queue)
import Queue qualified
import Witch (unsafeInto)

prettyColumn ::
  ColumnRow ->
  Bool ->
  [CheckConstraintRow] ->
  [ForeignKeyConstraintRow] ->
  [ForeignKeyConstraintRow] ->
  [Doc AnsiStyle]
prettyColumn row unique checkConstraints foreignKeyConstraints incomingForeignKeyConststraints
  | row.dropped = [annotate (color Black <> bold) "«dropped»"]
  | otherwise =
      [ fold
          [ annotate (color Green <> bold) (pretty row.name),
            prettyIf unique prettyUniqueMarker,
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
        ++ map (\c -> " " <> prettyIncomingForeignKeyConstraint False c) incomingForeignKeyConststraints
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
    [ prettyIf showSource (prettyCols row.sourceColumnNames),
      " → ",
      annotate (colorDull Green) (pretty row.targetTableName <> "."),
      prettyCols row.targetColumnNames,
      prettyOnDelete row.onDelete,
      prettyOnUpdate row.onUpdate
    ]
  where
    prettyCols :: [Text] -> Doc AnsiStyle
    prettyCols =
      annotate (colorDull Green) . pretty . Text.intercalate ","

prettyIncomingForeignKeyConstraint :: Bool -> ForeignKeyConstraintRow -> Doc AnsiStyle
prettyIncomingForeignKeyConstraint showTarget row =
  fold
    [ if showTarget then prettyCols row.targetColumnNames <> " ← " else " ↑ ",
      annotate (colorDull Green) (pretty row.sourceTableName <> "."),
      prettyCols row.sourceColumnNames,
      prettyOnDelete row.onDelete,
      prettyOnUpdate row.onUpdate
    ]
  where
    prettyCols :: [Text] -> Doc AnsiStyle
    prettyCols =
      annotate (colorDull Green) . pretty . Text.intercalate ","

prettyIndexKeyColumnsWith :: (Text -> Doc a) -> (Text -> Doc a) -> [Maybe Text] -> Maybe Text -> Int16 -> Doc a
prettyIndexKeyColumnsWith prettyColName prettyExpr columnNames expressions numKeyColumns =
  columnNames
    & take (fromIntegral @Int16 @Int numKeyColumns)
    & map (fmap prettyColName)
    -- Splitting expressions on ", " isn't perfect, because an expression can of course have a comma+space
    -- in it (e.g. a string literal), but that seems unlikely, so I don't want to invest more effort in
    -- this at the moment. I could not find a way to return the expressions in an array rather than a text
    -- - it seems our two options are do-or-do-not pass the column to pg_get_expr.
    & fillHoles "«bug»" (maybe [] (map prettyExpr . Text.splitOn ", ") expressions)
    & punctuate ","
    & fold

prettyOnDelete :: OnDelete -> Doc AnsiStyle
prettyOnDelete = \case
  OnDeleteCascade -> annotate italicized " on delete cascade"
  OnDeleteNoAction -> mempty
  OnDeleteRestrict -> mempty
  OnDeleteSetDefault -> annotate italicized " on delete set default"
  OnDeleteSetNull -> annotate italicized " on delete set null"

prettyOnUpdate :: OnUpdate -> Doc AnsiStyle
prettyOnUpdate = \case
  OnUpdateCascade -> annotate italicized " on update cascade"
  OnUpdateNoAction -> mempty
  OnUpdateRestrict -> mempty
  OnUpdateSetDefault -> annotate italicized " on update set default"
  OnUpdateSetNull -> annotate italicized " on update set null"

prettyTable ::
  TableRow ->
  [ColumnRow] ->
  [ForeignKeyConstraintRow] ->
  [ForeignKeyConstraintRow] ->
  [CheckConstraintRow] ->
  [IndexRow] ->
  Doc AnsiStyle
prettyTable table columns foreignKeyConstraints incomingForeignKeyConstraints checkConstraints indexes =
  fold
    [ "╭─",
      line,
      "│",
      if table.visible
        then prettyIf (table.schema /= "public") (annotate italicized ("(" <> pretty table.schema <> "." <> ")"))
        else annotate bold (pretty table.schema) <> ".",
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
        prettyColumn
          column
          (Set.member column.name oneColumnUniqueConstraints)
          (getOneColumnCheckConstraints column.num)
          (getForeignKeyConstraints column.name)
          (getIncomingForeignKeyConstraints column.name)
          & foldMap \doc -> line <> "│" <> doc,
      line,
      let twoColumnForeignKeyConstraints =
            foreignKeyConstraints
              -- Filter out single-column foreign keys because we show them with the column, not at the bottom
              & filter (not . one . (.sourceColumnNames))
       in prettyIf (not (null twoColumnForeignKeyConstraints)) $
            fold
              [ "│",
                line,
                "│",
                annotate underlined "Compound outgoing foreign key constraints",
                line,
                twoColumnForeignKeyConstraints
                  & foldMap \c -> "│" <> prettyForeignKeyConstraint True c <> line
              ],
      let twoColumnIncomingForeignKeyConstraints =
            incomingForeignKeyConstraints
              -- Filter out single-column foreign keys because we show them with the column, not at the bottom
              & filter (not . one . (.sourceColumnNames))
       in prettyIf (not (null twoColumnIncomingForeignKeyConstraints)) $
            fold
              [ "│",
                line,
                "│",
                annotate underlined "Compound incoming foreign key references",
                line,
                twoColumnIncomingForeignKeyConstraints
                  & foldMap \c -> "│" <> prettyIncomingForeignKeyConstraint True c <> line
              ],
      prettyIf (not (null twoColumnUniqueConstraints)) $
        fold
          [ "│",
            line,
            "│",
            annotate underlined "Compound unique columns",
            line,
            twoColumnUniqueConstraints & foldMap \index ->
              "│" <> prettyIndexKeyColumns index <> prettyUniqueMarker <> line
          ],
      prettyIf (not (null twoColumnCheckConstraints)) $
        fold
          [ "│",
            line,
            "│",
            annotate underlined "Compound check constraints",
            line,
            twoColumnCheckConstraints & foldMap \constraint ->
              "│" <> prettyCheckConstraintDefinition constraint.definition <> line
          ],
      prettyIf (not (null indexes)) $
        fold
          [ "│",
            line,
            "│",
            annotate underlined "Indexes",
            line,
            indexes & foldMap \index ->
              fold
                [ "│",
                  prettyIndexKeyColumns index,
                  case index.predicate of
                    Nothing -> mempty
                    Just predicate -> " ∣ " <> annotate (color Magenta) (pretty predicate),
                  case drop (fromIntegral @Int16 @Int index.numKeyColumns) index.columnNames of
                    [] -> mempty
                    -- fromJust is safe here because all non-key things are columns, i.e. you can't INCLUDE (foo + 1)
                    names -> " +" <> fold (punctuate "," (map (prettyIndexColumnName . fromJust) names)),
                  line
                ]
          ],
      "╰─"
    ]
  where
    prettyIndexColumnName :: Text -> Doc AnsiStyle
    prettyIndexColumnName =
      annotate (color Green) . pretty

    prettyIndexKeyColumns :: IndexRow -> Doc AnsiStyle
    prettyIndexKeyColumns index =
      prettyIndexKeyColumnsWith
        prettyIndexColumnName
        (annotate (color Magenta) . pretty)
        index.columnNames
        index.expressions
        index.numKeyColumns

    getForeignKeyConstraints :: Text -> [ForeignKeyConstraintRow]
    getForeignKeyConstraints =
      rowsByMaybeKey (asOne . (.sourceColumnNames)) foreignKeyConstraints

    getIncomingForeignKeyConstraints :: Text -> [ForeignKeyConstraintRow]
    getIncomingForeignKeyConstraints =
      rowsByMaybeKey (asOne . (.targetColumnNames)) incomingForeignKeyConstraints

    getOneColumnCheckConstraints :: Int16 -> [CheckConstraintRow]
    getOneColumnCheckConstraints =
      rowsByMaybeKey (asOne . (.columns)) checkConstraints

    twoColumnCheckConstraints :: [CheckConstraintRow]
    twoColumnCheckConstraints =
      filter (two . (.columns)) checkConstraints
      where
        two (_ : _ : _) = True
        two _ = False

    oneColumnUniqueConstraints :: Set Text
    oneColumnUniqueConstraints =
      indexes & foldMap \index ->
        case (index.isUnique, index.columnNames) of
          (True, [Just column]) -> Set.singleton column
          _ -> Set.empty

    twoColumnUniqueConstraints :: [IndexRow]
    twoColumnUniqueConstraints =
      filter (\index -> index.isUnique && index.numKeyColumns >= 2) indexes

    one [_] = True
    one _ = False

    asOne [x] = Just x
    asOne _ = Nothing

prettyUniqueMarker :: Doc AnsiStyle
prettyUniqueMarker =
  annotate (color Green) "*"

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
