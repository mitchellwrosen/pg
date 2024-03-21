module PgTablePretty
  ( prettyTable,
  )
where

import Data.Foldable (fold)
import Data.Int (Int64)
import Data.Maybe (catMaybes)
import Data.Text (Text)
import PgPrettyUtils (prettyBytes, prettyInt)
import PgQueries (GeneratedAsIdentity (..))
import Prettyprinter
import Prettyprinter.Render.Terminal (AnsiStyle, Color (..), bold, color, colorDull, italicized)
import Witch (unsafeInto)

prettyColumn :: (Text, Text, GeneratedAsIdentity, Bool, Maybe Text) -> [Doc AnsiStyle]
prettyColumn
  ( name,
    typ,
    generatedAsIdentity,
    nullable,
    maybeDefault
    ) =
    catMaybes
      [ Just $
          annotate (color Green) (pretty name)
            <> " :: "
            <> annotate
              (color Yellow <> italicized)
              (pretty typ <> (if nullable then "?" else mempty))
            <> case (maybeDefault, generatedAsIdentity) of
              (Just default_, _) -> " = " <> annotate (color Magenta) (pretty default_)
              (_, GeneratedAlwaysAsIdentity) -> " = " <> annotate (color Magenta) "«autoincrement»"
              (_, GeneratedByDefaultAsIdentity) -> " = " <> annotate (color Magenta) "«autoincrement»"
              (_, NotGeneratedAsIdentity) -> mempty
      ]

prettyForeignKeyConstraint :: (Text, Int64, Text) -> Doc AnsiStyle
prettyForeignKeyConstraint (_name, _targetOid, text) =
  fold
    [ pretty text
    ]

prettyTable ::
  Text ->
  Text ->
  Maybe Text ->
  [(Text, Text, GeneratedAsIdentity, Bool, Maybe Text)] ->
  [(Text, Int64, Text)] ->
  Float ->
  Int64 ->
  Doc AnsiStyle
prettyTable schema name maybeType columns foreignKeyConstraints tuples bytes =
  fold
    [ "╭─",
      line,
      "│" <> annotate bold ((if schema /= "public" then (pretty schema <> ".") else mempty) <> pretty name),
      case maybeType of
        Nothing -> mempty
        Just typ -> " :: " <> annotate (color Yellow <> italicized) (pretty typ),
      foldMap (foldMap (\c -> line <> "│  " <> c) . prettyColumn) columns,
      foldMap (\c -> line <> "│  " <> prettyForeignKeyConstraint c) foreignKeyConstraints,
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
