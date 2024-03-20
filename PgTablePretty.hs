module PgTablePretty
  ( prettyTable,
  )
where

import Data.Foldable (fold)
import Data.Functor ((<&>))
import Data.Int (Int64)
import Data.Maybe (catMaybes)
import Data.Text (Text)
import PgPrettyUtils (prettyBytes, prettyInt)
import Prettyprinter
import Prettyprinter.Render.Terminal (AnsiStyle, Color (..), bold, color, colorDull, italicized)
import Witch (unsafeInto)

prettyColumn :: (Text, Text, Bool, Maybe Text) -> [Doc AnsiStyle]
prettyColumn (name, typ, nullable, maybeDefault) =
  catMaybes
    [ Just $
        pretty name
          <> " :: "
          <> annotate
            italicized
            ( ( if nullable
                  then "nullable "
                  else mempty
              )
                <> pretty typ
            ),
      maybeDefault <&> \default_ ->
        "  default " <> pretty default_
    ]

prettyTable :: Text -> Text -> [(Text, Text, Bool, Maybe Text)] -> Float -> Int64 -> Doc AnsiStyle
prettyTable schema name columns tuples bytes =
  fold
    [ "╭─",
      line,
      "│" <> annotate bold (pretty schema <> "." <> pretty name),
      foldMap (foldMap (\c -> line <> "│  " <> c) . prettyColumn) columns,
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
