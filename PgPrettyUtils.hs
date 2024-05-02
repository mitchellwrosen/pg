module PgPrettyUtils
  ( putPretty,
    prettyBytes,
    prettyDouble,
    prettyInt,
    prettyMilliseconds,
  )
where

import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.IO qualified as Text
import Data.Text.Lazy.Builder qualified as Builder
import Data.Text.Lazy.Builder.RealFloat qualified as Builder
import Prettyprinter
import Prettyprinter.Render.Terminal (AnsiStyle, renderStrict)
import Text.Printf (printf)
import Prelude hiding (filter)

putPretty :: Doc AnsiStyle -> IO ()
putPretty =
  Text.putStrLn
    . Prettyprinter.Render.Terminal.renderStrict
    . Prettyprinter.layoutPretty (Prettyprinter.LayoutOptions Prettyprinter.Unbounded)

prettyBytes :: Int -> Doc a
prettyBytes b
  | b < 995 = pretty b <> " b"
  | b < 9950 = prettyDouble 2 kb <> " kb"
  | b < 99500 = prettyDouble 1 kb <> " kb"
  | b < 995000 = prettyDouble 0 kb <> " kb"
  | b < 9950000 = prettyDouble 2 mb <> " mb"
  | b < 99500000 = prettyDouble 1 mb <> " mb"
  | b < 995000000 = prettyDouble 0 mb <> " mb"
  | b < 9950000000 = prettyDouble 2 gb <> " gb"
  | b < 99500000000 = prettyDouble 1 gb <> " gb"
  | otherwise = prettyDouble 0 gb <> " gb"
  where
    kb = realToFrac @Int @Double b / 1_000
    mb = realToFrac @Int @Double b / 1_000_000
    gb = realToFrac @Int @Double b / 1_000_000_000

prettyDouble :: Int -> Double -> Doc a
prettyDouble i =
  pretty . Builder.toLazyText . Builder.formatRealFloat Builder.Fixed (Just i)

prettyInt :: Int -> Doc a
prettyInt =
  let loop acc d
        | d < 1000 = d : acc
        | otherwise = let (x, y) = divMod d 1000 in loop (y : acc) x
      pp [] = ""
      pp (n : ns) = Text.pack (show n) <> pps ns
      pps = Text.concat . map (("," <>) . pp1)
      pp1 = Text.pack . printf "%03d" :: Int -> Text
   in pretty . pp . loop []

prettyMilliseconds :: Double -> Doc AnsiStyle
prettyMilliseconds ms
  | us < 0.5 = "0 us"
  | us < 995 = prettyDouble 0 us <> " µs"
  | us < 9_950 = prettyDouble 2 ms <> " ms"
  | us < 99_500 = prettyDouble 1 ms <> " ms"
  | ms < 995 = prettyDouble 0 ms <> " ms"
  | ms < 9_950 = prettyDouble 2 s <> " s"
  | ms < 99_500 = prettyDouble 1 s <> " s"
  | otherwise = prettyDouble 0 s <> " s"
  where
    us = ms * 1000
    s = ms / 1_000
