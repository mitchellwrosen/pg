module PgQueries
  ( GeneratedAsIdentity (..),
    OnDelete (..),
    readColumns,
    ForeignKeyConstraintRow (..),
    readForeignKeyConstraints,
    readTables,
  )
where

import Data.Functor ((<&>))
import Data.Int (Int64)
import Data.Text (Text)
import GHC.Generics (Generic)
import Hasql.Decoders qualified as Decoder
import Hasql.Interpolate (DecodeRow, DecodeValue, decodeValue, interp, sql)
import Hasql.Statement (Statement)

data GeneratedAsIdentity
  = GeneratedAlwaysAsIdentity
  | GeneratedByDefaultAsIdentity
  | NotGeneratedAsIdentity

instance DecodeValue GeneratedAsIdentity where
  decodeValue :: Decoder.Value GeneratedAsIdentity
  decodeValue =
    Decoder.text <&> \case
      "a" -> GeneratedAlwaysAsIdentity
      "d" -> GeneratedByDefaultAsIdentity
      _ -> NotGeneratedAsIdentity

data OnDelete
  = OnDeleteCascade
  | OnDeleteNoAction
  | OnDeleteRestrict
  | OnDeleteSetDefault
  | OnDeleteSetNull

instance DecodeValue OnDelete where
  decodeValue :: Decoder.Value OnDelete
  decodeValue =
    Decoder.text <&> \case
      "c" -> OnDeleteCascade
      "d" -> OnDeleteSetDefault
      "n" -> OnDeleteSetNull
      "r" -> OnDeleteRestrict
      _ -> OnDeleteNoAction

readColumns ::
  [Int64] ->
  Statement
    ()
    [ ( Int64, -- table oid
        Text, -- name
        Text, -- type
        GeneratedAsIdentity,
        Bool, -- nullable?
        Maybe Text -- default value
      )
    ]
readColumns oids =
  interp
    False
    [sql|
      SELECT
        a.attrelid :: pg_catalog.int8,
        a.attname :: pg_catalog.text,
        pg_catalog.format_type(a.atttypid, a.atttypmod),
        a.attidentity,
        NOT a.attnotnull,
        pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
      FROM pg_catalog.pg_attribute AS a
      LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum AND a.atthasdef
      WHERE a.attrelid = ANY(#{oids})
        AND a.attnum > 0
        AND NOT a.attisdropped
      ORDER BY a.attrelid, a.attnum
    |]

data ForeignKeyConstraintRow = ForeignKeyConstraintRow
  { constraintName :: !Text,
    tableOid :: !Int64,
    columnNames :: ![Text],
    targetTableOid :: !Int64,
    targetTableName :: !Text,
    targetColumnNames :: ![Text],
    onDelete :: !OnDelete,
    fullText :: !Text
  }
  deriving stock (Generic)
  deriving anyclass (DecodeRow)

readForeignKeyConstraints :: [Int64] -> Statement () [ForeignKeyConstraintRow]
readForeignKeyConstraints oids =
  interp
    False
    [sql|
      SELECT
        c.conname :: pg_catalog.text,
        c.conrelid :: pg_catalog.int8,
        (
          SELECT array_agg(a.attname ORDER BY a.attnum)
          FROM pg_catalog.pg_attribute a
          WHERE a.attrelid = c.conrelid
            AND a.attnum = ANY(c.conkey)
        ),
        c.confrelid :: pg_catalog.int8,
        c.confrelid :: pg_catalog.regclass :: pg_catalog.text,
        (
          SELECT array_agg(a.attname ORDER BY a.attnum)
          FROM pg_catalog.pg_attribute a
          WHERE a.attrelid = c.confrelid
            AND a.attnum = ANY(c.confkey)
        ),
        c.confdeltype,
        pg_catalog.pg_get_constraintdef(oid, true)
      FROM pg_catalog.pg_constraint c
      WHERE c.confrelid = ANY(#{oids})
        AND c.contype = 'f'
      ORDER BY c.conname;
    |]

-- oid, schema, name, type, tuples, bytes
readTables :: Statement () [(Int64, Text, Text, Maybe Text, Float, Int64)]
readTables =
  interp
    False
    [sql|
      SELECT
        c.oid :: pg_catalog.int8,
        n.nspname :: pg_catalog.text,
        c.relname :: pg_catalog.text,
        CASE
          WHEN c.reloftype = 0
            THEN null
            ELSE c.reloftype :: pg_catalog.regtype :: pg_catalog.text
        END,
        c.reltuples,
        (c.relpages :: pg_catalog.int8) * (current_setting('block_size') :: pg_catalog.int8)
      FROM pg_catalog.pg_class AS c
      JOIN pg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
      WHERE c.relkind = 'r'
        AND n.nspname != 'information_schema'
        AND n.nspname != 'pg_catalog'
        AND n.nspname !~ '^pg_toast'
      ORDER BY n.nspname, c.relname
   |]
