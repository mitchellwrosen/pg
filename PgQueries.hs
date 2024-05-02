module PgQueries
  ( GeneratedAsIdentity (..),
    Oid,
    OnDelete (..),
    ColumnRow (..),
    readColumns,
    ForeignKeyConstraintRow (..),
    readForeignKeyConstraints,
    IndexRow (..),
    readIndexes,
    TableRow (..),
    readTables,
  )
where

import Data.Functor ((<&>))
import Data.Int (Int16, Int64)
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

type Oid =
  Int64

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

data ColumnRow = ColumnRow
  { tableOid :: !Oid,
    name :: !Text,
    type_ :: !Text,
    generatedAsIdentity :: !GeneratedAsIdentity,
    nullable :: !Bool,
    default_ :: !(Maybe Text),
    -- we read this column out too rather than filter them out with a WHERE clause because the pg_index table refers to
    -- columns by their index offset (never pointing directly to a dropped column, of course, but we need the accurate
    -- offset nonetheless)
    dropped :: !Bool
  }
  deriving stock (Generic)
  deriving anyclass (DecodeRow)

readColumns :: [Oid] -> Statement () [ColumnRow]
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
        pg_catalog.pg_get_expr(d.adbin, d.adrelid, true),
        a.attisdropped
      FROM pg_catalog.pg_attribute AS a
        LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum AND a.atthasdef
      WHERE a.attrelid = ANY(#{oids})
        AND a.attnum > 0
      ORDER BY a.attrelid, a.attnum
    |]

data ForeignKeyConstraintRow = ForeignKeyConstraintRow
  { constraintName :: !Text,
    tableOid :: !Oid,
    columnNames :: ![Text],
    targetTableOid :: !Oid,
    targetTableName :: !Text,
    targetColumnNames :: ![Text],
    onDelete :: !OnDelete,
    fullText :: !Text
  }
  deriving stock (Generic)
  deriving anyclass (DecodeRow)

readForeignKeyConstraints :: [Oid] -> Statement () [ForeignKeyConstraintRow]
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

data IndexRow = IndexRow
  { tableOid :: !Oid,
    indexName :: !Text,
    isUnique :: !Bool,
    columnIndexes :: ![Int16],
    numKeyColumns :: !Int16,
    expressions :: !(Maybe Text),
    predicate :: !(Maybe Text)
  }
  deriving stock (Generic)
  deriving anyclass (DecodeRow)

readIndexes :: [Oid] -> Statement () [IndexRow]
readIndexes oids =
  interp
    False
    [sql|
      SELECT
        c1.oid :: pg_catalog.int8,
        c2.relname :: pg_catalog.text,
        i.indisunique,
        i.indkey :: pg_catalog.int2[],
        i.indnkeyatts,
        pg_catalog.pg_get_expr(i.indexprs, c1.oid, true) :: pg_catalog.text,
        pg_catalog.pg_get_expr(i.indpred, c1.oid, true) :: pg_catalog.text
      FROM pg_catalog.pg_class AS c1
        JOIN pg_catalog.pg_index AS i ON c1.oid = i.indrelid
        JOIN pg_catalog.pg_class AS c2 ON i.indexrelid = c2.oid
      WHERE c1.oid = ANY(#{oids})
    |]

data TableRow = TableRow
  { oid :: !Oid,
    schema :: !Text,
    name :: !Text,
    type_ :: !(Maybe Text),
    tuples :: !Float,
    bytes :: !Int64
  }
  deriving stock (Generic)
  deriving anyclass (DecodeRow)

-- oid, schema, name, type, tuples, bytes
readTables :: Statement () [TableRow]
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