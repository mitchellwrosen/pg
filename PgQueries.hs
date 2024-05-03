module PgQueries
  ( GeneratedAsIdentity (..),
    Oid,
    OnDelete (..),
    OnUpdate (..),
    CheckConstraintRow (..),
    readCheckConstraints,
    ColumnRow (..),
    readColumns,
    ForeignKeyConstraintRow (..),
    readForeignKeyConstraints,
    readIncomingForeignKeyConstraints,
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
import Hasql.Interpolate (DecodeRow, DecodeValue, Sql, decodeValue, interp, sql)
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

data OnUpdate
  = OnUpdateCascade
  | OnUpdateNoAction
  | OnUpdateRestrict
  | OnUpdateSetDefault
  | OnUpdateSetNull

instance DecodeValue OnUpdate where
  decodeValue :: Decoder.Value OnUpdate
  decodeValue =
    Decoder.text <&> \case
      "c" -> OnUpdateCascade
      "d" -> OnUpdateSetDefault
      "n" -> OnUpdateSetNull
      "r" -> OnUpdateRestrict
      _ -> OnUpdateNoAction

data CheckConstraintRow = CheckConstraintRow
  { tableOid :: !Oid,
    columns :: ![Int16],
    definition :: !Text
  }
  deriving stock (Generic)
  deriving anyclass (DecodeRow)

readCheckConstraints :: [Oid] -> Statement () [CheckConstraintRow]
readCheckConstraints oids =
  interp
    False
    [sql|
      SELECT
        conrelid :: pg_catalog.int8,
        conkey,
        pg_catalog.pg_get_constraintdef(oid, true)
      FROM pg_catalog.pg_constraint
      WHERE conrelid = ANY(#{oids})
        AND contype = 'c'
      ORDER BY conrelid, conkey
    |]

data ColumnRow = ColumnRow
  { tableOid :: !Oid,
    num :: !Int16, -- 1-based column number
    name :: !Text,
    type_ :: !Text,
    generatedAsIdentity :: !GeneratedAsIdentity,
    nullable :: !Bool,
    default_ :: !(Maybe Text),
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
        a.attnum,
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
  { sourceTableOid :: !Oid,
    sourceTableName :: !Text,
    sourceColumnNames :: ![Text],
    targetTableOid :: !Oid,
    targetTableName :: !Text,
    targetColumnNames :: ![Text],
    constraintName :: !Text,
    onDelete :: !OnDelete,
    onUpdate :: !OnUpdate,
    fullText :: !Text
  }
  deriving stock (Generic)
  deriving anyclass (DecodeRow)

readForeignKeyConstraints :: [Oid] -> Statement () [ForeignKeyConstraintRow]
readForeignKeyConstraints =
  readForeignKeyConstraints_ [sql|conrelid|]

readIncomingForeignKeyConstraints :: [Oid] -> Statement () [ForeignKeyConstraintRow]
readIncomingForeignKeyConstraints =
  readForeignKeyConstraints_ [sql|confrelid|]

readForeignKeyConstraints_ :: Sql -> [Oid] -> Statement () [ForeignKeyConstraintRow]
readForeignKeyConstraints_ col oids =
  interp
    False
    [sql|
      SELECT
        c.conrelid :: pg_catalog.int8,
        t.relname :: pg_catalog.text,
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
        c.conname :: pg_catalog.text,
        c.confdeltype,
        c.confupdtype,
        pg_catalog.pg_get_constraintdef(c.oid, true)
      FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON c.conrelid = t.oid
      WHERE c.^{col} = ANY(#{oids})
        AND c.contype = 'f'
      ORDER BY c.conname;
    |]

data IndexRow = IndexRow
  { tableOid :: !Oid,
    indexName :: !Text,
    isUnique :: !Bool,
    columnNames :: ![Maybe Text], -- Nothing means hole for expression
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
        (
          SELECT array_agg(a.attname :: pg_catalog.text ORDER BY n.r)
          FROM (
            SELECT row_number() OVER () r, n
            FROM (SELECT unnest(i.indkey) n) n
          ) n
          LEFT JOIN pg_catalog.pg_attribute a ON
            a.attrelid = i.indrelid
              AND a.attnum != 0
              AND n.n = a.attnum
        ),
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
    unlogged :: !Bool,
    visible :: !Bool,
    tuples :: !Float,
    bytes :: !Int64
  }
  deriving stock (Generic)
  deriving anyclass (DecodeRow)

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
        c.relpersistence = 'u',
        pg_catalog.pg_table_is_visible(c.oid),
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
