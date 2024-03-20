module PgQueries
  ( readColumns,
    readTables,
  )
where

import Data.Int (Int64)
import Data.Text (Text)
import Hasql.Interpolate (interp, sql)
import Hasql.Statement (Statement)

-- name, type, nullable, default
readColumns :: Int64 -> Statement () [(Text, Text, Bool, Maybe Text)]
readColumns oid =
  interp
    False
    [sql|
      SELECT
        a.attname :: text,
        pg_catalog.format_type(a.atttypid, a.atttypmod),
        NOT a.attnotnull,
        ( SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
          FROM pg_catalog.pg_attrdef d
          WHERE a.attrelid = d.adrelid
            AND a.attnum = d.adnum
            AND a.atthasdef
        )
      FROM pg_catalog.pg_attribute AS a
      WHERE a.attrelid = #{oid}
        AND a.attnum > 0
        AND NOT a.attisdropped
      ORDER BY a.attnum
    |]

-- oid, schema, name, tuples, bytes
readTables :: Statement () [(Int64, Text, Text, Float, Int64)]
readTables =
  interp
    False
    [sql|
      SELECT
        c.oid :: int8,
        n.nspname :: text,
        c.relname :: text,
        c.reltuples,
        (c.relpages :: int8) * (current_setting('block_size') :: int8)
      FROM pg_catalog.pg_class AS c
      JOIN pg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
      WHERE c.relkind = 'r'
      ORDER BY n.nspname, c.relname
   |]
