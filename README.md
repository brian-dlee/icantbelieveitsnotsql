# butter — I can't believe it's not SQL

`butter` turns plain SQL files into typed data-access code, in the spirit of
[sqlc](https://sqlc.dev). You write the SQL you would run anyway, annotate each
statement with a name and a result shape, and `butter generate` writes one
module per SQL file with a function per query and a
[pydantic](https://docs.pydantic.dev) model tailored to each result set.

It generates **Python + pydantic** for these drivers:

| Dialect      | Drivers                                   | Placeholders in generated SQL |
| ------------ | ----------------------------------------- | ----------------------------- |
| `sqlite`     | `aiosqlite` (default)                     | as written (`:name`, `?`)     |
| `mysql`      | `aiomysql` (default), `pymysql` (sync)    | `%(name)s`, `%s`              |
| `postgresql` | `asyncpg` (default)                       | `$1`, `$2`, ...               |

The parser ([sqlparser-rs](https://github.com/apache/datafusion-sqlparser-rs))
understands SQLite, PostgreSQL, MySQL and a generic dialect.

## Install

```sh
cargo install --path .
butter --version
```

## Quick start

```
my-project/
├── butter.toml
├── sql/butter/
│   └── cache_entry.sql
└── my_package/
    └── butter/          # generated, one .py per .sql
        ├── __init__.py
        └── cache_entry.py
```

`butter.toml`:

```toml
[generate]
dialect = "sqlite"
queries-dir = "sql/butter"

[generate.python]
output-dir = "my_package/butter"
```

`sql/butter/cache_entry.sql`:

```sql
-- name: create_table_cache_entry :exec
CREATE TABLE IF NOT EXISTS cache_entry (
    key TEXT NOT NULL PRIMARY KEY,
    value BLOB NOT NULL
);

-- name: select_many_cache_entry_by_key :many
SELECT * FROM cache_entry WHERE key = :key;

-- name: insert_many_cache_entry :execmany
INSERT INTO cache_entry (key, value) VALUES (:key, :value);
```

Then:

```sh
butter generate          # writes my_package/butter/cache_entry.py
```

```python
from my_package.butter.cache_entry import (
    InsertManyCacheEntryParams, create_table_cache_entry, insert_many_cache_entry, select_many_cache_entry_by_key,
)

async with aiosqlite.connect("state.sqlite") as connection, connection.cursor() as cursor:
    await create_table_cache_entry(cursor)
    await insert_many_cache_entry(cursor, [InsertManyCacheEntryParams(key="greeting", value=b"...")])
    rows = await select_many_cache_entry_by_key(cursor, key="greeting")   # list[SelectManyCacheEntryByKeyRow]
```

The tables are learned from the `CREATE TABLE` statements butter sees, whether
they live in a query file (as above, so the same file both creates and queries
the table) or in separate files listed under `schema-files`.

A complete, runnable sample lives in [`example/aiosqlite`](example/aiosqlite)
(its generated `app/butter/` is committed so it runs without the Rust toolchain):

```sh
cd example/aiosqlite
butter generate
uv run python sample_butter_script.py
uv run pytest
```

[`example/sqlite`](example/sqlite) exercises a wider range of SQLite DDL with a
separate schema file; its output is not committed:

```sh
cd example/sqlite
butter generate
uv run python smoke_test.py
```

## Writing queries

Every generated function starts with a header comment:

```sql
-- name: <function_name> :<command>
-- optional free-form lines become the docstring
-- param: <name> <python type>      (optional type override)
-- column: <name> <python type>     (optional type override)
SELECT ...;
```

| command       | executes                 | returns                          |
| ------------- | ------------------------ | -------------------------------- |
| `:one`        | `execute` + `fetchone`   | `<Name>Row \| None`              |
| `:many`       | `execute` + `fetchall`   | `list[<Name>Row]`                |
| `:exec`       | `execute` per statement  | `None`                           |
| `:execrows`   | `execute`                | `int` (`cursor.rowcount`)        |
| `:execlastid` | `execute`                | `int \| None` (`cursor.lastrowid`) |
| `:execmany`   | `executemany`            | `int` (`cursor.rowcount`)        |

- `:exec` is the only command that may contain **several statements** (handy for
  `CREATE TABLE` + `CREATE INDEX` blocks). Multi-statement queries must use
  named placeholders.
- `:execmany` takes `rows: Iterable[<Name>Params]` instead of keyword arguments.
- Statements before the first `-- name:` header may only be schema statements
  (`CREATE TABLE`, `CREATE INDEX`, ...). They feed the analyzer and generate nothing.
- The older two-word header `-- my_query :one` is still accepted.

### Parameters

Named placeholders (`:name`, `@name`, `$name`) become keyword-only arguments.
PostgreSQL supports numbered positional placeholders (`$1`, `$2`, ...) in
addition to named dollar placeholders: `$N` may be reused (e.g., `$1` appears
multiple times) and must form a dense 1-based sequence (no gaps). You cannot
mix `?` and `$N`, nor can you mix positional (`?` or `$N`) and named
placeholders in the same statement.

`?` placeholders are bound by position and named after the column they are
compared with or assigned to (`WHERE key = ?` → `key`), falling back to
`param_1`, `param_2`, .... A statement cannot mix the two styles.

Parameter types come from context:

| SQL                                        | inferred type                       |
| ------------------------------------------ | ----------------------------------- |
| `WHERE col = :p`, `col IN (:p)`, `BETWEEN` | type of `col`, not nullable         |
| `(:p IS NULL OR col = :p)`                 | type of `col`, nullable (`= None`)  |
| `INSERT ... VALUES (:p)`, `SET col = :p`   | type and nullability of `col`       |
| `LIMIT :p`, `OFFSET :p`                    | `int`                               |
| `datetime('now', :p)`, `lower(:p)`, ...    | `str` (per-function argument table) |
| anything else                              | `typing.Any` plus a warning         |

Override any of them with `-- param: <name> <python type>`.

#### List parameters

Declaring a `list[...]` type makes a parameter a list, for `IN` filters:

```sql
-- name: list_widgets_by_id :many
-- param: ids list[int]
SELECT id, name FROM widgets WHERE id IN (:ids);
```

- **asyncpg:** `IN (:ids)` becomes `= ANY($1)` and `NOT IN (:ids)` becomes
  `<> ALL($1)`, with the list bound as an array. Elsewhere (`= ANY(:ids)`),
  the list is simply passed as an array.
- **Other drivers:** the generated function expands the list into one
  placeholder per element at call time. An empty list becomes an empty
  subquery, so `IN` matches no rows and `NOT IN` matches every row.
- **Restrictions:** outside asyncpg, a list parameter may only appear as the
  sole item of an `IN (...)`. List parameters need named placeholders, and
  `:execmany` can't take them.

### Queries that would otherwise be built at runtime

butter generates code for fixed SQL, so assembling a query from strings at
runtime has no direct equivalent. Each common reason for doing that has a
static form:

| Instead of building... | Write |
| --- | --- |
| optional conditions (`where += " AND price >= %s"`) | `(:min_price IS NULL OR price >= :min_price)`, and pass `None` to skip it. PostgreSQL needs a cast on the tested parameter: `(:min_price::float8 IS NULL OR ...)` |
| `LIMIT {n}` / `OFFSET {n}` | `LIMIT :limit OFFSET :offset` |
| `IN (%s, %s, ...)` from `", ".join(...)` | a list parameter: `-- param: ids list[int]` with `IN (:ids)` |
| a sort chosen at runtime | `ORDER BY CASE WHEN :sort = 'price' THEN price END, id` |
| keyset pagination with an optional cursor | `(:before_id IS NULL OR id < :before_id) ORDER BY id DESC LIMIT :limit`. In PostgreSQL, cast the tested parameter: `(:before_id::bigint IS NULL OR ...)` |
| `... FOR UPDATE` only sometimes | two named queries, e.g. `get_listing` and `get_listing_for_update` |
| a table or column name from code | one named query per table or column. Identifiers can't be parameters |
| `SET SESSION max_execution_time = {ms}` (MySQL) | `SET SESSION max_execution_time = :ms` with `-- param: ms int` |
| `SET LOCAL lock_timeout = '{t}'` (PostgreSQL) | `SELECT set_config('lock_timeout', :timeout, true)`. `SET` can't take a parameter |

The `set_config(..., true)` form only lasts until the end of the current
transaction. Run it and the queries it should affect on one connection inside
`connection.transaction()`. Given a pool, the generated function runs it on
whichever connection it borrows, and the setting is gone before the next call.

The `mysql_static_idioms` and `postgresql_static_idioms` corpus cases both cover
the optional filter, `LIMIT`, list, sort and setting rows. MySQL also has the
`FOR UPDATE` pair and PostgreSQL the keyset query. Both were checked against
MySQL 5.7 and PostgreSQL 16.

### Result columns

`SELECT *` and `alias.*` expand from the schema. Expression columns are typed
through a table of SQLite functions (`COUNT` → `int`, `MAX(col)` → nullable type
of `col`, `COALESCE`, `CASE`, `||`, arithmetic, `CAST`, ...). Columns from the
outer side of a `LEFT`/`RIGHT`/`FULL JOIN` become nullable. CTEs, derived
tables, `UNION`s, subqueries and `RETURNING` clauses are all resolved.

Unaliased expressions are exposed as `column_N` (with a warning); alias them
with `AS`. Rows are mapped onto the model **by position**, so the SQL column
names never have to be valid Python identifiers.

### Type mapping

Declared SQL types follow SQLite's affinity rules:

| SQL type                                     | Python type          |
| -------------------------------------------- | -------------------- |
| `INTEGER`, `INT`, `BIGINT`, ... (`*INT*`)    | `int`                |
| `SERIAL`, `SMALLSERIAL`, `BIGSERIAL` (PostgreSQL) | `int`           |
| `REAL`, `FLOAT`, `DOUBLE`                    | `float`              |
| `NUMERIC`, `DECIMAL`                         | `float`              |
| `TEXT`, `VARCHAR`, `CHAR`, `CLOB`            | `str`                |
| `UUID` (PostgreSQL)                          | `str`                |
| `ENUM`, `SET` (MySQL)                        | `str`                |
| `BLOB`, or no declared type                  | `bytes`              |
| `BOOLEAN`                                    | `bool`               |
| `DATE` / `TIME` / `DATETIME`, `TIMESTAMP`    | `datetime.date` / `datetime.time` / `datetime.datetime` |
| `JSON`                                       | `typing.Any`         |

`NOT NULL` and `PRIMARY KEY` columns are non-optional; everything else gets `| None`.
PostgreSQL array columns become nested `list[...]` annotations. Array elements
remain optional because SQL arrays may contain `NULL`, independently of whether
the array column itself is nullable.

Project-wide overrides live in `butter.toml`. Dotted names are imported by the
generated modules, so you can point at your own enums or `Annotated` types with
custom validators and serializers:

```toml
[generate.python.column-types]        # by table.column
"ticket.status" = "app.types.Status"
"ticket.last_seen_at" = "app.types.SqlUtcTimestamp"

[generate.python.sql-types]           # by declared SQL type
"DATETIME" = "app.types.SqlUtcTimestamp"
```

Overrides apply to result columns **and** to parameters compared with or
assigned to that column. Parameters are serialized through the generated
`<Name>Params` model (`model_dump(by_alias=True)`), so pydantic serializers on
your custom types run before values reach the driver.

## Configuration reference

```toml
[generate]
dialect = "sqlite"              # sqlite | postgresql | mysql | generic
queries-dir = "sql/butter"      # every *.sql here becomes one module
schema-files = ["schema.sql"]   # optional extra CREATE TABLE files

# output-dir = "pkg/butter"    # older spelling; same as [generate.python] output-dir

[generate.python]
output-dir = "pkg/butter"       # receives <stem>.py per query file (+ __init__.py once)
driver = "aiosqlite"            # aiosqlite | pymysql | aiomysql | asyncpg; defaults by dialect

[generate.python.column-types]  # "table.column" = "python type"
[generate.python.sql-types]     # "SQLTYPE" = "python type"
```

## Commands

```sh
butter generate [PATH]   # PATH holds butter.toml; default "."
butter check [PATH]      # analyze and report, write nothing
```

Errors are reported as `file.sql:LINE: message` and nothing is written when any
query fails. Warnings (untyped parameters, unaliased expressions, unknown
functions, `"double quoted"` string literals) go to stderr.

## Generated code

- One module per SQL file; a private `_row_dict` helper only where needed.
- `<PascalName>Row` and `<PascalName>Params` pydantic models per query.
- The SQL text is emitted as `<NAME>` (or `<NAME>_1`, `<NAME>_2`, ...) module
  constants, so it can be logged or reused. Placeholders are rewritten into the
  driver's parameter style; nothing else changes.
- `__init__.py` is created once and never overwritten.
- Output passes `ruff check` and `ty`; `ruff format` may still reflow long lines.

### Drivers

You always write `:name` (or `?`, or `$N` for PostgreSQL) in the `.sql` files.
butter rewrites them for the configured driver:

- **`aiosqlite`**: `async def f(cursor: aiosqlite.Cursor, ...)`. The SQL is
  unchanged.
- **`pymysql`**: `def f(cursor: pymysql.cursors.Cursor, ...)`, synchronous.
  **`aiomysql`**: the same with `async def` and `aiomysql.Cursor`. Placeholders
  become `%(name)s` / `%s`. Literal `%` is doubled when the statement binds
  parameters, because the driver formats the query only then. Plain cursors
  and `DictCursor` both work: rows are mapped onto the model by position.
- **`asyncpg`**: `async def f(connection: asyncpg.Connection | asyncpg.Pool, ...)`.
  Named placeholders become `$1, $2, ...` in parameter order. `:execrows`
  reads the count from the command tag (`UPDATE 3`), and `:execmany` returns
  `None` because asyncpg reports no count. `:execlastid` is rejected: use
  `RETURNING id` with `:one`. A query with several statements cannot bind
  parameters, and it takes an `asyncpg.Connection` only, because a pool may
  run each statement on a different connection. `UUID` maps to `uuid.UUID`.

For the MySQL drivers and asyncpg, JSON/JSONB *parameters* are typed `str`:
pass serialized JSON (`json.dumps(value)`), which is what those drivers bind
without a custom codec. If you register a codec, override the type with
`[generate.python.sql-types]`.

## Limitations

- Type inference started with SQLite. PostgreSQL and MySQL functions are being
  added, and unknown ones fall back to `typing.Any` with a warning.
- No dynamic SQL. See "Queries that would otherwise be built at runtime" for
  the static form of each common pattern.
- Numbered `?NNN` placeholders are rejected.
- `SELECT *` depends on the declared column order matching the live database.

### MySQL-specific notes

MySQL's `VALUES(column)` function in `ON DUPLICATE KEY UPDATE` clauses is
properly typed: it refers to the value proposed by the `INSERT` for that column.

`TIME` values (columns, `TIME(...)`, `CURTIME()`) are typed
`datetime.timedelta` for `pymysql` and `aiomysql`, because that is what those
drivers return: MySQL `TIME` can exceed 24 hours.

MySQL date and time functions follow MySQL's own result types:
- `DATE()` gives a date and `TIME()` a time (in SQLite both return text).
- `DATE_ADD`/`DATE_SUB` keep a `DATE` for day-or-larger units and give a
  `DATETIME` otherwise.
- `STR_TO_DATE` gives a date or datetime depending on its format string.
- `UNIX_TIMESTAMP` of a fractional `DATETIME(n)` is a decimal.
- `CONVERT_TZ` is nullable, because it returns `NULL` for an unknown time zone.

`TINYINT(1)` (and its alias `BOOL`/`BOOLEAN`) is typed `bool`, following the
MySQL convention Prisma also uses. The driver returns `0`/`1`, which pydantic
converts. Wider `TINYINT(n)` columns stay `int`. Use
`[generate.python.column-types]` for a `TINYINT(1)` column that holds numbers.
