# butter — I can't believe it's not SQL

`butter` turns plain SQL files into typed data-access code, in the spirit of
[sqlc](https://sqlc.dev). You write the SQL you would run anyway, annotate each
statement with a name and a result shape, and `butter generate` writes one
module per SQL file with an `async` function per query and a
[pydantic](https://docs.pydantic.dev) model tailored to each result set.

Today it generates **Python for `aiosqlite` + pydantic**. The parser
([sqlparser-rs](https://github.com/apache/datafusion-sqlparser-rs)) understands
SQLite, PostgreSQL, MySQL and a generic dialect, but type inference and the
generated driver code are SQLite-first.

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
| `REAL`, `FLOAT`, `DOUBLE`                    | `float`              |
| `NUMERIC`, `DECIMAL`                         | `float`              |
| `TEXT`, `VARCHAR`, `CHAR`, `CLOB`            | `str`                |
| `BLOB`, or no declared type                  | `bytes`              |
| `BOOLEAN`                                    | `bool`               |
| `DATE` / `TIME` / `DATETIME`, `TIMESTAMP`    | `datetime.date` / `datetime.time` / `datetime.datetime` |
| `JSON`                                       | `str`                |

`NOT NULL` and `PRIMARY KEY` columns are non-optional; everything else gets `| None`.

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
driver = "aiosqlite"            # the only driver for now

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
- The SQL text is emitted verbatim as `<NAME>` (or `<NAME>_1`, `<NAME>_2`, ...)
  module constants, so it can be logged or reused.
- `__init__.py` is created once and never overwritten.
- Output passes `ruff check` and `ty`; `ruff format` may still reflow long lines.

## Limitations

- SQLite-oriented type inference. Other dialects parse, but their functions and
  types mostly fall back to `typing.Any` with warnings.
- No dynamic SQL: write one named query per variant (`..._by_status`,
  `..._ignore_conflicts`) or use `(:p IS NULL OR col = :p)`.
- Numbered `?NNN` placeholders are rejected.
- `SELECT *` depends on the declared column order matching the live database.
