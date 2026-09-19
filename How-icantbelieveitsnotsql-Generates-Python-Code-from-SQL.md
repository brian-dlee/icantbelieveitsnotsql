# How butter Generates Python Code from SQL

## Overview

`butter` is a Rust CLI tool that generates typed async Python code directly from annotated SQL files. Inspired by [sqlc](https://sqlc.dev/), the project turns plain `.sql` files into Python modules with typed, async functions — you write SQL with `-- name: function_name` and `:one|:many|:exec|:execrows|:execlastid|:execmany` annotations, and the tool produces async Python functions with pydantic models and zero runtime dependencies beyond `aiosqlite` and `pydantic`.

The core approach from the [README](https://app.dosu.dev/documents/24f032c8-700c-4190-b2ff-abe38b38187f):

> `butter` turns plain SQL files into typed data-access code, in the spirit of sqlc. You write the SQL you would run anyway, annotate each statement with a name and a result shape, and `butter generate` writes one module per SQL file with an `async` function per query and a pydantic model tailored to each result set.

At a high level, the pipeline is:

1. **Configure** — a `butter.toml` file in your project directory specifies the SQL dialect, paths to your schema and query files, and where to write generated output.
2. **Parse** — the tool reads `CREATE TABLE` statements from query files and optional `schema-files` to build a column-type map, then parses each `.sql` query file using [sqlparser-rs](https://github.com/apache/datafusion-sqlparser-rs) to produce an AST.
3. **Analyze** — the analyzer extracts `-- name: function_name :cardinality` headers, resolves output columns through `*` expansion, joins, CTEs, unions, subqueries, and `RETURNING`, and infers parameter types from context (compared columns, assignments, function arguments, `LIMIT`/`OFFSET`, or `IS NULL` patterns).
4. **Generate** — for each `.sql` file, a corresponding `.py` file is emitted containing pydantic `<Name>Row` and `<Name>Params` models, SQL string constants, and fully-typed async functions that use `aiosqlite.Cursor`.

The tool currently targets Python with the aiosqlite driver as its only output, with the module structure split across [[1]](https://github.com/brian-dlee/icantbelieveitsnotsql/tree/HEAD/src):

| File | Responsibility |
|------|---------------|
| `src/main.rs` | CLI entry point (`generate`, `check` subcommands) |
| `src/config.rs` | Configuration types and TOML parsing |
| `src/dialect.rs` | SQL dialect enum mapping |
| `src/schema.rs` | Schema parsing and column-type lookup |
| `src/queryfile.rs` | Query file splitting and header extraction |
| `src/analyze.rs` | Type inference, column resolution, parameter analysis |
| `src/python.rs` | Python source file emission (pydantic models, async functions) |
| `src/generate.rs` | Orchestration loop |

## CLI and Configuration

### Invocation

The CLI provides two subcommands, each taking an optional path to the project directory (defaults to `.`) [[2]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/main.rs):

```sh
# Generate from the current directory
butter generate

# Point at a specific project
butter generate ./example/aiosqlite

# Analyze without writing files
butter check ./example/aiosqlite
```

The `generate` command writes output files; the `check` command analyzes queries and reports errors without writing anything.

### `butter.toml`

On startup the CLI reads a `butter.toml` file from the project directory [[3]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/config.rs). Configuration has two main sections:

**`[generate]`** (top-level settings):

| Field | Default | Description |
|-------|---------|-------------|
| `dialect` | `"sqlite"` | SQL dialect used during parsing |
| `queries-dir` | `"sql/butter"` | Directory containing `.sql` query files |
| `schema-files` | `[]` | Optional list of paths to DDL schema files |

**`[generate.python]`** (Python-specific settings):

| Field | Default | Description |
|-------|---------|-------------|
| `output-dir` | (required) | Directory where generated `.py` files are written |
| `driver` | `"aiosqlite"` | Database driver (only `aiosqlite` supported currently) |

**`[generate.python.column-types]`** and **`[generate.python.sql-types]`** (optional type overrides):

Type overrides allow you to replace the inferred Python type for specific columns or SQL types with custom types (e.g., enums, `Annotated` types with validators). Dotted names are imported by the generated modules.

Example from the aiosqlite example project [[5]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/example/aiosqlite/butter.toml):

```toml
[generate]
dialect = "sqlite"
queries-dir = "sql/butter"
# Tables can be declared inside query files or in separate schema files
# schema-files = ["sql/schema.sql"]

[generate.python]
output-dir = "app/butter"
driver = "aiosqlite"

[generate.python.column-types]
"ticket.status" = "app.types.Status"
"ticket.last_seen_at" = "app.types.SqlUtcTimestamp"

# [generate.python.sql-types]
# "DATETIME" = "app.types.SqlUtcTimestamp"
```

### SQL Dialects

The `dialect` field maps to one of four supported [sqlparser-rs](https://github.com/apache/datafusion-sqlparser-rs) dialects [[6]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/dialect.rs):

| Config value | sqlparser dialect |
|---|---|
| `"generic"` | `GenericDialect` |
| `"sqlite"` | `SQLiteDialect` |
| `"postgresql"` | `PostgreSqlDialect` |
| `"mysql"` | `MySqlDialect` |

Only the `sqlite` dialect has full type inference support; other dialects parse but produce `typing.Any` for most types with warnings.

### Processing Loop

After reading configuration the tool [[7]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/generate.rs):

1. Parses the selected dialect and reads optional `schema-files` using `parse_schema_file()`, building a schema that maps each table name to its column types and constraints.
2. Iterates over every `.sql` file in `queries-dir`.
3. For each `.sql` file:
   - Runs a header extraction pass (`split_query_file()`) that separates preamble schema statements (before the first `-- name:` header) from annotated query blocks.
   - Parses preamble statements and merges any `CREATE TABLE` definitions into the schema.
   - Parses each annotated query block, runs the analyzer to infer parameter types and resolve output columns, and collects any warnings or errors.
4. If any query has errors, prints diagnostics as `file.sql:LINE: message` and exits without writing files.
5. Creates the `output-dir` if it does not exist, writes `__init__.py` if missing (once), and calls `generate_python_file()` to write a `.py` file whose stem matches the `.sql` file's stem.

Files with no annotated queries are skipped.

## Query Annotation Syntax

### Annotation Headers

Every SQL statement that should produce generated code must be preceded by an annotation header in the form [[8]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/queryfile.rs):

```
-- name: <function_name> :<command>
-- optional free-form docstring lines
-- param: <name> <python type>    (optional parameter type override)
-- column: <name> <python type>   (optional result column type override)
SELECT ...;
```

- **`function_name`** — a valid Python identifier (cannot be a Python keyword) that becomes the name of the generated function.
- **`command`** — one of `:one`, `:many`, `:exec`, `:execrows`, `:execlastid`, or `:execmany`.

The six command values map to different execution patterns [[9]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/types.rs):

| Command | Generated behavior |
|---|---|
| `:one` | `execute()` + `fetchone()` → `<Name>Row \| None` |
| `:many` | `execute()` + `fetchall()` → `list[<Name>Row]` |
| `:exec` | `execute()` per statement → `None` (no fetch) |
| `:execrows` | `execute()` → `int` (`cursor.rowcount`) |
| `:execlastid` | `execute()` → `int \| None` (`cursor.lastrowid`) |
| `:execmany` | `executemany()` → `int` (`cursor.rowcount`), takes `rows: Iterable[<Name>Params]` |

**Special rules:**

- `:exec` is the only command that may contain **multiple statements** (e.g., `CREATE TABLE` + several `CREATE INDEX`). Multi-statement `:exec` queries must use named placeholders.
- Statements before the first `-- name:` header may only be schema statements (`CREATE TABLE`, `CREATE INDEX`, ...). They are parsed to populate the schema and generate nothing.

#### Example

From `example/aiosqlite/sql/butter/ticket.sql` [[11]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/example/aiosqlite/sql/butter/ticket.sql):

```sql
-- name: create_table_ticket :exec
CREATE TABLE IF NOT EXISTS ticket (
    reference TEXT NOT NULL UNIQUE COLLATE NOCASE,
    label TEXT,
    status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ticket_status ON ticket (status);

-- name: select_many_ticket_by_status :many
SELECT *
FROM ticket
WHERE status = :status;

-- name: upsert_many_ticket :execmany
-- Refreshes the label, reason and last-seen time of an existing ticket.
INSERT INTO ticket (reference, label, reason, last_seen_at, active_since, status)
VALUES (:reference, :label, :reason, :last_seen_at, :active_since, :status)
ON CONFLICT(reference) DO UPDATE SET
    label = excluded.label,
    reason = excluded.reason,
    last_seen_at = excluded.last_seen_at;
```

### Header Extraction

`split_query_file()` in `src/queryfile.rs` performs a line-by-line scan of the raw SQL text [[8]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/queryfile.rs). It:

1. Separates preamble schema statements (before the first `-- name:` header) from annotated query blocks.
2. For each `-- name:` header, parses the function name and command, then collects subsequent comment lines into a docstring until the SQL statement begins.
3. Extracts optional `-- param:` and `-- column:` type overrides from the header block.
4. Detects statement boundaries at `;` and emits one query block per `-- name:` header.

The resulting `Vec<QueryBlock>` is then parsed and analyzed one block at a time.

### Placeholder Styles

Named placeholders (`:name`, `@name`, `$name`) and anonymous `?` placeholders are recognized [[16]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/analyze.rs):

| Style | Example | Python argument | Parameter binding |
|---|---|---|---|
| Named | `:id`, `@email`, `$name` | keyword-only (`*, id: int, email: str`) | `dict` |
| Anonymous | `?` | keyword-only, named after column or `param_N` | `tuple` |

Numbered placeholders (`?1`, `?2`) are rejected. A single statement cannot mix named and anonymous styles.

Placeholders are collected by recursively walking the sqlparser expression tree, handling `BinaryOp`, `UnaryOp`, `Between`, `InList`, `Like`, `Case`, `Function`, subqueries, and more. Duplicate named placeholders are deduplicated; duplicate `?` placeholders each get their own positional slot.

### Type Inference for Input Parameters

Parameter types are inferred from the surrounding SQL context [[19]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/analyze.rs):

| Context | Inferred type |
|---|---|
| `WHERE col = :p`, `col IN (:p)`, `BETWEEN :a AND :b` | type of `col`, not nullable |
| `(:p IS NULL OR col = :p)` | type of `col`, nullable (default `None`) |
| `INSERT ... VALUES (:p)`, `SET col = :p` | type and nullability of `col` |
| `LIMIT :p`, `OFFSET :p` | `int` |
| `datetime('now', :p)`, `lower(:p)`, function arguments | per-function table (e.g., `str` for `datetime` modifiers) |
| anything else | `typing.Any` (with a warning) |

Type resolution follows SQLite's affinity rules [[20]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/analyze.rs):

| SQL type | Python type |
|---|---|
| `INTEGER`, `INT`, `BIGINT`, `SMALLINT`, … (`*INT*`) | `int` |
| `REAL`, `FLOAT`, `DOUBLE` | `float` |
| `NUMERIC`, `DECIMAL` | `float` |
| `TEXT`, `VARCHAR`, `CHAR`, `CLOB` | `str` |
| `BLOB`, or no declared type | `bytes` |
| `BOOLEAN` | `bool` |
| `DATE` / `TIME` / `DATETIME`, `TIMESTAMP` | `datetime.date` / `datetime.time` / `datetime.datetime` |
| `JSON` | `str` |

`NOT NULL` and `PRIMARY KEY` columns produce non-optional parameters; all others get `| None`. Column-specific and SQL-type overrides from `butter.toml` apply to both parameters and result columns.

### Output Field Resolution

For `SELECT` queries, each item in the projection is resolved through [[21]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/analyze.rs):

- `*` and `alias.*` expansion from the schema
- Column resolution: unqualified names are looked up across active tables; ambiguous names raise an error
- Expression type inference: a table of SQLite functions (`COUNT` → `int`, `MAX(col)` → nullable type of `col`, `COALESCE`, `CASE`, `||`, arithmetic, `CAST`, ...)
- Join nullability: columns from the outer side of a `LEFT`/`RIGHT`/`FULL JOIN` become nullable
- CTE, derived table, `UNION`, subquery, and `RETURNING` clause resolution

Unaliased expressions are exposed as `column_N` with a warning; alias them with `AS`. Rows are mapped by position, so SQL column names need not be valid Python identifiers.

## Code Generation

Python code is generated by `generate_python_file()` in `src/python.rs` [[22]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/python.rs). Each `.sql` input file produces exactly one `.py` output file with the same stem (e.g., `sql/butter/ticket.sql` → `app/butter/ticket.py`).

### File Structure

Every generated file follows this layout [[23]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/python.rs):

```python
# Code generated by butter. DO NOT EDIT.
# Source: sql/butter/ticket.sql

import typing

import aiosqlite
import pydantic

import app.types  # only if custom column types are imported


def _row_dict(model: type[pydantic.BaseModel], row: typing.Sequence[object]) -> dict[str, object]:
    """Maps a result row onto the model's fields by position, so unaliased expressions work."""
    return dict(zip(model.model_fields, row, strict=True))

# ... one block per annotated query ...
```

The `_row_dict` helper is only emitted when the file contains at least one `:one` or `:many` query (i.e., queries that return rows). Custom type imports (from `column-types` overrides) are added to the import block as needed. The generated code depends on `aiosqlite` and `pydantic`, and optionally on your custom types.

### Per-Query Block

For each annotated query the generator emits three to four elements [[25]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/python.rs):

**1. SQL constant(s)**

For single-statement queries, the SQL is emitted as a module constant in `SCREAMING_SNAKE_CASE`:

```python
SELECT_MANY_TICKET_BY_STATUS = """
SELECT *
FROM ticket
WHERE status = :status
"""
```

For multi-statement `:exec` queries, each statement becomes `<NAME>_1`, `<NAME>_2`, etc.:

```python
CREATE_TABLE_TICKET_1 = """CREATE TABLE IF NOT EXISTS ticket (...)"""
CREATE_TABLE_TICKET_2 = """CREATE INDEX IF NOT EXISTS idx_ticket_status ON ticket (status)"""
```

**2. Row model** (`:one` and `:many` only)

A pydantic `BaseModel` subclass named `{PascalCase}Row` is emitted with one field per output column. Field names come from the SQL `AS` aliases or bare column names; Python keywords are escaped by appending `_`. Types are inferred from the schema and expression analysis [[26]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/python.rs):

```python
class SelectManyTicketByStatusRow(pydantic.BaseModel):
    """One result row of `select_many_ticket_by_status`."""

    reference: str
    label: str | None
    status: app.types.Status
```

**3. Params model** (when the query has parameters)

A pydantic `BaseModel` subclass named `{PascalCase}Params` is emitted with one field per parameter. Default values are `None` for optional parameters, `pydantic.Field(default=None)` for nullable columns:

```python
class SelectManyTicketByStatusParams(pydantic.BaseModel):
    """Parameters of `select_many_ticket_by_status`."""

    status: app.types.Status
```

**4. Async function**

A typed async function is emitted with `cursor: aiosqlite.Cursor` as the first parameter. If the query has input parameters, a bare `*` is inserted to make them keyword-only (except for `:execmany`, which takes `rows: Iterable[<Name>Params]`). The function body constructs the params dict or tuple via `model_dump(by_alias=True)`, awaits `cursor.execute()`, and returns the appropriate result [[27]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/python.rs):

```python
async def select_many_ticket_by_status(
    cursor: aiosqlite.Cursor,
    *,
    status: app.types.Status,
) -> list[SelectManyTicketByStatusRow]:
    """Runs `select_many_ticket_by_status`."""
    params = SelectManyTicketByStatusParams(status=status).model_dump(by_alias=True)
    await cursor.execute(SELECT_MANY_TICKET_BY_STATUS, params)
    return [SelectManyTicketByStatusRow.model_validate(_row_dict(SelectManyTicketByStatusRow, row)) for row in await cursor.fetchall()]
```

Docstrings are populated from the `-- name:` header comment block, or default to `"Runs \`<name>\`."`.

### Parameter Binding Style

Parameters are serialized through the generated `<Name>Params` model [[28]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/python.rs):

| Placeholder style | Binding code |
|---|---|
| Named (`:name`, `@name`, `$name`) | `params = <Name>Params(...).model_dump(by_alias=True)` (dict) |
| Anonymous (`?`) | `params = tuple(<Name>Params(...).model_dump(by_alias=True).values())` (tuple) |

This ensures pydantic validators and serializers (e.g., enum conversions, timestamp formatting) run before values reach the database driver.

### Fetch and Return Patterns per Command

| Command | Execution | Return type | Return value |
|---|---|---|---|
| `:exec` | `await cursor.execute(...)` per statement | `None` | implicit |
| `:execrows` | `await cursor.execute(...)` | `int` | `cursor.rowcount` |
| `:execlastid` | `await cursor.execute(...)` | `int \| None` | `cursor.lastrowid` |
| `:execmany` | `await cursor.executemany(...)` | `int` | `cursor.rowcount` |
| `:one` | `await cursor.fetchone()` | `<Name>Row \| None` | `None` or validated model |
| `:many` | `await cursor.fetchall()` | `list[<Name>Row]` | list comprehension of validated models |

[[29]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/python.rs)

### Duplicate Name Detection

Before any code is written, `generate_python_file()` scans all query names and raises an error if two queries in the same file would produce the same model name [[30]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/python.rs). This prevents silent class shadowing in the output module.

## Example Project

The `example/aiosqlite/` directory in the repository provides a complete, runnable illustration of the full pipeline [[31]](https://github.com/brian-dlee/icantbelieveitsnotsql/tree/HEAD/example/aiosqlite).

### Directory Layout

```
example/aiosqlite/
├── butter.toml              # generator configuration
├── sql/butter/
│   ├── cache_entry.sql      # CREATE TABLE + queries
│   ├── membership.sql
│   ├── tag.sql
│   └── ticket.sql
├── app/
│   ├── types.py             # custom Status enum, SqlUtcTimestamp type
│   └── butter/              # generated modules
│       ├── __init__.py
│       ├── cache_entry.py
│       ├── membership.py
│       ├── tag.py
│       └── ticket.py
├── sample_butter_script.py  # demo usage
└── tests/
    └── test_butter.py       # pytest suite with 8 tests
```

### `butter.toml` [[5]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/example/aiosqlite/butter.toml)

```toml
[generate]
dialect = "sqlite"
queries-dir = "sql/butter"

[generate.python]
output-dir = "app/butter"
driver = "aiosqlite"

[generate.python.column-types]
"ticket.status" = "app.types.Status"
"ticket.last_seen_at" = "app.types.SqlUtcTimestamp"
"ticket.active_since" = "app.types.SqlUtcTimestamp"
```

### `sql/butter/ticket.sql`

The query file contains schema definitions, lifecycle management, and reads [[32]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/example/aiosqlite/sql/butter/ticket.sql):

```sql
-- name: create_table_ticket :exec
CREATE TABLE IF NOT EXISTS ticket (
    reference TEXT NOT NULL UNIQUE COLLATE NOCASE,
    label TEXT,
    status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ticket_status ON ticket (status);

-- name: select_many_ticket_by_status :many
SELECT *
FROM ticket
WHERE status = :status;

-- name: upsert_many_ticket :execmany
-- Refreshes the label, reason and last-seen time of an existing ticket.
INSERT INTO ticket (reference, label, reason, last_seen_at, active_since, status)
VALUES (:reference, :label, :reason, :last_seen_at, :active_since, :status)
ON CONFLICT(reference) DO UPDATE SET
    label = excluded.label,
    reason = excluded.reason,
    last_seen_at = excluded.last_seen_at;

-- name: archive_stale_ticket :execrows
-- `duration` is a SQLite modifier such as '-7 days'.
UPDATE ticket
SET status = 'ARCHIVED', active_since = NULL
WHERE last_seen_at < datetime('now', :duration);
```

### Running the Example

```sh
cd example/aiosqlite
butter generate
uv run python sample_butter_script.py
uv run pytest
```

### `sample_butter_script.py`

The sample script [[33]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/example/aiosqlite/sample_butter_script.py) demonstrates the generated async functions:

```python
import asyncio
import aiosqlite
from app.butter.cache_entry import (
    InsertManyCacheEntryParams, create_table_cache_entry,
    insert_many_cache_entry, select_many_cache_entry_by_key,
)

async def main():
    async with aiosqlite.connect(":memory:") as connection, connection.cursor() as cursor:
        await create_table_cache_entry(cursor)
        await insert_many_cache_entry(cursor, [InsertManyCacheEntryParams(key="greeting", value=b"...")])
        rows = await select_many_cache_entry_by_key(cursor, key="greeting")
        print(rows)

asyncio.run(main())
```

### `tests/test_butter.py`

The pytest suite [[34]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/example/aiosqlite/tests/test_butter.py) contains 8 tests exercising every generated function:

- `:exec` queries that create tables
- `:one` queries that return `None` for missing rows
- `:many` queries with custom enum types (`Status`) and timestamps
- `:execmany` queries (`insert_many_ticket`, `upsert_many_ticket`, `insert_many_ticket_ignore_conflicts`)
- `:execrows` queries that return `cursor.rowcount` (`archive_stale_ticket`, `activate_recent_ticket`)
- `LEFT JOIN` nullability, `ON CONFLICT` variants, aggregate functions
- `aiosqlite.Row` compatibility (unaliased expressions mapped by position)

## Current Limitations

`butter` focuses on SQLite and async Python for aiosqlite. The following constraints apply to the current implementation.

### Dialect Support

Four dialects are supported for parsing: `generic`, `sqlite`, `postgresql`, and `mysql` [[35]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/dialect.rs). Only `sqlite` has full type inference; other dialects parse but produce `typing.Any` for most types with warnings. CockroachDB is not supported (sqlparser-rs does not include a CockroachDB dialect).

### Python and aiosqlite Only

Only async Python for the `aiosqlite` driver is currently implemented [[36]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/python.rs). No synchronous drivers (e.g., `sqlite3`) or other languages are supported.

### No Dynamic SQL

`butter` generates one function per query variant. Dynamic `WHERE` clauses or optional filters require multiple named queries (e.g., `select_many_ticket`, `select_many_ticket_by_status`) or the `(:p IS NULL OR col = :p)` pattern for nullable parameters [[37]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/example/aiosqlite/sql/butter/ticket.sql).

### Numbered Placeholders Rejected

`?1`, `?2`, etc. (numbered positional placeholders) are not supported [[38]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/analyze.rs). Use `?` (anonymous) or named placeholders (`:name`, `@name`, `$name`).

### `SELECT *` Depends on Schema Order

Wildcard projections expand from the schema's declared column order. If the live database has a different order (e.g., due to `ALTER TABLE ADD COLUMN`), row mapping by position will break [[39]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/HEAD/src/analyze.rs). Explicit column lists avoid this.
