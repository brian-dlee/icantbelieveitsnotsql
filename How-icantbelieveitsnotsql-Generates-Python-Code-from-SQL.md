# How icantbelieveitsnotsql Generates Python Code from SQL

## Overview

`icantbelieveitsnotsql` is a Rust CLI tool (v0.1.0) that generates typed Python code directly from annotated SQL files. Inspired by [sqlc](https://sqlc.dev/), the project is explicitly [designed to be "the thinnest possible layer between SQL and a desired programming language"](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/README.md#L1-L6) — you write plain `.sql` files, annotate each query with a name and cardinality, and the tool produces callable Python functions with zero runtime dependencies in the generated code.

The core promise from the [README](https://app.dosu.dev/documents/24f032c8-700c-4190-b2ff-abe38b38187f):

> Create code for any target application language from SQL. Inspired by sqlc, but designed to be the thinnest possible layer between SQL and a desired programming language. Write SQL files directly, and construct callable code with zero dependencies.

At a high level, the pipeline is:

1. **Configure** — a `butter.toml` file in your project directory specifies the SQL dialect, paths to your schema and query files, and where to write generated output.
2. **Parse** — the tool reads your `schema.sql` to build a column-type map, then parses each `.sql` query file using [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs) to produce an AST.
3. **Annotate** — a pre-pass over the raw SQL text extracts `-- name :cardinality` comments that name each query and declare whether it returns one row, many rows, or no rows.
4. **Generate** — for each `.sql` file, a corresponding `.py` file is emitted containing a `_Cursor` protocol, SQL string constants, row dataclasses, and fully-typed query functions.

The tool currently targets Python as its only output language, with the module structure split across [[1]](https://github.com/brian-dlee/icantbelieveitsnotsql/tree/HEAD/src):

| File | Responsibility |
|------|---------------|
| `src/main.rs` | CLI entry point and orchestration loop |
| `src/config.rs` | Configuration types and dialect enum |
| `src/schema.rs` | Schema file parsing and column-type lookup |
| `src/query.rs` | Annotation extraction, placeholder parsing, type inference |
| `src/codegen/python.rs` | Python source file emission |

## CLI and Configuration

### Invocation

The binary takes a single optional positional argument — a path to the project directory. When omitted, it defaults to the current working directory [[2]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/main.rs#L23-L26):

```sh
# Run from inside the project directory
icantbelieveitsnotsql

# Or point at a specific project
icantbelieveitsnotsql ./example/sqlite
```

### `butter.toml`

On startup the CLI reads a `butter.toml` file from the project directory [[3]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/main.rs#L26-L31). All fields live under a `[generate]` table and are optional, with the defaults shown below [[4]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/config.rs#L36-L53):

| Field | Default | Description |
|-------|---------|-------------|
| `dialect` | `"generic"` | SQL dialect used during parsing |
| `queries-dir` | `"queries"` | Directory containing `.sql` query files |
| `schema-file` | `"schema.sql"` | Path to the DDL schema file |
| `output-dir` | `"generated"` | Directory where generated `.py` files are written |

Example from the SQLite example project [[5]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/example/sqlite/butter.toml):

```toml
[generate]
dialect = "sqlite"
queries-dir = "./queries"
schema-file = "schema.sql"
output-dir = "./generated"
```

### SQL Dialects

The `dialect` field maps to one of four supported [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs) dialects [[6]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/config.rs#L6-L28):

| Config value | sqlparser dialect |
|---|---|
| `"generic"` | `GenericDialect` |
| `"sqlite"` | `SQLiteDialect` |
| `"postgresql"` | `PostgreSqlDialect` |
| `"mysql"` | `MySqlDialect` |

Any other value causes an `SQLDialectError::Unsupported` error at startup.

### Processing Loop

After reading configuration the tool [[7]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/main.rs#L80-L200):

1. Reads and parses `schema.sql` using `parse_schema_file()`, building a `SchemaParseResult` that maps each table name to its column types.
2. Creates the `output-dir` if it does not exist.
3. Iterates over every file in `queries-dir`; files without a `.sql` extension are silently skipped.
4. For each `.sql` file: runs the SQL through `SQLParser::parse_sql()` to produce an AST, runs `extract_query_annotations()` as a pre-pass over the raw text, zips the two results together, warns and skips any statement without an annotation, and calls `process_sql_statement()` on each annotated query.
5. Calls `generate_python_file()` to write a `.py` file whose stem matches the `.sql` file's stem.

Files with no annotated queries produce a warning and no output file.

## Query Annotation Syntax

### Annotation Comments

Every SQL statement that should produce generated code must be preceded by an annotation comment in the form [[8]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L154-L231):

```
-- <name> :<cardinality>
```

- **`name`** — a valid Python identifier (cannot be a Python keyword) that becomes the name of the generated function.
- **`cardinality`** — one of `:one`, `:many`, or `:exec`.

The three cardinality values map to the `QueryCardinality` enum in `src/query.rs` [[9]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L12-L26):

| Annotation | Enum variant | Generated behavior |
|---|---|---|
| `:one` | `QueryCardinality::One` | `fetchone()` → `Optional[XxxRow]` |
| `:many` | `QueryCardinality::Many` | `fetchall()` → `list[XxxRow]` |
| `:exec` | `QueryCardinality::Exec` | no fetch → `None` |

A blank line between the annotation comment and the SQL statement is allowed and does not break the association. Statements without an annotation comment are warned about and skipped entirely [[10]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/main.rs#L139-L148).

#### Example

From `example/sqlite/queries/main.sql` [[11]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/example/sqlite/queries/main.sql#L1-L18):

```sql
-- get_user_by_id :one
SELECT id AS user_id, email, created_at
FROM users
WHERE id = :id;

-- get_orders_with_items :many
SELECT o.order_id, o.status, oi.product_id, oi.quantity
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.customer_id = :customer_id;

-- create_user :exec
INSERT INTO users (email, created_at)
VALUES (:email, datetime('now'));
```

### The Annotation Pre-Pass

`extract_query_annotations()` in `src/query.rs` performs a line-by-line scan of the raw SQL text before the AST is involved [[8]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L154-L231). It:

1. Strips leading whitespace from each line.
2. Skips blank lines.
3. For comment lines (starting with `--`), attempts to parse `<name> :<cardinality>`. Valid annotations are held in `pending_annotation`.
4. Detects statement boundaries at `;` and emits the pending annotation (or `None` if none was set) into the output `Vec`.
5. Validates the annotation name: it must be a valid Python identifier and must not be a Python keyword; invalid names produce a warning and are skipped rather than hard-failing [[12]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L182-L213).

The resulting `Vec<Option<QueryAnnotation>>` is zipped with the sqlparser AST statements one-to-one in `src/main.rs` [[13]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/main.rs#L135-L163). This annotation logic was originally implemented in `src/main.rs` in [PR #1](https://github.com/brian-dlee/icantbelieveitsnotsql/pull/1) [[14]](https://github.com/brian-dlee/icantbelieveitsnotsql/pull/1) and later moved to `src/query.rs` as part of the module-split refactor in [PR #5](https://github.com/brian-dlee/icantbelieveitsnotsql/pull/5) [[15]](https://github.com/brian-dlee/icantbelieveitsnotsql/pull/5).

### Placeholder Styles

Three placeholder syntaxes are recognized [[16]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L39-L47):

| Style | Example | Normalized name | Parameter style emitted |
|---|---|---|---|
| Named | `:id`, `:email` | `id`, `email` | `dict` (named) |
| Dollar | `$1`, `$2` | `p1`, `p2` | `tuple` (positional) |
| Anonymous | `?` | `p1`, `p2`, … (counted) | `tuple` (positional) |

Placeholders are collected by recursively walking the sqlparser expression tree in `collect_placeholders()` [[17]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L238-L308), which handles `BinaryOp`, `UnaryOp`, `Between`, `InList`, `Like`, `Case`, `Function`, and more. Duplicate named placeholders are deduplicated; duplicate anonymous `?` placeholders each get their own positional slot [[18]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L314-L369).

### Type Inference for Input Parameters

For **named** (`:name`) placeholders, `resolve_param_type()` looks the parameter name up against the active tables' column definitions in the parsed schema [[19]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L371-L391). If exactly one match is found, `sql_type_to_python()` converts the SQL type to a Python type annotation [[20]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L397-L460):

| SQL types | Python type |
|---|---|
| `INTEGER`, `INT`, `BIGINT`, `SMALLINT`, … | `int` |
| `TEXT`, `VARCHAR`, `CHAR`, `CHARACTER VARYING`, … | `str` |
| `REAL`, `FLOAT`, `DOUBLE`, `DOUBLE PRECISION`, … | `float` |
| `BOOLEAN`, `BOOL` | `bool` |
| `BLOB`, `BYTEA`, `BINARY`, `VARBINARY`, … | `bytes` |
| `NUMERIC`, `DECIMAL`, `DEC`, `MONEY`, … | `Decimal` |
| `DATE` | `datetime.date` |
| `TIME`, `TIMETZ`, `TIME WITH TIME ZONE` | `datetime.time` |
| `TIMESTAMP`, `TIMESTAMPTZ`, `DATETIME`, … | `datetime.datetime` |
| `UUID` | `str` |
| `JSON`, `JSONB` | `Any` |

If no column match is found, the name matches multiple tables (ambiguous), or the placeholder is positional (`?` / `$N`), the type falls back to `Any`.

### Output Field Resolution

For `SELECT` queries, each item in the projection is resolved back to its source table and column so it can be used in the generated row dataclass. Unqualified column names (e.g., `email`) are looked up across all active tables [[21]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L546-L581). If the column appears in more than one table an `AmbiguousFieldReference` error is raised; if it appears in none an `InvalidFieldReference` error is raised. Fully qualified column references (`table.column`) resolve directly. `AS` aliases are used as the generated Python field name.

## Code Generation

Python code is generated by `generate_python_file()` in `src/codegen/python.rs` [[22]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/codegen/python.rs#L78-L257). Each `.sql` input file produces exactly one `.py` output file with the same stem (e.g., `queries/main.sql` → `generated/main.py`).

### File Structure

Every generated file follows this layout [[23]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/codegen/python.rs#L121-L152):

```python
# GENERATED BY icantbelieveitsnotsql -- DO NOT EDIT
# Source: main.sql

from __future__ import annotations

import dataclasses
import datetime           # only if datetime types are needed
from decimal import Decimal  # only if Decimal types are needed
from typing import Any, Optional, Protocol


class _Cursor(Protocol):
    def execute(self, sql: str, parameters: Any = ...) -> Any: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...

# ... one block per annotated query ...
```

The `_Cursor` Protocol is the only structural type the generated code requires from the caller — any database cursor that implements `execute`, `fetchone`, and `fetchall` is compatible. This keeps the generated module usable with any DB-API 2.0-compliant driver. The stdlib imports (`datetime`, `Decimal`) are only emitted when the inferred parameter types actually need them [[24]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/codegen/python.rs#L65-L75). No third-party packages are ever imported.

### Per-Query Block

For each annotated query the generator emits three to four elements [[25]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/codegen/python.rs#L154-L251):

**1. SQL constant**

The SQL statement is serialized back from the AST (trailing semicolons stripped) into a triple-quoted string constant. The name is the query name converted to `SCREAMING_SNAKE_CASE` prefixed with `_` and suffixed with `_SQL`:

```python
_GET_USER_BY_ID_SQL = """
SELECT id AS user_id, email, created_at FROM users WHERE id = :id
"""
```

**2. Row dataclass** (`:one` and `:many` only)

A `@dataclasses.dataclass` named `{PascalCase}Row` is emitted with one field per output column. Field names come from the SQL `AS` aliases or bare column names; Python keywords are escaped by appending `_`. Currently all output field types are annotated as `Any` [[26]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/codegen/python.rs#L300-L304):

```python
@dataclasses.dataclass
class GetUserByIdRow:
    user_id: Any
    email: Any
    created_at: Any
```

**3. Query function**

A typed function is emitted with `cursor: _Cursor` as the first parameter. If the query has input parameters, a bare `*` is inserted to make them keyword-only, followed by each parameter with its inferred Python type [[27]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/codegen/python.rs#L202-L217):

```python
def get_user_by_id(cursor: _Cursor, *, id: int) -> Optional[GetUserByIdRow]:
    cursor.execute(_GET_USER_BY_ID_SQL, {"id": id})
    row = cursor.fetchone()
    if row is None:
        return None
    return GetUserByIdRow(
        user_id=row[0],
        email=row[1],
        created_at=row[2],
    )
```

### Parameter Binding Style

The second argument to `cursor.execute()` depends on the placeholder style found in the original SQL [[28]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/codegen/python.rs#L263-L297):

| Placeholder style | Execute call |
|---|---|
| Named (`:name`) | `cursor.execute(SQL, {"name": name, ...})` |
| Positional (`?` or `$1`) | `cursor.execute(SQL, (p1, p2, ...))` |

A single-element positional tuple is emitted with a trailing comma to avoid Python misinterpreting it as parenthesized expression (e.g., `(p1,)`).

### Fetch and Return Patterns per Cardinality

| Cardinality | Fetch call | Return type | Return value |
|---|---|---|---|
| `:exec` | none | `None` | implicit |
| `:one` | `cursor.fetchone()` | `Optional[XxxRow]` | `None` or `XxxRow(field=row[i], ...)` |
| `:many` | `cursor.fetchall()` | `list[XxxRow]` | list comprehension of `XxxRow` |

[[29]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/codegen/python.rs#L229-L251)

### Duplicate Name Detection

Before any code is written, `generate_python_file()` scans all query names and raises an `io::Error` if two queries in the same file would produce the same row class name [[30]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/codegen/python.rs#L83-L100). This prevents silent class shadowing in the output module.

## Example Project

The `example/sqlite/` directory in the repository provides a complete, runnable illustration of the full pipeline [[31]](https://github.com/brian-dlee/icantbelieveitsnotsql/pull/6).

### Directory Layout

```
example/sqlite/
├── butter.toml          # generator configuration
├── schema.sql           # DDL for all tables
├── queries/
│   └── main.sql         # annotated SQL queries
├── generated/
│   └── main.py          # generated Python module (committed for reference)
└── smoke_test.py        # end-to-end verification script
```

### `butter.toml` [[5]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/example/sqlite/butter.toml)

```toml
[generate]
dialect = "sqlite"
queries-dir = "./queries"
schema-file = "schema.sql"
output-dir = "./generated"
```

### `queries/main.sql`

The query file contains eight annotated queries spanning all cardinality types and DML operations [[32]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/example/sqlite/queries/main.sql):

```sql
-- get_user_by_id :one
SELECT id AS user_id, email, created_at
FROM users
WHERE id = :id;

-- get_orders_with_items :many
SELECT o.order_id, o.status, oi.product_id, oi.quantity
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.customer_id = :customer_id;

-- create_user :exec
INSERT INTO users (email, created_at)
VALUES (:email, datetime('now'));

-- update_user_email :exec
UPDATE users
SET email = :email
WHERE id = :id;

-- delete_user :exec
DELETE FROM users
WHERE id = :id;
```

Each query uses the named placeholder style (`:name`), so the generated functions accept keyword arguments and pass a dict to `cursor.execute()`.

### Running the Example

```sh
# From the repository root, regenerate the Python module:
cargo run -- example/sqlite

# Then run the smoke test (no dependencies beyond the Python stdlib):
python example/sqlite/smoke_test.py
```

### `smoke_test.py`

The smoke test [[33]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/example/sqlite/smoke_test.py) imports the generated `generated/main.py` module and runs a full CRUD cycle against an in-memory SQLite database:

```python
import sqlite3
import main as queries  # the generated module

conn = sqlite3.connect(":memory:")
# ... load schema.sql ...
cursor = conn.cursor()

# :exec — insert
queries.create_user(cursor, email="alice@example.com")

# :one — fetch with typed result
result = queries.get_user_by_id(cursor, id=user_id)
assert result is not None
assert result.email == "alice@example.com"

# :exec — update and delete
queries.update_user_email(cursor, email="alice-new@example.com", id=user_id)
queries.delete_user(cursor, id=user_id)
```

The test verifies that the generated code compiles, that `:one` queries return `None` for missing rows, and that mutation functions (`update_user_email`, `delete_user`) take effect correctly [[34]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/example/sqlite/smoke_test.py#L65-L135). Because the generated code has zero runtime dependencies, the only import needed is the Python standard library `sqlite3` module.

## Current Limitations

`icantbelieveitsnotsql` is explicitly an early-stage tool (v0.1.0). The following constraints apply to the current implementation.

### SELECT Query Support

Only simple `SELECT … FROM … WHERE` statements are fully handled. When the query body is something other than a plain `SELECT` (e.g., `UNION`, `INTERSECT`, `VALUES`), the tool emits a warning and returns an empty result rather than failing hard [[35]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L476-L491):

> Unsupported query type: only simple SELECT statements are supported (UNION, VALUES, etc. are not yet handled)

### No Wildcard Projections

`SELECT *` (and qualified wildcards like `table.*`) is explicitly rejected. The tool requires that every output column be listed explicitly so it can build a named row dataclass [[36]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L745-L749):

```
Unsupported expression: wildcard SELECT (*) is not supported; list columns explicitly
```

### Output Field Types Are Not Inferred

While input parameter types are inferred from the schema, **output field types are not**. Every field in a generated row dataclass is typed as `Any`, regardless of what column type the schema declares [[26]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/codegen/python.rs#L300-L304):

```rust
fn output_field_python_type(_field: &QueryOutputField) -> &'static str {
    // Currently falls back to Any — output fields don't carry data_type yet.
    "Any"
}
```

### Positional Placeholders Have No Type Inference

Parameters using `?` (anonymous) or `$N` (dollar) placeholder styles cannot be matched against schema column types because the parameter name carries no semantic meaning. These always fall back to `Any` [[37]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/query.rs#L323-L355).

### Dialect Support

Only four dialects are supported: `generic`, `sqlite`, `postgresql`, and `mysql`. Any other value is rejected at startup [[38]](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/src/config.rs#L20-L27). In particular, CockroachDB is not supported. As the [README](https://github.com/brian-dlee/icantbelieveitsnotsql/blob/209e58bf1ac2a1a27297a7bf039db909f5c34f55/README.md#L10) notes:

> If CockroachDB support is required then I'll need a different parser. sqlparser-rs supports a lot of dialects but not CockroachDB.

### Python-Only Output

Despite the tool's stated goal of supporting "any target application language," only Python code generation is currently implemented. The `src/codegen/` directory contains a single module, `python.rs` [[39]](https://github.com/brian-dlee/icantbelieveitsnotsql/tree/HEAD/src/codegen).
