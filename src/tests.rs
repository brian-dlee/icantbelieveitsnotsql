//! Unit and end-to-end tests for the analyzer, the query-file splitter and
//! the Python generator.

use sqlparser::ast::Statement;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::analyze::{analyze_statement, StatementAnalysis};
use crate::queryfile::{parse_query_file, Command};
use crate::schema::Schema;
use crate::types::SqlType;

const SCHEMA: &str = "
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT,
    age INT,
    created_at DATETIME NOT NULL
);
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    total REAL NOT NULL,
    note TEXT
);
";

fn schema() -> Schema {
    let mut schema = Schema::default();
    for statement in Parser::parse_sql(&SQLiteDialect {}, SCHEMA).unwrap() {
        if let Statement::CreateTable(create_table) = &statement {
            schema.add_create_table(create_table);
        }
    }
    schema
}

fn analyze(sql: &str) -> Result<StatementAnalysis, String> {
    let statements = Parser::parse_sql(&SQLiteDialect {}, sql).expect("sql parses");
    analyze_statement(&statements[0], &schema())
}

fn ok(sql: &str) -> StatementAnalysis {
    analyze(sql).unwrap_or_else(|err| panic!("{sql}: {err}"))
}

fn column_summary(analysis: &StatementAnalysis) -> Vec<(String, SqlType, bool)> {
    analysis
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.type_info.sql_type, c.type_info.nullable))
        .collect()
}

fn param_summary(analysis: &StatementAnalysis) -> Vec<(String, SqlType, bool)> {
    analysis
        .params
        .iter()
        .map(|p| (p.name.clone(), p.type_info.sql_type, p.type_info.nullable))
        .collect()
}

#[test]
fn select_star_expands_schema_columns() {
    let analysis = ok("SELECT * FROM users");
    assert_eq!(
        column_summary(&analysis),
        vec![
            ("id".into(), SqlType::Int, false),
            ("email".into(), SqlType::Text, false),
            ("name".into(), SqlType::Text, true),
            ("age".into(), SqlType::Int, true),
            ("created_at".into(), SqlType::DateTime, false),
        ]
    );
    assert!(analysis.params.is_empty());
}

#[test]
fn named_param_takes_compared_column_type() {
    let analysis = ok("SELECT id FROM users WHERE email = :email AND age > :min_age");
    assert_eq!(
        param_summary(&analysis),
        vec![("email".into(), SqlType::Text, false), ("min_age".into(), SqlType::Int, false)]
    );
    assert!(!analysis.positional);
    assert_eq!(analysis.params[0].source, Some(("users".into(), "email".into())));
}

#[test]
fn positional_params_are_named_after_their_column_in_order() {
    let analysis = ok("SELECT id FROM users WHERE name = ? AND id = ? LIMIT ?");
    assert!(analysis.positional);
    assert_eq!(
        param_summary(&analysis),
        vec![
            ("name".into(), SqlType::Text, false),
            ("id".into(), SqlType::Int, false),
            ("limit".into(), SqlType::Int, false),
        ]
    );
}

#[test]
fn repeated_positional_names_get_suffixes() {
    let analysis = ok("SELECT id FROM users WHERE age BETWEEN ? AND ?");
    let names: Vec<&str> = analysis.params.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, vec!["age", "age_2"]);
}

#[test]
fn is_null_check_makes_param_nullable() {
    let analysis = ok("SELECT id FROM users WHERE (:name IS NULL OR name = :name)");
    assert_eq!(param_summary(&analysis), vec![("name".into(), SqlType::Text, true)]);
}

#[test]
fn left_join_makes_right_side_nullable() {
    let analysis = ok(
        "SELECT u.id, o.total, o.id AS order_id FROM users u LEFT JOIN orders o ON o.user_id = u.id",
    );
    assert_eq!(
        column_summary(&analysis),
        vec![
            ("id".into(), SqlType::Int, false),
            ("total".into(), SqlType::Float, true),
            ("order_id".into(), SqlType::Int, true),
        ]
    );
}

#[test]
fn inner_join_keeps_not_null() {
    let analysis = ok("SELECT u.id, o.total FROM users u JOIN orders o ON o.user_id = u.id");
    assert_eq!(
        column_summary(&analysis),
        vec![("id".into(), SqlType::Int, false), ("total".into(), SqlType::Float, false)]
    );
}

#[test]
fn ambiguous_column_is_an_error() {
    let err = analyze("SELECT id FROM users JOIN orders ON orders.user_id = users.id").unwrap_err();
    assert!(err.contains("ambiguous"), "{err}");
}

#[test]
fn unknown_table_is_an_error() {
    let err = analyze("SELECT * FROM nope").unwrap_err();
    assert!(err.contains("unknown table `nope`"), "{err}");
}

#[test]
fn unknown_projection_column_is_an_error() {
    let err = analyze("SELECT emil FROM users").unwrap_err();
    assert!(err.contains("unknown column `emil`"), "{err}");
}

#[test]
fn mixing_placeholder_styles_is_an_error() {
    let err = analyze("SELECT id FROM users WHERE id = ? AND email = :email").unwrap_err();
    assert!(err.contains("cannot mix"), "{err}");
}

#[test]
fn insert_values_take_column_types_and_nullability() {
    let analysis = ok("INSERT INTO users (email, name, created_at) VALUES (:email, :name, datetime('now'))");
    assert_eq!(
        param_summary(&analysis),
        vec![("email".into(), SqlType::Text, false), ("name".into(), SqlType::Text, true)]
    );
    assert!(analysis.columns.is_empty());
}

#[test]
fn insert_with_wrong_value_count_is_an_error() {
    let err = analyze("INSERT INTO users (email, name) VALUES (:email)").unwrap_err();
    assert!(err.contains("1 values for 2 columns"), "{err}");
}

#[test]
fn insert_on_conflict_excluded_resolves() {
    let analysis = ok(
        "INSERT INTO users (id, email) VALUES (:id, :email) \
         ON CONFLICT(id) DO UPDATE SET email = excluded.email, name = :name RETURNING id, email",
    );
    assert_eq!(
        param_summary(&analysis),
        vec![
            ("id".into(), SqlType::Int, false),
            ("email".into(), SqlType::Text, false),
            ("name".into(), SqlType::Text, true),
        ]
    );
    assert_eq!(
        column_summary(&analysis),
        vec![("id".into(), SqlType::Int, false), ("email".into(), SqlType::Text, false)]
    );
}

#[test]
fn update_set_and_where_params() {
    let analysis = ok("UPDATE users SET name = :name, age = age + :delta WHERE id = :id");
    assert_eq!(
        param_summary(&analysis),
        vec![
            ("name".into(), SqlType::Text, true),
            ("delta".into(), SqlType::Int, true),
            ("id".into(), SqlType::Int, false),
        ]
    );
}

#[test]
fn delete_returning_produces_columns() {
    let analysis = ok("DELETE FROM orders WHERE user_id = :user_id RETURNING id, total");
    assert_eq!(param_summary(&analysis), vec![("user_id".into(), SqlType::Int, false)]);
    assert_eq!(
        column_summary(&analysis),
        vec![("id".into(), SqlType::Int, false), ("total".into(), SqlType::Float, false)]
    );
}

#[test]
fn aggregates_are_typed() {
    let analysis = ok(
        "SELECT COUNT(*) AS n, MAX(age) AS oldest, SUM(o.total) AS spent, AVG(o.total) AS mean, \
         COALESCE(name, 'anon') AS display_name, name || '!' AS shout \
         FROM users u JOIN orders o ON o.user_id = u.id GROUP BY u.id",
    );
    assert_eq!(
        column_summary(&analysis),
        vec![
            ("n".into(), SqlType::Int, false),
            ("oldest".into(), SqlType::Int, true),
            ("spent".into(), SqlType::Float, true),
            ("mean".into(), SqlType::Float, true),
            ("display_name".into(), SqlType::Text, false),
            ("shout".into(), SqlType::Text, true),
        ]
    );
}

#[test]
fn case_without_else_is_nullable() {
    let analysis = ok("SELECT CASE WHEN age > 18 THEN 'adult' END AS bucket, CASE WHEN age > 18 THEN 1 ELSE 0 END AS flag FROM users");
    assert_eq!(
        column_summary(&analysis),
        vec![("bucket".into(), SqlType::Text, true), ("flag".into(), SqlType::Int, false)]
    );
}

#[test]
fn cte_union_and_subquery_params() {
    let analysis = ok(
        "WITH recent AS (SELECT id, email FROM users WHERE created_at > :since) \
         SELECT r.* FROM recent r \
         UNION ALL \
         SELECT id, email FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > :min_total) \
         LIMIT :limit",
    );
    assert_eq!(
        column_summary(&analysis),
        vec![("id".into(), SqlType::Int, false), ("email".into(), SqlType::Text, false)]
    );
    assert_eq!(
        param_summary(&analysis),
        vec![
            ("since".into(), SqlType::DateTime, false),
            ("min_total".into(), SqlType::Float, false),
            ("limit".into(), SqlType::Int, false),
        ]
    );
}

#[test]
fn rowid_and_datetime_modifier_are_typed() {
    let analysis = ok(
        "UPDATE users SET name = 'x' WHERE rowid IN (SELECT rowid FROM users ORDER BY created_at DESC LIMIT :count) \
         AND created_at < datetime('now', :duration)",
    );
    assert_eq!(
        param_summary(&analysis),
        vec![("count".into(), SqlType::Int, false), ("duration".into(), SqlType::Text, false)]
    );
}

#[test]
fn double_quoted_string_literal_warns() {
    let analysis = ok("UPDATE users SET name = \"BLOCK\" WHERE id = :id");
    assert!(
        analysis.warnings.iter().any(|w| w.contains("Prefer single quotes")),
        "{:?}",
        analysis.warnings
    );
}

#[test]
fn unknown_function_is_any_with_warning() {
    let analysis = ok("SELECT mystery(name) AS m FROM users WHERE id = :id");
    assert_eq!(column_summary(&analysis), vec![("m".into(), SqlType::Any, true)]);
    assert!(analysis.warnings.iter().any(|w| w.contains("unknown function `mystery`")));
}

#[test]
fn unaliased_expression_gets_generated_name_and_warning() {
    let analysis = ok("SELECT age * 2, COUNT(*) FROM users");
    let names: Vec<&str> = analysis.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["column_1", "count"]);
    assert!(analysis.warnings.iter().any(|w| w.contains("add `AS <name>`")));
}

#[test]
fn schema_type_affinity() {
    assert_eq!(SqlType::from_type_name("INTEGER"), SqlType::Int);
    assert_eq!(SqlType::from_type_name("BIGINT UNSIGNED"), SqlType::Int);
    assert_eq!(SqlType::from_type_name("VARCHAR(255)"), SqlType::Text);
    assert_eq!(SqlType::from_type_name("CHARACTER VARYING"), SqlType::Text);
    assert_eq!(SqlType::from_type_name("BLOB"), SqlType::Blob);
    assert_eq!(SqlType::from_type_name(""), SqlType::Blob);
    assert_eq!(SqlType::from_type_name("DOUBLE PRECISION"), SqlType::Float);
    assert_eq!(SqlType::from_type_name("NUMERIC(10, 2)"), SqlType::Numeric);
    assert_eq!(SqlType::from_type_name("BOOLEAN"), SqlType::Bool);
    assert_eq!(SqlType::from_type_name("DATETIME"), SqlType::DateTime);
    assert_eq!(SqlType::from_type_name("TIMESTAMP"), SqlType::DateTime);
    assert_eq!(SqlType::from_type_name("JSON"), SqlType::Json);
}

// ------------------------------------------------------------- query files

#[test]
fn query_file_blocks_and_statements() {
    let contents = "\
CREATE TABLE t (id INTEGER);

-- name: get_t :one
-- Fetch a row.
-- param: id int
SELECT * FROM t WHERE id = :id;

-- name: setup :exec
CREATE TABLE a (x TEXT);
-- a comment between statements; note the ';' inside 'it''s; quoted'
CREATE TABLE b (y TEXT DEFAULT 'it''s; quoted');
";
    let blocks = parse_query_file(std::path::Path::new("q.sql"), contents, &SQLiteDialect {}).unwrap();
    assert_eq!(blocks.len(), 3);

    assert_eq!(blocks[0].name, None);
    assert_eq!(blocks[0].statements.len(), 1);
    assert_eq!(blocks[0].statements[0].line, 1);

    assert_eq!(blocks[1].name.as_deref(), Some("get_t"));
    assert_eq!(blocks[1].command, Some(Command::One));
    assert_eq!(blocks[1].line, 3);
    assert_eq!(blocks[1].doc, vec!["Fetch a row."]);
    assert_eq!(blocks[1].param_types, vec![("id".to_string(), "int".to_string())]);
    assert_eq!(blocks[1].statements[0].sql, "SELECT * FROM t WHERE id = :id");
    assert_eq!(blocks[1].statements[0].line, 6);

    assert_eq!(blocks[2].name.as_deref(), Some("setup"));
    assert_eq!(blocks[2].command, Some(Command::Exec));
    assert_eq!(blocks[2].statements.len(), 2);
    assert_eq!(blocks[2].statements[0].sql, "CREATE TABLE a (x TEXT)");
    assert_eq!(blocks[2].statements[0].line, 9);
    assert!(blocks[2].statements[1].sql.starts_with("CREATE TABLE b"));
    assert_eq!(blocks[2].statements[1].line, 11);
}

#[test]
fn query_file_accepts_legacy_header_style() {
    let contents = "-- get_user :one\nSELECT 1;\n\n-- just a comment :) not a header\n-- name: other :exec\nSELECT 2;\n";
    let blocks = parse_query_file(std::path::Path::new("q.sql"), contents, &SQLiteDialect {}).unwrap();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].name.as_deref(), Some("get_user"));
    assert_eq!(blocks[0].command, Some(Command::One));
    assert_eq!(blocks[1].name.as_deref(), Some("other"));
}

#[test]
fn top_level_output_dir_selects_python_output() {
    let config = "[generate]\ndialect = \"sqlite\"\nqueries-dir = \"sql\"\noutput-dir = \"gen\"\n";
    let queries = format!("{SCHEMA}\n-- get_user :one\nSELECT * FROM users WHERE id = :id;\n");
    let project = TempProject::new("legacy", config, &[("sql/users.sql", &queries)]);
    let report = crate::generate::run(&project.root, true).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(report.outputs.len(), 1);
    assert!(project.root.join("gen/users.py").exists());
}

#[test]
fn query_file_rejects_unknown_command() {
    let contents = "-- name: broken :fetch\nSELECT 1;\n";
    let err = parse_query_file(std::path::Path::new("q.sql"), contents, &SQLiteDialect {}).unwrap_err();
    assert_eq!(err.len(), 1);
    assert!(err[0].message.contains("unknown command `:fetch`"), "{}", err[0]);
    assert_eq!(err[0].line, 1);
}

// --------------------------------------------------------------- end to end

struct TempProject {
    root: std::path::PathBuf,
}

impl TempProject {
    fn new(name: &str, config: &str, files: &[(&str, &str)]) -> TempProject {
        let root = std::env::temp_dir().join(format!("butter-test-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("sql")).unwrap();
        std::fs::write(root.join("butter.toml"), config).unwrap();
        for (path, contents) in files {
            let full = root.join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(full, contents).unwrap();
        }
        TempProject { root }
    }
}

impl Drop for TempProject {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

const CONFIG: &str = "
[generate]
dialect = \"sqlite\"
queries-dir = \"sql\"

[generate.python]
output-dir = \"out\"

[generate.python.column-types]
\"users.created_at\" = \"mytypes.Stamp\"
";

#[test]
fn end_to_end_generates_python_module() {
    let queries = format!(
        "{SCHEMA}
-- name: get_user :one
SELECT * FROM users WHERE id = :id;

-- name: list_users :many
-- All users, newest first.
SELECT id, email, created_at FROM users ORDER BY created_at DESC LIMIT :limit;

-- name: create_user :execlastid
INSERT INTO users (email, name, created_at) VALUES (:email, :name, datetime('now'));

-- name: add_orders :execmany
INSERT INTO orders (user_id, total) VALUES (:user_id, :total);

-- name: rename_user :execrows
UPDATE users SET name = :name WHERE id = ?;
"
    );
    let queries = queries.replace("WHERE id = ?;", "WHERE id = :id;");

    let project = TempProject::new("ok", CONFIG, &[("sql/users.sql", &queries)]);
    let report = crate::generate::run(&project.root, true).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(report.outputs.len(), 1);
    assert!(report.warnings.is_empty(), "{:?}", report.warnings);

    let code = std::fs::read_to_string(project.root.join("out/users.py")).unwrap();
    assert!(project.root.join("out/__init__.py").exists());

    assert!(code.contains("import mytypes\n"), "{code}");
    assert!(code.contains("class GetUserRow(pydantic.BaseModel):"), "{code}");
    assert!(code.contains("    created_at: mytypes.Stamp\n"), "{code}");
    assert!(code.contains("    name: str | None\n"), "{code}");
    assert!(code.contains("async def get_user(\n    cursor: aiosqlite.Cursor,\n    *,\n    id: int,\n) -> GetUserRow | None:"), "{code}");
    assert!(code.contains("\"\"\"All users, newest first.\"\"\""), "{code}");
    assert!(code.contains("    limit: int,\n) -> list[ListUsersRow]:"), "{code}");
    assert!(code.contains(") -> int | None:\n    \"\"\"Runs `create_user`.\"\"\""), "{code}");
    assert!(code.contains("    return cursor.lastrowid\n"), "{code}");
    assert!(code.contains("rows: typing.Iterable[AddOrdersParams],\n) -> int:"), "{code}");
    assert!(code.contains("await cursor.executemany(ADD_ORDERS, [row.model_dump(by_alias=True) for row in rows])"), "{code}");
    assert!(code.contains("return GetUserRow.model_validate(_row_dict(GetUserRow, row)) if row is not None else None"), "{code}");
    assert!(code.contains("\nimport typing\n\nimport aiosqlite\nimport mytypes\nimport pydantic\n\n"), "{code}");
    assert!(code.contains("    name: str | None = pydantic.Field(default=None)\n"), "{code}");
    assert!(code.contains("    return cursor.rowcount\n"), "{code}");
}

#[test]
fn exec_only_module_has_no_unused_imports() {
    let queries = format!("{SCHEMA}\n-- name: reset :exec\nDELETE FROM orders;\nDELETE FROM users;\n");
    let project = TempProject::new("exec-only", CONFIG, &[("sql/reset.sql", &queries)]);
    crate::generate::run(&project.root, true).unwrap_or_else(|e| panic!("{e}"));
    let code = std::fs::read_to_string(project.root.join("out/reset.py")).unwrap();
    assert!(code.contains("import aiosqlite\n"), "{code}");
    assert!(!code.contains("import typing"), "{code}");
    assert!(!code.contains("import pydantic"), "{code}");
    assert!(!code.contains("_row_dict"), "{code}");
}

#[test]
fn end_to_end_reports_errors_with_locations() {
    let queries = format!(
        "{SCHEMA}
SELECT 1;

-- name: bad_many :many
UPDATE users SET name = :name;

-- name: bad_column :one
SELECT emial FROM users;
"
    );
    let project = TempProject::new("errors", CONFIG, &[("sql/bad.sql", &queries)]);
    let err = crate::generate::run(&project.root, true).unwrap_err();
    let text = err.to_string();
    assert!(text.contains("returns no rows"), "{text}");
    assert!(text.contains("unknown column `emial`"), "{text}");
    assert!(text.contains("needs a header"), "{text}");
    assert!(!project.root.join("out").exists(), "nothing is written when there are errors");
}

#[test]
fn pascal_case_names() {
    assert_eq!(crate::python::pascal_case("select_many_ipinfo_by_ip"), "SelectManyIpinfoByIp");
    assert_eq!(crate::python::pascal_case("x"), "X");
}
