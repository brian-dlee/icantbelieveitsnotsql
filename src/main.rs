use clap::Parser;
use serde::Deserialize;
use sqlparser::ast::{Expr, FromTable, SelectItem, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect;
use sqlparser::parser::{Parser as SQLParser, ParserError};
use std::collections::HashMap;
use std::fs::{self};
use std::path::{Path, PathBuf};
use thiserror;
use toml;

#[derive(Clone, Debug)]
enum QueryCardinality {
    One,
    Many,
    Exec,
}

impl std::fmt::Display for QueryCardinality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryCardinality::One => write!(f, ":one"),
            QueryCardinality::Many => write!(f, ":many"),
            QueryCardinality::Exec => write!(f, ":exec"),
        }
    }
}

#[derive(Clone, Debug)]
struct QueryAnnotation {
    name: String,
    cardinality: QueryCardinality,
}

fn extract_query_annotations(sql: &str) -> Vec<Option<QueryAnnotation>> {
    let mut annotations: Vec<Option<QueryAnnotation>> = Vec::new();

    // Track the most recently seen valid annotation comment
    let mut pending_annotation: Option<QueryAnnotation> = None;
    // Track whether we have seen any non-blank, non-comment content since the
    // last `;` (i.e. whether we are inside a statement body)
    let mut in_statement = false;

    for line in sql.lines() {
        let trimmed = line.trim();

        if trimmed.is_empty() {
            // Blank lines do not break the association between an annotation
            // and the statement that follows it.
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("--") {
            // This is a comment line. Try to parse it as an annotation.
            let comment = rest.trim();
            let parts: Vec<&str> = comment.splitn(2, ':').collect();

            if parts.len() == 2 {
                let name = parts[0].trim();
                let cardinality_str = parts[1].trim();

                let cardinality = match cardinality_str {
                    "one" => Some(QueryCardinality::One),
                    "many" => Some(QueryCardinality::Many),
                    "exec" => Some(QueryCardinality::Exec),
                    _ => None,
                };

                if let (Some(cardinality), true) = (cardinality, !name.is_empty()) {
                    // Warn if the name isn't a valid Python identifier
                    let valid_python_ident = name.chars().enumerate().all(|(i, c)| {
                        if i == 0 {
                            c.is_alphabetic() || c == '_'
                        } else {
                            c.is_alphanumeric() || c == '_'
                        }
                    });

                    if !valid_python_ident {
                        eprintln!(
                            "Warning: annotation name {:?} may not be a valid Python identifier",
                            name
                        );
                    }

                    // A new annotation comment resets any previous pending one
                    pending_annotation = Some(QueryAnnotation {
                        name: name.to_string(),
                        cardinality,
                    });
                    // We are not inside a statement yet
                    in_statement = false;
                }
            }
            // Any other comment line is ignored (doesn't clear pending_annotation)
            continue;
        }

        // Non-blank, non-comment line: we are inside a SQL statement body.
        in_statement = true;

        // If this line ends with `;` the statement is complete — record the
        // association and reset state.
        if trimmed.ends_with(';') {
            annotations.push(pending_annotation.take());
            in_statement = false;
            // A fresh statement could start on the very next line, so we do
            // not reset pending_annotation here — take() already cleared it.
        }
    }

    // If the last statement had no trailing `;`, record whatever we have.
    if in_statement {
        annotations.push(pending_annotation.take());
    }

    annotations
}

/// Recursively walk an expression tree and collect all placeholder strings
/// (`:name`, `$1`, `?`) into `out`, preserving order of first appearance.
fn collect_placeholders(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Value(v) => {
            if let Value::Placeholder(name) = &v.value {
                out.push(name.clone());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_placeholders(left, out);
            collect_placeholders(right, out);
        }
        Expr::UnaryOp { expr, .. } => collect_placeholders(expr, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_placeholders(expr, out);
            collect_placeholders(low, out);
            collect_placeholders(high, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_placeholders(expr, out);
            for e in list {
                collect_placeholders(e, out);
            }
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => collect_placeholders(e, out),
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. } => {
            collect_placeholders(expr, out);
            collect_placeholders(pattern, out);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_placeholders(op, out);
            }
            for condition in conditions {
                collect_placeholders(&condition.condition, out);
                collect_placeholders(&condition.result, out);
            }
            if let Some(e) = else_result {
                collect_placeholders(e, out);
            }
        }
        Expr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &f.args {
                for arg in &arg_list.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Named { arg, .. }
                        | sqlparser::ast::FunctionArg::Unnamed(arg) => {
                            if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg {
                                collect_placeholders(e, out);
                            }
                        }
                        sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => {
                            if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg {
                                collect_placeholders(e, out);
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

/// Given a list of raw placeholder strings, a set of table names active in the
/// query, and the schema, build a deduplicated list of `QueryInputField`s with
/// best-effort type inference.
///
/// For `:name`-style params, the name has the leading `:` stripped.
/// For `$1`-style params, the name is kept as `$1`.
/// For `?` anonymous params, they are named `p1`, `p2`, … in order.
fn build_input_fields(
    raw: &[String],
    active_tables: &[&str],
    schema: &SchemaParseResult,
) -> Vec<QueryInputField> {
    let mut seen: Vec<String> = Vec::new(); // tracks canonical names for dedup
    let mut fields: Vec<QueryInputField> = Vec::new();
    let mut anon_counter = 0usize;

    for placeholder in raw {
        if placeholder == "?" {
            anon_counter += 1;
            let name = format!("p{}", anon_counter);
            if !seen.contains(&name) {
                seen.push(name.clone());
                fields.push(QueryInputField {
                    name,
                    data_type: "Any".to_string(),
                });
            }
        } else if let Some(name) = placeholder.strip_prefix(':') {
            // :name style — named parameter
            if !seen.contains(&name.to_string()) {
                seen.push(name.to_string());
                // Try to resolve type from schema by looking for a column with
                // this name in the active tables.
                let data_type = resolve_param_type(name, active_tables, schema);
                fields.push(QueryInputField {
                    name: name.to_string(),
                    data_type,
                });
            }
        } else if placeholder.starts_with('$') {
            // $1 positional style
            if !seen.contains(placeholder) {
                seen.push(placeholder.clone());
                fields.push(QueryInputField {
                    name: placeholder.clone(),
                    data_type: "Any".to_string(),
                });
            }
        } else {
            // Unknown style — emit as-is
            if !seen.contains(placeholder) {
                seen.push(placeholder.clone());
                fields.push(QueryInputField {
                    name: placeholder.clone(),
                    data_type: "Any".to_string(),
                });
            }
        }
    }

    fields
}

/// Try to find the SQL type of a column named `param_name` within the given
/// `active_tables`. Returns a Python type string or `"Any"` if not resolvable.
fn resolve_param_type(
    param_name: &str,
    active_tables: &[&str],
    schema: &SchemaParseResult,
) -> String {
    let mut matches: Vec<&str> = Vec::new();

    for &table_name in active_tables {
        if let Some(columns) = schema.table_fields.get(table_name) {
            if let Some(sql_type) = columns.get(param_name) {
                matches.push(sql_type.as_str());
            }
        }
    }

    if matches.len() == 1 {
        sql_type_to_python(matches[0]).to_string()
    } else {
        // Ambiguous or not found — fall back to Any
        "Any".to_string()
    }
}

/// Map a SQL type string (as produced by sqlparser-rs `.to_string()`) to a
/// Python type annotation string. Unknown types fall back to `"Any"`.
fn sql_type_to_python(sql_type: &str) -> &'static str {
    // Normalise: uppercase and strip trailing (…) precision/length specifiers.
    let upper = sql_type.to_uppercase();
    let normalised = if let Some(pos) = upper.find('(') {
        upper[..pos].trim()
    } else {
        upper.trim()
    };

    match normalised {
        // Integer types
        "INTEGER" | "INT" | "INT2" | "INT4" | "INT8" | "INT16" | "INT32" | "INT64" | "BIGINT"
        | "SMALLINT" | "TINYINT" | "MEDIUMINT" | "BYTEINT" | "HUGEINT" | "UBIGINT"
        | "USMALLINT" | "UTINYINT" | "UINTEGER" => "int",

        // Text types
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" | "CHARACTER" | "CLOB" | "TINYTEXT"
        | "MEDIUMTEXT" | "LONGTEXT" | "STRING" | "NCHAR" | "NVARCHAR" | "NCLOB" | "BPCHAR" => "str",

        // Float types
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT16" | "FLOAT32" | "FLOAT64" | "DOUBLE"
        | "DOUBLE PRECISION" => "float",

        // Boolean types
        "BOOLEAN" | "BOOL" => "bool",

        // Blob/binary types
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB"
        | "BIT" | "BIT VARYING" => "bytes",

        // Decimal/numeric types
        "NUMERIC" | "DECIMAL" | "DEC" | "MONEY" | "SMALLMONEY" => "Decimal",

        // Date types
        "DATE" => "datetime.date",

        // Time types
        "TIME" | "TIMETZ" | "TIME WITH TIME ZONE" | "TIME WITHOUT TIME ZONE" => "datetime.time",

        // Timestamp / datetime types
        "TIMESTAMP"
        | "TIMESTAMPTZ"
        | "TIMESTAMP WITH TIME ZONE"
        | "TIMESTAMP WITHOUT TIME ZONE"
        | "DATETIME"
        | "DATETIME2"
        | "SMALLDATETIME"
        | "DATETIMEOFFSET" => "datetime.datetime",

        // UUID — represent as str
        "UUID" => "str",

        // JSON types — too dynamic, use Any
        "JSON" | "JSONB" => "Any",

        _ => {
            // SQLite-style affinity fallback: match by substring
            if normalised.contains("INT") {
                "int"
            } else if normalised.contains("CHAR")
                || normalised.contains("CLOB")
                || normalised.contains("TEXT")
            {
                "str"
            } else if normalised.contains("REAL")
                || normalised.contains("FLOA")
                || normalised.contains("DOUB")
            {
                "float"
            } else if normalised.contains("BLOB") || normalised.is_empty() {
                "bytes"
            } else {
                "Any"
            }
        }
    }
}

enum SQLDialect {
    Generic,
    SQLite,
    PostgreSQL,
    MySQL,
}

#[derive(thiserror::Error, Debug)]
enum SQLDialectError {
    #[error("unsupported dialect: {0}")]
    Unsupported(String),
}

impl SQLDialect {
    fn from_str(value: &str) -> Result<SQLDialect, SQLDialectError> {
        match value.to_lowercase().as_str() {
            "generic" => Ok(SQLDialect::Generic),
            "sqlite" => Ok(SQLDialect::SQLite),
            "postgresql" => Ok(SQLDialect::PostgreSQL),
            "mysql" => Ok(SQLDialect::MySQL),
            _ => Err(SQLDialectError::Unsupported(String::from(value))),
        }
    }
}

#[derive(Parser)]
struct Args {
    project_path: Option<PathBuf>,
}

fn extract_debug_block_with_line_number_range(
    text: &str,
    line_start: i32,
    line_end: i32,
) -> String {
    let mut block_lines = Vec::new();

    for (offset, line) in text
        .split("\n")
        .skip(line_start as usize)
        .take((line_end - line_start) as usize)
        .enumerate()
    {
        let line_number = line_start as usize + offset + 1;

        block_lines.push(format!("{}\t{}", line_number, line));
    }

    return block_lines.join("\n");
}

fn extract_line_number_from_parse_error(parse_error: &str) -> i32 {
    let parts: Vec<&str> = parse_error.split("Line: ").collect();
    if parts.len() < 2 {
        return -1;
    }

    let after_line = parts[1];
    let before_comma = match after_line.split(',').next() {
        Some(s) => s,
        None => return -1,
    };

    match before_comma.parse::<i32>() {
        Ok(n) => n,
        Err(_) => -1,
    }
}

#[derive(Debug, Deserialize)]
struct GenerateConfig {
    dialect: Option<String>,

    #[serde(rename = "queries-dir")]
    queries_dir: Option<PathBuf>,

    #[serde(rename = "schema-file")]
    schema_file: Option<PathBuf>,

    #[serde(rename = "output-dir")]
    output_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct Config {
    generate: GenerateConfig,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let project_path = args.project_path.unwrap_or_else(|| PathBuf::from("."));
    let config_file_path = project_path.join("butter.toml");

    println!("Reading config file at {}", config_file_path.display());

    let content = fs::read_to_string(&config_file_path)?;
    let config: Config = toml::from_str(&content)?;

    let selected_dialect = &config.generate.dialect.unwrap_or(String::from("generic"));
    let sql_dialect = SQLDialect::from_str(&selected_dialect)?;

    println!("Using SQL dialect: {}", selected_dialect);

    let parser_dialect: &dyn dialect::Dialect = match sql_dialect {
        SQLDialect::Generic => &dialect::GenericDialect {},
        SQLDialect::SQLite => &dialect::SQLiteDialect {},
        SQLDialect::PostgreSQL => &dialect::PostgreSqlDialect {},
        SQLDialect::MySQL => &dialect::MySqlDialect {},
    };

    let schema_file_path = project_path.join(
        config
            .generate
            .schema_file
            .unwrap_or(PathBuf::from("schema.sql")),
    );

    let queries_dir_path = project_path.join(
        config
            .generate
            .queries_dir
            .unwrap_or(PathBuf::from("queries")),
    );

    let output_dir_path = project_path.join(
        config
            .generate
            .output_dir
            .unwrap_or(PathBuf::from("generated")),
    );

    println!("Output directory: {}", output_dir_path.display());

    if let Err(err) = fs::create_dir_all(&output_dir_path) {
        eprintln!(
            "Failed to create output directory \"{}\": {}",
            output_dir_path.display(),
            err
        );
        std::process::exit(1);
    }

    println!("Reading schema file: {}", schema_file_path.display());

    let sql = match fs::read_to_string(&schema_file_path) {
        Err(err) => {
            eprintln!(
                "Failed to read schema file \"{}\": {}",
                schema_file_path.display(),
                err
            );

            std::process::exit(1);
        }
        Ok(contents) => contents,
    };

    let schema = match parse_schema_file(&sql, parser_dialect) {
        Err(err) => {
            eprintln!(
                "Failed to parse schema file \"{}\": {}",
                schema_file_path.display(),
                err
            );

            std::process::exit(1);
        }
        Ok(result) => result,
    };

    for query_file_path in fs::read_dir(&queries_dir_path)? {
        match query_file_path {
            Ok(dir_entry) => {
                let path = dir_entry.path();

                // Only process .sql files
                if path.extension().and_then(|e| e.to_str()) != Some("sql") {
                    continue;
                }

                println!("Processing {}", path.display());

                let sql = match fs::read_to_string(&path) {
                    Err(err) => {
                        eprintln!("Failed to read query file \"{}\": {}", path.display(), err);
                        continue;
                    }
                    Ok(contents) => contents,
                };

                let queries = match SQLParser::parse_sql(parser_dialect, &sql) {
                    Err(err) => {
                        eprintln!(
                            "Failed to parse query file \"{}\": {}",
                            path.display(),
                            format_sql_parser_error(&err, &sql)
                        );
                        continue;
                    }
                    Ok(ast) => {
                        let annotations = extract_query_annotations(&sql);
                        let mut file_queries: Vec<QueryParseResult> = Vec::new();

                        for (statement, annotation) in ast.iter().zip(annotations.into_iter()) {
                            let annotation = match annotation {
                                Some(a) => a,
                                None => {
                                    eprintln!(
                                        "Warning: unannotated query in \"{}\" skipped: {}",
                                        path.display(),
                                        statement,
                                    );
                                    continue;
                                }
                            };

                            eprintln!("  query {:?} ({})", annotation.name, annotation.cardinality,);

                            match process_sql_statement(statement, annotation, &schema) {
                                Ok(result) => file_queries.push(result),
                                Err(err) => {
                                    eprintln!(
                                        "Failed to process SQL statement \"{}\": {:?}",
                                        path.display(),
                                        err,
                                    );
                                }
                            }
                        }

                        file_queries
                    }
                };

                if queries.is_empty() {
                    eprintln!(
                        "Warning: no annotated queries found in \"{}\", skipping codegen",
                        path.display()
                    );
                    continue;
                }

                // Derive the output file path: same stem as the .sql file, .py extension
                let stem = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("queries");
                let source_filename = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown.sql");
                let output_path = output_dir_path.join(format!("{}.py", stem));

                match generate_python_file(&queries, source_filename, &output_path) {
                    Ok(()) => println!("  wrote {}", output_path.display()),
                    Err(err) => eprintln!("Failed to write \"{}\": {}", output_path.display(), err),
                }
            }
            Err(err) => {
                eprintln!(
                    "Failed to read directory \"{}\": {}",
                    queries_dir_path.display(),
                    err
                );
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
struct SchemaParseResult {
    table_fields: HashMap<String, HashMap<String, String>>,
}

impl SchemaParseResult {
    fn resolve_fields_by_name(&self, name: &str) -> Vec<FieldSource> {
        let mut result: Vec<FieldSource> = Vec::new();

        for (table_name, table_fields) in &self.table_fields {
            for (field_name, field_data_type) in table_fields {
                if name == field_name {
                    result.push(FieldSource::TableSource {
                        database: None,
                        schema: None,
                        table: table_name.clone(),
                        column: field_name.clone(),
                        data_type: field_data_type.clone(),
                    })
                }
            }
        }

        return result;
    }
}

fn parse_schema_file(
    schema_file_contents: &str,
    parser_dialect: &dyn dialect::Dialect,
) -> Result<SchemaParseResult, ParserError> {
    match SQLParser::parse_sql(parser_dialect, schema_file_contents) {
        Err(err) => Err(err),
        Ok(ast) => {
            let mut tables: HashMap<String, HashMap<String, String>> = HashMap::new();

            for statement in ast {
                match statement {
                    Statement::CreateTable(create_table) => {
                        let table_name = create_table.name.to_string();

                        let mut columns: HashMap<String, String> = HashMap::new();

                        for column in create_table.columns.iter() {
                            columns.insert(column.name.value.clone(), column.data_type.to_string());
                        }

                        tables.insert(table_name, columns);
                    }
                    _ => {}
                }
            }

            Ok(SchemaParseResult {
                table_fields: tables,
            })
        }
    }
}

#[derive(Debug)]
struct QueryInputField {
    name: String,
    data_type: String,
}

#[derive(Debug)]
struct QueryOutputFieldSource {
    database: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    field: String,
}

#[derive(Debug)]
struct QueryOutputField {
    source: QueryOutputFieldSource,
    name: String,
}

#[derive(Debug)]
struct QueryParseResult {
    statement: Statement,
    annotation: QueryAnnotation,
    input_fields: Vec<QueryInputField>,
    output_fields: Vec<QueryOutputField>,
}

fn format_sql_parser_error(error: &ParserError, sql: &str) -> String {
    if let ParserError::ParserError(msg) = &error {
        let line_number = extract_line_number_from_parse_error(&msg);
        let debug =
            extract_debug_block_with_line_number_range(sql, line_number - 2, line_number + 2);

        format!("{}: {}", error, debug)
    } else {
        error.to_string()
    }
}

#[derive(Clone, Debug)]
enum FieldSource {
    TableSource {
        database: Option<String>,
        schema: Option<String>,
        table: String,
        column: String,
        data_type: String,
    },
}

#[derive(Debug)]
enum QueryError {
    AmbiguousFieldReference {
        field_name: String,
        candidates: Vec<FieldSource>,
    },
    InvalidFieldReference {
        field_name: String,
    },
}

impl QueryError {
    fn format(&self, statement: &Statement, sql: &str) -> String {
        String::from("")
    }
}

fn process_sql_statement(
    statement: &Statement,
    annotation: QueryAnnotation,
    schema: &SchemaParseResult,
) -> Result<QueryParseResult, QueryError> {
    let mut raw_placeholders: Vec<String> = Vec::new();
    let mut active_tables: Vec<String> = Vec::new();
    let mut output_fields: Vec<QueryOutputField> = Vec::new();

    match statement {
        Statement::Query(query) => {
            let select = query.body.as_select().unwrap();

            let mut aliases: HashMap<String, String> = HashMap::new();

            for table_with_joins in &select.from {
                aliases.extend(extract_aliases_using_relation(&table_with_joins.relation));
                collect_table_name(&table_with_joins.relation, &mut active_tables);

                for join in &table_with_joins.joins {
                    aliases.extend(extract_aliases_using_relation(&join.relation));
                    collect_table_name(&join.relation, &mut active_tables);
                }
            }

            for entry in select.projection.iter() {
                output_fields.extend(extract_output_fields_from_select_item(entry, &aliases))
            }

            if let Some(expr) = &select.selection {
                collect_placeholders(expr, &mut raw_placeholders);
            }

            for output_field in output_fields.iter_mut() {
                if output_field.source.table.is_none() {
                    let resolved_fields = schema.resolve_fields_by_name(&output_field.source.field);

                    if resolved_fields.len() == 0 {
                        return Err(QueryError::InvalidFieldReference {
                            field_name: output_field.source.field.clone(),
                        });
                    } else if resolved_fields.len() > 1 {
                        return Err(QueryError::AmbiguousFieldReference {
                            field_name: output_field.source.field.clone(),
                            candidates: resolved_fields.clone(),
                        });
                    } else {
                        match resolved_fields.first().unwrap() {
                            FieldSource::TableSource {
                                database,
                                schema,
                                table,
                                column,
                                data_type: _,
                            } => {
                                output_field.source.database = database.clone();
                                output_field.source.schema = schema.clone();
                                output_field.source.table = Some(table.clone());
                                output_field.source.field = column.clone();
                            }
                        }
                    }
                }
            }
        }
        Statement::Insert(insert) => {
            // Collect the target table as an active table for type inference.
            active_tables.push(insert.table.to_string());

            // Walk all expressions in the VALUES rows.
            if let Some(source) = &insert.source {
                if let SetExpr::Values(values) = source.body.as_ref() {
                    for row in &values.rows {
                        for expr in row {
                            collect_placeholders(expr, &mut raw_placeholders);
                        }
                    }
                }
            }
        }
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => {
            // Collect the target table name.
            active_tables.push(table.to_string());

            // Walk the RHS of each SET assignment.
            for assignment in assignments {
                collect_placeholders(&assignment.value, &mut raw_placeholders);

                // Also record the column name from the LHS so type inference
                // can match `:value` params against named columns below.
                // (We do this implicitly via active_tables + resolve_param_type.)
            }

            // Walk the WHERE clause.
            if let Some(expr) = selection {
                collect_placeholders(expr, &mut raw_placeholders);
            }
        }
        Statement::Delete(delete) => {
            // Collect named table targets (DELETE t1, t2 FROM ...).
            for table_ref in &delete.tables {
                active_tables.push(table_ref.to_string());
            }
            // Collect from the FROM clause.
            let from_tables = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            for table_with_joins in from_tables {
                collect_table_name(&table_with_joins.relation, &mut active_tables);
            }

            if let Some(expr) = &delete.selection {
                collect_placeholders(expr, &mut raw_placeholders);
            }
        }
        _ => {}
    };

    let active_table_refs: Vec<&str> = active_tables.iter().map(|s| s.as_str()).collect();
    let input_fields = build_input_fields(&raw_placeholders, &active_table_refs, schema);

    Ok(QueryParseResult {
        statement: statement.clone(),
        annotation,
        input_fields,
        output_fields,
    })
}

/// Extract the real table name from a TableFactor and push it into `out` if
/// not already present. Used to build the active_tables list for type inference.
fn collect_table_name(table_factor: &TableFactor, out: &mut Vec<String>) {
    if let TableFactor::Table { name, .. } = table_factor {
        let table_name = name.to_string();
        if !out.contains(&table_name) {
            out.push(table_name);
        }
    }
}

fn extract_aliases_using_relation(table_factor: &TableFactor) -> HashMap<String, String> {
    let mut aliases: HashMap<String, String> = HashMap::new();

    match table_factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.to_string();

            if let Some(alias) = &alias {
                aliases.insert(alias.name.to_string(), table_name.clone());
            };
        }
        x => {
            eprintln!("Unsupported: cannot extract aliases from {:?}", x)
        }
    }

    aliases
}

fn extract_output_field_from_expr(
    expr: &Expr,
    aliases: &HashMap<String, String>,
) -> Option<QueryOutputField> {
    match expr {
        Expr::Identifier(ident) => Some(QueryOutputField {
            source: QueryOutputFieldSource {
                database: None,
                schema: None,
                table: None,
                field: ident.to_string(),
            },
            name: ident.to_string(),
        }),
        Expr::CompoundIdentifier(idents) => match &idents[..] {
            [alias_or_table, field] => {
                let mut table = alias_or_table.to_string();

                if let Some(aliased_table) = aliases.get(&table) {
                    table = aliased_table.clone();
                }

                Some(QueryOutputField {
                    source: QueryOutputFieldSource {
                        database: None,
                        schema: None,
                        table: Some(table.to_string()),
                        field: field.to_string(),
                    },
                    name: field.to_string(),
                })
            }
            [database_or_schema, table, field] => Some(QueryOutputField {
                source: QueryOutputFieldSource {
                    database: None,
                    schema: Some(database_or_schema.to_string()),
                    table: Some(table.to_string()),
                    field: field.to_string(),
                },
                name: field.to_string(),
            }),
            [database, schema, table, field] => Some(QueryOutputField {
                source: QueryOutputFieldSource {
                    database: Some(database.to_string()),
                    schema: Some(schema.to_string()),
                    table: Some(table.to_string()),
                    field: field.to_string(),
                },
                name: field.to_string(),
            }),
            _ => {
                eprintln!(
                    "unsupported compound ident ({}): {:?}",
                    idents.len(),
                    idents
                );

                None
            }
        },
        x => {
            eprintln!("SELECT expression not supported: {:#?}", x);

            None
        }
    }
}

fn extract_output_fields_from_select_item(
    select_item: &SelectItem,
    aliases: &HashMap<String, String>,
) -> Vec<QueryOutputField> {
    let mut output_fields: Vec<QueryOutputField> = Vec::new();

    match select_item {
        SelectItem::UnnamedExpr(expr) => {
            if let Some(output_field) = extract_output_field_from_expr(expr, aliases) {
                output_fields.push(output_field)
            }
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            if let Some(mut output_field) = extract_output_field_from_expr(expr, aliases) {
                output_field.name = alias.to_string();
                output_fields.push(output_field)
            }
        }
        SelectItem::QualifiedWildcard(..) => {
            eprintln!(
                "SELECT expression not supported: don't use wildcards. {:#?}",
                select_item
            );
        }
        SelectItem::Wildcard(..) => {
            eprintln!(
                "SELECT expression not supported: don't use wildcards. {:#?}",
                select_item
            );
        }
    }

    output_fields
}

// ---------------------------------------------------------------------------
// Python reserved keywords — field names matching these get a trailing `_`
// ---------------------------------------------------------------------------
const PYTHON_KEYWORDS: &[&str] = &[
    "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue",
    "def", "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import",
    "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try", "while",
    "with", "yield",
];

fn is_python_keyword(name: &str) -> bool {
    PYTHON_KEYWORDS.contains(&name)
}

/// Convert a `snake_case` or `PascalCase` name to `PascalCase`.
/// `get_user_by_id` → `GetUserById`
/// `GetUserById`    → `GetUserById`  (already Pascal)
fn to_pascal_case(name: &str) -> String {
    // If name contains underscores, treat as snake_case
    if name.contains('_') {
        name.split('_')
            .filter(|s| !s.is_empty())
            .map(|s| {
                let mut c = s.chars();
                match c.next() {
                    None => String::new(),
                    Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
                }
            })
            .collect()
    } else {
        // Already PascalCase or single word — capitalise first letter only
        let mut c = name.chars();
        match c.next() {
            None => String::new(),
            Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
        }
    }
}

/// Convert a name to `SCREAMING_SNAKE_CASE`.
/// `get_user_by_id` → `GET_USER_BY_ID`
/// `GetUserById`    → `GET_USER_BY_ID`
fn to_screaming_snake(name: &str) -> String {
    if name.contains('_') {
        name.to_uppercase()
    } else {
        // PascalCase → insert underscores before uppercase letters (except first)
        let mut result = String::new();
        for (i, c) in name.chars().enumerate() {
            if c.is_uppercase() && i > 0 {
                result.push('_');
            }
            result.push(c.to_uppercase().next().unwrap());
        }
        result
    }
}

/// Sanitise a field name: append `_` if it is a Python keyword.
fn sanitise_field_name(name: &str) -> String {
    if is_python_keyword(name) {
        format!("{}_", name)
    } else {
        name.to_string()
    }
}

/// Collect the set of non-typing extra imports needed given a list of Python
/// type strings. Typing imports (Any, Optional, Protocol) are handled
/// separately in a single consolidated `from typing import ...` line.
fn collect_stdlib_imports(types: &[&str]) -> Vec<&'static str> {
    let mut imports: Vec<&'static str> = Vec::new();

    if types.iter().any(|t| t.starts_with("datetime.")) {
        imports.push("import datetime");
    }
    if types.iter().any(|t| *t == "Decimal") {
        imports.push("from decimal import Decimal");
    }

    imports
}

/// Generate a `.py` file from a list of parsed queries and write it to `output_path`.
fn generate_python_file(
    queries: &[QueryParseResult],
    source_filename: &str,
    output_path: &Path,
) -> Result<(), std::io::Error> {
    // Detect naming collisions: two queries producing the same row class name
    let mut class_names_seen: Vec<String> = Vec::new();
    for query in queries {
        if matches!(query.annotation.cardinality, QueryCardinality::Exec) {
            continue;
        }
        let class_name = format!("{}Row", to_pascal_case(&query.annotation.name));
        if class_names_seen.contains(&class_name) {
            eprintln!(
                "Error: duplicate row class name \"{}\" in \"{}\", skipping file generation",
                class_name, source_filename
            );
            return Ok(());
        }
        class_names_seen.push(class_name);
    }

    // Gather all type strings across input and output fields to determine imports
    let mut all_types: Vec<String> = Vec::new();
    let has_one = queries
        .iter()
        .any(|q| matches!(q.annotation.cardinality, QueryCardinality::One));

    for query in queries {
        for f in &query.input_fields {
            all_types.push(f.data_type.clone());
        }
        for f in &query.output_fields {
            if let Some(sql_type) = output_field_sql_type(f) {
                all_types.push(sql_type_to_python(&sql_type).to_string());
            }
        }
    }
    // Optional needs Any too
    if has_one {
        all_types.push("Optional".to_string());
    }

    let type_refs: Vec<&str> = all_types.iter().map(|s| s.as_str()).collect();
    let stdlib_imports = collect_stdlib_imports(&type_refs);

    let needs_any = all_types.iter().any(|t| t == "Any");

    let mut out = String::new();

    // Header
    out.push_str(&format!(
        "# GENERATED BY icantbelieveitsnotsql -- DO NOT EDIT\n# Source: {}\n\n",
        source_filename
    ));

    // Always-present imports
    out.push_str("from __future__ import annotations\n\n");
    out.push_str("import dataclasses\n");
    for import in &stdlib_imports {
        out.push_str(import);
        out.push('\n');
    }
    // Single consolidated typing import line
    {
        let mut typing_names: Vec<&str> = Vec::new();
        // Any always needed (used by _Cursor protocol)
        typing_names.push("Any");
        if has_one {
            typing_names.push("Optional");
        }
        typing_names.push("Protocol");
        out.push_str(&format!("from typing import {}\n", typing_names.join(", ")));
    }
    let _ = needs_any; // always included in typing block above
    out.push('\n');

    // _Cursor protocol
    out.push_str("\nclass _Cursor(Protocol):\n");
    out.push_str("    def execute(self, sql: str, parameters: Any = ...) -> Any: ...\n");
    out.push_str("    def fetchone(self) -> tuple[Any, ...] | None: ...\n");
    out.push_str("    def fetchall(self) -> list[tuple[Any, ...]]: ...\n");

    // One block per query
    for query in queries {
        let fn_name = query.annotation.name.to_lowercase();
        let pascal_name = to_pascal_case(&query.annotation.name);
        let const_name = format!("_{}_SQL", to_screaming_snake(&query.annotation.name));
        let row_class = format!("{}Row", pascal_name);

        out.push_str("\n\n");
        out.push_str(&format!("# {}\n", "-".repeat(75 - fn_name.len().min(73))));
        out.push_str(&format!("# {}\n", fn_name));
        out.push_str(&format!("# {}\n", "-".repeat(75 - fn_name.len().min(73))));

        // SQL constant — embed verbatim, strip trailing semicolon
        let sql_text = query
            .statement
            .to_string()
            .trim_end_matches(';')
            .trim()
            .to_string();
        out.push_str(&format!(
            "\n{} = \"\"\"\n{}\n\"\"\"\n",
            const_name, sql_text
        ));

        let is_exec = matches!(query.annotation.cardinality, QueryCardinality::Exec);

        // Row dataclass (only for :one and :many)
        if !is_exec {
            out.push('\n');
            out.push('\n');
            out.push_str("@dataclasses.dataclass\n");
            out.push_str(&format!("class {}:\n", row_class));
            if query.output_fields.is_empty() {
                out.push_str("    pass\n");
            } else {
                for field in &query.output_fields {
                    let field_name = sanitise_field_name(&field.name);
                    let py_type = output_field_python_type(field);
                    out.push_str(&format!("    {}: {}\n", field_name, py_type));
                }
            }
        }

        // Function signature
        let return_type = match &query.annotation.cardinality {
            QueryCardinality::One => format!("Optional[{}]", row_class),
            QueryCardinality::Many => format!("list[{}]", row_class),
            QueryCardinality::Exec => "None".to_string(),
        };

        // Build parameter list
        let mut params = vec!["cursor: _Cursor".to_string()];
        if !query.input_fields.is_empty() {
            params.push("*".to_string());
            for f in &query.input_fields {
                let param_name = sanitise_field_name(&f.name);
                params.push(format!("{}: {}", param_name, f.data_type));
            }
        }

        // Blank line before def
        out.push('\n');
        out.push_str(&format!(
            "def {}({}) -> {}:\n",
            fn_name,
            params.join(", "),
            return_type
        ));

        // Function body: build execute call
        let execute_args = build_execute_args(&query.input_fields);
        if execute_args.is_empty() {
            out.push_str(&format!("    cursor.execute({})\n", const_name));
        } else {
            out.push_str(&format!(
                "    cursor.execute({}, {})\n",
                const_name, execute_args
            ));
        }

        match &query.annotation.cardinality {
            QueryCardinality::Exec => {
                // nothing to return
            }
            QueryCardinality::One => {
                out.push_str("    row = cursor.fetchone()\n");
                out.push_str("    if row is None:\n");
                out.push_str("        return None\n");
                out.push_str(&format!("    return {}(\n", row_class));
                for (i, field) in query.output_fields.iter().enumerate() {
                    let field_name = sanitise_field_name(&field.name);
                    out.push_str(&format!("        {}=row[{}],\n", field_name, i));
                }
                out.push_str("    )\n");
            }
            QueryCardinality::Many => {
                out.push_str("    rows = cursor.fetchall()\n");
                out.push_str(&format!("    return [\n        {}(\n", row_class));
                for (i, field) in query.output_fields.iter().enumerate() {
                    let field_name = sanitise_field_name(&field.name);
                    out.push_str(&format!("            {}=row[{}],\n", field_name, i));
                }
                out.push_str("        )\n        for row in rows\n    ]\n");
            }
        }
    }

    out.push('\n');

    fs::write(output_path, out)
}

/// Build the second argument to `cursor.execute()` based on the input fields.
/// Named params → dict literal.  Positional ($1) → tuple.  Empty → "".
fn build_execute_args(input_fields: &[QueryInputField]) -> String {
    if input_fields.is_empty() {
        return String::new();
    }

    // Check if any field is positional ($1-style)
    let all_named = input_fields.iter().all(|f| !f.name.starts_with('$'));

    if all_named {
        // {":name": name, ...} — but sqlite3 named style uses plain dict keys
        let pairs: Vec<String> = input_fields
            .iter()
            .map(|f| {
                let safe = sanitise_field_name(&f.name);
                format!("\"{}\": {}", f.name, safe)
            })
            .collect();
        format!("{{{}}}", pairs.join(", "))
    } else {
        // Tuple of positional args
        let args: Vec<String> = input_fields
            .iter()
            .map(|f| sanitise_field_name(&f.name))
            .collect();
        if args.len() == 1 {
            format!("({},)", args[0])
        } else {
            format!("({})", args.join(", "))
        }
    }
}

/// Get the SQL type string for an output field (for mapping to Python type).
/// We stored the resolved table + field in QueryOutputField; look it up from
/// the field's name since we don't currently carry the data_type through.
/// For now, return None and fall back to Any — the output field data_type
/// will be wired in a future improvement.
fn output_field_sql_type(_field: &QueryOutputField) -> Option<String> {
    None
}

/// Get the Python type annotation for an output field.
/// Currently falls back to Any since we don't carry data_type through
/// QueryOutputField yet. This will improve once output fields carry their type.
fn output_field_python_type(_field: &QueryOutputField) -> &'static str {
    "Any"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_type_to_python_integer() {
        assert_eq!(sql_type_to_python("INTEGER"), "int");
        assert_eq!(sql_type_to_python("INT"), "int");
        assert_eq!(sql_type_to_python("BIGINT"), "int");
        assert_eq!(sql_type_to_python("SMALLINT"), "int");
        assert_eq!(sql_type_to_python("TINYINT"), "int");
    }

    #[test]
    fn test_sql_type_to_python_text() {
        assert_eq!(sql_type_to_python("TEXT"), "str");
        assert_eq!(sql_type_to_python("VARCHAR"), "str");
        assert_eq!(sql_type_to_python("CHARACTER VARYING(255)"), "str");
        assert_eq!(sql_type_to_python("CHAR(10)"), "str");
    }

    #[test]
    fn test_sql_type_to_python_float() {
        assert_eq!(sql_type_to_python("REAL"), "float");
        assert_eq!(sql_type_to_python("FLOAT"), "float");
        assert_eq!(sql_type_to_python("DOUBLE"), "float");
        assert_eq!(sql_type_to_python("DOUBLE PRECISION"), "float");
    }

    #[test]
    fn test_sql_type_to_python_bool() {
        assert_eq!(sql_type_to_python("BOOLEAN"), "bool");
        assert_eq!(sql_type_to_python("BOOL"), "bool");
    }

    #[test]
    fn test_sql_type_to_python_bytes() {
        assert_eq!(sql_type_to_python("BLOB"), "bytes");
        assert_eq!(sql_type_to_python("blob"), "bytes");
        assert_eq!(sql_type_to_python("BYTEA"), "bytes");
    }

    #[test]
    fn test_sql_type_to_python_decimal() {
        assert_eq!(sql_type_to_python("NUMERIC"), "Decimal");
        assert_eq!(sql_type_to_python("DECIMAL"), "Decimal");
        assert_eq!(sql_type_to_python("NUMERIC(10,2)"), "Decimal");
    }

    #[test]
    fn test_sql_type_to_python_datetime() {
        assert_eq!(sql_type_to_python("DATE"), "datetime.date");
        assert_eq!(sql_type_to_python("TIME"), "datetime.time");
        assert_eq!(sql_type_to_python("TIMESTAMP"), "datetime.datetime");
        assert_eq!(sql_type_to_python("DATETIME"), "datetime.datetime");
        assert_eq!(sql_type_to_python("TIMESTAMPTZ"), "datetime.datetime");
    }

    #[test]
    fn test_sql_type_to_python_json() {
        assert_eq!(sql_type_to_python("JSON"), "Any");
        assert_eq!(sql_type_to_python("JSONB"), "Any");
    }

    #[test]
    fn test_sql_type_to_python_uuid() {
        assert_eq!(sql_type_to_python("UUID"), "str");
    }

    #[test]
    fn test_sql_type_to_python_unknown_fallback() {
        assert_eq!(sql_type_to_python("FROBNICATOR"), "Any");
    }

    #[test]
    fn test_sql_type_to_python_sqlite_affinity_fallback() {
        assert_eq!(sql_type_to_python("MYINTEGER"), "int");
        assert_eq!(sql_type_to_python("LONGTEXT"), "str");
    }

    #[test]
    fn test_collect_placeholders_named() {
        use sqlparser::dialect::SQLiteDialect;
        use sqlparser::parser::Parser as SQLParser;

        let sql = "SELECT id FROM users WHERE id = :id AND email = :email";
        let ast = SQLParser::parse_sql(&SQLiteDialect {}, sql).unwrap();
        if let Statement::Query(q) = &ast[0] {
            let select = q.body.as_select().unwrap();
            let mut placeholders = Vec::new();
            if let Some(expr) = &select.selection {
                collect_placeholders(expr, &mut placeholders);
            }
            assert_eq!(placeholders, vec![":id", ":email"]);
        } else {
            panic!("expected Query");
        }
    }

    #[test]
    fn test_build_input_fields_named_dedup() {
        let schema = SchemaParseResult {
            table_fields: HashMap::new(),
        };
        let raw = vec![":id".to_string(), ":email".to_string(), ":id".to_string()];
        let fields = build_input_fields(&raw, &[], &schema);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[1].name, "email");
    }

    #[test]
    fn test_build_input_fields_anon_positional() {
        let schema = SchemaParseResult {
            table_fields: HashMap::new(),
        };
        let raw = vec!["?".to_string(), "?".to_string(), "?".to_string()];
        let fields = build_input_fields(&raw, &[], &schema);
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, "p1");
        assert_eq!(fields[1].name, "p2");
        assert_eq!(fields[2].name, "p3");
    }

    #[test]
    fn test_build_input_fields_type_inference() {
        let mut users_cols = HashMap::new();
        users_cols.insert("id".to_string(), "INTEGER".to_string());
        users_cols.insert("email".to_string(), "TEXT".to_string());
        let mut table_fields = HashMap::new();
        table_fields.insert("users".to_string(), users_cols);

        let schema = SchemaParseResult { table_fields };
        let raw = vec![":id".to_string(), ":email".to_string()];
        let fields = build_input_fields(&raw, &["users"], &schema);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].data_type, "int");
        assert_eq!(fields[1].name, "email");
        assert_eq!(fields[1].data_type, "str");
    }
}
