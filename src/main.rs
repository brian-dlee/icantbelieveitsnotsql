use clap::Parser;
use serde::Deserialize;
use sqlparser::ast::{Expr, FromTable, SelectItem, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect;
use sqlparser::parser::{Parser as SQLParser, ParserError};
use std::collections::HashMap;
use std::fs::{self};
use std::path::PathBuf;
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

    let mut queries: Vec<QueryParseResult> = Vec::new();

    for query_file_path in fs::read_dir(&queries_dir_path)? {
        match query_file_path {
            Ok(dir_entry) => {
                let path = dir_entry.path();

                println!("Reading {}", path.display());

                let sql = match fs::read_to_string(&path) {
                    Err(err) => {
                        eprintln!("Failed to read query file \"{}\": {}", path.display(), err);
                        continue;
                    }
                    Ok(contents) => contents,
                };

                match SQLParser::parse_sql(parser_dialect, &sql) {
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

                            eprintln!(
                                "Processing query {:?} ({}) in \"{}\"",
                                annotation.name,
                                annotation.cardinality,
                                path.display(),
                            );

                            match process_sql_statement(statement, annotation, &schema) {
                                Ok(result) => {
                                    queries.push(result);
                                }
                                Err(err) => {
                                    eprintln!(
                                        "Failed to process SQL statement \"{}\": {:?}",
                                        path.display(),
                                        err,
                                    );
                                }
                            }
                        }
                    }
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

    println!(" ---------- SCHEMA --------- ");
    println!("{:#?}", schema.table_fields);
    println!("");

    println!(" ---------- QUERIES --------- ");
    for query in queries {
        println!(
            "SQL:{}\nAnnotation: {} ({})\nResult:\nInput Fields:\n{:#?}\nOutput Fields:\n{:#?}",
            query.statement,
            query.annotation.name,
            query.annotation.cardinality,
            query.input_fields,
            query.output_fields
        );
    }
    println!("");

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
