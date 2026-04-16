use clap::Parser;
use serde::Deserialize;
use sqlparser::ast::{Expr, SelectItem, Statement, TableFactor};
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
                    let valid_python_ident = name
                        .chars()
                        .enumerate()
                        .all(|(i, c)| if i == 0 { c.is_alphabetic() || c == '_' } else { c.is_alphanumeric() || c == '_' });

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
    let input_fields: Vec<QueryInputField> = Vec::new();
    let mut output_fields: Vec<QueryOutputField> = Vec::new();

    match statement {
        Statement::Query(query) => {
            let select = query.body.as_select().unwrap();

            let mut aliases: HashMap<String, String> = HashMap::new();

            for table_with_joins in &select.from {
                aliases.extend(extract_aliases_using_relation(&table_with_joins.relation));

                for join in &table_with_joins.joins {
                    aliases.extend(extract_aliases_using_relation(&join.relation));
                }
            }

            for entry in select.projection.iter() {
                output_fields.extend(extract_output_fields_from_select_item(entry, &aliases))
            }

            if let Some(expr) = &select.selection {
                match expr {
                    Expr::BinaryOp { left, op, right } => {
                        println!("binary op: {} {} {}", left, op, right);
                    }
                    _ => {
                        println!("where expr: {}", expr.clone());
                    }
                }
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

                    // output_field.source.table
                }
            }
        }
        Statement::Insert(query) => {}
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            or,
            limit,
        } => {}
        Statement::Delete(query) => {}
        _ => {}
    };

    Ok(QueryParseResult {
        statement: statement.clone(),
        annotation,
        input_fields,
        output_fields,
    })
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
