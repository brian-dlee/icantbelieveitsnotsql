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

                match parse_query_file(&sql, parser_dialect) {
                    Err(err) => {
                        eprintln!(
                            "Failed to parser query file \"{}\": {}",
                            path.display(),
                            err
                        );
                        continue;
                    }
                    Ok(result) => {
                        queries.extend(result);
                    }
                };
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
            "SQL:{}\nResult:\nInput Fields:\n{:#?}\nOutput Fields:\n{:#?}",
            query.statement, query.input_fields, query.output_fields
        );
    }
    println!("");

    Ok(())
}

#[derive(Debug)]
struct SchemaParseResult {
    table_fields: HashMap<String, HashMap<String, String>>,
}

#[derive(Debug)]
struct SchemaParseError {
    parser_error: ParserError,
    debug: Option<String>,
}

impl SchemaParseError {
    fn from_parser_error(
        schema_file_contents: &str,
        parser_error: &ParserError,
    ) -> SchemaParseError {
        let debug = if let ParserError::ParserError(msg) = &parser_error {
            let line_number = extract_line_number_from_parse_error(&msg);

            Some(extract_debug_block_with_line_number_range(
                schema_file_contents,
                line_number - 2,
                line_number + 2,
            ))
        } else {
            None
        };

        SchemaParseError {
            parser_error: parser_error.clone(),
            debug,
        }
    }
}

impl std::fmt::Display for SchemaParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(debug) = &self.debug {
            f.write_fmt(format_args!(
                "Failed to parse schema file: {}\n{}",
                self.parser_error, debug
            ))
        } else {
            f.write_fmt(format_args!(
                "Failed to parse schema file: {}",
                self.parser_error,
            ))
        }
    }
}

fn parse_schema_file(
    schema_file_contents: &str,
    parser_dialect: &dyn dialect::Dialect,
) -> Result<SchemaParseResult, SchemaParseError> {
    match SQLParser::parse_sql(parser_dialect, schema_file_contents) {
        Err(err) => Err(SchemaParseError::from_parser_error(
            schema_file_contents,
            &err,
        )),
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
enum QueryOutputFieldSource {
    TableField {
        database: Option<String>,
        schema: Option<String>,
        table: Option<String>,
        field: String,
    },
}

#[derive(Debug)]
struct QueryOutputField {
    source: QueryOutputFieldSource,
    name: String,
}

#[derive(Debug)]
struct QueryParseResult {
    statement: Statement,
    input_fields: Vec<QueryInputField>,
    output_fields: Vec<QueryOutputField>,
}

#[derive(Debug)]
struct QueryParseError {
    parser_error: ParserError,
    debug: Option<String>,
}

impl QueryParseError {
    fn from_parser_error(query_file_contents: &str, parser_error: &ParserError) -> QueryParseError {
        let debug = if let ParserError::ParserError(msg) = &parser_error {
            let line_number = extract_line_number_from_parse_error(&msg);

            Some(extract_debug_block_with_line_number_range(
                query_file_contents,
                line_number - 2,
                line_number + 2,
            ))
        } else {
            None
        };

        QueryParseError {
            parser_error: parser_error.clone(),
            debug,
        }
    }
}

impl std::fmt::Display for QueryParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(debug) = &self.debug {
            f.write_fmt(format_args!(
                "Failed to parse schema file: {}\n{}",
                self.parser_error, debug
            ))
        } else {
            f.write_fmt(format_args!(
                "Failed to parse schema file: {}",
                self.parser_error,
            ))
        }
    }
}

fn parse_query_file(
    query_file_contents: &str,
    parser_dialect: &dyn dialect::Dialect,
) -> Result<Vec<QueryParseResult>, QueryParseError> {
    match SQLParser::parse_sql(parser_dialect, query_file_contents) {
        Err(err) => Err(QueryParseError::from_parser_error(
            query_file_contents,
            &err,
        )),
        Ok(ast) => {
            let mut results: Vec<QueryParseResult> = Vec::new();

            for statement in &ast {
                let input_fields: Vec<QueryInputField> = Vec::new();
                let mut output_fields: Vec<QueryOutputField> = Vec::new();
                let debug_statement = statement.clone();

                match statement {
                    Statement::Query(query) => {
                        let select = query.body.as_select().unwrap();

                        let mut aliases: HashMap<String, String> = HashMap::new();

                        for table_with_joins in &select.from {
                            aliases
                                .extend(extract_aliases_using_relation(&table_with_joins.relation));

                            for join in &table_with_joins.joins {
                                aliases.extend(extract_aliases_using_relation(&join.relation));
                            }
                        }

                        for (i, entry) in select.projection.iter().enumerate() {
                            output_fields
                                .extend(extract_output_fields_from_select_item(entry, &aliases))
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
                }

                let result = QueryParseResult {
                    statement: statement.clone(),
                    input_fields,
                    output_fields,
                };

                results.push(result)
            }

            Ok(results)
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

fn extract_output_fields_from_select_item(
    select_item: &SelectItem,
    aliases: &HashMap<String, String>,
) -> Vec<QueryOutputField> {
    let mut output_fields: Vec<QueryOutputField> = Vec::new();

    match select_item {
        SelectItem::UnnamedExpr(expr) => match expr {
            Expr::Identifier(ident) => output_fields.push(QueryOutputField {
                source: QueryOutputFieldSource::TableField {
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

                    output_fields.push(QueryOutputField {
                        source: QueryOutputFieldSource::TableField {
                            database: None,
                            schema: None,
                            table: Some(table.to_string()),
                            field: field.to_string(),
                        },
                        name: field.to_string(),
                    });
                }
                [database_or_schema, table, field] => {
                    output_fields.push(QueryOutputField {
                        source: QueryOutputFieldSource::TableField {
                            database: None,
                            schema: Some(database_or_schema.to_string()),
                            table: Some(table.to_string()),
                            field: field.to_string(),
                        },
                        name: field.to_string(),
                    });
                }
                [database, schema, table, field] => {
                    output_fields.push(QueryOutputField {
                        source: QueryOutputFieldSource::TableField {
                            database: Some(database.to_string()),
                            schema: Some(schema.to_string()),
                            table: Some(table.to_string()),
                            field: field.to_string(),
                        },
                        name: field.to_string(),
                    });
                }
                _ => {
                    eprintln!(
                        "unsupported compound ident ({}): {:?}",
                        idents.len(),
                        idents
                    );
                }
            },
            x => {
                eprintln!("SELECT expression not supported: {:#?}", x);
            }
        },
        SelectItem::ExprWithAlias { expr, alias } => {}
        SelectItem::QualifiedWildcard(kind, options) => {}
        SelectItem::Wildcard(options) => {}
    }

    output_fields
}
