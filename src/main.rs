use clap::Parser;
use serde::Deserialize;
use sqlparser::ast::{Statement, TableFactor};
use sqlparser::dialect;
use sqlparser::parser::{Parser as SQLParser, ParserError};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Read;
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

fn print_block(text: &str, line_start: i32, line_end: i32) -> String {
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

    println!("{:#?}", config);

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

    let mut sql = String::new();

    println!("Reading schema file: {}", schema_file_path.display());

    File::open(schema_file_path)?.read_to_string(&mut sql)?;

    let mut tables: HashMap<String, HashMap<String, String>> = HashMap::new();

    match SQLParser::parse_sql(parser_dialect, &sql) {
        Err(err) => match err {
            ParserError::ParserError(msg) => {
                let line_number = extract_line_number_from_parse_error(&msg);

                println!("{}", print_block(&sql, line_number - 2, line_number + 2));

                eprintln!("parser error: {}", msg)
            }
            ParserError::TokenizerError(msg) => {
                eprintln!("tokenizer error: {}", msg)
            }
            ParserError::RecursionLimitExceeded => {
                eprintln!("recursion limit exceeded")
            }
        },
        Ok(ast) => {
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

            println!("{:#?}", tables);
        }
    }

    for query_file_path in fs::read_dir(queries_dir_path)? {
        match query_file_path {
            Ok(dir_entry) => {
                println!("Reading {}", dir_entry.path().display());

                let sql = fs::read_to_string(dir_entry.path())?;

                match SQLParser::parse_sql(parser_dialect, &sql) {
                    Err(err) => match err {
                        ParserError::ParserError(msg) => {
                            let line_number = extract_line_number_from_parse_error(&msg);

                            println!("{}", print_block(&sql, line_number - 2, line_number + 2));

                            eprintln!("parser error: {}", msg)
                        }
                        ParserError::TokenizerError(msg) => {
                            eprintln!("tokenizer error: {}", msg)
                        }
                        ParserError::RecursionLimitExceeded => {
                            eprintln!("recursion limit exceeded")
                        }
                    },
                    Ok(ast) => {
                        for statement in ast {
                            match statement {
                                Statement::Query(query) => {
                                    let select = query.body.as_select().unwrap();

                                    for (i, entry) in select.projection.iter().enumerate() {
                                        println!("SELECT ITEM {}: {}", i, entry.to_string());
                                    }

                                    if select.from.len() > 1 {
                                        eprintln!("Unsupported SELECT query has more than one FROM clause: {}", query);
                                    } else {
                                        let from = select.from.first().unwrap().clone();

                                        match from.relation {
                                            TableFactor::Table {
                                                name,
                                                alias,
                                                args,
                                                with_hints,
                                                version,
                                                with_ordinality,
                                                partitions,
                                                json_path,
                                                sample,
                                                index_hints,
                                            } => {
                                                let alias_name = if let Some(alias) = &alias {
                                                    alias.name.to_string()
                                                } else {
                                                    name.to_string()
                                                };

                                                println!(
                                                    "FROM TABLE {} (alias: {})",
                                                    name, alias_name
                                                );
                                            }
                                            x => {
                                                eprintln!("NOT SUPPORTED {}", x);
                                            }
                                        }
                                    }
                                }
                                Statement::Insert(query) => {
                                    let insert = query.source.unwrap();

                                    // println!("INSERT {:#?}", insert);
                                }
                                Statement::Update {
                                    table,
                                    assignments,
                                    from,
                                    selection,
                                    returning,
                                    or,
                                    limit,
                                } => {
                                    // println!("UPDATE {:#?}", table);
                                }
                                Statement::Delete(query) => {
                                    let delete = query.tables;

                                    // println!("DELETE {:#?}", delete);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("{}", e);
            }
        }
    }

    Ok(())
}
