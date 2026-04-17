use clap::Parser;
use sqlparser::dialect;
use sqlparser::parser::Parser as SQLParser;
use std::fs;
use std::path::PathBuf;
use toml;

mod codegen {
    pub mod python;
}
mod config;
mod query;
mod schema;
mod util;

use codegen::python::generate_python_file;
use config::{Args, Config, SQLDialect};
use query::{extract_query_annotations, process_sql_statement};
use schema::parse_schema_file;
use util::format_sql_parser_error;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let project_path = args.project_path.unwrap_or_else(|| PathBuf::from("."));
    let config_file_path = project_path.join("butter.toml");

    println!("Reading config file at {}", config_file_path.display());

    let content = fs::read_to_string(&config_file_path)?;
    let config: Config = toml::from_str(&content)?;

    let selected_dialect = config
        .generate
        .dialect
        .unwrap_or_else(|| "generic".to_string());
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
            .unwrap_or_else(|| PathBuf::from("schema.sql")),
    );

    let queries_dir_path = project_path.join(
        config
            .generate
            .queries_dir
            .unwrap_or_else(|| PathBuf::from("queries")),
    );

    let output_dir_path = project_path.join(
        config
            .generate
            .output_dir
            .unwrap_or_else(|| PathBuf::from("generated")),
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

    let schema_sql = match fs::read_to_string(&schema_file_path) {
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

    let schema = match parse_schema_file(&schema_sql, parser_dialect) {
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

    for entry in fs::read_dir(&queries_dir_path)? {
        match entry {
            Ok(dir_entry) => {
                let path = dir_entry.path();

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
                        let mut file_queries = Vec::new();

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
