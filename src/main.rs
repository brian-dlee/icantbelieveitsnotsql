use clap::Parser;
use sqlparser::ast::Statement;
use sqlparser::dialect;
use sqlparser::parser::{Parser as SQLParser, ParserError};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use thiserror;

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
    /// Input file (use "-" for stdin)
    file: String,

    #[arg(short, long, default_value_t = String::from("generic"))]
    dialect: String,
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut sql = String::new();
    if args.file == "-" {
        io::stdin().read_to_string(&mut sql)?;
    } else {
        File::open(&args.file)?.read_to_string(&mut sql)?;
    }

    let sql_dialect = SQLDialect::from_str(&args.dialect)?;

    println!("Using SQL dialect: {}", args.dialect);

    let parser_dialect: &dyn dialect::Dialect = match sql_dialect {
        SQLDialect::Generic => &dialect::GenericDialect {},
        SQLDialect::SQLite => &dialect::SQLiteDialect {},
        SQLDialect::PostgreSQL => &dialect::PostgreSqlDialect {},
        SQLDialect::MySQL => &dialect::MySqlDialect {},
    };

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
            let mut tables: HashMap<String, HashMap<String, String>> = HashMap::new();

            for statement in ast {
                match statement {
                    Statement::CreateTable(create_table) => {
                        let table_name = create_table.name.to_string();

                        println!("extracted table name: {}", table_name);

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

    Ok(())
}
