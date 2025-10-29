use clap::Parser;
use sqlparser::ast::{ObjectName, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser as SQLParser, ParserError};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::iter::Map;

#[derive(Parser)]
struct Args {
    /// Input file (use "-" for stdin)
    file: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut sql = String::new();
    if args.file == "-" {
        io::stdin().read_to_string(&mut sql)?;
    } else {
        File::open(&args.file)?.read_to_string(&mut sql)?;
    }

    let dialect = GenericDialect {};

    fn print_block(text: &str, line_start: usize, line_end: usize) -> String {
        let lines: Vec<&str> = text
            .split("\n")
            .skip(line_start)
            .take(line_end - line_start)
            .collect();

        return lines.join("\n");
    }

    match SQLParser::parse_sql(&dialect, &sql) {
        Err(err) => match err {
            ParserError::ParserError(msg) => {
                println!("{}", print_block(&sql, 33, 34));

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
                        fn extract_ident_from_object_name(
                            object_name: &ObjectName,
                        ) -> Result<String, String> {
                            let mut name_object_name_parts = object_name.0.iter();
                            let size = name_object_name_parts.clone().count();

                            if size != 1 {
                                Err(format!(
                                    "Unsupported ObjectName parts count: expected 1, got {}",
                                    size
                                ))
                            } else {
                                let part = name_object_name_parts.next().unwrap();
                                let ident = part.as_ident().unwrap();

                                Ok(ident.value.clone())
                            }
                        }

                        let table_name = extract_ident_from_object_name(&create_table.name)?;

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
