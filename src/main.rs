use clap::Parser;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser as SQLParser, ParserError};
use std::fs::File;
use std::io::{self, Read};

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
            for statement in ast {
                match statement {
                    Statement::CreateTable(create_table) => {
                        eprintln!("")
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}
