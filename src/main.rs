use clap::Parser;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SQLParser;
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
    let ast = SQLParser::parse_sql(&dialect, &sql)?;

    println!("{:#?}", ast);

    Ok(())
}
