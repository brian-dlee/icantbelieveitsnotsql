mod analyze;
mod config;
mod dialect;
mod error;
mod generate;
mod python;
mod queryfile;
mod schema;
mod types;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Generate typed data-access code from plain SQL files.
#[derive(Parser)]
#[command(name = "butter", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Read butter.toml, analyze the SQL files, and write the generated modules.
    Generate {
        /// Directory containing butter.toml.
        #[arg(default_value = ".")]
        project_path: PathBuf,
    },
    /// Analyze the SQL files and report problems without writing anything.
    Check {
        /// Directory containing butter.toml.
        #[arg(default_value = ".")]
        project_path: PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();

    let (project_path, write) = match cli.command {
        Commands::Generate { project_path } => (project_path, true),
        Commands::Check { project_path } => (project_path, false),
    };

    match generate::run(&project_path, write) {
        Ok(report) => {
            for warning in &report.warnings {
                eprintln!("warning: {}", warning);
            }

            println!(
                "dialect {}; {} table{} in schema: {}",
                report.dialect.name(),
                report.schema_tables.len(),
                if report.schema_tables.len() == 1 { "" } else { "s" },
                if report.schema_tables.is_empty() {
                    String::from("(none)")
                } else {
                    report.schema_tables.join(", ")
                }
            );

            for (path, count) in &report.outputs {
                println!(
                    "{} {} ({} quer{})",
                    if report.wrote_files { "wrote" } else { "checked" },
                    path.display(),
                    count,
                    if *count == 1 { "y" } else { "ies" }
                );
            }
        }
        Err(err) => {
            eprintln!("error: {}", err);
            std::process::exit(1);
        }
    }
}
