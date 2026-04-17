use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;
use thiserror;

pub enum SQLDialect {
    Generic,
    SQLite,
    PostgreSQL,
    MySQL,
}

#[derive(thiserror::Error, Debug)]
pub enum SQLDialectError {
    #[error("unsupported dialect: {0}")]
    Unsupported(String),
}

impl SQLDialect {
    pub fn from_str(value: &str) -> Result<SQLDialect, SQLDialectError> {
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
pub struct Args {
    pub project_path: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct GenerateConfig {
    pub dialect: Option<String>,

    #[serde(rename = "queries-dir")]
    pub queries_dir: Option<PathBuf>,

    #[serde(rename = "schema-file")]
    pub schema_file: Option<PathBuf>,

    #[serde(rename = "output-dir")]
    pub output_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub generate: GenerateConfig,
}
