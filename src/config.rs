//! `butter.toml` parsing.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::error::ButterError;

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PythonConfig {
    /// Directory that receives one `.py` module per `.sql` query file.
    #[serde(rename = "output-dir")]
    pub output_dir: PathBuf,

    /// Database driver the generated code targets. Only `aiosqlite` today.
    #[serde(default = "default_driver")]
    pub driver: String,

    /// Python type overrides keyed by `table.column`.
    #[serde(rename = "column-types", default)]
    pub column_types: BTreeMap<String, String>,

    /// Python type overrides keyed by the declared SQL type name (`DATETIME`).
    #[serde(rename = "sql-types", default)]
    pub sql_types: BTreeMap<String, String>,
}

fn default_driver() -> String {
    String::from("aiosqlite")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerateConfig {
    #[serde(default = "default_dialect")]
    pub dialect: String,

    /// Directory of `.sql` files. Every file becomes one generated module.
    #[serde(rename = "queries-dir", default = "default_queries_dir")]
    pub queries_dir: PathBuf,

    /// Optional schema files (`CREATE TABLE` statements only, no functions generated).
    #[serde(rename = "schema-files", default)]
    pub schema_files: Vec<PathBuf>,

    /// Single-file spelling of `schema-files`, kept for older configs.
    #[serde(rename = "schema-file")]
    pub schema_file: Option<PathBuf>,

    /// Older configs put `output-dir` here; it means Python output with defaults.
    #[serde(rename = "output-dir")]
    pub output_dir: Option<PathBuf>,

    pub python: Option<PythonConfig>,
}

fn default_dialect() -> String {
    String::from("sqlite")
}

fn default_queries_dir() -> PathBuf {
    PathBuf::from("queries")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub generate: GenerateConfig,
}

impl Config {
    pub fn load(path: &Path) -> Result<Config, ButterError> {
        let content = std::fs::read_to_string(path).map_err(|err| {
            ButterError::Config(format!("cannot read {}: {}", path.display(), err))
        })?;

        let config: Config = toml::from_str(&content)
            .map_err(|err| ButterError::Config(format!("{}: {}", path.display(), err)))?;

        Ok(config)
    }

    /// The Python section, or one synthesized from a top-level `output-dir`.
    pub fn python(&self) -> Option<PythonConfig> {
        if let Some(python) = &self.generate.python {
            return Some(PythonConfig {
                output_dir: python.output_dir.clone(),
                driver: python.driver.clone(),
                column_types: python.column_types.clone(),
                sql_types: python.sql_types.clone(),
            });
        }
        self.generate.output_dir.as_ref().map(|output_dir| PythonConfig {
            output_dir: output_dir.clone(),
            driver: default_driver(),
            column_types: BTreeMap::new(),
            sql_types: BTreeMap::new(),
        })
    }

    pub fn schema_files(&self) -> Vec<PathBuf> {
        let mut files = self.generate.schema_files.clone();
        if let Some(single) = &self.generate.schema_file {
            files.push(single.clone());
        }
        files
    }
}
