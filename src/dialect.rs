use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};

use crate::error::ButterError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Generic,
    SQLite,
    PostgreSQL,
    MySQL,
}

impl SqlDialect {
    pub fn parse(value: &str) -> Result<SqlDialect, ButterError> {
        match value.to_lowercase().as_str() {
            "generic" => Ok(SqlDialect::Generic),
            "sqlite" => Ok(SqlDialect::SQLite),
            "postgresql" | "postgres" => Ok(SqlDialect::PostgreSQL),
            "mysql" => Ok(SqlDialect::MySQL),
            _ => Err(ButterError::UnsupportedDialect(String::from(value))),
        }
    }

    pub fn parser_dialect(&self) -> Box<dyn Dialect> {
        match self {
            SqlDialect::Generic => Box::new(GenericDialect {}),
            SqlDialect::SQLite => Box::new(SQLiteDialect {}),
            SqlDialect::PostgreSQL => Box::new(PostgreSqlDialect {}),
            SqlDialect::MySQL => Box::new(MySqlDialect {}),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            SqlDialect::Generic => "generic",
            SqlDialect::SQLite => "sqlite",
            SqlDialect::PostgreSQL => "postgresql",
            SqlDialect::MySQL => "mysql",
        }
    }
}
