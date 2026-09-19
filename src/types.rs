//! Abstract SQL value types and their mapping to target-language types.

use sqlparser::ast::DataType;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlType {
    Int,
    Float,
    Numeric,
    Text,
    Blob,
    Bool,
    Date,
    Time,
    DateTime,
    Json,
    /// Unknown: the analyzer could not determine a type.
    Any,
}

impl SqlType {
    pub fn from_data_type(data_type: &DataType) -> SqlType {
        SqlType::from_type_name(&data_type.to_string())
    }

    /// Follows SQLite's type-affinity rules, with a few extra names
    /// (BOOLEAN, DATE, DATETIME, TIMESTAMP, JSON) recognized before the affinity fallbacks.
    pub fn from_type_name(name: &str) -> SqlType {
        let upper = name.trim().to_uppercase();
        let base = upper.split('(').next().unwrap_or("").trim().to_string();

        match base.as_str() {
            "BOOL" | "BOOLEAN" => return SqlType::Bool,
            "DATE" => return SqlType::Date,
            "TIME" => return SqlType::Time,
            "DATETIME" | "TIMESTAMP" | "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE"
            | "TIMESTAMP WITHOUT TIME ZONE" => return SqlType::DateTime,
            "JSON" | "JSONB" => return SqlType::Json,
            "" => return SqlType::Blob,
            _ => {}
        }

        if upper.contains("INT") {
            SqlType::Int
        } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
            SqlType::Text
        } else if upper.contains("BLOB") || upper.contains("BYTEA") || upper.contains("BINARY") {
            SqlType::Blob
        } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            SqlType::Float
        } else {
            SqlType::Numeric
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, SqlType::Int | SqlType::Float | SqlType::Numeric | SqlType::Bool)
    }

    /// Combine two types that flow into the same slot (UNION, CASE, COALESCE).
    pub fn unify(a: SqlType, b: SqlType) -> SqlType {
        use SqlType::*;
        match (a, b) {
            (x, y) if x == y => x,
            (Any, y) => y,
            (x, Any) => x,
            (Int, Float) | (Float, Int) => Float,
            (Int, Numeric) | (Numeric, Int) | (Float, Numeric) | (Numeric, Float) => Numeric,
            (Bool, Int) | (Int, Bool) => Int,
            _ => Any,
        }
    }
}

/// A type plus nullability, as the analyzer tracks it through expressions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeInfo {
    pub sql_type: SqlType,
    pub nullable: bool,
    /// The declared SQL type text when the value comes straight from a column
    /// (`TEXT`, `DATETIME`, ...). Used for `sql-types` overrides.
    pub declared: Option<String>,
}

impl TypeInfo {
    pub fn new(sql_type: SqlType, nullable: bool) -> TypeInfo {
        TypeInfo {
            sql_type,
            nullable,
            declared: None,
        }
    }

    pub fn any() -> TypeInfo {
        TypeInfo::new(SqlType::Any, true)
    }

    pub fn not_null(sql_type: SqlType) -> TypeInfo {
        TypeInfo::new(sql_type, false)
    }

    pub fn unify(a: &TypeInfo, b: &TypeInfo) -> TypeInfo {
        TypeInfo {
            sql_type: SqlType::unify(a.sql_type, b.sql_type),
            nullable: a.nullable || b.nullable,
            declared: if a.declared == b.declared {
                a.declared.clone()
            } else {
                None
            },
        }
    }
}
