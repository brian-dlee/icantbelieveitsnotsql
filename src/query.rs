use crate::schema::{FieldSource, SchemaParseResult};
use sqlparser::ast::{Expr, FromTable, SelectItem, SetExpr, Statement, TableFactor, Value};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Annotation types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum QueryCardinality {
    One,
    Many,
    Exec,
}

impl std::fmt::Display for QueryCardinality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryCardinality::One => write!(f, ":one"),
            QueryCardinality::Many => write!(f, ":many"),
            QueryCardinality::Exec => write!(f, ":exec"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueryAnnotation {
    pub name: String,
    pub cardinality: QueryCardinality,
}

// ---------------------------------------------------------------------------
// Query result types
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct QueryInputField {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug)]
pub struct QueryOutputFieldSource {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub field: String,
}

#[derive(Debug)]
pub struct QueryOutputField {
    pub source: QueryOutputFieldSource,
    pub name: String,
}

#[derive(Debug)]
pub struct QueryParseResult {
    pub statement: Statement,
    pub annotation: QueryAnnotation,
    pub input_fields: Vec<QueryInputField>,
    pub output_fields: Vec<QueryOutputField>,
}

// ---------------------------------------------------------------------------
// Query errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum QueryError {
    AmbiguousFieldReference {
        field_name: String,
        candidates: Vec<FieldSource>,
    },
    InvalidFieldReference {
        field_name: String,
    },
}

impl QueryError {
    pub fn format(&self, _statement: &Statement, _sql: &str) -> String {
        String::from("")
    }
}

// ---------------------------------------------------------------------------
// Annotation pre-pass
// ---------------------------------------------------------------------------

pub fn extract_query_annotations(sql: &str) -> Vec<Option<QueryAnnotation>> {
    let mut annotations: Vec<Option<QueryAnnotation>> = Vec::new();

    let mut pending_annotation: Option<QueryAnnotation> = None;
    let mut in_statement = false;

    for line in sql.lines() {
        let trimmed = line.trim();

        if trimmed.is_empty() {
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("--") {
            let comment = rest.trim();
            let parts: Vec<&str> = comment.splitn(2, ':').collect();

            if parts.len() == 2 {
                let name = parts[0].trim();
                let cardinality_str = parts[1].trim();

                let cardinality = match cardinality_str {
                    "one" => Some(QueryCardinality::One),
                    "many" => Some(QueryCardinality::Many),
                    "exec" => Some(QueryCardinality::Exec),
                    _ => None,
                };

                if let (Some(cardinality), true) = (cardinality, !name.is_empty()) {
                    let valid_python_ident = name.chars().enumerate().all(|(i, c)| {
                        if i == 0 {
                            c.is_alphabetic() || c == '_'
                        } else {
                            c.is_alphanumeric() || c == '_'
                        }
                    });

                    if !valid_python_ident {
                        eprintln!(
                            "Warning: annotation name {:?} may not be a valid Python identifier",
                            name
                        );
                    }

                    pending_annotation = Some(QueryAnnotation {
                        name: name.to_string(),
                        cardinality,
                    });
                    in_statement = false;
                }
            }
            continue;
        }

        in_statement = true;

        if trimmed.ends_with(';') {
            annotations.push(pending_annotation.take());
            in_statement = false;
        }
    }

    if in_statement {
        annotations.push(pending_annotation.take());
    }

    annotations
}

// ---------------------------------------------------------------------------
// Placeholder extraction
// ---------------------------------------------------------------------------

/// Recursively walk an expression tree and collect all placeholder strings.
pub fn collect_placeholders(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Value(v) => {
            if let Value::Placeholder(name) = &v.value {
                out.push(name.clone());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_placeholders(left, out);
            collect_placeholders(right, out);
        }
        Expr::UnaryOp { expr, .. } => collect_placeholders(expr, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_placeholders(expr, out);
            collect_placeholders(low, out);
            collect_placeholders(high, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_placeholders(expr, out);
            for e in list {
                collect_placeholders(e, out);
            }
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => collect_placeholders(e, out),
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. } => {
            collect_placeholders(expr, out);
            collect_placeholders(pattern, out);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_placeholders(op, out);
            }
            for condition in conditions {
                collect_placeholders(&condition.condition, out);
                collect_placeholders(&condition.result, out);
            }
            if let Some(e) = else_result {
                collect_placeholders(e, out);
            }
        }
        Expr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &f.args {
                for arg in &arg_list.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Named { arg, .. }
                        | sqlparser::ast::FunctionArg::Unnamed(arg) => {
                            if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg {
                                collect_placeholders(e, out);
                            }
                        }
                        sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => {
                            if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg {
                                collect_placeholders(e, out);
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Input field building
// ---------------------------------------------------------------------------

pub fn build_input_fields(
    raw: &[String],
    active_tables: &[&str],
    schema: &SchemaParseResult,
) -> Vec<QueryInputField> {
    let mut seen: Vec<String> = Vec::new();
    let mut fields: Vec<QueryInputField> = Vec::new();
    let mut anon_counter = 0usize;

    for placeholder in raw {
        if placeholder == "?" {
            anon_counter += 1;
            let name = format!("p{}", anon_counter);
            if !seen.contains(&name) {
                seen.push(name.clone());
                fields.push(QueryInputField {
                    name,
                    data_type: "Any".to_string(),
                });
            }
        } else if let Some(name) = placeholder.strip_prefix(':') {
            if !seen.contains(&name.to_string()) {
                seen.push(name.to_string());
                let data_type = resolve_param_type(name, active_tables, schema);
                fields.push(QueryInputField {
                    name: name.to_string(),
                    data_type,
                });
            }
        } else if placeholder.starts_with('$') {
            if !seen.contains(placeholder) {
                seen.push(placeholder.clone());
                fields.push(QueryInputField {
                    name: placeholder.clone(),
                    data_type: "Any".to_string(),
                });
            }
        } else {
            if !seen.contains(placeholder) {
                seen.push(placeholder.clone());
                fields.push(QueryInputField {
                    name: placeholder.clone(),
                    data_type: "Any".to_string(),
                });
            }
        }
    }

    fields
}

pub fn resolve_param_type(
    param_name: &str,
    active_tables: &[&str],
    schema: &SchemaParseResult,
) -> String {
    let mut matches: Vec<&str> = Vec::new();

    for &table_name in active_tables {
        if let Some(columns) = schema.table_fields.get(table_name) {
            if let Some(sql_type) = columns.get(param_name) {
                matches.push(sql_type.as_str());
            }
        }
    }

    if matches.len() == 1 {
        sql_type_to_python(matches[0]).to_string()
    } else {
        "Any".to_string()
    }
}

// ---------------------------------------------------------------------------
// SQL → Python type mapping
// ---------------------------------------------------------------------------

pub fn sql_type_to_python(sql_type: &str) -> &'static str {
    let upper = sql_type.to_uppercase();
    let normalised = if let Some(pos) = upper.find('(') {
        upper[..pos].trim()
    } else {
        upper.trim()
    };

    match normalised {
        "INTEGER" | "INT" | "INT2" | "INT4" | "INT8" | "INT16" | "INT32" | "INT64" | "BIGINT"
        | "SMALLINT" | "TINYINT" | "MEDIUMINT" | "BYTEINT" | "HUGEINT" | "UBIGINT"
        | "USMALLINT" | "UTINYINT" | "UINTEGER" => "int",

        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" | "CHARACTER" | "CLOB" | "TINYTEXT"
        | "MEDIUMTEXT" | "LONGTEXT" | "STRING" | "NCHAR" | "NVARCHAR" | "NCLOB" | "BPCHAR" => "str",

        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT16" | "FLOAT32" | "FLOAT64" | "DOUBLE"
        | "DOUBLE PRECISION" => "float",

        "BOOLEAN" | "BOOL" => "bool",

        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB"
        | "BIT" | "BIT VARYING" => "bytes",

        "NUMERIC" | "DECIMAL" | "DEC" | "MONEY" | "SMALLMONEY" => "Decimal",

        "DATE" => "datetime.date",

        "TIME" | "TIMETZ" | "TIME WITH TIME ZONE" | "TIME WITHOUT TIME ZONE" => "datetime.time",

        "TIMESTAMP"
        | "TIMESTAMPTZ"
        | "TIMESTAMP WITH TIME ZONE"
        | "TIMESTAMP WITHOUT TIME ZONE"
        | "DATETIME"
        | "DATETIME2"
        | "SMALLDATETIME"
        | "DATETIMEOFFSET" => "datetime.datetime",

        "UUID" => "str",

        "JSON" | "JSONB" => "Any",

        _ => {
            if normalised.contains("INT") {
                "int"
            } else if normalised.contains("CHAR")
                || normalised.contains("CLOB")
                || normalised.contains("TEXT")
            {
                "str"
            } else if normalised.contains("REAL")
                || normalised.contains("FLOA")
                || normalised.contains("DOUB")
            {
                "float"
            } else if normalised.contains("BLOB") || normalised.is_empty() {
                "bytes"
            } else {
                "Any"
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Statement processing
// ---------------------------------------------------------------------------

pub fn process_sql_statement(
    statement: &Statement,
    annotation: QueryAnnotation,
    schema: &SchemaParseResult,
) -> Result<QueryParseResult, QueryError> {
    let mut raw_placeholders: Vec<String> = Vec::new();
    let mut active_tables: Vec<String> = Vec::new();
    let mut output_fields: Vec<QueryOutputField> = Vec::new();

    match statement {
        Statement::Query(query) => {
            let select = query.body.as_select().unwrap();
            let mut aliases: HashMap<String, String> = HashMap::new();

            for table_with_joins in &select.from {
                aliases.extend(extract_aliases_using_relation(&table_with_joins.relation));
                collect_table_name(&table_with_joins.relation, &mut active_tables);

                for join in &table_with_joins.joins {
                    aliases.extend(extract_aliases_using_relation(&join.relation));
                    collect_table_name(&join.relation, &mut active_tables);
                }
            }

            for entry in select.projection.iter() {
                output_fields.extend(extract_output_fields_from_select_item(entry, &aliases))
            }

            if let Some(expr) = &select.selection {
                collect_placeholders(expr, &mut raw_placeholders);
            }

            for output_field in output_fields.iter_mut() {
                if output_field.source.table.is_none() {
                    let resolved_fields = schema.resolve_fields_by_name(&output_field.source.field);

                    if resolved_fields.len() == 0 {
                        return Err(QueryError::InvalidFieldReference {
                            field_name: output_field.source.field.clone(),
                        });
                    } else if resolved_fields.len() > 1 {
                        return Err(QueryError::AmbiguousFieldReference {
                            field_name: output_field.source.field.clone(),
                            candidates: resolved_fields.clone(),
                        });
                    } else {
                        match resolved_fields.first().unwrap() {
                            FieldSource::TableSource {
                                database,
                                schema,
                                table,
                                column,
                                data_type: _,
                            } => {
                                output_field.source.database = database.clone();
                                output_field.source.schema = schema.clone();
                                output_field.source.table = Some(table.clone());
                                output_field.source.field = column.clone();
                            }
                        }
                    }
                }
            }
        }
        Statement::Insert(insert) => {
            active_tables.push(insert.table.to_string());

            if let Some(source) = &insert.source {
                if let SetExpr::Values(values) = source.body.as_ref() {
                    for row in &values.rows {
                        for expr in row {
                            collect_placeholders(expr, &mut raw_placeholders);
                        }
                    }
                }
            }
        }
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => {
            active_tables.push(table.to_string());

            for assignment in assignments {
                collect_placeholders(&assignment.value, &mut raw_placeholders);
            }

            if let Some(expr) = selection {
                collect_placeholders(expr, &mut raw_placeholders);
            }
        }
        Statement::Delete(delete) => {
            for table_ref in &delete.tables {
                active_tables.push(table_ref.to_string());
            }

            let from_tables = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            for table_with_joins in from_tables {
                collect_table_name(&table_with_joins.relation, &mut active_tables);
            }

            if let Some(expr) = &delete.selection {
                collect_placeholders(expr, &mut raw_placeholders);
            }
        }
        _ => {}
    };

    let active_table_refs: Vec<&str> = active_tables.iter().map(|s| s.as_str()).collect();
    let input_fields = build_input_fields(&raw_placeholders, &active_table_refs, schema);

    Ok(QueryParseResult {
        statement: statement.clone(),
        annotation,
        input_fields,
        output_fields,
    })
}

fn collect_table_name(table_factor: &TableFactor, out: &mut Vec<String>) {
    if let TableFactor::Table { name, .. } = table_factor {
        let table_name = name.to_string();
        if !out.contains(&table_name) {
            out.push(table_name);
        }
    }
}

fn extract_aliases_using_relation(table_factor: &TableFactor) -> HashMap<String, String> {
    let mut aliases: HashMap<String, String> = HashMap::new();

    match table_factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.to_string();
            if let Some(alias) = &alias {
                aliases.insert(alias.name.to_string(), table_name.clone());
            };
        }
        x => {
            eprintln!("Unsupported: cannot extract aliases from {:?}", x)
        }
    }

    aliases
}

fn extract_output_field_from_expr(
    expr: &Expr,
    aliases: &HashMap<String, String>,
) -> Option<QueryOutputField> {
    match expr {
        Expr::Identifier(ident) => Some(QueryOutputField {
            source: QueryOutputFieldSource {
                database: None,
                schema: None,
                table: None,
                field: ident.to_string(),
            },
            name: ident.to_string(),
        }),
        Expr::CompoundIdentifier(idents) => match &idents[..] {
            [alias_or_table, field] => {
                let mut table = alias_or_table.to_string();
                if let Some(aliased_table) = aliases.get(&table) {
                    table = aliased_table.clone();
                }
                Some(QueryOutputField {
                    source: QueryOutputFieldSource {
                        database: None,
                        schema: None,
                        table: Some(table.to_string()),
                        field: field.to_string(),
                    },
                    name: field.to_string(),
                })
            }
            [database_or_schema, table, field] => Some(QueryOutputField {
                source: QueryOutputFieldSource {
                    database: None,
                    schema: Some(database_or_schema.to_string()),
                    table: Some(table.to_string()),
                    field: field.to_string(),
                },
                name: field.to_string(),
            }),
            [database, schema, table, field] => Some(QueryOutputField {
                source: QueryOutputFieldSource {
                    database: Some(database.to_string()),
                    schema: Some(schema.to_string()),
                    table: Some(table.to_string()),
                    field: field.to_string(),
                },
                name: field.to_string(),
            }),
            _ => {
                eprintln!(
                    "unsupported compound ident ({}): {:?}",
                    idents.len(),
                    idents
                );
                None
            }
        },
        x => {
            eprintln!("SELECT expression not supported: {:#?}", x);
            None
        }
    }
}

fn extract_output_fields_from_select_item(
    select_item: &SelectItem,
    aliases: &HashMap<String, String>,
) -> Vec<QueryOutputField> {
    let mut output_fields: Vec<QueryOutputField> = Vec::new();

    match select_item {
        SelectItem::UnnamedExpr(expr) => {
            if let Some(output_field) = extract_output_field_from_expr(expr, aliases) {
                output_fields.push(output_field)
            }
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            if let Some(mut output_field) = extract_output_field_from_expr(expr, aliases) {
                output_field.name = alias.to_string();
                output_fields.push(output_field)
            }
        }
        SelectItem::QualifiedWildcard(..) => {
            eprintln!(
                "SELECT expression not supported: don't use wildcards. {:#?}",
                select_item
            );
        }
        SelectItem::Wildcard(..) => {
            eprintln!(
                "SELECT expression not supported: don't use wildcards. {:#?}",
                select_item
            );
        }
    }

    output_fields
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_sql_type_to_python_integer() {
        assert_eq!(sql_type_to_python("INTEGER"), "int");
        assert_eq!(sql_type_to_python("INT"), "int");
        assert_eq!(sql_type_to_python("BIGINT"), "int");
        assert_eq!(sql_type_to_python("SMALLINT"), "int");
        assert_eq!(sql_type_to_python("TINYINT"), "int");
    }

    #[test]
    fn test_sql_type_to_python_text() {
        assert_eq!(sql_type_to_python("TEXT"), "str");
        assert_eq!(sql_type_to_python("VARCHAR"), "str");
        assert_eq!(sql_type_to_python("CHARACTER VARYING(255)"), "str");
        assert_eq!(sql_type_to_python("CHAR(10)"), "str");
    }

    #[test]
    fn test_sql_type_to_python_float() {
        assert_eq!(sql_type_to_python("REAL"), "float");
        assert_eq!(sql_type_to_python("FLOAT"), "float");
        assert_eq!(sql_type_to_python("DOUBLE"), "float");
        assert_eq!(sql_type_to_python("DOUBLE PRECISION"), "float");
    }

    #[test]
    fn test_sql_type_to_python_bool() {
        assert_eq!(sql_type_to_python("BOOLEAN"), "bool");
        assert_eq!(sql_type_to_python("BOOL"), "bool");
    }

    #[test]
    fn test_sql_type_to_python_bytes() {
        assert_eq!(sql_type_to_python("BLOB"), "bytes");
        assert_eq!(sql_type_to_python("blob"), "bytes");
        assert_eq!(sql_type_to_python("BYTEA"), "bytes");
    }

    #[test]
    fn test_sql_type_to_python_decimal() {
        assert_eq!(sql_type_to_python("NUMERIC"), "Decimal");
        assert_eq!(sql_type_to_python("DECIMAL"), "Decimal");
        assert_eq!(sql_type_to_python("NUMERIC(10,2)"), "Decimal");
    }

    #[test]
    fn test_sql_type_to_python_datetime() {
        assert_eq!(sql_type_to_python("DATE"), "datetime.date");
        assert_eq!(sql_type_to_python("TIME"), "datetime.time");
        assert_eq!(sql_type_to_python("TIMESTAMP"), "datetime.datetime");
        assert_eq!(sql_type_to_python("DATETIME"), "datetime.datetime");
        assert_eq!(sql_type_to_python("TIMESTAMPTZ"), "datetime.datetime");
    }

    #[test]
    fn test_sql_type_to_python_json() {
        assert_eq!(sql_type_to_python("JSON"), "Any");
        assert_eq!(sql_type_to_python("JSONB"), "Any");
    }

    #[test]
    fn test_sql_type_to_python_uuid() {
        assert_eq!(sql_type_to_python("UUID"), "str");
    }

    #[test]
    fn test_sql_type_to_python_unknown_fallback() {
        assert_eq!(sql_type_to_python("FROBNICATOR"), "Any");
    }

    #[test]
    fn test_sql_type_to_python_sqlite_affinity_fallback() {
        assert_eq!(sql_type_to_python("MYINTEGER"), "int");
        assert_eq!(sql_type_to_python("LONGTEXT"), "str");
    }

    #[test]
    fn test_collect_placeholders_named() {
        use sqlparser::dialect::SQLiteDialect;
        use sqlparser::parser::Parser as SQLParser;

        let sql = "SELECT id FROM users WHERE id = :id AND email = :email";
        let ast = SQLParser::parse_sql(&SQLiteDialect {}, sql).unwrap();
        if let sqlparser::ast::Statement::Query(q) = &ast[0] {
            let select = q.body.as_select().unwrap();
            let mut placeholders = Vec::new();
            if let Some(expr) = &select.selection {
                collect_placeholders(expr, &mut placeholders);
            }
            assert_eq!(placeholders, vec![":id", ":email"]);
        } else {
            panic!("expected Query");
        }
    }

    #[test]
    fn test_build_input_fields_named_dedup() {
        let schema = SchemaParseResult {
            table_fields: HashMap::new(),
        };
        let raw = vec![":id".to_string(), ":email".to_string(), ":id".to_string()];
        let fields = build_input_fields(&raw, &[], &schema);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[1].name, "email");
    }

    #[test]
    fn test_build_input_fields_anon_positional() {
        let schema = SchemaParseResult {
            table_fields: HashMap::new(),
        };
        let raw = vec!["?".to_string(), "?".to_string(), "?".to_string()];
        let fields = build_input_fields(&raw, &[], &schema);
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, "p1");
        assert_eq!(fields[1].name, "p2");
        assert_eq!(fields[2].name, "p3");
    }

    #[test]
    fn test_build_input_fields_type_inference() {
        let mut users_cols = HashMap::new();
        users_cols.insert("id".to_string(), "INTEGER".to_string());
        users_cols.insert("email".to_string(), "TEXT".to_string());
        let mut table_fields = HashMap::new();
        table_fields.insert("users".to_string(), users_cols);

        let schema = SchemaParseResult { table_fields };
        let raw = vec![":id".to_string(), ":email".to_string()];
        let fields = build_input_fields(&raw, &["users"], &schema);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].data_type, "int");
        assert_eq!(fields[1].name, "email");
        assert_eq!(fields[1].data_type, "str");
    }
}
