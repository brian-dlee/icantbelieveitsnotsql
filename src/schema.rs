use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::parser::{Parser as SQLParser, ParserError};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum FieldSource {
    TableSource {
        database: Option<String>,
        schema: Option<String>,
        table: String,
        column: String,
        data_type: String,
    },
}

#[derive(Debug)]
pub struct SchemaParseResult {
    pub table_fields: HashMap<String, HashMap<String, String>>,
}

impl SchemaParseResult {
    /// Search all tables in the schema for a column named `name`.
    pub fn resolve_fields_by_name(&self, name: &str) -> Vec<FieldSource> {
        self.resolve_fields_in_tables(name, &[])
    }

    /// Search only the specified `tables` for a column named `name`.
    /// If `tables` is empty the full schema is searched (same as
    /// `resolve_fields_by_name`).
    pub fn resolve_fields_in_tables(&self, name: &str, tables: &[&str]) -> Vec<FieldSource> {
        let mut result: Vec<FieldSource> = Vec::new();

        for (table_name, table_fields) in &self.table_fields {
            if !tables.is_empty() && !tables.contains(&table_name.as_str()) {
                continue;
            }
            for (field_name, field_data_type) in table_fields {
                if name == field_name {
                    result.push(FieldSource::TableSource {
                        database: None,
                        schema: None,
                        table: table_name.clone(),
                        column: field_name.clone(),
                        data_type: field_data_type.clone(),
                    })
                }
            }
        }

        result
    }
}

pub fn parse_schema_file(
    schema_file_contents: &str,
    parser_dialect: &dyn Dialect,
) -> Result<SchemaParseResult, ParserError> {
    match SQLParser::parse_sql(parser_dialect, schema_file_contents) {
        Err(err) => Err(err),
        Ok(ast) => {
            let mut tables: HashMap<String, HashMap<String, String>> = HashMap::new();

            for statement in ast {
                match statement {
                    Statement::CreateTable(create_table) => {
                        let table_name = create_table.name.to_string();
                        let mut columns: HashMap<String, String> = HashMap::new();

                        for column in create_table.columns.iter() {
                            columns.insert(column.name.value.clone(), column.data_type.to_string());
                        }

                        tables.insert(table_name, columns);
                    }
                    _ => {}
                }
            }

            Ok(SchemaParseResult {
                table_fields: tables,
            })
        }
    }
}
