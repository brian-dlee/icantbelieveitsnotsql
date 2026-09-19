//! Table definitions collected from `CREATE TABLE` / `CREATE VIEW` statements.

use sqlparser::ast::{ColumnOption, CreateTable, Expr, ObjectName, ObjectNamePart, TableConstraint};

use crate::types::{SqlType, TypeInfo};

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub type_info: TypeInfo,
    /// `(table, column)` the value originates from, when it is a plain column reference.
    pub source: Option<(String, String)>,
}

impl Column {
    pub fn new(name: impl Into<String>, type_info: TypeInfo) -> Column {
        Column {
            name: name.into(),
            type_info,
            source: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }
}

#[derive(Debug, Default)]
pub struct Schema {
    pub tables: Vec<Table>,
}

/// Last identifier of a possibly qualified name (`main.users` -> `users`).
pub fn object_name_last(name: &ObjectName) -> String {
    name.0
        .iter()
        .rev()
        .find_map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            ObjectNamePart::Function(f) => Some(f.name.value.clone()),
        })
        .unwrap_or_default()
}

impl Schema {
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
    }

    pub fn add_table(&mut self, table: Table) {
        self.tables.retain(|t| !t.name.eq_ignore_ascii_case(&table.name));
        self.tables.push(table);
    }

    pub fn add_create_table(&mut self, create_table: &CreateTable) {
        let table_name = object_name_last(&create_table.name);

        let mut primary_key_columns: Vec<String> = Vec::new();
        for constraint in &create_table.constraints {
            if let TableConstraint::PrimaryKey { columns, .. } = constraint {
                for index_column in columns {
                    if let Expr::Identifier(ident) = &index_column.column.expr {
                        primary_key_columns.push(ident.value.to_lowercase());
                    }
                }
            }
        }

        let mut columns = Vec::new();
        for column_def in &create_table.columns {
            let declared = column_def.data_type.to_string();
            let sql_type = SqlType::from_data_type(&column_def.data_type);

            let mut not_null = primary_key_columns.contains(&column_def.name.value.to_lowercase());
            for option in &column_def.options {
                match &option.option {
                    ColumnOption::NotNull => not_null = true,
                    ColumnOption::Unique { is_primary, .. } if *is_primary => not_null = true,
                    _ => {}
                }
            }

            columns.push(Column {
                name: column_def.name.value.clone(),
                type_info: TypeInfo {
                    sql_type,
                    nullable: !not_null,
                    declared: Some(declared),
                },
                source: Some((table_name.clone(), column_def.name.value.clone())),
            });
        }

        self.add_table(Table {
            name: table_name,
            columns,
        });
    }
}
