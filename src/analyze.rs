//! Resolves the output columns and the parameters of one statement against
//! the schema, so the code generator knows what to name and type.

use std::collections::HashSet;

use sqlparser::ast::{
    AccessExpr, Assignment, AssignmentTarget, BinaryOperator, DateTimeField, Delete, Expr, FromTable, Function,
    FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Insert, JoinConstraint,
    JoinOperator, LimitClause, OnConflictAction, OnInsert, OrderByKind, Query,
    Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement, Subscript, TableAlias,
    TableFactor, TableObject, TableWithJoins, UnaryOperator, UpdateTableFromKind, Value,
};

use crate::dialect::SqlDialect;
use crate::queryfile::is_identifier;
use crate::schema::{object_name_last, Column, Schema, Table};
use crate::types::{SqlType, TypeInfo};

#[derive(Clone, Debug)]
pub struct Param {
    pub name: String,
    pub type_info: TypeInfo,
    /// `(table, column)` the parameter is compared with or assigned to, if any.
    pub source: Option<(String, String)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementKind {
    Select,
    Insert,
    Update,
    Delete,
    Ddl,
    Other,
}

#[derive(Clone, Debug)]
pub struct StatementAnalysis {
    pub kind: StatementKind,
    pub columns: Vec<Column>,
    pub params: Vec<Param>,
    /// True when the statement uses `?` placeholders (bound by position).
    pub positional: bool,
    pub warnings: Vec<String>,
}

struct RawParam {
    token: String,
    order: usize,
    span: (u64, u64),
    type_info: Option<TypeInfo>,
    nullable_hint: bool,
    hint: Option<String>,
    source: Option<(String, String)>,
}

struct ScopeTable {
    alias: Option<String>,
    table: Table,
    /// True when the table sits on the outer side of an outer join.
    nullable: bool,
}

#[derive(Default)]
struct Scope<'a> {
    parent: Option<&'a Scope<'a>>,
    tables: Vec<ScopeTable>,
    ctes: Vec<Table>,
    /// Projection aliases, visible to ORDER BY / HAVING.
    output_aliases: Vec<Column>,
}

enum Resolution {
    Found(Column),
    Ambiguous(Vec<String>),
    NotFound,
}

fn is_rowid(name: &str) -> bool {
    matches!(name.to_lowercase().as_str(), "rowid" | "_rowid_" | "oid")
}

fn with_join_nullability(column: &Column, nullable: bool) -> Column {
    let mut column = column.clone();
    column.type_info.nullable |= nullable;
    column
}

impl<'a> Scope<'a> {
    fn child(parent: &'a Scope<'a>) -> Scope<'a> {
        Scope {
            parent: Some(parent),
            ..Default::default()
        }
    }

    fn find_cte(&self, name: &str) -> Option<&Table> {
        self.ctes
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
            .or_else(|| self.parent.and_then(|p| p.find_cte(name)))
    }

    fn find_table(&self, qualifier: &str) -> Option<&ScopeTable> {
        self.tables.iter().find(|t| match &t.alias {
            Some(alias) => alias.eq_ignore_ascii_case(qualifier),
            None => t.table.name.eq_ignore_ascii_case(qualifier),
        })
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .tables
            .iter()
            .map(|t| match &t.alias {
                Some(alias) => format!("{} ({})", t.table.name, alias),
                None => t.table.name.clone(),
            })
            .collect();
        if let Some(parent) = self.parent {
            names.extend(parent.table_names());
        }
        names
    }

    fn resolve_column(&self, qualifier: Option<&str>, name: &str) -> Resolution {
        if let Some(qualifier) = qualifier {
            if let Some(scope_table) = self.find_table(qualifier) {
                return match scope_table.table.column(name) {
                    Some(column) => {
                        Resolution::Found(with_join_nullability(column, scope_table.nullable))
                    }
                    None if is_rowid(name) => Resolution::Found(Column::new(
                        name,
                        TypeInfo::not_null(SqlType::Int),
                    )),
                    None => Resolution::NotFound,
                };
            }
            return match self.parent {
                Some(parent) => parent.resolve_column(Some(qualifier), name),
                None => Resolution::NotFound,
            };
        }

        let mut matches: Vec<(String, Column)> = Vec::new();
        for scope_table in &self.tables {
            if let Some(column) = scope_table.table.column(name) {
                matches.push((
                    scope_table.table.name.clone(),
                    with_join_nullability(column, scope_table.nullable),
                ));
            }
        }

        match matches.len() {
            1 => return Resolution::Found(matches.pop().unwrap().1),
            0 => {}
            _ => return Resolution::Ambiguous(matches.into_iter().map(|m| m.0).collect()),
        }

        if let Some(column) = self
            .output_aliases
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
        {
            return Resolution::Found(column.clone());
        }

        if is_rowid(name) && !self.tables.is_empty() {
            return Resolution::Found(Column::new(name, TypeInfo::not_null(SqlType::Int)));
        }

        match self.parent {
            Some(parent) => parent.resolve_column(None, name),
            None => Resolution::NotFound,
        }
    }
}

fn apply_alias_columns(columns: &mut [Column], alias: &TableAlias) {
    for (column, alias_column) in columns.iter_mut().zip(alias.columns.iter()) {
        column.name = alias_column.name.value.clone();
    }
}

fn is_placeholder(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Value(v) if matches!(v.value, Value::Placeholder(_))
    )
}

fn numbered_dollar_placeholder(token: &str) -> Option<usize> {
    token
        .strip_prefix('$')
        .filter(|suffix| !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()))
        .and_then(|suffix| suffix.parse().ok())
}

/// Name to suggest for a `?` placeholder compared with this expression.
fn column_hint(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|i| i.value.clone()),
        Expr::Nested(inner) => column_hint(inner),
        Expr::Collate { expr, .. } => column_hint(expr),
        Expr::Cast { expr, .. } => column_hint(expr),
        Expr::Function(function) => Some(object_name_last(&function.name).to_lowercase()),
        _ => None,
    }
}

fn split_qualified(idents: &[sqlparser::ast::Ident]) -> (Option<&str>, &str) {
    let len = idents.len();
    let name = idents[len - 1].value.as_str();
    let qualifier = if len >= 2 {
        Some(idents[len - 2].value.as_str())
    } else {
        None
    };
    (qualifier, name)
}

fn join_constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    match operator {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c) => Some(c),
        _ => None,
    }
}

fn anonymous(sql_type: SqlType, nullable: bool) -> Column {
    Column::new("", TypeInfo::new(sql_type, nullable))
}

fn typed_hint(name: &str, sql_type: SqlType) -> Column {
    Column::new(name, TypeInfo::not_null(sql_type))
}

fn array_element_declared_type(declared: &str) -> Option<String> {
    let mut base = declared.trim();
    let mut dimensions = 0;

    loop {
        let trimmed = base.trim_end();
        if !trimmed.ends_with(']') {
            break;
        }
        let Some(open) = trimmed.rfind('[') else {
            break;
        };
        let bound = &trimmed[open + 1..trimmed.len() - 1];
        if !bound.is_empty() && !bound.chars().all(|ch| ch.is_ascii_digit()) {
            break;
        }
        dimensions += 1;
        base = &trimmed[..open];
    }

    (dimensions > 0).then(|| base.trim().to_string())
}

pub struct Analyzer<'s> {
    schema: &'s Schema,
    dialect: SqlDialect,
    params: Vec<RawParam>,
    warnings: Vec<String>,
}

pub fn analyze_statement(
    statement: &Statement,
    schema: &Schema,
    dialect: SqlDialect,
) -> Result<StatementAnalysis, String> {
    let mut analyzer = Analyzer {
        schema,
        dialect,
        params: Vec::new(),
        warnings: Vec::new(),
    };
    let root = Scope::default();

    let (kind, columns) = match statement {
        Statement::Query(query) => (StatementKind::Select, analyzer.analyze_query(query, None)?),
        Statement::Insert(insert) => (StatementKind::Insert, analyzer.analyze_insert(insert, &root)?),
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            limit,
            ..
        } => (
            StatementKind::Update,
            analyzer.analyze_update(
                table,
                assignments,
                from.as_ref(),
                selection.as_ref(),
                returning.as_deref(),
                limit.as_ref(),
                &root,
            )?,
        ),
        Statement::Delete(delete) => (StatementKind::Delete, analyzer.analyze_delete(delete, &root)?),
        Statement::CreateTable(_)
        | Statement::CreateIndex(_)
        | Statement::CreateView { .. }
        | Statement::AlterTable { .. }
        | Statement::Drop { .. }
        | Statement::CreateTrigger { .. }
        | Statement::DropTrigger { .. } => (StatementKind::Ddl, Vec::new()),
        _ => (StatementKind::Other, Vec::new()),
    };

    let (params, positional) = analyzer.finalize_params()?;

    Ok(StatementAnalysis {
        kind,
        columns,
        params,
        positional,
        warnings: analyzer.warnings,
    })
}

/// Output columns of a `CREATE VIEW` body, for registering views as tables.
pub fn analyze_view_columns(query: &Query, schema: &Schema, dialect: SqlDialect) -> Result<Vec<Column>, String> {
    let mut analyzer = Analyzer {
        schema,
        dialect,
        params: Vec::new(),
        warnings: Vec::new(),
    };
    analyzer.analyze_query(query, None)
}

impl<'s> Analyzer<'s> {
    fn warn(&mut self, message: String) {
        if !self.warnings.contains(&message) {
            self.warnings.push(message);
        }
    }

    // ------------------------------------------------------------------ params

    fn record_param(
        &mut self,
        token: &str,
        span: (u64, u64),
        expected: Option<&Column>,
        hint: Option<&str>,
    ) {
        let hint = hint
            .map(|h| h.to_string())
            .or_else(|| expected.filter(|c| !c.name.is_empty()).map(|c| c.name.clone()));

        self.params.push(RawParam {
            token: token.to_string(),
            order: self.params.len(),
            span,
            type_info: expected.map(|c| c.type_info.clone()),
            nullable_hint: false,
            hint,
            source: expected.and_then(|c| c.source.clone()),
        });
    }

    fn mark_nullable(&mut self, expr: &Expr) {
        // `$1::text IS NULL` and `(:x) IS NULL` test the placeholder itself.
        let mut expr = expr;
        while let Expr::Cast { expr: inner, .. } | Expr::Nested(inner) = expr {
            expr = inner;
        }
        if let Expr::Value(v) = expr {
            if let Value::Placeholder(token) = &v.value {
                let span = (v.span.start.line, v.span.start.column);
                if let Some(raw) = self
                    .params
                    .iter_mut()
                    .rev()
                    .find(|p| p.token == *token && p.span == span)
                {
                    raw.nullable_hint = true;
                }
            }
        }
    }

    fn finalize_params(&mut self) -> Result<(Vec<Param>, bool), String> {
        let mut raws = std::mem::take(&mut self.params);
        if raws.is_empty() {
            return Ok((Vec::new(), false));
        }

        let spans_known = raws.iter().all(|p| p.span.0 > 0);
        if spans_known {
            raws.sort_by_key(|p| (p.span, p.order));
        } else {
            raws.sort_by_key(|p| p.order);
        }

        if raws
            .iter()
            .any(|p| p.token.starts_with('?') && p.token.len() > 1)
        {
            return Err(String::from(
                "numbered placeholders (?NNN) are not supported; use `?` or a named placeholder like `:name`",
            ));
        }

        let has_question = raws.iter().any(|p| p.token == "?");
        let has_numbered_dollar = raws
            .iter()
            .any(|p| numbered_dollar_placeholder(&p.token).is_some());
        let positional = has_question || has_numbered_dollar;
        let named = raws
            .iter()
            .any(|p| p.token != "?" && numbered_dollar_placeholder(&p.token).is_none());
        if positional && named {
            return Err(String::from(
                "cannot mix positional (`?` or `$N`) and named placeholders in one statement",
            ));
        }
        if has_question && has_numbered_dollar {
            return Err(String::from(
                "cannot mix `?` and numbered `$N` placeholders in one statement",
            ));
        }

        let mut params: Vec<Param> = Vec::new();

        if has_numbered_dollar {
            // PostgreSQL reuses `$N` references and binds arguments by their
            // explicit number. Coalesce repeated references before creating
            // function parameters, then require a dense 1-based sequence so
            // the generated positional tuple has the same shape as the SQL.
            let mut unique: Vec<RawParam> = Vec::new();
            for raw in raws {
                if let Some(existing) = unique.iter_mut().find(|p| p.token == raw.token) {
                    if existing.type_info.is_none() {
                        existing.type_info = raw.type_info;
                        existing.source = raw.source;
                    }
                    existing.nullable_hint |= raw.nullable_hint;
                    if existing.hint.is_none() {
                        existing.hint = raw.hint;
                    }
                } else {
                    unique.push(raw);
                }
            }
            unique.sort_by_key(|p| numbered_dollar_placeholder(&p.token).unwrap());
            for (index, raw) in unique.iter().enumerate() {
                let number = numbered_dollar_placeholder(&raw.token).unwrap();
                if number != index + 1 {
                    return Err(format!(
                        "numbered placeholders must be contiguous from `$1`; expected `${}`, found `{}`",
                        index + 1,
                        raw.token
                    ));
                }
            }

            let mut used: HashSet<String> = HashSet::new();
            for (index, raw) in unique.iter().enumerate() {
                let base = raw
                    .hint
                    .as_deref()
                    .filter(|h| is_identifier(h))
                    .map(|h| h.to_lowercase())
                    .unwrap_or_else(|| format!("param_{}", index + 1));
                let mut name = base.clone();
                let mut counter = 2;
                while used.contains(&name) {
                    name = format!("{}_{}", base, counter);
                    counter += 1;
                }
                used.insert(name.clone());
                params.push(Param {
                    name,
                    type_info: raw_type(raw),
                    source: raw.source.clone(),
                });
            }
        } else if positional {
            let mut used: HashSet<String> = HashSet::new();
            for (index, raw) in raws.iter().enumerate() {
                let base = raw
                    .hint
                    .as_deref()
                    .filter(|h| is_identifier(h))
                    .map(|h| h.to_lowercase())
                    .unwrap_or_else(|| format!("param_{}", index + 1));

                let mut name = base.clone();
                let mut counter = 2;
                while used.contains(&name) {
                    name = format!("{}_{}", base, counter);
                    counter += 1;
                }
                used.insert(name.clone());

                params.push(Param {
                    name,
                    type_info: raw_type(raw),
                    source: raw.source.clone(),
                });
            }
        } else {
            for raw in raws {
                let name = raw.token[1..].to_string();
                if !is_identifier(&name) {
                    return Err(format!(
                        "placeholder `{}` is not a valid parameter name",
                        raw.token
                    ));
                }

                if let Some(existing) = params.iter_mut().find(|p| p.name == name) {
                    if existing.type_info.sql_type == SqlType::Any {
                        if let Some(type_info) = &raw.type_info {
                            let nullable = existing.type_info.nullable || type_info.nullable;
                            existing.type_info = type_info.clone();
                            existing.type_info.nullable = nullable;
                            existing.source = raw.source.clone();
                        }
                    }
                    if raw.nullable_hint {
                        existing.type_info.nullable = true;
                    }
                } else {
                    params.push(Param {
                        name,
                        type_info: raw_type(&raw),
                        source: raw.source.clone(),
                    });
                }
            }
        }

        Ok((params, positional))
    }

    // --------------------------------------------------------------- queries

    fn analyze_query(&mut self, query: &Query, parent: Option<&Scope>) -> Result<Vec<Column>, String> {
        let mut cte_scope = Scope {
            parent,
            ..Default::default()
        };

        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let mut columns = self.analyze_query(&cte.query, Some(&cte_scope))?;
                apply_alias_columns(&mut columns, &cte.alias);
                cte_scope.ctes.push(Table {
                    name: cte.alias.name.value.clone(),
                    columns,
                });
            }
        }

        self.analyze_set_expr(&query.body, &cte_scope, Some(query))
    }

    fn analyze_set_expr(
        &mut self,
        body: &SetExpr,
        scope: &Scope,
        tail: Option<&Query>,
    ) -> Result<Vec<Column>, String> {
        match body {
            SetExpr::Select(select) => self.analyze_select(select, scope, tail),
            SetExpr::Query(query) => {
                let columns = self.analyze_query(query, Some(scope))?;
                self.walk_tail_with_aliases(tail, scope, &columns)?;
                Ok(columns)
            }
            SetExpr::SetOperation { left, right, .. } => {
                let left_columns = self.analyze_set_expr(left, scope, None)?;
                let right_columns = self.analyze_set_expr(right, scope, None)?;

                if left_columns.len() != right_columns.len() {
                    return Err(format!(
                        "set operation arms select {} and {} columns",
                        left_columns.len(),
                        right_columns.len()
                    ));
                }

                let columns: Vec<Column> = left_columns
                    .iter()
                    .zip(right_columns.iter())
                    .map(|(l, r)| Column {
                        name: l.name.clone(),
                        type_info: TypeInfo::unify(&l.type_info, &r.type_info),
                        source: if l.source == r.source { l.source.clone() } else { None },
                    })
                    .collect();

                self.walk_tail_with_aliases(tail, scope, &columns)?;
                Ok(columns)
            }
            SetExpr::Values(values) => {
                let mut width = 0;
                for row in &values.rows {
                    width = width.max(row.len());
                    for expr in row {
                        self.walk_expr(expr, None, None, scope)?;
                    }
                }
                Ok((0..width)
                    .map(|i| Column::new(format!("column_{}", i + 1), TypeInfo::any()))
                    .collect())
            }
            other => Err(format!("unsupported query body: {}", other)),
        }
    }

    fn walk_tail_with_aliases(
        &mut self,
        tail: Option<&Query>,
        scope: &Scope,
        columns: &[Column],
    ) -> Result<(), String> {
        let mut alias_scope = Scope::child(scope);
        alias_scope.output_aliases = columns.to_vec();
        self.walk_tail(tail, &alias_scope)
    }

    /// ORDER BY / LIMIT / OFFSET of a query, evaluated in the given scope.
    fn walk_tail(&mut self, tail: Option<&Query>, scope: &Scope) -> Result<(), String> {
        let Some(query) = tail else {
            return Ok(());
        };

        if let Some(order_by) = &query.order_by {
            if let OrderByKind::Expressions(exprs) = &order_by.kind {
                for order_expr in exprs {
                    self.walk_expr(&order_expr.expr, None, None, scope)?;
                }
            }
        }

        if let Some(limit_clause) = &query.limit_clause {
            match limit_clause {
                LimitClause::LimitOffset { limit, offset, .. } => {
                    if let Some(limit) = limit {
                        let expected = typed_hint("limit", SqlType::Int);
                        self.walk_expr(limit, Some(&expected), Some("limit"), scope)?;
                    }
                    if let Some(offset) = offset {
                        let expected = typed_hint("offset", SqlType::Int);
                        self.walk_expr(&offset.value, Some(&expected), Some("offset"), scope)?;
                    }
                }
                LimitClause::OffsetCommaLimit { offset, limit } => {
                    let expected = typed_hint("offset", SqlType::Int);
                    self.walk_expr(offset, Some(&expected), Some("offset"), scope)?;
                    let expected = typed_hint("limit", SqlType::Int);
                    self.walk_expr(limit, Some(&expected), Some("limit"), scope)?;
                }
            }
        }

        Ok(())
    }

    fn analyze_select(
        &mut self,
        select: &Select,
        outer: &Scope,
        tail: Option<&Query>,
    ) -> Result<Vec<Column>, String> {
        let mut scope = Scope::child(outer);

        for table_with_joins in &select.from {
            self.add_table_with_joins(&mut scope, table_with_joins)?;
        }

        let columns = self.analyze_projection(&select.projection, &scope)?;
        scope.output_aliases = columns.clone();

        if let Some(selection) = &select.selection {
            self.walk_expr(selection, None, None, &scope)?;
        }

        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            for expr in exprs {
                self.walk_expr(expr, None, None, &scope)?;
            }
        }

        if let Some(having) = &select.having {
            self.walk_expr(having, None, None, &scope)?;
        }

        self.walk_tail(tail, &scope)?;

        Ok(columns)
    }

    fn analyze_projection(&mut self, items: &[SelectItem], scope: &Scope) -> Result<Vec<Column>, String> {
        let mut columns: Vec<Column> = Vec::new();

        for (index, item) in items.iter().enumerate() {
            match item {
                SelectItem::Wildcard(_) => {
                    if scope.tables.is_empty() {
                        return Err(String::from("`SELECT *` needs a FROM clause"));
                    }
                    for scope_table in &scope.tables {
                        for column in &scope_table.table.columns {
                            columns.push(with_join_nullability(column, scope_table.nullable));
                        }
                    }
                }
                SelectItem::QualifiedWildcard(kind, _) => match kind {
                    SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        let qualifier = object_name_last(name);
                        let scope_table = scope.find_table(&qualifier).ok_or_else(|| {
                            format!(
                                "unknown table or alias `{}` in `{}.*`; tables in scope: {}",
                                qualifier,
                                qualifier,
                                scope.table_names().join(", ")
                            )
                        })?;
                        for column in &scope_table.table.columns {
                            columns.push(with_join_nullability(column, scope_table.nullable));
                        }
                    }
                    SelectItemQualifiedWildcardKind::Expr(expr) => {
                        return Err(format!("unsupported wildcard expression `{}.*`", expr));
                    }
                },
                SelectItem::UnnamedExpr(expr) => {
                    let mut column = self.walk_projection_expr(expr, scope)?;
                    column.name = self.derive_column_name(expr, index);
                    columns.push(column);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let mut column = self.walk_projection_expr(expr, scope)?;
                    column.name = alias.value.clone();
                    columns.push(column);
                }
            }
        }

        Ok(columns)
    }

    fn derive_column_name(&mut self, expr: &Expr, index: usize) -> String {
        match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(idents) => idents.last().map(|i| i.value.clone()).unwrap_or_default(),
            Expr::Function(function) => object_name_last(&function.name).to_lowercase(),
            _ => {
                let name = format!("column_{}", index + 1);
                self.warn(format!(
                    "unaliased expression `{}` in SELECT is exposed as `{}`; add `AS <name>`",
                    expr, name
                ));
                name
            }
        }
    }

    /// Like `walk_expr`, but a bare column reference must resolve.
    fn walk_projection_expr(&mut self, expr: &Expr, scope: &Scope) -> Result<Column, String> {
        match expr {
            Expr::Identifier(ident) => self.column_strict(scope, None, &ident.value),
            Expr::CompoundIdentifier(idents) => {
                let (qualifier, name) = split_qualified(idents);
                self.column_strict(scope, qualifier, name)
            }
            _ => self.walk_expr(expr, None, None, scope),
        }
    }

    fn column_strict(&mut self, scope: &Scope, qualifier: Option<&str>, name: &str) -> Result<Column, String> {
        match scope.resolve_column(qualifier, name) {
            Resolution::Found(column) => Ok(column),
            Resolution::Ambiguous(tables) => Err(format!(
                "column `{}` is ambiguous; it exists in tables {}. Qualify it with a table name or alias",
                name,
                tables.join(", ")
            )),
            Resolution::NotFound => {
                let reference = match qualifier {
                    Some(q) => format!("{}.{}", q, name),
                    None => name.to_string(),
                };
                Err(format!(
                    "unknown column `{}`; tables in scope: {}",
                    reference,
                    if scope.table_names().is_empty() {
                        String::from("(none)")
                    } else {
                        scope.table_names().join(", ")
                    }
                ))
            }
        }
    }

    fn column_lenient(&mut self, scope: &Scope, qualifier: Option<&str>, name: &str, quoted: bool) -> Column {
        match scope.resolve_column(qualifier, name) {
            Resolution::Found(column) => column,
            Resolution::Ambiguous(tables) => {
                self.warn(format!(
                    "column `{}` is ambiguous (tables {}); typed as Any",
                    name,
                    tables.join(", ")
                ));
                anonymous(SqlType::Any, true)
            }
            Resolution::NotFound => {
                if quoted {
                    self.warn(format!(
                        "`\"{}\"` is not a known column; SQLite treats it as the string literal '{}'. Prefer single quotes",
                        name, name
                    ));
                    anonymous(SqlType::Text, false)
                } else {
                    let reference = match qualifier {
                        Some(q) => format!("{}.{}", q, name),
                        None => name.to_string(),
                    };
                    self.warn(format!("unknown column `{}`; typed as Any", reference));
                    anonymous(SqlType::Any, true)
                }
            }
        }
    }

    // ------------------------------------------------------------ FROM clause

    fn add_table_with_joins(&mut self, scope: &mut Scope, table_with_joins: &TableWithJoins) -> Result<(), String> {
        let start = scope.tables.len();
        self.add_table_factor(scope, &table_with_joins.relation, false)?;

        for join in &table_with_joins.joins {
            let (new_nullable, existing_nullable) = match &join.join_operator {
                JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => (true, false),
                JoinOperator::Right(_) | JoinOperator::RightOuter(_) => (false, true),
                JoinOperator::FullOuter(_) => (true, true),
                _ => (false, false),
            };

            if existing_nullable {
                for scope_table in &mut scope.tables[start..] {
                    scope_table.nullable = true;
                }
            }

            self.add_table_factor(scope, &join.relation, new_nullable)?;

            if let Some(JoinConstraint::On(expr)) = join_constraint(&join.join_operator) {
                self.walk_expr(expr, None, None, scope)?;
            }
        }

        Ok(())
    }

    fn add_table_factor(&mut self, scope: &mut Scope, factor: &TableFactor, nullable: bool) -> Result<(), String> {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = object_name_last(name);
                let table = scope
                    .find_cte(&table_name)
                    .cloned()
                    .or_else(|| self.schema.table(&table_name).cloned())
                    .ok_or_else(|| {
                        format!(
                            "unknown table `{}`; declare it with a CREATE TABLE statement in a schema file or query file",
                            table_name
                        )
                    })?;

                let mut table = table;
                if let Some(alias) = alias {
                    apply_alias_columns(&mut table.columns, alias);
                }

                scope.tables.push(ScopeTable {
                    alias: alias.as_ref().map(|a| a.name.value.clone()),
                    table,
                    nullable,
                });
            }
            TableFactor::Derived { subquery, alias, .. } => {
                let mut columns = self.analyze_query(subquery, Some(&*scope))?;
                let name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_default();
                if let Some(alias) = alias {
                    apply_alias_columns(&mut columns, alias);
                }
                scope.tables.push(ScopeTable {
                    alias: alias.as_ref().map(|a| a.name.value.clone()),
                    table: Table { name, columns },
                    nullable,
                });
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                self.add_table_with_joins(scope, table_with_joins)?;
                if alias.is_some() {
                    self.warn(String::from("aliases on parenthesized joins are ignored"));
                }
            }
            other => return Err(format!("unsupported FROM item: {}", other)),
        }

        Ok(())
    }

    // ---------------------------------------------------- INSERT/UPDATE/DELETE

    fn analyze_insert(&mut self, insert: &Insert, root: &Scope) -> Result<Vec<Column>, String> {
        let table_name = match &insert.table {
            TableObject::TableName(name) => object_name_last(name),
            other => return Err(format!("unsupported INSERT target: {}", other)),
        };

        let table = self
            .schema
            .table(&table_name)
            .cloned()
            .ok_or_else(|| format!("unknown table `{}`", table_name))?;

        let mut scope = Scope::child(root);
        scope.tables.push(ScopeTable {
            alias: insert.table_alias.as_ref().map(|a| a.value.clone()),
            table: table.clone(),
            nullable: false,
        });

        let target_columns: Vec<Column> = if insert.columns.is_empty() {
            table.columns.clone()
        } else {
            insert
                .columns
                .iter()
                .map(|ident| {
                    table.column(&ident.value).cloned().ok_or_else(|| {
                        format!("unknown column `{}` in table `{}`", ident.value, table_name)
                    })
                })
                .collect::<Result<Vec<Column>, String>>()?
        };

        if let Some(source) = &insert.source {
            match &*source.body {
                SetExpr::Values(values) => {
                    for row in &values.rows {
                        if row.len() != target_columns.len() {
                            return Err(format!(
                                "INSERT supplies {} values for {} columns",
                                row.len(),
                                target_columns.len()
                            ));
                        }
                        for (expr, column) in row.iter().zip(target_columns.iter()) {
                            self.walk_expr(expr, Some(column), Some(&column.name), &scope)?;
                        }
                    }
                }
                _ => {
                    let columns = self.analyze_query(source, Some(root))?;
                    if columns.len() != target_columns.len() {
                        return Err(format!(
                            "INSERT ... SELECT provides {} columns for {} target columns",
                            columns.len(),
                            target_columns.len()
                        ));
                    }
                }
            }
        }

        if let Some(on) = &insert.on {
            match on {
                OnInsert::OnConflict(on_conflict) => {
                    let mut conflict_scope = Scope::child(&scope);
                    conflict_scope.tables.push(ScopeTable {
                        alias: None,
                        table: Table {
                            name: String::from("excluded"),
                            columns: table.columns.clone(),
                        },
                        nullable: false,
                    });

                    if let OnConflictAction::DoUpdate(do_update) = &on_conflict.action {
                        for assignment in &do_update.assignments {
                            self.walk_assignment(assignment, &conflict_scope)?;
                        }
                        if let Some(selection) = &do_update.selection {
                            self.walk_expr(selection, None, None, &conflict_scope)?;
                        }
                    }
                }
                OnInsert::DuplicateKeyUpdate(assignments) => {
                    for assignment in assignments {
                        self.walk_assignment(assignment, &scope)?;
                    }
                }
                _ => {}
            }
        }

        match &insert.returning {
            Some(returning) => self.analyze_projection(returning, &scope),
            None => Ok(Vec::new()),
        }
    }

    fn walk_assignment(&mut self, assignment: &Assignment, scope: &Scope) -> Result<(), String> {
        match &assignment.target {
            AssignmentTarget::ColumnName(name) => {
                let idents: Vec<sqlparser::ast::Ident> = name
                    .0
                    .iter()
                    .filter_map(|part| part.as_ident().cloned())
                    .collect();
                if idents.is_empty() {
                    return Err(format!("unsupported assignment target `{}`", name));
                }
                let (qualifier, column_name) = split_qualified(&idents);
                let column = self.column_strict(scope, qualifier, column_name)?;
                self.walk_expr(&assignment.value, Some(&column), Some(&column.name), scope)?;
            }
            AssignmentTarget::Tuple(_) => {
                self.warn(String::from("tuple assignments are not typed"));
                self.walk_expr(&assignment.value, None, None, scope)?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn analyze_update(
        &mut self,
        table: &TableWithJoins,
        assignments: &[Assignment],
        from: Option<&UpdateTableFromKind>,
        selection: Option<&Expr>,
        returning: Option<&[SelectItem]>,
        limit: Option<&Expr>,
        root: &Scope,
    ) -> Result<Vec<Column>, String> {
        let mut scope = Scope::child(root);
        self.add_table_with_joins(&mut scope, table)?;

        if let Some(from) = from {
            let tables = match from {
                UpdateTableFromKind::BeforeSet(tables) | UpdateTableFromKind::AfterSet(tables) => tables,
            };
            for table_with_joins in tables {
                self.add_table_with_joins(&mut scope, table_with_joins)?;
            }
        }

        for assignment in assignments {
            self.walk_assignment(assignment, &scope)?;
        }

        if let Some(selection) = selection {
            self.walk_expr(selection, None, None, &scope)?;
        }

        let columns = match returning {
            Some(returning) => self.analyze_projection(returning, &scope)?,
            None => Vec::new(),
        };

        if let Some(limit) = limit {
            let expected = typed_hint("limit", SqlType::Int);
            self.walk_expr(limit, Some(&expected), Some("limit"), &scope)?;
        }

        Ok(columns)
    }

    fn analyze_delete(&mut self, delete: &Delete, root: &Scope) -> Result<Vec<Column>, String> {
        let mut scope = Scope::child(root);

        let from = match &delete.from {
            FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
        };
        for table_with_joins in from {
            self.add_table_with_joins(&mut scope, table_with_joins)?;
        }
        if let Some(using) = &delete.using {
            for table_with_joins in using {
                self.add_table_with_joins(&mut scope, table_with_joins)?;
            }
        }

        if let Some(selection) = &delete.selection {
            self.walk_expr(selection, None, None, &scope)?;
        }

        let columns = match &delete.returning {
            Some(returning) => self.analyze_projection(returning, &scope)?,
            None => Vec::new(),
        };

        if let Some(limit) = &delete.limit {
            let expected = typed_hint("limit", SqlType::Int);
            self.walk_expr(limit, Some(&expected), Some("limit"), &scope)?;
        }

        Ok(columns)
    }

    // ------------------------------------------------------------ expressions

    /// Walks an expression, recording placeholders it meets, and returns the
    /// expression's type. `expected` is the type a placeholder here should
    /// take (from the column it is compared with or assigned to).
    fn walk_expr(
        &mut self,
        expr: &Expr,
        expected: Option<&Column>,
        hint: Option<&str>,
        scope: &Scope,
    ) -> Result<Column, String> {
        match expr {
            Expr::Value(value) => match &value.value {
                Value::Placeholder(token) => {
                    let span = (value.span.start.line, value.span.start.column);
                    self.record_param(token, span, expected, hint);
                    Ok(expected
                        .cloned()
                        .unwrap_or_else(|| anonymous(SqlType::Any, true)))
                }
                Value::Number(text, _) => {
                    let is_float = text.contains('.') || text.contains('e') || text.contains('E');
                    Ok(anonymous(if is_float { SqlType::Float } else { SqlType::Int }, false))
                }
                Value::SingleQuotedString(_)
                | Value::DoubleQuotedString(_)
                | Value::EscapedStringLiteral(_)
                | Value::NationalStringLiteral(_)
                | Value::UnicodeStringLiteral(_)
                | Value::DollarQuotedString(_) => Ok(anonymous(SqlType::Text, false)),
                Value::HexStringLiteral(_) => Ok(anonymous(SqlType::Blob, false)),
                Value::Boolean(_) => Ok(anonymous(SqlType::Bool, false)),
                Value::Null => Ok(anonymous(SqlType::Any, true)),
                _ => Ok(anonymous(SqlType::Any, true)),
            },
            Expr::Identifier(ident) => {
                Ok(self.column_lenient(scope, None, &ident.value, ident.quote_style == Some('"')))
            }
            Expr::CompoundIdentifier(idents) => {
                let (qualifier, name) = split_qualified(idents);
                Ok(self.column_lenient(scope, qualifier, name, false))
            }
            Expr::Nested(inner) => self.walk_expr(inner, expected, hint, scope),
            Expr::Collate { expr, .. } => self.walk_expr(expr, expected, hint, scope),
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    self.walk_expr(expr, None, None, scope)?;
                    Ok(anonymous(SqlType::Bool, false))
                }
                UnaryOperator::Minus | UnaryOperator::Plus => {
                    let inner = self.walk_expr(expr, expected, hint, scope)?;
                    Ok(anonymous(
                        if inner.type_info.sql_type.is_numeric()
                            || inner.type_info.sql_type == SqlType::Interval
                        {
                            inner.type_info.sql_type
                        } else {
                            SqlType::Numeric
                        },
                        inner.type_info.nullable,
                    ))
                }
                _ => {
                    self.walk_expr(expr, None, None, scope)?;
                    Ok(anonymous(SqlType::Any, true))
                }
            },
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                self.walk_expr(inner, None, None, scope)?;
                self.mark_nullable(inner);
                Ok(anonymous(SqlType::Bool, false))
            }
            Expr::IsTrue(inner)
            | Expr::IsNotTrue(inner)
            | Expr::IsFalse(inner)
            | Expr::IsNotFalse(inner)
            | Expr::IsUnknown(inner)
            | Expr::IsNotUnknown(inner) => {
                self.walk_expr(inner, None, None, scope)?;
                Ok(anonymous(SqlType::Bool, false))
            }
            Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
                self.walk_pair(left, right, scope, true)?;
                Ok(anonymous(SqlType::Bool, false))
            }
            Expr::BinaryOp { left, op, right } => self.walk_binary(left, op, right, scope),
            Expr::InList { expr, list, .. } => {
                let subject = self.walk_expr(expr, None, None, scope)?;
                let expected = comparison_target(&subject);
                let hint = column_hint(expr);
                for item in list {
                    self.walk_expr(item, Some(&expected), hint.as_deref(), scope)?;
                }
                Ok(anonymous(SqlType::Bool, false))
            }
            Expr::InSubquery { expr, subquery, .. } => {
                let columns = self.analyze_query(subquery, Some(scope))?;
                if columns.len() != 1 {
                    return Err(format!(
                        "IN (subquery) must select exactly one column, got {}",
                        columns.len()
                    ));
                }
                let expected = comparison_target(&columns[0]);
                self.walk_expr(expr, Some(&expected), Some(&columns[0].name), scope)?;
                Ok(anonymous(SqlType::Bool, false))
            }
            Expr::Between { expr, low, high, .. } => {
                let subject = self.walk_expr(expr, None, None, scope)?;
                let expected = comparison_target(&subject);
                let hint = column_hint(expr);
                self.walk_expr(low, Some(&expected), hint.as_deref(), scope)?;
                self.walk_expr(high, Some(&expected), hint.as_deref(), scope)?;
                Ok(anonymous(SqlType::Bool, false))
            }
            Expr::Like { expr, pattern, .. }
            | Expr::ILike { expr, pattern, .. }
            | Expr::SimilarTo { expr, pattern, .. }
            | Expr::RLike { expr, pattern, .. } => {
                self.walk_expr(expr, None, None, scope)?;
                let expected = typed_hint("", SqlType::Text);
                let hint = column_hint(expr);
                self.walk_expr(pattern, Some(&expected), hint.as_deref(), scope)?;
                Ok(anonymous(SqlType::Bool, false))
            }
            Expr::Cast { expr, data_type, .. } => {
                let target = Column {
                    name: String::new(),
                    type_info: TypeInfo {
                        sql_type: SqlType::from_data_type(data_type),
                        nullable: false,
                        declared: Some(data_type.to_string()),
                    },
                    source: None,
                };
                let inner = self.walk_expr(expr, Some(&target), hint, scope)?;
                Ok(Column {
                    name: String::new(),
                    type_info: TypeInfo {
                        sql_type: target.type_info.sql_type,
                        nullable: inner.type_info.nullable,
                        declared: target.type_info.declared,
                    },
                    source: None,
                })
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let operand_type = match operand {
                    Some(operand) => Some(self.walk_expr(operand, None, None, scope)?),
                    None => None,
                };
                let operand_expected = operand_type.as_ref().map(comparison_target);

                let mut result: Option<Column> = None;
                for case_when in conditions {
                    self.walk_expr(&case_when.condition, operand_expected.as_ref(), None, scope)?;
                    let branch = self.walk_expr(&case_when.result, expected, hint, scope)?;
                    result = Some(match result {
                        Some(previous) => unify_columns(&previous, &branch),
                        None => branch,
                    });
                }

                let mut result = result.unwrap_or_else(|| anonymous(SqlType::Any, true));
                match else_result {
                    Some(else_result) => {
                        let branch = self.walk_expr(else_result, expected, hint, scope)?;
                        result = unify_columns(&result, &branch);
                    }
                    None => result.type_info.nullable = true,
                }
                result.name = String::new();
                Ok(result)
            }
            // `x = ANY(array_or_subquery)` / `x <> ALL(...)`. An array
            // placeholder's type can't be inferred; declare it with
            // `-- param: <name> list[...]`.
            Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
                let l = self.walk_expr(left, None, None, scope)?;
                let r = self.walk_expr(right, None, None, scope)?;
                Ok(anonymous(SqlType::Bool, l.type_info.nullable || r.type_info.nullable))
            }
            Expr::Exists { subquery, .. } => {
                self.analyze_query(subquery, Some(scope))?;
                Ok(anonymous(SqlType::Bool, false))
            }
            Expr::Subquery(subquery) => {
                let columns = self.analyze_query(subquery, Some(scope))?;
                let mut column = columns
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| anonymous(SqlType::Any, true));
                column.name = String::new();
                column.type_info.nullable = true;
                Ok(column)
            }
            Expr::CompoundFieldAccess { root, access_chain } => {
                let column = self.walk_expr(root, None, None, scope)?;
                let is_array_index = access_chain
                    .iter()
                    .all(|access| matches!(access, AccessExpr::Subscript(Subscript::Index { .. })));

                if is_array_index {
                    if let Some(element_type) = column
                        .type_info
                        .declared
                        .as_deref()
                        .and_then(array_element_declared_type)
                    {
                        for access in access_chain {
                            if let AccessExpr::Subscript(Subscript::Index { index }) = access {
                                let expected = typed_hint("", SqlType::Int);
                                self.walk_expr(index, Some(&expected), None, scope)?;
                            }
                        }

                        return Ok(Column::new(
                            "",
                            TypeInfo {
                                sql_type: column.type_info.sql_type,
                                nullable: true,
                                declared: Some(element_type),
                            },
                        ));
                    }
                }

                for access in access_chain {
                    match access {
                        AccessExpr::Dot(expr) => {
                            self.walk_expr(expr, None, None, scope)?;
                        }
                        AccessExpr::Subscript(Subscript::Index { index }) => {
                            self.walk_expr(index, None, None, scope)?;
                        }
                        AccessExpr::Subscript(Subscript::Slice {
                            lower_bound,
                            upper_bound,
                            stride,
                        }) => {
                            for bound in [lower_bound, upper_bound, stride].into_iter().flatten() {
                                self.walk_expr(bound, None, None, scope)?;
                            }
                        }
                    }
                }

                self.warn(format!("expression `{}` is not understood; typed as Any", expr));
                Ok(anonymous(SqlType::Any, true))
            }
            Expr::Function(function) => self.walk_function(function, expected, hint, scope),
            Expr::Tuple(items) => {
                for item in items {
                    self.walk_expr(item, None, None, scope)?;
                }
                Ok(anonymous(SqlType::Any, true))
            }
            Expr::TypedString(typed) => Ok(Column::new(
                "",
                TypeInfo {
                    sql_type: SqlType::from_data_type(&typed.data_type),
                    nullable: false,
                    declared: Some(typed.data_type.to_string()),
                },
            )),
            Expr::Interval(interval) => {
                // `INTERVAL ? DAY` (MySQL) takes a count for a single unit;
                // otherwise the value is an interval string such as '1 day'.
                let single_unit = interval.last_field.is_none()
                    && matches!(
                        interval.leading_field,
                        Some(
                            DateTimeField::Year
                                | DateTimeField::Quarter
                                | DateTimeField::Month
                                | DateTimeField::Week(_)
                                | DateTimeField::Day
                                | DateTimeField::Hour
                                | DateTimeField::Minute
                                | DateTimeField::Second
                                | DateTimeField::Microsecond
                        )
                    );
                let expected = typed_hint("", if single_unit { SqlType::Int } else { SqlType::Text });
                let value = self.walk_expr(&interval.value, Some(&expected), None, scope)?;
                Ok(anonymous(SqlType::Interval, value.type_info.nullable))
            }
            Expr::Extract { expr, .. } => {
                let inner = self.walk_expr(expr, None, None, scope)?;
                // PostgreSQL 14+ returns numeric; MySQL returns an integer.
                let sql_type = match self.dialect {
                    SqlDialect::MySQL => SqlType::Int,
                    _ => SqlType::Numeric,
                };
                Ok(anonymous(sql_type, inner.type_info.nullable))
            }
            Expr::Wildcard(_) | Expr::QualifiedWildcard(..) => Ok(anonymous(SqlType::Any, true)),
            other => {
                self.warn(format!("expression `{}` is not understood; typed as Any", other));
                Ok(anonymous(SqlType::Any, true))
            }
        }
    }

    /// Walks both sides of a binary expression, letting a placeholder on one
    /// side take the type of the other side.
    fn walk_pair(
        &mut self,
        left: &Expr,
        right: &Expr,
        scope: &Scope,
        comparison: bool,
    ) -> Result<(Column, Column), String> {
        let adjust = |column: &Column| {
            if comparison {
                comparison_target(column)
            } else {
                column.clone()
            }
        };

        if is_placeholder(left) && !is_placeholder(right) {
            let right_type = self.walk_expr(right, None, None, scope)?;
            let expected = adjust(&right_type);
            let hint = column_hint(right);
            let left_type = self.walk_expr(left, Some(&expected), hint.as_deref(), scope)?;
            Ok((left_type, right_type))
        } else {
            let left_type = self.walk_expr(left, None, None, scope)?;
            let expected = adjust(&left_type);
            let hint = column_hint(left);
            let right_type = self.walk_expr(right, Some(&expected), hint.as_deref(), scope)?;
            Ok((left_type, right_type))
        }
    }

    fn walk_binary(
        &mut self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        scope: &Scope,
    ) -> Result<Column, String> {
        use BinaryOperator::*;

        match op {
            And | Or | Xor => {
                self.walk_expr(left, None, None, scope)?;
                self.walk_expr(right, None, None, scope)?;
                Ok(anonymous(SqlType::Bool, false))
            }
            Eq | NotEq | Lt | LtEq | Gt | GtEq | Spaceship => {
                self.walk_pair(left, right, scope, true)?;
                Ok(anonymous(SqlType::Bool, false))
            }
            Plus | Minus | Multiply | Divide | Modulo => {
                let (l, r) = self.walk_pair(left, right, scope, false)?;
                let (a, b) = (l.type_info.sql_type, r.type_info.sql_type);
                let sql_type = match self.dialect {
                    SqlDialect::PostgreSQL => postgresql_temporal_arithmetic(op, a, b),
                    _ => None,
                }
                .unwrap_or_else(|| arithmetic_type(a, b));
                Ok(anonymous(sql_type, l.type_info.nullable || r.type_info.nullable))
            }
            StringConcat => {
                let (l, r) = self.walk_pair(left, right, scope, false)?;
                Ok(anonymous(SqlType::Text, l.type_info.nullable || r.type_info.nullable))
            }
            Arrow | HashArrow => {
                self.walk_expr(left, None, None, scope)?;
                self.walk_expr(right, None, None, scope)?;
                // JSON extraction can return SQL NULL when the requested
                // key or path is absent, even when the source column is NOT NULL.
                Ok(anonymous(SqlType::Json, true))
            }
            LongArrow | HashLongArrow => {
                self.walk_expr(left, None, None, scope)?;
                self.walk_expr(right, None, None, scope)?;
                Ok(anonymous(SqlType::Text, true))
            }
            AtArrow | ArrowAt => {
                let (l, r) = self.walk_pair(left, right, scope, false)?;
                Ok(anonymous(
                    SqlType::Bool,
                    l.type_info.nullable || r.type_info.nullable,
                ))
            }
            _ => {
                let (l, r) = self.walk_pair(left, right, scope, false)?;
                Ok(anonymous(SqlType::Any, l.type_info.nullable || r.type_info.nullable))
            }
        }
    }

    fn walk_function(
        &mut self,
        function: &Function,
        expected: Option<&Column>,
        hint: Option<&str>,
        scope: &Scope,
    ) -> Result<Column, String> {
        let name = object_name_last(&function.name).to_lowercase();
        let mut arg_types: Vec<Column> = Vec::new();
        let mut arg_exprs: Vec<&Expr> = Vec::new();
        let mut has_wildcard = false;

        match &function.args {
            FunctionArguments::None => {}
            FunctionArguments::Subquery(query) => {
                self.analyze_query(query, Some(scope))?;
            }
            FunctionArguments::List(list) => {
                for (index, arg) in list.args.iter().enumerate() {
                    let arg_expr = match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        }
                        | FunctionArg::ExprNamed {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        } => expr,
                        _ => {
                            has_wildcard = true;
                            continue;
                        }
                    };

                    // MySQL's TIMESTAMPDIFF(MINUTE, a, b) takes a bare unit keyword.
                    if index == 0
                        && matches!(name.as_str(), "timestampdiff" | "timestampadd")
                        && matches!(arg_expr, Expr::Identifier(_))
                    {
                        arg_exprs.push(arg_expr);
                        arg_types.push(anonymous(SqlType::Text, false));
                        continue;
                    }

                    let arg_expected = function_arg_expectation(&name, index, &arg_types, expected);
                    let arg_hint = if matches!(name.as_str(), "coalesce" | "ifnull" | "nullif" | "iif" | "max" | "min" | "greatest" | "least") {
                        hint
                    } else {
                        None
                    };
                    let arg_type = self.walk_expr(arg_expr, arg_expected.as_ref(), arg_hint, scope)?;
                    arg_exprs.push(arg_expr);
                    arg_types.push(arg_type);
                }
            }
        }

        if let Some(filter) = &function.filter {
            self.walk_expr(filter, None, None, scope)?;
        }

        if self.dialect == SqlDialect::MySQL {
            if let Some(column) = mysql_function_result(&name, &arg_exprs, &arg_types) {
                return Ok(column);
            }
        }
        Ok(self.function_result(&name, &arg_types, has_wildcard))
    }

    fn dialect_temporal(&self, sql_type: SqlType) -> SqlType {
        match self.dialect {
            SqlDialect::SQLite => SqlType::Text,
            _ => sql_type,
        }
    }

    fn function_result(&mut self, name: &str, args: &[Column], has_wildcard: bool) -> Column {
        let first = args.first();
        let any_nullable = args.iter().any(|a| a.type_info.nullable);
        let all_nullable = !args.is_empty() && args.iter().all(|a| a.type_info.nullable);
        let first_type = first.map(|a| a.type_info.sql_type).unwrap_or(SqlType::Any);

        match name {
            "count" => anonymous(SqlType::Int, false),
            "sum" => anonymous(
                if matches!(first_type, SqlType::Float | SqlType::Numeric) {
                    first_type
                } else {
                    SqlType::Int
                },
                true,
            ),
            "total" => anonymous(SqlType::Float, false),
            "avg" => anonymous(SqlType::Float, true),
            "min" | "max" => {
                if args.len() > 1 {
                    let mut result = args
                        .iter()
                        .skip(1)
                        .fold(args[0].clone(), |acc, a| unify_columns(&acc, a));
                    result.name = String::new();
                    result
                } else {
                    let mut result = first.cloned().unwrap_or_else(|| anonymous(SqlType::Any, true));
                    result.name = String::new();
                    result.type_info.nullable = true;
                    result
                }
            }
            "group_concat" | "string_agg" => anonymous(SqlType::Text, true),
            "coalesce" | "ifnull" => {
                let mut result = args
                    .iter()
                    .skip(1)
                    .fold(
                        first.cloned().unwrap_or_else(|| anonymous(SqlType::Any, true)),
                        |acc, a| unify_columns(&acc, a),
                    );
                result.name = String::new();
                result.type_info.nullable = all_nullable;
                result
            }
            "nullif" => {
                let mut result = first.cloned().unwrap_or_else(|| anonymous(SqlType::Any, true));
                result.name = String::new();
                result.type_info.nullable = true;
                result
            }
            "iif" | "if" => {
                let mut result = match (args.get(1), args.get(2)) {
                    (Some(a), Some(b)) => unify_columns(a, b),
                    (Some(a), None) => {
                        let mut a = a.clone();
                        a.type_info.nullable = true;
                        a
                    }
                    _ => anonymous(SqlType::Any, true),
                };
                result.name = String::new();
                result
            }
            "length" | "instr" | "unicode" | "octet_length" | "json_array_length"
            | "jsonb_array_length" | "json_valid" => {
                anonymous(SqlType::Int, any_nullable)
            }
            "cardinality" => anonymous(SqlType::Int, any_nullable),
            "array_ndims" => anonymous(SqlType::Int, true),
            "array_length" | "array_lower" | "array_upper" | "array_position" => {
                // These can return SQL NULL for missing dimensions or values,
                // even when the array input itself is non-null.
                anonymous(SqlType::Int, true)
            }
            "lower" | "upper" | "trim" | "ltrim" | "rtrim" | "substr" | "substring" | "replace"
            | "hex" | "printf" | "format" | "char" | "soundex" | "json_type" | "json_typeof"
            | "jsonb_typeof"
            | "json_quote" | "json_pretty" | "jsonb_pretty"
            | "json" | "json_object" | "json_array" | "json_set" | "json_insert" | "json_replace"
            | "json_remove" | "json_patch" | "json_group_array" | "json_group_object" => {
                anonymous(SqlType::Text, any_nullable)
            }
            "concat" | "concat_ws" | "quote" | "typeof" | "sqlite_version" | "sqlite_source_id" => {
                anonymous(SqlType::Text, false)
            }
            "json_extract_path" | "jsonb_extract_path" => anonymous(SqlType::Json, true),
            "json_extract_path_text" | "jsonb_extract_path_text" => anonymous(SqlType::Text, true),
            "json_extract" | "->" | "->>" => anonymous(SqlType::Any, true),
            "abs" | "likely" | "unlikely" | "likelihood" => {
                let mut result = first.cloned().unwrap_or_else(|| anonymous(SqlType::Any, true));
                result.name = String::new();
                result
            }
            // MySQL's VALUES(column) in ON DUPLICATE KEY UPDATE refers to
            // the value proposed by the INSERT for that column.
            "values" => first.cloned().unwrap_or_else(|| anonymous(SqlType::Any, true)),
            "round" | "julianday" | "sqrt" | "power" | "pow" | "exp" | "ln" | "log" | "log10"
            | "log2" | "pi" | "sin" | "cos" | "tan" | "asin" | "acos" | "atan" | "atan2"
            | "degrees" | "radians" | "trunc" | "ceil" | "ceiling" | "floor" | "mod" => {
                anonymous(SqlType::Float, any_nullable)
            }
            "random" | "last_insert_rowid" | "changes" | "total_changes" | "sign" => {
                anonymous(SqlType::Int, false)
            }
            "randomblob" | "zeroblob" | "unhex" => anonymous(SqlType::Blob, false),
            "date" | "time" | "datetime" | "strftime" | "timediff" => anonymous(SqlType::Text, true),
            "unixepoch" => anonymous(SqlType::Int, true),
            "row_number" | "rank" | "dense_rank" | "ntile" => anonymous(SqlType::Int, false),
            "lag" | "lead" | "first_value" | "last_value" | "nth_value" => {
                let mut result = first.cloned().unwrap_or_else(|| anonymous(SqlType::Any, true));
                result.name = String::new();
                result.type_info.nullable = true;
                result
            }
            "percent_rank" | "cume_dist" => anonymous(SqlType::Float, false),
            "greatest" | "least" => {
                let mut result = args
                    .iter()
                    .skip(1)
                    .fold(
                        first.cloned().unwrap_or_else(|| anonymous(SqlType::Any, true)),
                        |acc, a| unify_columns(&acc, a),
                    );
                result.name = String::new();
                // MySQL returns NULL if any argument is NULL; PostgreSQL ignores NULLs.
                result.type_info.nullable = if self.dialect == SqlDialect::MySQL {
                    any_nullable
                } else {
                    all_nullable
                };
                result
            }
            "now" | "transaction_timestamp" | "statement_timestamp" | "clock_timestamp" => {
                anonymous(SqlType::DateTime, false)
            }
            // SQLite returns these as text; PostgreSQL and MySQL return temporal values.
            "current_timestamp" | "localtimestamp" => anonymous(self.dialect_temporal(SqlType::DateTime), false),
            "current_date" => anonymous(self.dialect_temporal(SqlType::Date), false),
            "current_time" | "localtime" => anonymous(self.dialect_temporal(SqlType::Time), false),
            "date_trunc" => {
                // Intervals (and times, which PostgreSQL casts to interval)
                // yield an interval; dates and timestamps yield a timestamp.
                let source = args.get(1).map(|a| a.type_info.sql_type);
                let sql_type = if matches!(source, Some(SqlType::Interval | SqlType::Time)) {
                    SqlType::Interval
                } else {
                    SqlType::DateTime
                };
                anonymous(sql_type, any_nullable)
            }
            "date_bin" | "to_timestamp" | "make_timestamp" | "make_timestamptz" => {
                anonymous(SqlType::DateTime, any_nullable)
            }
            "to_date" | "make_date" => anonymous(SqlType::Date, any_nullable),
            "make_time" => anonymous(SqlType::Time, any_nullable),
            "age" | "make_interval" | "justify_days" | "justify_hours" | "justify_interval" => {
                anonymous(SqlType::Interval, any_nullable)
            }
            "date_part" => anonymous(SqlType::Float, any_nullable),
            "to_char" => anonymous(SqlType::Text, any_nullable),
            _ => {
                let _ = has_wildcard;
                self.warn(format!(
                    "unknown function `{}`; its result is typed as Any (alias the column and add `-- column: <name> <type>` to override)",
                    name
                ));
                anonymous(SqlType::Any, true)
            }
        }
    }
}

/// Fractional-second precision of a declared `DATETIME(n)`, `TIMESTAMP(n)` or
/// `TIME(n)` column (0 when declared without one); `None` for anything else.
fn temporal_precision(column: &Column) -> Option<u32> {
    let declared = column.type_info.declared.as_deref()?.trim().to_uppercase();
    let (base, rest) = match declared.split_once('(') {
        Some((base, rest)) => (base.trim().to_string(), Some(rest)),
        None => (declared.clone(), None),
    };
    if !matches!(base.as_str(), "DATETIME" | "TIMESTAMP" | "TIME" | "DATE") {
        return None;
    }
    match rest {
        None => Some(0),
        Some(rest) => rest.trim_end_matches(')').trim().parse().ok(),
    }
}

/// MySQL functions whose result type differs from the shared table (SQLite
/// semantics) or depends on the raw arguments. `None` falls back to it.
fn mysql_function_result(name: &str, exprs: &[&Expr], args: &[Column]) -> Option<Column> {
    use SqlType::*;
    let any_nullable = args.iter().any(|a| a.type_info.nullable);
    let first_type = args.first().map(|a| a.type_info.sql_type);

    let column = match name {
        "date" => anonymous(Date, any_nullable),
        "time" => anonymous(Time, any_nullable),
        "timestamp" | "timestampadd" => anonymous(DateTime, any_nullable),
        "date_add" | "date_sub" | "adddate" | "subdate" => {
            // DATE_ADD(date, INTERVAL n DAY) stays a DATE; any time unit (or a
            // compound unit such as DAY_HOUR) makes it a DATETIME. String
            // inputs produce strings.
            let time_unit = matches!(
                exprs.get(1),
                Some(Expr::Interval(interval))
                    if interval.last_field.is_some()
                        || !matches!(
                            interval.leading_field,
                            Some(DateTimeField::Year | DateTimeField::Quarter | DateTimeField::Month
                                | DateTimeField::Week(_) | DateTimeField::Day)
                        )
            );
            let sql_type = match first_type {
                Some(Date) if !time_unit => Date,
                Some(Text) => Text,
                _ => DateTime,
            };
            anonymous(sql_type, any_nullable)
        }
        "str_to_date" => {
            // The format decides: any time specifier gives a DATETIME.
            let format = match exprs.get(1) {
                Some(Expr::Value(v)) => match &v.value {
                    Value::SingleQuotedString(format) | Value::DoubleQuotedString(format) => Some(format.clone()),
                    _ => None,
                },
                _ => None,
            };
            let has_time = format.map_or(true, |f| {
                ["%H", "%h", "%I", "%i", "%k", "%l", "%p", "%r", "%S", "%s", "%T", "%f"]
                    .iter()
                    .any(|spec| f.contains(spec))
            });
            // Invalid input yields NULL.
            anonymous(if has_time { DateTime } else { Date }, true)
        }
        "unix_timestamp" => {
            // An integer for whole-second temporal columns, a DECIMAL when the
            // argument carries fractional seconds. When that depends on the
            // value (parameters, strings), use a numeric type that takes both.
            let sql_type = match args.first() {
                None => Int,
                Some(arg) => match temporal_precision(arg) {
                    Some(0) => Int,
                    _ => Numeric,
                },
            };
            anonymous(sql_type, any_nullable)
        }
        // Out-of-range timestamps (e.g. negative ones) give NULL.
        "from_unixtime" => anonymous(if args.len() > 1 { Text } else { DateTime }, true),
        // NULL when a time zone is unknown (or the zone tables are not loaded).
        "convert_tz" => anonymous(DateTime, true),
        "timestampdiff" | "datediff" | "to_days" | "dayofweek" | "dayofmonth" | "dayofyear" | "weekday"
        | "week" | "weekofyear" | "yearweek" | "year" | "month" | "day" | "hour" | "minute" | "second"
        | "quarter" | "microsecond" => anonymous(Int, any_nullable),
        "dayname" | "monthname" | "date_format" | "time_format" => anonymous(Text, any_nullable),
        "curdate" | "utc_date" => anonymous(Date, false),
        "curtime" | "utc_time" => anonymous(Time, false),
        "sysdate" | "utc_timestamp" => anonymous(DateTime, false),
        "rand" => anonymous(Float, false),
        "left" | "right" | "lpad" | "rpad" | "repeat" | "reverse" | "space" | "upper" | "lower" | "ucase"
        | "lcase" | "substring_index" | "md5" | "sha1" | "sha2" | "uuid" => anonymous(Text, any_nullable),
        "json_unquote" => anonymous(Text, true),
        "json_extract" | "json_arrayagg" | "json_objectagg" => anonymous(Json, true),
        "json_array" | "json_object" => anonymous(Json, false),
        "json_set" | "json_insert" | "json_replace" | "json_remove" | "json_merge_patch"
        | "json_merge_preserve" => anonymous(Json, any_nullable),
        "json_contains" | "json_length" | "json_depth" => anonymous(Int, true),
        "found_rows" | "row_count" | "last_insert_id" | "connection_id" => anonymous(Int, false),
        _ => return None,
    };
    Some(column)
}

fn raw_type(raw: &RawParam) -> TypeInfo {
    match &raw.type_info {
        Some(type_info) => {
            let mut type_info = type_info.clone();
            type_info.nullable |= raw.nullable_hint;
            type_info
        }
        None => TypeInfo::new(SqlType::Any, raw.nullable_hint),
    }
}

/// A placeholder compared with a column should not be nullable by default:
/// `col = NULL` never matches. `IS NULL` checks re-enable nullability.
fn comparison_target(column: &Column) -> Column {
    let mut target = column.clone();
    target.type_info.nullable = false;
    target
}

fn unify_columns(a: &Column, b: &Column) -> Column {
    Column {
        name: a.name.clone(),
        type_info: TypeInfo::unify(&a.type_info, &b.type_info),
        source: if a.source == b.source { a.source.clone() } else { None },
    }
}

/// PostgreSQL's date/time operators, or `None` when the operands are not temporal.
fn postgresql_temporal_arithmetic(op: &BinaryOperator, a: SqlType, b: SqlType) -> Option<SqlType> {
    use SqlType::*;
    let numeric = |t: SqlType| matches!(t, Int | Float | Numeric);
    match op {
        BinaryOperator::Plus => match (a, b) {
            (DateTime, Interval) | (Interval, DateTime) => Some(DateTime),
            (Date, Interval) | (Interval, Date) => Some(DateTime),
            (Date, Time) | (Time, Date) => Some(DateTime),
            (Date, Int) | (Int, Date) => Some(Date),
            (Time, Interval) | (Interval, Time) => Some(Time),
            (Interval, Interval) => Some(Interval),
            _ => None,
        },
        BinaryOperator::Minus => match (a, b) {
            (DateTime, Interval) | (Date, Interval) => Some(DateTime),
            (DateTime, DateTime) | (DateTime, Date) | (Date, DateTime) => Some(Interval),
            (Date, Date) => Some(Int),
            (Date, Int) => Some(Date),
            (Time, Time) => Some(Interval),
            (Time, Interval) => Some(Time),
            (Interval, Interval) => Some(Interval),
            _ => None,
        },
        BinaryOperator::Multiply => match (a, b) {
            (Interval, t) | (t, Interval) if numeric(t) => Some(Interval),
            _ => None,
        },
        BinaryOperator::Divide => match (a, b) {
            (Interval, t) if numeric(t) => Some(Interval),
            _ => None,
        },
        _ => None,
    }
}

fn arithmetic_type(a: SqlType, b: SqlType) -> SqlType {
    use SqlType::*;
    if a == Any || b == Any || a == Interval || b == Interval {
        return Any;
    }
    if a == Float || b == Float {
        return Float;
    }
    if a == Numeric || b == Numeric {
        return Numeric;
    }
    if matches!(a, Int | Bool) && matches!(b, Int | Bool) {
        return Int;
    }
    Numeric
}

/// The type a placeholder should take as the `index`-th argument of `name`.
fn function_arg_expectation(
    name: &str,
    index: usize,
    previous: &[Column],
    outer_expected: Option<&Column>,
) -> Option<Column> {
    let text = || Some(typed_hint("", SqlType::Text));
    let int = || Some(typed_hint("", SqlType::Int));
    let float = || Some(typed_hint("", SqlType::Float));

    match name {
        "date" | "time" | "datetime" | "julianday" | "unixepoch" | "timediff" => text(),
        "strftime" => text(),
        "length" | "lower" | "upper" | "trim" | "ltrim" | "rtrim" | "replace" | "hex" | "unhex"
        | "quote" | "printf" | "format" | "concat" | "concat_ws" | "like" | "glob" | "instr"
        | "json" | "json_extract" | "json_valid" | "json_type" | "json_array_length" => text(),
        "array_length" | "array_lower" | "array_upper" if index == 1 => int(),
        "array_position" if index == 2 => int(),
        "convert_tz" => {
            if index == 0 {
                Some(typed_hint("", SqlType::DateTime))
            } else {
                text()
            }
        }
        "date_add" | "date_sub" | "adddate" | "subdate" | "date_format" | "unix_timestamp" | "dayname"
        | "dayofweek" | "timestamp"
            if index == 0 =>
        {
            Some(typed_hint("", SqlType::DateTime))
        }
        "timestampdiff" if index > 0 => Some(typed_hint("", SqlType::DateTime)),
        "timestampadd" if index == 1 => int(),
        "timestampadd" if index == 2 => Some(typed_hint("", SqlType::DateTime)),
        "date_format" | "str_to_date" => text(),
        // Unix timestamps may carry fractional seconds.
        "from_unixtime" if index == 0 => float(),
        "date_trunc" | "date_part" if index == 0 => text(),
        "to_char" | "to_date" | "to_timestamp" if index == 1 => text(),
        "to_date" => text(),
        "age" => Some(typed_hint("", SqlType::DateTime)),
        "make_date" => int(),
        "substr" | "substring" => {
            if index == 0 {
                text()
            } else {
                int()
            }
        }
        "round" => {
            if index == 0 {
                float()
            } else {
                int()
            }
        }
        "abs" | "sqrt" | "power" | "pow" | "exp" | "ln" | "log" | "log10" | "log2" | "sin"
        | "cos" | "tan" | "asin" | "acos" | "atan" | "atan2" | "degrees" | "radians" | "trunc"
        | "ceil" | "ceiling" | "floor" | "mod" | "sign" => float(),
        "randomblob" | "zeroblob" | "ntile" => int(),
        "coalesce" | "ifnull" | "nullif" | "max" | "min" | "greatest" | "least" => {
            previous
                .iter()
                .find(|c| c.type_info.sql_type != SqlType::Any)
                .cloned()
                .or_else(|| outer_expected.cloned())
        }
        "iif" | "if" => {
            if index == 0 {
                None
            } else {
                previous
                    .iter()
                    .skip(1)
                    .find(|c| c.type_info.sql_type != SqlType::Any)
                    .cloned()
                    .or_else(|| outer_expected.cloned())
            }
        }
        _ => None,
    }
}
