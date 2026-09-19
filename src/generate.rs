//! Ties everything together: read the config, collect the schema, analyze the
//! query blocks, and hand the result to a code generator.

use std::path::{Path, PathBuf};

use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser as SqlParser;

use crate::analyze::{analyze_statement, analyze_view_columns, Param, StatementKind};
use crate::config::Config;
use crate::dialect::SqlDialect;
use crate::error::{ButterError, Diagnostic};
use crate::python;
use crate::queryfile::{parse_query_file, Command, QueryBlock, RawStatement};
use crate::schema::{object_name_last, Column, Schema, Table};
use crate::types::SqlType;

/// One statement of a query, ready for code generation.
#[derive(Clone, Debug)]
pub struct UnitStatement {
    pub sql: String,
    pub kind: StatementKind,
    pub has_params: bool,
}

/// One named query: everything the generator needs to emit a function.
#[derive(Clone, Debug)]
pub struct QueryUnit {
    pub name: String,
    pub command: Command,
    pub doc: Vec<String>,
    pub statements: Vec<UnitStatement>,
    pub params: Vec<Param>,
    pub positional: bool,
    pub columns: Vec<Column>,
    pub param_overrides: Vec<(String, String)>,
    pub column_overrides: Vec<(String, String)>,
}

/// One `.sql` file, which becomes one generated module.
#[derive(Clone, Debug)]
pub struct Module {
    /// Path of the source file relative to the project root.
    pub source: PathBuf,
    /// File stem, sanitized to an identifier.
    pub stem: String,
    pub units: Vec<QueryUnit>,
}

#[derive(Debug)]
pub struct Report {
    pub dialect: SqlDialect,
    pub schema_tables: Vec<String>,
    pub outputs: Vec<(PathBuf, usize)>,
    pub warnings: Vec<Diagnostic>,
    pub wrote_files: bool,
}

struct ParsedBlock {
    block: QueryBlock,
    statements: Vec<(RawStatement, Statement)>,
}

struct ParsedFile {
    relative_path: PathBuf,
    stem: String,
    blocks: Vec<ParsedBlock>,
}

fn extract_parser_error_line(message: &str) -> Option<usize> {
    let after = message.split("Line: ").nth(1)?;
    let number = after.split(',').next()?.trim();
    number.parse::<usize>().ok()
}

fn clean_parser_error(message: &str) -> String {
    message
        .trim_start_matches("sql parser error: ")
        .to_string()
}

fn parse_single_statement(dialect: &dyn Dialect, sql: &str) -> Result<Statement, (String, usize)> {
    match SqlParser::parse_sql(dialect, sql) {
        Ok(mut statements) => match statements.len() {
            1 => Ok(statements.remove(0)),
            0 => Err((String::from("statement is empty"), 1)),
            n => Err((format!("expected one statement, found {}", n), 1)),
        },
        Err(err) => {
            let message = err.to_string();
            let line = extract_parser_error_line(&message).unwrap_or(1);
            Err((clean_parser_error(&message), line))
        }
    }
}

fn sanitize_stem(stem: &str) -> String {
    let mut out: String = stem
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    if out.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true) {
        out = format!("_{}", out);
    }
    out.to_lowercase()
}

pub fn run(project_root: &Path, write: bool) -> Result<Report, ButterError> {
    let config_path = project_root.join("butter.toml");
    let config = Config::load(&config_path)?;

    let dialect = SqlDialect::parse(&config.generate.dialect)?;
    let parser_dialect = dialect.parser_dialect();

    let python_config = config.generate.python.as_ref().ok_or_else(|| {
        ButterError::Config(format!(
            "{}: add a [generate.python] section with `output-dir` to choose what to generate",
            config_path.display()
        ))
    })?;

    if python_config.driver != "aiosqlite" {
        return Err(ButterError::Config(format!(
            "{}: unsupported python driver `{}`; only `aiosqlite` is available",
            config_path.display(),
            python_config.driver
        )));
    }

    let mut errors: Vec<Diagnostic> = Vec::new();
    let mut warnings: Vec<Diagnostic> = Vec::new();
    let mut schema = Schema::default();
    let mut views: Vec<(PathBuf, usize, Statement)> = Vec::new();

    // ---------------------------------------------------------- schema files
    for schema_file in config.schema_files() {
        let path = project_root.join(&schema_file);
        let contents = match std::fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(err) => {
                errors.push(Diagnostic::new(&schema_file, 0, format!("cannot read schema file: {}", err)));
                continue;
            }
        };

        match SqlParser::parse_sql(parser_dialect.as_ref(), &contents) {
            Ok(statements) => {
                for statement in statements {
                    match &statement {
                        Statement::CreateTable(create_table) => schema.add_create_table(create_table),
                        Statement::CreateView { .. } => views.push((schema_file.clone(), 0, statement.clone())),
                        _ => {}
                    }
                }
            }
            Err(err) => {
                let message = err.to_string();
                let line = extract_parser_error_line(&message).unwrap_or(0);
                errors.push(Diagnostic::new(&schema_file, line, clean_parser_error(&message)));
            }
        }
    }

    // ----------------------------------------------------------- query files
    let queries_dir = project_root.join(&config.generate.queries_dir);
    let mut query_paths: Vec<PathBuf> = match std::fs::read_dir(&queries_dir) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|path| path.extension().map(|ext| ext == "sql").unwrap_or(false))
            .collect(),
        Err(err) => {
            return Err(ButterError::Config(format!(
                "cannot read queries directory {}: {}",
                queries_dir.display(),
                err
            )));
        }
    };
    query_paths.sort();

    let mut parsed_files: Vec<ParsedFile> = Vec::new();

    for path in &query_paths {
        let relative_path = path
            .strip_prefix(project_root)
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|_| path.clone());

        let contents = match std::fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(err) => {
                errors.push(Diagnostic::new(&relative_path, 0, format!("cannot read: {}", err)));
                continue;
            }
        };

        let blocks = match parse_query_file(&relative_path, &contents, parser_dialect.as_ref()) {
            Ok(blocks) => blocks,
            Err(diagnostics) => {
                errors.extend(diagnostics);
                continue;
            }
        };

        let mut parsed_blocks: Vec<ParsedBlock> = Vec::new();

        for block in blocks {
            let mut statements: Vec<(RawStatement, Statement)> = Vec::new();
            let mut block_ok = true;

            for raw in &block.statements {
                match parse_single_statement(parser_dialect.as_ref(), &raw.sql) {
                    Ok(statement) => {
                        match &statement {
                            Statement::CreateTable(create_table) => schema.add_create_table(create_table),
                            Statement::CreateView { .. } => {
                                views.push((relative_path.clone(), raw.line, statement.clone()))
                            }
                            _ => {}
                        }
                        statements.push((raw.clone(), statement));
                    }
                    Err((message, line)) => {
                        block_ok = false;
                        errors.push(Diagnostic::new(
                            &relative_path,
                            raw.line + line.saturating_sub(1),
                            message,
                        ));
                    }
                }
            }

            if block.name.is_none() {
                for (raw, statement) in &statements {
                    if !is_schema_statement(statement) {
                        errors.push(Diagnostic::new(
                            &relative_path,
                            raw.line,
                            "statement needs a header like `-- name: my_query :many` to generate a function",
                        ));
                    }
                }
                continue;
            }

            if block_ok {
                parsed_blocks.push(ParsedBlock { block, statements });
            }
        }

        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .map(sanitize_stem)
            .unwrap_or_else(|| String::from("queries"));

        parsed_files.push(ParsedFile {
            relative_path,
            stem,
            blocks: parsed_blocks,
        });
    }

    // Views need every table first, so they are resolved after the file pass.
    for (path, line, statement) in &views {
        if let Statement::CreateView { name, columns, query, .. } = statement {
            match analyze_view_columns(query, &schema) {
                Ok(mut view_columns) => {
                    for (column, def) in view_columns.iter_mut().zip(columns.iter()) {
                        column.name = def.name.value.clone();
                    }
                    schema.add_table(Table {
                        name: object_name_last(name),
                        columns: view_columns,
                    });
                }
                Err(message) => errors.push(Diagnostic::new(path, *line, format!("view: {}", message))),
            }
        }
    }

    // -------------------------------------------------------------- analysis
    let mut modules: Vec<Module> = Vec::new();

    for file in parsed_files {
        let mut units: Vec<QueryUnit> = Vec::new();
        let mut seen_names: Vec<String> = Vec::new();

        for parsed in &file.blocks {
            let block = &parsed.block;
            let Some(name) = &block.name else { continue };
            let Some(command) = block.command else { continue };

            if seen_names.iter().any(|n| n == name) {
                errors.push(Diagnostic::new(
                    &file.relative_path,
                    block.line,
                    format!("duplicate query name `{}`", name),
                ));
                continue;
            }
            seen_names.push(name.clone());

            match build_unit(&file.relative_path, block, command, &parsed.statements, &schema, &mut warnings) {
                Ok(unit) => units.push(unit),
                Err(diagnostics) => errors.extend(diagnostics),
            }
        }

        modules.push(Module {
            source: file.relative_path,
            stem: file.stem,
            units,
        });
    }

    let mut stems: Vec<&str> = modules.iter().map(|m| m.stem.as_str()).collect();
    stems.sort_unstable();
    for pair in stems.windows(2) {
        if pair[0] == pair[1] {
            errors.push(Diagnostic::new(
                &config.generate.queries_dir,
                0,
                format!("two query files map to the same module name `{}`", pair[0]),
            ));
        }
    }

    if !errors.is_empty() {
        return Err(ButterError::Diagnostics(errors));
    }

    // ------------------------------------------------------------- rendering
    let output_dir = project_root.join(&python_config.output_dir);
    let mut outputs: Vec<(PathBuf, usize)> = Vec::new();
    let mut rendered: Vec<(PathBuf, String)> = Vec::new();

    for module in &modules {
        let code = python::render_module(module, python_config).map_err(|message| {
            ButterError::Diagnostics(vec![Diagnostic::new(&module.source, 0, message)])
        })?;
        let output_path = output_dir.join(format!("{}.py", module.stem));
        outputs.push((output_path.clone(), module.units.len()));
        rendered.push((output_path, code));
    }

    if write {
        std::fs::create_dir_all(&output_dir)?;

        let init_path = output_dir.join("__init__.py");
        if !init_path.exists() {
            std::fs::write(&init_path, python::render_init())?;
        }

        for (path, code) in &rendered {
            std::fs::write(path, code)?;
        }
    }

    let mut schema_tables: Vec<String> = schema.tables.iter().map(|t| t.name.clone()).collect();
    schema_tables.sort();

    Ok(Report {
        dialect,
        schema_tables,
        outputs,
        warnings,
        wrote_files: write,
    })
}

fn is_schema_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::CreateTable(_)
            | Statement::CreateIndex(_)
            | Statement::CreateView { .. }
            | Statement::AlterTable { .. }
            | Statement::Drop { .. }
            | Statement::CreateTrigger { .. }
            | Statement::Pragma { .. }
    )
}

fn build_unit(
    path: &Path,
    block: &QueryBlock,
    command: Command,
    statements: &[(RawStatement, Statement)],
    schema: &Schema,
    warnings: &mut Vec<Diagnostic>,
) -> Result<QueryUnit, Vec<Diagnostic>> {
    let name = block.name.clone().unwrap_or_default();
    let mut errors: Vec<Diagnostic> = Vec::new();

    if statements.is_empty() {
        return Err(vec![Diagnostic::new(
            path,
            block.line,
            format!("query `{}` has no SQL statement", name),
        )]);
    }

    if statements.len() > 1 && command != Command::Exec {
        return Err(vec![Diagnostic::new(
            path,
            block.line,
            format!(
                "query `{}` contains {} statements; only `:exec` queries may run several statements",
                name,
                statements.len()
            ),
        )]);
    }

    let mut unit_statements: Vec<UnitStatement> = Vec::new();
    let mut params: Vec<Param> = Vec::new();
    let mut columns: Vec<Column> = Vec::new();
    let mut positional = false;

    for (raw, statement) in statements {
        let analysis = match analyze_statement(statement, schema) {
            Ok(analysis) => analysis,
            Err(message) => {
                errors.push(Diagnostic::new(path, raw.line, format!("{}: {}", name, message)));
                continue;
            }
        };

        for warning in &analysis.warnings {
            warnings.push(Diagnostic::new(path, raw.line, format!("{}: {}", name, warning)));
        }

        if statements.len() > 1 && analysis.positional {
            errors.push(Diagnostic::new(
                path,
                raw.line,
                format!("{}: multi-statement queries must use named placeholders (`:name`), not `?`", name),
            ));
        }

        positional |= analysis.positional;
        let statement_has_params = !analysis.params.is_empty();

        for param in analysis.params {
            match params.iter_mut().find(|p| p.name == param.name) {
                Some(existing) => {
                    if existing.type_info.sql_type == SqlType::Any && param.type_info.sql_type != SqlType::Any {
                        existing.type_info = param.type_info.clone();
                        existing.source = param.source.clone();
                    }
                    existing.type_info.nullable |= param.type_info.nullable;
                }
                None => params.push(param),
            }
        }

        if !analysis.columns.is_empty() {
            columns = analysis.columns.clone();
        }

        unit_statements.push(UnitStatement {
            sql: raw.sql.clone(),
            kind: analysis.kind,
            has_params: statement_has_params,
        });
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    if command.returns_rows() && columns.is_empty() {
        errors.push(Diagnostic::new(
            path,
            block.line,
            format!(
                "query `{}` is declared `{}` but returns no rows; use `:exec` or `:execrows`, or add RETURNING",
                name,
                command_label(command)
            ),
        ));
    }

    if command == Command::ExecMany && params.is_empty() {
        errors.push(Diagnostic::new(
            path,
            block.line,
            format!("query `{}` is declared `:execmany` but has no parameters", name),
        ));
    }

    if command == Command::ExecLastId
        && !unit_statements.iter().any(|s| s.kind == StatementKind::Insert)
    {
        warnings.push(Diagnostic::new(
            path,
            block.line,
            format!("{}: `:execlastid` is meant for INSERT statements", name),
        ));
    }

    for (param_name, _) in &block.param_types {
        if !params.iter().any(|p| &p.name == param_name) {
            errors.push(Diagnostic::new(
                path,
                block.line,
                format!(
                    "query `{}` has no parameter `{}` to override; parameters: {}",
                    name,
                    param_name,
                    if params.is_empty() {
                        String::from("(none)")
                    } else {
                        params.iter().map(|p| p.name.clone()).collect::<Vec<_>>().join(", ")
                    }
                ),
            ));
        }
    }

    for (column_name, _) in &block.column_types {
        if !columns.iter().any(|c| c.name.eq_ignore_ascii_case(column_name)) {
            errors.push(Diagnostic::new(
                path,
                block.line,
                format!(
                    "query `{}` has no output column `{}` to override; columns: {}",
                    name,
                    column_name,
                    if columns.is_empty() {
                        String::from("(none)")
                    } else {
                        columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>().join(", ")
                    }
                ),
            ));
        }
    }

    let mut seen_columns: Vec<String> = Vec::new();
    for column in &columns {
        let lower = column.name.to_lowercase();
        if seen_columns.contains(&lower) {
            errors.push(Diagnostic::new(
                path,
                block.line,
                format!(
                    "query `{}` returns two columns named `{}`; alias one of them with `AS`",
                    name, column.name
                ),
            ));
        }
        seen_columns.push(lower);
    }

    for param in &params {
        if param.type_info.sql_type == SqlType::Any
            && !block.param_types.iter().any(|(n, _)| n == &param.name)
        {
            warnings.push(Diagnostic::new(
                path,
                block.line,
                format!(
                    "{}: could not infer a type for parameter `{}`; it is typed `typing.Any`. Add `-- param: {} <python type>` to the query header",
                    name, param.name, param.name
                ),
            ));
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(QueryUnit {
        name,
        command,
        doc: block.doc.clone(),
        statements: unit_statements,
        params,
        positional,
        columns,
        param_overrides: block.param_types.clone(),
        column_overrides: block.column_types.clone(),
    })
}

fn command_label(command: Command) -> &'static str {
    match command {
        Command::One => ":one",
        Command::Many => ":many",
        Command::Exec => ":exec",
        Command::ExecRows => ":execrows",
        Command::ExecLastId => ":execlastid",
        Command::ExecMany => ":execmany",
    }
}
