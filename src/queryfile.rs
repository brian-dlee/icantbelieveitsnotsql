//! Splits a `.sql` file into annotated query blocks and individual statements.
//!
//! A block starts with a header comment in the sqlc style:
//!
//! ```sql
//! -- name: select_many_ipinfo_by_ip :many
//! -- param: duration str
//! SELECT * FROM ipinfo WHERE ip = :ip;
//! ```
//!
//! Statements before the first header have no name. They may only contain
//! schema statements (CREATE TABLE ...), which feed the analyzer but generate
//! no functions.

use sqlparser::dialect::Dialect;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::path::Path;

use crate::error::Diagnostic;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Command {
    /// Fetch one row or `None`.
    One,
    /// Fetch all rows.
    Many,
    /// Execute, return nothing. The only command that allows several statements.
    Exec,
    /// Execute, return the affected row count.
    ExecRows,
    /// Execute, return the last inserted row id.
    ExecLastId,
    /// `executemany` over a list of parameter models, return the affected row count.
    ExecMany,
}

impl Command {
    pub fn parse(value: &str) -> Option<Command> {
        match value {
            ":one" => Some(Command::One),
            ":many" => Some(Command::Many),
            ":exec" => Some(Command::Exec),
            ":execrows" => Some(Command::ExecRows),
            ":execlastid" => Some(Command::ExecLastId),
            ":execmany" | ":copyfrom" | ":batchexec" => Some(Command::ExecMany),
            _ => None,
        }
    }

    pub fn returns_rows(&self) -> bool {
        matches!(self, Command::One | Command::Many)
    }
}

#[derive(Clone, Debug)]
pub struct RawStatement {
    /// Statement text without the trailing semicolon, dedented.
    pub sql: String,
    /// 1-based line in the source file where the statement starts.
    pub line: usize,
}

#[derive(Clone, Debug)]
pub struct QueryBlock {
    pub name: Option<String>,
    pub command: Option<Command>,
    /// 1-based line of the `-- name:` header (or 1 for the unnamed leading block).
    pub line: usize,
    /// Free-form header comment lines, used as the docstring.
    pub doc: Vec<String>,
    /// `-- param: <name> <python type>` overrides.
    pub param_types: Vec<(String, String)>,
    /// `-- column: <name> <python type>` overrides.
    pub column_types: Vec<(String, String)>,
    pub statements: Vec<RawStatement>,
}

struct PendingBlock {
    name: Option<String>,
    command: Option<Command>,
    line: usize,
    doc: Vec<String>,
    param_types: Vec<(String, String)>,
    column_types: Vec<(String, String)>,
    in_header: bool,
    /// (1-based line number, text) of the SQL lines.
    lines: Vec<(usize, String)>,
}

impl PendingBlock {
    fn new(name: Option<String>, command: Option<Command>, line: usize) -> PendingBlock {
        PendingBlock {
            name,
            command,
            line,
            doc: Vec::new(),
            param_types: Vec::new(),
            column_types: Vec::new(),
            in_header: true,
            lines: Vec::new(),
        }
    }
}

/// Parses `-- name: <identifier> :<command>` headers, and the older
/// `-- <identifier> :<command>` spelling (exactly two words, the second
/// starting with a colon).
fn parse_header(line: &str) -> Option<(String, Option<Command>, String)> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix("--")?.trim_start();

    if let Some(rest) = rest.strip_prefix("name:") {
        let mut parts = rest.trim().split_whitespace();
        let name = parts.next()?.to_string();
        let command_text = parts.next().unwrap_or("").to_string();
        let command = Command::parse(&command_text);
        return Some((name, command, command_text));
    }

    let parts: Vec<&str> = rest.split_whitespace().collect();
    if let [name, command_text] = parts[..] {
        if is_identifier(name) && command_text.starts_with(':') {
            return Some((
                name.to_string(),
                Command::parse(command_text),
                command_text.to_string(),
            ));
        }
    }

    None
}

/// Parses `-- <key>: <name> <type...>` annotation lines.
fn parse_annotation(line: &str, key: &str) -> Option<(String, String)> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix("--")?.trim_start();
    let rest = rest.strip_prefix(key)?;
    let rest = rest.strip_prefix(':')?.trim();

    let mut parts = rest.splitn(2, char::is_whitespace);
    let name = parts.next()?.trim().to_string();
    let type_text = parts.next().unwrap_or("").trim().to_string();

    if name.is_empty() || type_text.is_empty() {
        return None;
    }

    Some((name, type_text))
}

pub fn parse_query_file(
    path: &Path,
    contents: &str,
    dialect: &dyn Dialect,
) -> Result<Vec<QueryBlock>, Vec<Diagnostic>> {
    let mut diagnostics: Vec<Diagnostic> = Vec::new();
    let mut pending: Vec<PendingBlock> = vec![PendingBlock::new(None, None, 1)];

    for (index, line) in contents.lines().enumerate() {
        let line_number = index + 1;

        if let Some((name, command, command_text)) = parse_header(line) {
            if command.is_none() {
                diagnostics.push(Diagnostic::new(
                    path,
                    line_number,
                    format!(
                        "query `{}` has unknown command `{}`; expected one of :one :many :exec :execrows :execlastid :execmany",
                        name, command_text
                    ),
                ));
            }
            if !is_identifier(&name) {
                diagnostics.push(Diagnostic::new(
                    path,
                    line_number,
                    format!("query name `{}` must be a snake_case identifier", name),
                ));
            }
            pending.push(PendingBlock::new(Some(name), command, line_number));
            continue;
        }

        let current = pending.last_mut().expect("at least one block");

        if current.in_header && current.name.is_some() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("--") {
                if let Some(annotation) = parse_annotation(line, "param") {
                    current.param_types.push(annotation);
                } else if let Some(annotation) = parse_annotation(line, "column") {
                    current.column_types.push(annotation);
                } else {
                    let text = trimmed.trim_start_matches('-').trim();
                    current.doc.push(text.to_string());
                }
                continue;
            } else if trimmed.is_empty() {
                continue;
            } else {
                current.in_header = false;
            }
        }

        current.lines.push((line_number, line.to_string()));
    }

    let mut blocks = Vec::new();

    for block in pending {
        let statements = match split_statements(path, &block.lines, dialect) {
            Ok(statements) => statements,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                continue;
            }
        };

        if block.name.is_none() && statements.is_empty() {
            continue;
        }

        blocks.push(QueryBlock {
            name: block.name,
            command: block.command,
            line: block.line,
            doc: block.doc,
            param_types: block.param_types,
            column_types: block.column_types,
            statements,
        });
    }

    if diagnostics.is_empty() {
        Ok(blocks)
    } else {
        Err(diagnostics)
    }
}

pub fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Splits block text on top-level semicolons using the tokenizer so that
/// semicolons inside string literals and comments are ignored.
fn split_statements(
    path: &Path,
    lines: &[(usize, String)],
    dialect: &dyn Dialect,
) -> Result<Vec<RawStatement>, Diagnostic> {
    if lines.is_empty() {
        return Ok(Vec::new());
    }

    let first_line = lines[0].0;
    let text: String = lines
        .iter()
        .map(|(_, line)| line.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    let tokens = Tokenizer::new(dialect, &text)
        .tokenize_with_location()
        .map_err(|err| {
            Diagnostic::new(
                path,
                first_line + err.location.line.saturating_sub(1) as usize,
                format!("tokenizer error: {}", err.message),
            )
        })?;

    // Byte offsets of every line start, to convert (line, column) to an offset.
    let mut line_starts = vec![0usize];
    for (offset, ch) in text.char_indices() {
        if ch == '\n' {
            line_starts.push(offset + 1);
        }
    }

    let location_to_offset = |line: u64, column: u64| -> usize {
        let line_index = (line as usize).saturating_sub(1);
        let line_start = line_starts.get(line_index).copied().unwrap_or(text.len());
        let line_text = &text[line_start..];
        let column_index = (column as usize).saturating_sub(1);
        line_text
            .char_indices()
            .nth(column_index)
            .map(|(byte, _)| line_start + byte)
            .unwrap_or(text.len())
    };

    let mut statements = Vec::new();
    let mut segment_start = 0usize;

    let mut cut_points: Vec<usize> = tokens
        .iter()
        .filter(|t| matches!(t.token, Token::SemiColon))
        .map(|t| location_to_offset(t.span.start.line, t.span.start.column))
        .collect();
    cut_points.push(text.len());

    for cut in cut_points {
        if cut < segment_start {
            continue;
        }
        let segment_begin = segment_start;
        let segment = &text[segment_begin..cut];
        segment_start = (cut + 1).min(text.len());

        if let Some(statement) = make_statement(segment, &text, first_line, segment_begin) {
            statements.push(statement);
        }
    }

    Ok(statements)
}

fn make_statement(segment: &str, text: &str, first_line: usize, segment_start: usize) -> Option<RawStatement> {
    // Drop leading blank lines and comment-only lines that would otherwise be
    // part of the statement; keep comments that sit between SQL lines.
    let mut kept: Vec<&str> = Vec::new();
    let mut skipped_prefix_lines = 0usize;
    let mut seen_sql = false;
    for line in segment.split('\n') {
        let trimmed = line.trim();
        let is_comment = trimmed.starts_with("--");
        if !seen_sql && (trimmed.is_empty() || is_comment) {
            skipped_prefix_lines += 1;
            continue;
        }
        seen_sql = true;
        kept.push(line);
    }
    while kept.last().map(|l| l.trim().is_empty()).unwrap_or(false) {
        kept.pop();
    }
    if kept.is_empty() {
        return None;
    }

    let indent = kept
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);

    let sql = kept
        .iter()
        .map(|l| if l.len() >= indent { &l[indent..] } else { l.trim_start() })
        .collect::<Vec<_>>()
        .join("\n")
        .trim_end()
        .to_string();

    let lines_before_segment = text[..segment_start].matches('\n').count();
    let line = first_line + lines_before_segment + skipped_prefix_lines;

    Some(RawStatement { sql, line })
}
