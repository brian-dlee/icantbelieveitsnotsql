//! Rewrites butter's placeholders (`:name`, `@name`, `?`, `$N`) into the
//! parameter style a Python driver expects.

use sqlparser::dialect::Dialect;
use sqlparser::tokenizer::{Location, Token, Tokenizer};

use crate::analyze::Param;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParamStyle {
    /// Leave the SQL untouched (sqlite3/aiosqlite understand `:name` and `?`).
    Native,
    /// `%(name)s` / `%s`, with literal `%` doubled (PyMySQL, aiomysql).
    Pyformat,
    /// `$1`, `$2`, ... bound positionally (asyncpg).
    Numeric,
}

/// Byte offset of a 1-based line/column (columns count characters).
fn byte_offset(sql: &str, line_starts: &[usize], location: Location) -> usize {
    let line_start = line_starts[(location.line as usize).saturating_sub(1).min(line_starts.len() - 1)];
    sql[line_start..]
        .char_indices()
        .nth((location.column as usize).saturating_sub(1))
        .map(|(offset, _)| line_start + offset)
        .unwrap_or(sql.len())
}

/// Returns `sql` with every placeholder rewritten for `style`.
///
/// `params` are the unit's parameters in binding order; named placeholders
/// are looked up by name and `?` placeholders are numbered by position.
/// `bound` says whether the driver call passes arguments: pyformat drivers
/// only interpolate (and so only need `%` escaped) when it does.
pub fn rewrite(
    sql: &str,
    dialect: &dyn Dialect,
    style: ParamStyle,
    params: &[Param],
    bound: bool,
) -> Result<String, String> {
    if style == ParamStyle::Native {
        return Ok(sql.to_string());
    }

    let tokens = Tokenizer::new(dialect, sql)
        .tokenize_with_location()
        .map_err(|err| err.to_string())?;

    let mut line_starts = vec![0];
    line_starts.extend(sql.match_indices('\n').map(|(i, _)| i + 1));

    let mut out = String::with_capacity(sql.len() + 16);
    let mut cursor = 0;
    let mut question_index = 0;

    let mut index = 0;
    while index < tokens.len() {
        let token = &tokens[index];
        index += 1;

        // The parser also reads `:` or `@` directly followed by a word as a
        // placeholder (the tokenizer splits them when the word is a keyword,
        // as in `:limit`). Only treat the pair as one when it names a parameter.
        let (text, span_end) = match &token.token {
            Token::Placeholder(text) => (text.clone(), token.span.end),
            Token::Colon | Token::AtSign => match tokens.get(index) {
                Some(next)
                    if matches!(&next.token, Token::Word(w) if params.iter().any(|p| p.name == w.value)) =>
                {
                    index += 1;
                    let Token::Word(word) = &next.token else { unreachable!() };
                    (format!("{}{}", token.token, word.value), next.span.end)
                }
                _ => continue,
            },
            _ => continue,
        };
        let text = &text;
        let start = byte_offset(sql, &line_starts, token.span.start);
        let end = byte_offset(sql, &line_starts, span_end);

        let replacement = match (style, text.as_str()) {
            (ParamStyle::Pyformat, "?") => String::from("%s"),
            (ParamStyle::Numeric, "?") => {
                question_index += 1;
                format!("${}", question_index)
            }
            (ParamStyle::Numeric, numbered)
                if numbered.starts_with('$') && numbered[1..].chars().all(|c| c.is_ascii_digit()) =>
            {
                numbered.to_string()
            }
            (ParamStyle::Pyformat, numbered)
                if numbered.starts_with('$') && numbered[1..].chars().all(|c| c.is_ascii_digit()) =>
            {
                return Err(format!(
                    "`{}` placeholders need a PostgreSQL driver; use `:name` or `?` with this driver",
                    numbered
                ));
            }
            (_, named) => {
                let name = &named[1..];
                let index = params
                    .iter()
                    .position(|p| p.name == name)
                    .ok_or_else(|| format!("placeholder `{}` has no matching parameter", named))?;
                match style {
                    ParamStyle::Pyformat => format!("%({})s", name),
                    _ => format!("${}", index + 1),
                }
            }
        };

        push_literal(&mut out, &sql[cursor..start], style, bound);
        out.push_str(&replacement);
        cursor = end;
    }
    push_literal(&mut out, &sql[cursor..], style, bound);

    Ok(out)
}

fn push_literal(out: &mut String, text: &str, style: ParamStyle, bound: bool) {
    if style == ParamStyle::Pyformat && bound {
        out.push_str(&text.replace('%', "%%"));
    } else {
        out.push_str(text);
    }
}
