//! Rewrites butter's placeholders (`:name`, `@name`, `?`, `$N`) into the
//! parameter style a Python driver expects.

use sqlparser::dialect::Dialect;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

use crate::analyze::Param;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParamStyle {
    /// `:name` / `?` as written (sqlite3/aiosqlite).
    Native,
    /// `%(name)s` / `%s`, with literal `%` doubled (PyMySQL, aiomysql).
    Pyformat,
    /// `$1`, `$2`, ... bound positionally (asyncpg).
    Numeric,
}

/// The text a list parameter's `(...)` takes in the generated SQL constant;
/// the generated code expands it into one placeholder per element at call time.
pub fn list_token(style: ParamStyle, name: &str) -> String {
    match style {
        ParamStyle::Pyformat => format!("(%({})s)", name),
        _ => format!("(:{})", name),
    }
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

fn is_numbered(text: &str) -> bool {
    text.len() > 1 && text.starts_with('$') && text[1..].chars().all(|c| c.is_ascii_digit())
}

/// Index of the nearest token before `index` that is not whitespace or a comment.
fn previous_significant(tokens: &[TokenWithSpan], index: usize) -> Option<usize> {
    (0..index).rev().find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
}

/// Index of the nearest token after `index` that is not whitespace or a comment.
fn next_significant(tokens: &[TokenWithSpan], index: usize) -> Option<usize> {
    (index + 1..tokens.len()).find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
}

fn is_keyword(token: &Token, keyword: Keyword) -> bool {
    matches!(token, Token::Word(w) if w.keyword == keyword)
}

/// For a placeholder spanning tokens `first..=last`, the token indexes of an
/// enclosing `[NOT] IN ( <placeholder> )`: `(in_or_not, open, close, negated)`.
fn enclosing_in_list(tokens: &[TokenWithSpan], first: usize, last: usize) -> Option<(usize, usize, usize, bool)> {
    let open = previous_significant(tokens, first)?;
    let close = next_significant(tokens, last)?;
    if tokens[open].token != Token::LParen || tokens[close].token != Token::RParen {
        return None;
    }
    let in_index = previous_significant(tokens, open)?;
    if !is_keyword(&tokens[in_index].token, Keyword::IN) {
        return None;
    }
    match previous_significant(tokens, in_index) {
        Some(not_index) if is_keyword(&tokens[not_index].token, Keyword::NOT) => Some((not_index, open, close, true)),
        _ => Some((in_index, open, close, false)),
    }
}

/// Returns `sql` with every placeholder rewritten for `style`.
///
/// `params` are the unit's parameters in binding order; named placeholders
/// are looked up by name and `?` placeholders are numbered by position.
/// `lists` names the parameters declared as lists: under `Numeric` an
/// `IN (:p)` becomes `= ANY($n)`, otherwise the parenthesized placeholder is
/// left as [`list_token`] for the generated code to expand.
/// `bound` says whether the driver call passes arguments: pyformat drivers
/// only interpolate (and so only need `%` escaped) when it does.
pub fn rewrite(
    sql: &str,
    dialect: &dyn Dialect,
    style: ParamStyle,
    params: &[Param],
    lists: &[String],
    bound: bool,
) -> Result<String, String> {
    if style == ParamStyle::Native && lists.is_empty() {
        return Ok(sql.to_string());
    }

    let tokens = Tokenizer::new(dialect, sql)
        .tokenize_with_location()
        .map_err(|err| err.to_string())?;

    let mut line_starts = vec![0];
    line_starts.extend(sql.match_indices('\n').map(|(i, _)| i + 1));
    let offset = |location: Location| byte_offset(sql, &line_starts, location);

    let mut out = String::with_capacity(sql.len() + 16);
    let mut cursor = 0;
    let mut question_index = 0;

    let mut index = 0;
    while index < tokens.len() {
        let first = index;
        let token = &tokens[index];
        index += 1;

        // The parser also reads `:` or `@` directly followed by a word as a
        // placeholder (the tokenizer splits them when the word is a keyword,
        // as in `:limit`). Only treat the pair as one when it names a parameter.
        let text = match &token.token {
            Token::Placeholder(text) => text.clone(),
            Token::Colon | Token::AtSign => match tokens.get(index) {
                Some(next)
                    if matches!(&next.token, Token::Word(w) if params.iter().any(|p| p.name == w.value)) =>
                {
                    index += 1;
                    let Token::Word(word) = &next.token else { unreachable!() };
                    format!("{}{}", token.token, word.value)
                }
                _ => continue,
            },
            _ => continue,
        };
        let last = index - 1;
        let mut start = offset(tokens[first].span.start);
        let mut end = offset(tokens[last].span.end);

        if text == "?" {
            question_index += 1;
        }
        let list_name = match text.as_str() {
            "?" => params
                .get(question_index - 1)
                .filter(|p| lists.contains(&p.name))
                .map(|p| p.name.clone()),
            numbered if is_numbered(numbered) => {
                let position = numbered[1..].parse::<usize>().unwrap_or(0);
                params
                    .get(position.wrapping_sub(1))
                    .filter(|p| lists.contains(&p.name))
                    .map(|p| p.name.clone())
            }
            named => Some(named[1..].to_string()).filter(|name| lists.contains(name)),
        };

        let replacement = if let Some(name) = list_name {
            let enclosing = enclosing_in_list(&tokens, first, last);
            match (style, enclosing) {
                (ParamStyle::Numeric, Some((keyword, _, close, negated))) => {
                    let number = params.iter().position(|p| p.name == name).unwrap_or(0) + 1;
                    start = offset(tokens[keyword].span.start);
                    end = offset(tokens[close].span.end);
                    index = close + 1;
                    let comparison = if negated { "<> ALL" } else { "= ANY" };
                    format!("{}(${})", comparison, number)
                }
                // asyncpg binds a list anywhere else as a PostgreSQL array.
                (ParamStyle::Numeric, None) => {
                    let number = params.iter().position(|p| p.name == name).unwrap_or(0) + 1;
                    format!("${}", number)
                }
                (_, Some((_, open, close, _))) => {
                    start = offset(tokens[open].span.start);
                    end = offset(tokens[close].span.end);
                    index = close + 1;
                    list_token(style, &name)
                }
                (_, None) => {
                    return Err(format!(
                        "list parameter `{}` can only be used as `IN ({})` with this driver",
                        name, text
                    ));
                }
            }
        } else {
            match (style, text.as_str()) {
                (ParamStyle::Native, _) => text.clone(),
                (ParamStyle::Pyformat, "?") => String::from("%s"),
                (ParamStyle::Numeric, "?") => format!("${}", question_index),
                (ParamStyle::Numeric, numbered) if is_numbered(numbered) => numbered.to_string(),
                (ParamStyle::Pyformat, numbered) if is_numbered(numbered) => {
                    return Err(format!(
                        "`{}` placeholders need a PostgreSQL driver; use `:name` or `?` with this driver",
                        numbered
                    ));
                }
                (_, named) => {
                    let name = &named[1..];
                    let position = params
                        .iter()
                        .position(|p| p.name == name)
                        .ok_or_else(|| format!("placeholder `{}` has no matching parameter", named))?;
                    match style {
                        ParamStyle::Pyformat => format!("%({})s", name),
                        _ => format!("${}", position + 1),
                    }
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
