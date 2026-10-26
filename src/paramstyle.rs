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
pub fn byte_offset(sql: &str, line_starts: &[usize], location: Location) -> usize {
    let line_start = line_starts[(location.line as usize)
        .saturating_sub(1)
        .min(line_starts.len() - 1)];
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
    (0..index)
        .rev()
        .find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
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
fn enclosing_in_list(
    tokens: &[TokenWithSpan],
    first: usize,
    last: usize,
) -> Option<(usize, usize, usize, bool)> {
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
        Some(not_index) if is_keyword(&tokens[not_index].token, Keyword::NOT) => {
            Some((not_index, open, close, true))
        }
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
                Some(next) if matches!(&next.token, Token::Word(w) if params.iter().any(|p| p.name == w.value)) =>
                {
                    index += 1;
                    let Token::Word(word) = &next.token else {
                        unreachable!()
                    };
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
                    let position = params.iter().position(|p| p.name == name).ok_or_else(|| {
                        format!("placeholder `{}` has no matching parameter", named)
                    })?;
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

/// PostgreSQL accepts a locking clause (`FOR UPDATE ... SKIP LOCKED`) before
/// `LIMIT`/`OFFSET`, but sqlparser only parses it after them. Returns `sql`
/// with each statement's top-level locking clause moved after a following
/// `LIMIT`/`OFFSET`, or `None` when there is nothing to move. A clause never
/// moves past the `;` ending its statement, so this is safe on a whole file.
/// Used only to parse: the original text is what gets executed.
pub fn move_locking_after_limit(sql: &str, dialect: &dyn Dialect) -> Option<String> {
    let tokens = Tokenizer::new(dialect, sql).tokenize_with_location().ok()?;
    let mut line_starts = vec![0];
    line_starts.extend(sql.match_indices('\n').map(|(i, _)| i + 1));
    let offset = |location: Location| byte_offset(sql, &line_starts, location);

    // (lock clause start, end of its last token, end of the statement's last
    // token). LIMIT/OFFSET lies between the last two.
    let mut moves: Vec<(usize, usize, usize)> = Vec::new();
    let mut depth = 0usize;
    let mut lock_start: Option<usize> = None;
    let mut lock_end: Option<usize> = None;
    // Ends before trailing whitespace and comments, so a moved clause is never
    // placed inside a trailing `-- comment`.
    let mut last_token_end = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        match &token.token {
            Token::Whitespace(_) => continue,
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::SemiColon if depth == 0 => {
                if let (Some(lock), Some(lock_end)) = (lock_start, lock_end) {
                    moves.push((lock, lock_end, last_token_end));
                }
                lock_start = None;
                lock_end = None;
            }
            Token::Word(w) if depth == 0 => {
                if lock_start.is_none() && w.keyword == Keyword::FOR {
                    let next = next_significant(&tokens, index).map(|i| &tokens[i].token);
                    if matches!(next, Some(t) if is_keyword(t, Keyword::UPDATE)
                        || is_keyword(t, Keyword::SHARE)
                        || is_keyword(t, Keyword::NO)
                        || is_keyword(t, Keyword::KEY))
                    {
                        lock_start = Some(offset(token.span.start));
                    }
                } else if lock_start.is_some()
                    && lock_end.is_none()
                    && matches!(w.keyword, Keyword::LIMIT | Keyword::OFFSET)
                {
                    lock_end = Some(last_token_end);
                }
            }
            _ => {}
        }
        last_token_end = offset(token.span.end);
    }
    if let (Some(lock), Some(lock_end)) = (lock_start, lock_end) {
        moves.push((lock, lock_end, last_token_end));
    }
    if moves.is_empty() {
        return None;
    }

    let mut out = String::with_capacity(sql.len() + moves.len());
    let mut cursor = 0;
    for (lock, lock_end, end) in moves {
        // Whitespace and comments between the clause and LIMIT stay in place,
        // so a line comment there keeps the newline that ends it.
        out.push_str(&sql[cursor..lock]);
        out.push_str(sql[lock_end..end].trim_start());
        out.push(' ');
        out.push_str(&sql[lock..lock_end]);
        cursor = end;
    }
    out.push_str(&sql[cursor..]);
    Some(out)
}

/// MySQL reads `--` followed by any whitespace or control character (or the
/// end of input) as a comment, but sqlparser only accepts `-- ` with a space.
/// Returns `sql` with a space inserted after each such `--`, so a bare `--`
/// line parses the way MySQL reads it. Line numbers are unchanged.
///
/// This scans MySQL's lexical rules directly instead of tokenizing, because
/// the comment text itself may not tokenize as SQL (`--\tuser's note`).
pub fn normalize_mysql_dash_comments(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 8);
    let mut cursor = 0;
    let mut i = 0;

    let skip_to_line_end = |from: usize| {
        bytes[from..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(bytes.len(), |n| from + n)
    };

    while i < bytes.len() {
        match bytes[i] {
            // Quoted strings and identifiers: backslash escapes and doubled quotes.
            quote @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' && quote != b'`' {
                        i += 2;
                    } else if bytes[i] == quote {
                        if bytes.get(i + 1) == Some(&quote) {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i = sql[i + 2..]
                    .find("*/")
                    .map_or(bytes.len(), |n| i + 2 + n + 2);
            }
            b'#' => i = skip_to_line_end(i),
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                match bytes.get(i + 2) {
                    // Already `-- `, which sqlparser accepts.
                    Some(b' ') => {}
                    // Other whitespace, a control character, or the end of input.
                    None => {
                        out.push_str(&sql[cursor..i + 2]);
                        out.push(' ');
                        cursor = i + 2;
                    }
                    Some(&next) if next.is_ascii_whitespace() || next.is_ascii_control() => {
                        out.push_str(&sql[cursor..i + 2]);
                        out.push(' ');
                        cursor = i + 2;
                    }
                    // `5--1` is 5 minus -1, not a comment.
                    Some(_) => {
                        i += 2;
                        continue;
                    }
                }
                i = skip_to_line_end(i);
            }
            _ => i += 1,
        }
    }
    out.push_str(&sql[cursor..]);
    out
}

fn push_literal(out: &mut String, text: &str, style: ParamStyle, bound: bool) {
    if style == ParamStyle::Pyformat && bound {
        out.push_str(&text.replace('%', "%%"));
    } else {
        out.push_str(text);
    }
}
