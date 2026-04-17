use sqlparser::parser::ParserError;

pub fn extract_debug_block_with_line_number_range(
    text: &str,
    line_start: i32,
    line_end: i32,
) -> String {
    let mut block_lines = Vec::new();

    // line_start is 1-based; convert to 0-based index and clamp to >= 0
    let start_idx = (line_start - 1).max(0) as usize;
    let count = (line_end - line_start + 1).max(0) as usize;

    for (offset, line) in text.split("\n").skip(start_idx).take(count).enumerate() {
        let line_number = start_idx + offset + 1;
        block_lines.push(format!("{}\t{}", line_number, line));
    }

    block_lines.join("\n")
}

pub fn extract_line_number_from_parse_error(parse_error: &str) -> i32 {
    let parts: Vec<&str> = parse_error.split("Line: ").collect();
    if parts.len() < 2 {
        return -1;
    }

    let after_line = parts[1];
    let before_comma = match after_line.split(',').next() {
        Some(s) => s,
        None => return -1,
    };

    match before_comma.parse::<i32>() {
        Ok(n) => n,
        Err(_) => -1,
    }
}

pub fn format_sql_parser_error(error: &ParserError, sql: &str) -> String {
    if let ParserError::ParserError(msg) = &error {
        let line_number = extract_line_number_from_parse_error(msg);
        if line_number > 0 {
            let start = (line_number - 2).max(1);
            let debug = extract_debug_block_with_line_number_range(sql, start, line_number + 2);
            format!("{}: {}", error, debug)
        } else {
            error.to_string()
        }
    } else {
        error.to_string()
    }
}
