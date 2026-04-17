use sqlparser::parser::ParserError;

pub fn extract_debug_block_with_line_number_range(
    text: &str,
    line_start: i32,
    line_end: i32,
) -> String {
    let mut block_lines = Vec::new();

    for (offset, line) in text
        .split("\n")
        .skip(line_start as usize)
        .take((line_end - line_start) as usize)
        .enumerate()
    {
        let line_number = line_start as usize + offset + 1;
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
        let debug =
            extract_debug_block_with_line_number_range(sql, line_number - 2, line_number + 2);
        format!("{}: {}", error, debug)
    } else {
        error.to_string()
    }
}
