//! Redis protocol spec: https://redis.io/docs/reference/protocol-spec/

pub fn format_bulk_string_lines(text: &str) -> String {
    let lines = text.lines();
    let mut bulked_lines: String = "".to_string();
    for line in lines {
        let bulk_line = format_single_bulk_string_line(line);
        bulked_lines = bulked_lines + &bulk_line + "\r\n";
    }

    bulked_lines.trim_end_matches("\r\n").to_string()
}

pub fn format_single_bulk_string_line(line: &str) -> String {
    format!("${}\r\n{}", line.len(), line)
}

pub fn format_success_simple_string(line: &str) -> String {
    format!("+{}", line)
}

pub fn format_error_simple_string(line: &str) -> String {
    format!("-{}", line)
}
