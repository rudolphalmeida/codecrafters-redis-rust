//! Redis protocol spec: https://redis.io/docs/reference/protocol-spec/

pub fn format_bulk_string_line(line: &str) -> String {
    format!("${}\r\n{}", line.len(), line)
}

pub fn format_success_simple_string(line: &str) -> String {
    format!("+{}", line)
}

pub fn format_error_simple_string(line: &str) -> String {
    format!("-{}", line)
}
