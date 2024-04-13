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

pub fn format_resp_array(value: &str) -> String {
    let lines = value.lines().count();
    let mut formatted = format!("*{}", lines);
    for line in value.lines() {
        let bulk_formatted = format_bulk_string_line(line);
        formatted = format!("{}\r\n{}", formatted, bulk_formatted);
    }

    formatted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_success_simple_string() {
        assert_eq!(format_success_simple_string("foo"), "+foo");
    }

    #[test]
    fn test_format_resp_array() {
        assert_eq!(format_resp_array("ping"), "*1\r\n$4\r\nping");
    }
}
