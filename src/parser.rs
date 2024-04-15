//! Redis protocol spec: https://redis.io/docs/reference/protocol-spec/

use std::time::Duration;

#[derive(Debug, Clone)]
pub enum ReplConfOption {
    ListeningPort(u16),
    Capability(String),
}

#[derive(Debug, Clone, Default)]
pub enum RedisCommand {
    #[default]
    Ping,
    Echo(String),
    Get(String),
    Set(String, String, Option<Duration>),
    Info(String),
    ReplConf(ReplConfOption),
}

pub fn parse_input(input: &str) -> Result<RedisCommand, String> {
    let mut lines = input.lines();
    let mut lines = parse_bulk_string_array(&mut lines)?.into_iter();

    let command = parse_bulk_string(&mut lines)?;
    // commands are case-insensitive
    match command.to_lowercase().as_str() {
        "ping" => Ok(RedisCommand::Ping),
        "echo" => parse_echo_command(&mut lines),
        "get" => parse_get_command(&mut lines),
        "set" => parse_set_command(&mut lines),
        "info" => parse_info_command(&mut lines),
        "replconf" => parse_replconf_command(&mut lines),
        _ => Err(format!("unknown command '{}'", command)),
    }
}

fn parse_bulk_string_array<'a>(
    lines: &'a mut dyn Iterator<Item = &str>,
) -> Result<Vec<&'a str>, String> {
    let len_line = lines.next().ok_or("no size line")?;
    let num_lines: usize = len_line[1..]
        .parse()
        .map_err(|e| format!("failed to parse line count: {}", e))?;
    if len_line.chars().next() != Some('*') || num_lines < 1 {
        return Err("expected first line to have format  *<size> where size >= 1".to_string());
    }

    let lines: Vec<_> = lines.collect();
    if lines.len() != num_lines * 2 {
        return Err(format!(
            "expected {} lines, got {}",
            num_lines * 2,
            lines.len()
        ));
    }

    Ok(lines)
}

fn parse_bulk_string(lines: &mut dyn Iterator<Item = &str>) -> Result<String, String> {
    let size_line = lines
        .next()
        .ok_or("expected a line with format  $<size>".to_string())?;
    let size = parse_integer_line(size_line)?;

    let line = lines
        .next()
        .ok_or(format!("expected a line with size {}", size))?;

    if size as usize != line.len() {
        return Err(format!("expected a line with size {}", size));
    }

    Ok(line.into())
}

fn parse_integer_line(line: &str) -> Result<i32, String> {
    if line.chars().next().unwrap_or('_') != '$' {
        return Err("expected integer line to begin with '$'".to_string());
    }
    let value: i32 = line[1..]
        .parse()
        .map_err(|_| format!("could not parse integer on {}", line))?;

    Ok(value)
}

fn parse_echo_command(lines: &mut dyn Iterator<Item = &str>) -> Result<RedisCommand, String> {
    let line = parse_bulk_string(lines)?;
    Ok(RedisCommand::Echo(line))
}

fn parse_get_command(lines: &mut dyn Iterator<Item = &str>) -> Result<RedisCommand, String> {
    let line = parse_bulk_string(lines)?;
    Ok(RedisCommand::Get(line))
}

fn parse_set_command(lines: &mut dyn Iterator<Item = &str>) -> Result<RedisCommand, String> {
    let key = parse_bulk_string(lines)?;
    let value = parse_bulk_string(lines)?;
    let timeout = match parse_optional(parse_argument, lines) {
        Some((arg, value)) if arg == "px" => {
            let millis = value
                .parse::<u64>()
                .map_err(|e| format!("failed to parse value for 'px' {} with {}", value, e))?;
            Some(Duration::from_millis(millis))
        }
        Some((arg, _)) => return Err(format!("unknown argument '{}' to SET", arg)),
        None => None,
    };

    Ok(RedisCommand::Set(key, value, timeout))
}

fn parse_info_command(lines: &mut dyn Iterator<Item = &str>) -> Result<RedisCommand, String> {
    let section = parse_bulk_string(lines)?;
    Ok(RedisCommand::Info(section))
}

fn parse_replconf_command(lines: &mut dyn Iterator<Item = &str>) -> Result<RedisCommand, String> {
    let (arg, value) = parse_argument(lines)?;
    let option = match arg.to_lowercase().as_str() {
        "listening-port" => ReplConfOption::ListeningPort(
            value
                .parse()
                .map_err(|_| "failed to parse listening-port")?,
        ),
        "capa" => ReplConfOption::Capability(value),
        _ => return Err(format!("unknown option '{}' to REPLCONG", arg)),
    };
    Ok(RedisCommand::ReplConf(option))
}

fn parse_argument(lines: &mut dyn Iterator<Item = &str>) -> Result<(String, String), String> {
    let arg = parse_bulk_string(lines)?;
    let value = parse_bulk_string(lines)?;
    Ok((arg, value))
}

// Utilities
fn parse_optional<T, F>(func: F, lines: &mut dyn Iterator<Item = &str>) -> Option<T>
where
    F: Fn(&mut dyn Iterator<Item = &str>) -> Result<T, String>,
{
    let mut lines = lines.peekable();
    match func(&mut lines) {
        Ok(value) => Some(value),
        Err(_) => None,
    }
}
