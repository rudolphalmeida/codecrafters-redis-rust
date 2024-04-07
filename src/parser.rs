//! Redis protocol spec: https://redis.io/docs/reference/protocol-spec/

use std::str::Lines;

#[derive(Debug, Clone, Default)]
pub enum RedisCommand {
    #[default]
    Ping,
    Echo(String),
    Get(String),
    Set(String, String),
}

pub fn parse_input(input: &str) -> Result<RedisCommand, String> {
    let mut lines = input.lines();
    let first_line = lines.next().ok_or("no line found")?;
    let num_lines: usize = first_line[1..]
        .parse()
        .map_err(|_| "expected first line to have format *<size>".to_string())?;
    if first_line.chars().next().unwrap_or('_') != '*' || num_lines < 1 {
        return Err("expected first line to have format *<size> where size >= 1".to_string());
    }

    let command = parse_bulk_string(&mut lines)?;
    // commands are case-insensitive
    match command.to_lowercase().as_str() {
        "ping" => return Ok(RedisCommand::Ping),
        "echo" => return Ok(parse_echo_command(&mut lines)?),
        "get" => return Ok(parse_get_command(&mut lines)?),
        "set" => return Ok(parse_set_command(&mut lines)?),
        _ => return Err(format!("unknown command '{}'", command)),
    }
}

fn parse_bulk_string(lines: &mut Lines) -> Result<String, String> {
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
        return Err("expected size line to begin with '$'".to_string());
    }
    let size: i32 = line[1..]
        .parse()
        .map_err(|_| format!("could not parse size on {}", line))?;

    Ok(size)
}

fn parse_echo_command(lines: &mut Lines) -> Result<RedisCommand, String> {
    let line = parse_bulk_string(lines)?;
    Ok(RedisCommand::Echo(line))
}

fn parse_get_command(lines: &mut Lines) -> Result<RedisCommand, String> {
    let line = parse_bulk_string(lines)?;
    Ok(RedisCommand::Get(line))
}

fn parse_set_command(lines: &mut Lines) -> Result<RedisCommand, String> {
    let key = parse_bulk_string(lines)?;
    let value = parse_bulk_string(lines)?;
    Ok(RedisCommand::Set(key, value))
}
