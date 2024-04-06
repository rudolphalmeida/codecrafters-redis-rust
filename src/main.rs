use std::{error::Error, str::Lines};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            match handle_connection(&mut socket).await {
                Ok(_) => {}
                Err(err) => socket
                    // Redis simple error string format: https://redis.io/docs/reference/protocol-spec/
                    .write_all(format!("-{}\r\n", err).as_bytes())
                    .await
                    .unwrap(),
            }
        });
    }
}

#[derive(Debug, Clone, Default)]
enum RedisCommand {
    #[default]
    Ping,
    Echo(String),
}

async fn handle_connection(socket: &mut TcpStream) -> Result<(), String> {
    loop {
        socket
            .readable()
            .await
            .map_err(|e| format!("error code: {}", e.to_string()))?;
        let mut input = [0; 512];
        let bytes_read = socket
            .read(&mut input)
            .await
            .map_err(|e| format!("error code: {}", e.to_string()))?;
        if bytes_read == 0 {
            break;
        }

        let input = String::from_utf8(input.into()).map_err(|_| "invalid utf-8".to_string())?;
        let command = parse_input(&input)?;
        let response = execute_command(command)?;
        write_response(socket, response).await?;
    }

    Ok(())
}

async fn write_response(socket: &mut TcpStream, response: String) -> Result<(), String> {
    socket
        .writable()
        .await
        .map_err(|e| format!("error code: {}", e.to_string()))?;
    socket
        .write_all(format!("{}\r\n", response).as_bytes())
        .await
        .map_err(|e| format!("error code: {}", e.to_string()))
}

fn parse_input(input: &str) -> Result<RedisCommand, String> {
    let mut lines = input.lines();
    let first_line = lines.next().ok_or("no line found")?;
    let num_lines: usize = first_line[1..]
        .parse()
        .map_err(|_| "expected first line to have format *<size>".to_string())?;
    if first_line.chars().next().unwrap_or('_') != '*' || num_lines < 1 {
        return Err("expected first line to have format *<size> where size >= 1".to_string());
    }

    let command = next_input_line(&mut lines)?;
    // commands are case-insensitive
    match command.to_lowercase().as_str() {
        "ping" => return Ok(RedisCommand::Ping),
        "echo" => return Ok(parse_echo_command(&mut lines)?),
        _ => return Err(format!("unknown command '{}'", command)),
    }
}

fn next_input_line(lines: &mut Lines) -> Result<String, String> {
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
    let line = next_input_line(lines)?;
    Ok(RedisCommand::Echo(line))
}

fn execute_command(command: RedisCommand) -> Result<String, String> {
    Ok(match command {
        RedisCommand::Ping => "+PONG".to_string(),
        RedisCommand::Echo(line) => format!("${}\r\n{}", line.len(), line),
    })
}
