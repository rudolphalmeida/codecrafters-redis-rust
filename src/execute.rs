use crate::{
    format::{format_bulk_string, format_success_simple_string},
    parser::RedisCommand,
};

pub fn execute_command(command: RedisCommand) -> Result<String, String> {
    Ok(match command {
        RedisCommand::Ping => format_success_simple_string("PONG"),
        RedisCommand::Echo(line) => format_bulk_string(&line),
    })
}
