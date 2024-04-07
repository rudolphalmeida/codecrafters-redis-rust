use std::collections::HashMap;

use crate::{
    format::{format_bulk_string, format_success_simple_string},
    parser::RedisCommand,
};

#[derive(Debug, Clone)]
pub struct StorageContext {
    storage: HashMap<String, String>,
}

impl StorageContext {
    pub fn new() -> Self {
        Self {
            storage: HashMap::new(),
        }
    }

    pub fn execute_command(&mut self, command: RedisCommand) -> Result<String, String> {
        Ok(match command {
            RedisCommand::Ping => format_success_simple_string("PONG"),
            RedisCommand::Echo(line) => format_bulk_string(&line),
            RedisCommand::Get(key) => self.execute_get_command(&key),
            RedisCommand::Set(key, value) => self.execute_set_command(&key, value),
        })
    }

    fn execute_get_command(&self, key: &str) -> String {
        if let Some(value) = self.storage.get(key) {
            format_success_simple_string(&value)
        } else {
            format!("$-1\r\n")
        }
    }

    fn execute_set_command(&mut self, key: &str, value: String) -> String {
        self.storage.insert(key.to_string(), value);
        format_success_simple_string("OK")
    }
}
