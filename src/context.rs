use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    format::{format_bulk_string, format_success_simple_string},
    parser::RedisCommand,
};

#[derive(Debug, Clone)]
struct Value {
    value: String,
    timeout: Option<Duration>,
    created_on: Instant,
}

impl Value {
    pub fn new(value: String) -> Self {
        Self {
            value,
            timeout: None,
            created_on: Instant::now(),
        }
    }

    pub fn with_timeout(value: String, timeout: Duration) -> Self {
        Self {
            value,
            timeout: Some(timeout),
            created_on: Instant::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StorageContext {
    storage: HashMap<String, Value>,
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
            RedisCommand::Set(key, value, timeout) => {
                self.execute_set_command(&key, value, timeout)
            }
        })
    }

    fn execute_get_command(&mut self, key: &str) -> String {
        if self.storage.contains_key(key) {
            let value = self.storage.get(key).unwrap().clone();
            if let Some(timeout) = value.timeout {
                if value.created_on + timeout <= Instant::now() {
                    self.storage.remove(key);
                    return "$-1\r\n".to_string();
                }
            }
            format_success_simple_string(&value.value)
        } else {
            "$-1\r\n".to_string()
        }
    }

    fn execute_set_command(
        &mut self,
        key: &str,
        value: String,
        timeout: Option<Duration>,
    ) -> String {
        if let Some(timeout) = timeout {
            self.storage
                .insert(key.to_string(), Value::with_timeout(value, timeout));
        } else {
            self.storage.insert(key.to_string(), Value::new(value));
        }
        format_success_simple_string("OK")
    }
}
