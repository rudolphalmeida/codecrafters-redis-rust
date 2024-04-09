use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    format::{format_bulk_string_line, format_success_simple_string},
    parser::RedisCommand,
    Config,
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

#[derive(Debug, Default, Clone, Copy)]
enum Role {
    #[default]
    Master,
    Slave,
}

#[derive(Debug, Clone)]
pub struct Replication {
    pub id: String,
    pub offset: u32,
}

#[derive(Debug, Clone)]
pub struct StorageContext {
    role: Role,
    storage: HashMap<String, Value>,
    replication: Replication,
}

impl StorageContext {
    pub fn new(config: &Config) -> Self {
        let replication = Replication {
            id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            offset: 0,
        };
        let role = if config.replica_of.is_some() {
            Role::Slave
        } else {
            Role::Master
        };
        Self {
            storage: HashMap::new(),
            role,
            replication,
        }
    }

    pub fn execute_command(&mut self, command: RedisCommand) -> Result<String, String> {
        Ok(match command {
            RedisCommand::Ping => format_success_simple_string("PONG"),
            RedisCommand::Echo(line) => format_bulk_string_line(&line),
            RedisCommand::Get(key) => self.execute_get_command(&key),
            RedisCommand::Set(key, value, timeout) => {
                self.execute_set_command(&key, value, timeout)
            }
            RedisCommand::Info(section) => self.execute_info_command(&section),
        })
    }

    fn execute_get_command(&mut self, key: &str) -> String {
        if self.storage.contains_key(key) {
            let value = self.storage.get(key).unwrap().clone();
            if let Some(timeout) = value.timeout {
                if value.created_on + timeout <= Instant::now() {
                    self.storage.remove(key);
                    return "$-1".to_string();
                }
            }
            format_success_simple_string(&value.value)
        } else {
            "$-1".to_string()
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

    fn execute_info_command(&self, section: &str) -> String {
        if section != "replication" {
            return "$-1".to_string();
        }

        let role = match self.role {
            Role::Master => "master",
            Role::Slave => "slave",
        };
        let mut additional = String::new();
        if matches!(self.role, Role::Master) {
            additional = format!(
                "master_replid:{}\nmaster_repl_offset:{}",
                self.replication.id, self.replication.offset
            );
        }

        format_bulk_string_line(&format!("role:{}\n{}", role, additional))
    }
}
