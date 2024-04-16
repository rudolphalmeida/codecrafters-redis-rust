use std::{
    collections::HashMap,
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use crate::{
    connection::Connection,
    format::{format_bulk_string_line, format_error_simple_string, format_success_simple_string},
    parser::{parse_input, RedisCommand},
    Config,
};

const NULL_BULK_STRING: &'static str = "$-1\r\n";
const RDB_EMPTY_FILE: [u8; 88] = [
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
    0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69,
    0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
    0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0,
    0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff,
    0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
];

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

#[derive(Debug)]
pub struct AppContext {
    role: Role,
    storage: RwLock<HashMap<String, Value>>,
    replication: Replication,
}

impl AppContext {
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
            storage: RwLock::new(HashMap::new()),
            role,
            replication,
        }
    }

    async fn handle_client_loop(
        self: Arc<Self>,
        connection: &mut Connection,
    ) -> Result<(), String> {
        loop {
            let input = connection.read().await.map_err(|e| e.to_string())?;
            let command = parse_input(&input)?;
            let response = Arc::clone(&self).execute_command(command).await?;
            connection
                .write_bytes(&response)
                .await
                .map_err(|e| format!("error: {}", e))?;
        }
    }

    pub async fn handle(self: Arc<Self>, connection: &mut Connection) -> io::Result<()> {
        match self.handle_client_loop(connection).await {
            Ok(_) => Ok(()),
            Err(err) => connection.write(format_error_simple_string(&err)).await,
        }
    }

    async fn execute_command(self: Arc<Self>, command: RedisCommand) -> Result<Vec<u8>, String> {
        Ok(match command {
            RedisCommand::Ping => format_success_simple_string("PONG").as_bytes().to_vec(),
            RedisCommand::Echo(line) => format_bulk_string_line(&line).as_bytes().to_vec(),
            RedisCommand::Get(key) => self.execute_get_command(&key).await.as_bytes().to_vec(),
            RedisCommand::Set(key, value, timeout) => self
                .execute_set_command(&key, value, timeout)
                .await
                .as_bytes()
                .to_vec(),
            RedisCommand::Info(section) => self.execute_info_command(&section).as_bytes().to_vec(),
            RedisCommand::ReplConf(arg, value) => self
                .execute_replconf_command(arg, value)
                .as_bytes()
                .to_vec(),
            RedisCommand::PSync(arg, value) => self.execute_psync_command(arg, value),
        })
    }

    async fn execute_get_command(self: Arc<Self>, key: &str) -> String {
        if self.storage.read().await.contains_key(key) {
            let value = self.storage.read().await.get(key).unwrap().clone();
            if let Some(timeout) = value.timeout {
                if value.created_on + timeout <= Instant::now() {
                    self.storage.write().await.remove(key);
                    return NULL_BULK_STRING.to_string();
                }
            }
            format_success_simple_string(&value.value)
        } else {
            NULL_BULK_STRING.to_string()
        }
    }

    async fn execute_set_command(
        self: Arc<Self>,
        key: &str,
        value: String,
        timeout: Option<Duration>,
    ) -> String {
        if let Some(timeout) = timeout {
            self.storage
                .write()
                .await
                .insert(key.to_string(), Value::with_timeout(value, timeout));
        } else {
            self.storage
                .write()
                .await
                .insert(key.to_string(), Value::new(value));
        }
        format_success_simple_string("OK")
    }

    fn execute_replconf_command(self: Arc<Self>, _arg: String, _value: String) -> String {
        format_success_simple_string("OK")
    }

    fn execute_psync_command(self: Arc<Self>, arg: String, _value: String) -> Vec<u8> {
        if arg != "?" {
            return format!("unknown option '{}' to PSYNC", arg)
                .as_bytes()
                .to_vec();
        }

        let mut resp =
            format_success_simple_string(&format!("FULLRESYNC {} 0", self.replication.id))
                .as_bytes()
                .to_vec();
        let rdb_resp = self.serialize_rdb();
        resp.extend(&rdb_resp);
        resp
    }

    fn execute_info_command(self: Arc<Self>, section: &str) -> String {
        if section != "replication" {
            return NULL_BULK_STRING.to_string();
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

    fn serialize_rdb(self: Arc<Self>) -> Vec<u8> {
        let size = RDB_EMPTY_FILE.len();
        let mut resp = format!("${size}\r\n").as_bytes().to_vec();
        resp.extend(&RDB_EMPTY_FILE);
        resp
    }
}
