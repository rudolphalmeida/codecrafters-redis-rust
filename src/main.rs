use std::{env::Args, error::Error, sync::Arc};

use format::format_error_simple_string;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use context::StorageContext;
use parser::parse_input;

mod context;
mod format;
mod parser;

#[derive(Debug, Clone)]
struct Config {
    pub port: u16,
    pub replica_of: Option<(String, u16)>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 6379,
            replica_of: None,
        }
    }
}

impl Config {
    fn from_args(args: Args) -> Result<Self, String> {
        let mut config = Config::default();

        let mut args = args.skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--port" => config.port = Config::read_port(&mut args)?,
                "--replicaof" => config.replica_of = Some(Config::read_replicaof(&mut args)?),
                unknown_arg => return Err(format!("unknown argument '{}'", unknown_arg)),
            }
        }

        Ok(config)
    }

    fn read_port(args: &mut dyn Iterator<Item = String>) -> Result<u16, String> {
        let port = args.next().ok_or("expected port number to follow --port")?;
        Ok(port.parse::<u16>().map_err(|e| e.to_string())?)
    }

    fn read_replicaof(args: &mut dyn Iterator<Item = String>) -> Result<(String, u16), String> {
        let ip = args.next().ok_or(format!(
            "expected ip address of master to follow --replicaof"
        ))?;
        let port = args.next().ok_or("expected port to follow ip address")?;
        let port = port.parse::<u16>().map_err(|e| e.to_string())?;
        Ok((ip, port))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args(std::env::args())?;
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;
    // TODO: Replace with std::sync::Mutex
    let context = Arc::new(Mutex::new(StorageContext::new(&config)));

    loop {
        let (mut socket, _) = listener.accept().await?;
        let context = Arc::clone(&context);
        tokio::spawn(async move {
            match handle_connection(&mut socket, context).await {
                Ok(_) => {}
                Err(err) => socket
                    .write_all(format_error_simple_string(&err).as_bytes())
                    .await
                    .unwrap(),
            }
        });
    }
}

async fn handle_connection(
    socket: &mut TcpStream,
    context: Arc<Mutex<StorageContext>>,
) -> Result<(), String> {
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
        // TODO: This guard needs to be localized to the `execute_command` line
        let mut guard = context.lock().await;
        let response = guard.execute_command(command)?;
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
