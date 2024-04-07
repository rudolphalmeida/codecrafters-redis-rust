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

#[derive(Debug, Clone, Copy)]
struct Config {
    port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self { port: 6379 }
    }
}

impl Config {
    fn from_args(args: Args) -> Result<Self, String> {
        let mut args = args.skip(1);
        let port = if let Some(arg) = args.next() {
            if arg == "--port" {
                let port = args.next().ok_or("expected port number to follow --port")?;
                port.parse::<u16>().map_err(|e| {
                    format!(
                        "failed to parse port number '{}' with '{}'",
                        port,
                        e.to_string()
                    )
                })?
            } else {
                return Err(format!("unknown parameter '{}'", arg));
            }
        } else {
            6379
        };

        Ok(Self { port })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args(std::env::args())?;
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;
    // TODO: Replace with std::sync::Mutex
    let context = Arc::new(Mutex::new(StorageContext::new()));

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
