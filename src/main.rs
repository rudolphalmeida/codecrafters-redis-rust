use std::{error::Error, sync::Arc};

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
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
