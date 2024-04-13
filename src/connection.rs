use std::{io, sync::Arc};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use crate::{
    context::StorageContext,
    format::{format_error_simple_string, format_resp_array},
    parser::parse_input,
};

pub trait Connection {
    async fn handle(&mut self, context: Arc<Mutex<StorageContext>>) -> io::Result<()>;
}

async fn write_response(stream: &mut TcpStream, response: String) -> Result<(), String> {
    stream
        .writable()
        .await
        .map_err(|e| format!("error code: {}", e))?;
    stream
        .write_all(format!("{}\r\n", response).as_bytes())
        .await
        .map_err(|e| format!("error code: {}", e))
}

pub struct ClientConnection {
    stream: TcpStream,
}

impl ClientConnection {
    pub fn new(socket: TcpStream) -> Self {
        Self { stream: socket }
    }

    async fn handle_client_loop(
        &mut self,
        context: Arc<Mutex<StorageContext>>,
    ) -> Result<(), String> {
        loop {
            self.stream
                .readable()
                .await
                .map_err(|e| format!("error code: {}", e))?;
            let mut input = [0; 512];
            let bytes_read = self
                .stream
                .read(&mut input)
                .await
                .map_err(|e| format!("error code: {}", e))?;
            if bytes_read == 0 {
                break;
            }

            let input = String::from_utf8(input.into()).map_err(|_| "invalid utf-8".to_string())?;
            let command = parse_input(&input)?;
            let response = {
                let mut guard = context.lock().await;
                guard.execute_command(command)?
            };
            write_response(&mut self.stream, response).await?;
        }

        Ok(())
    }
}

impl Connection for ClientConnection {
    async fn handle(&mut self, context: Arc<Mutex<StorageContext>>) -> io::Result<()> {
        match self.handle_client_loop(context).await {
            Ok(_) => Ok(()),
            Err(err) => {
                self.stream
                    .write_all(format_error_simple_string(&err).as_bytes())
                    .await
            }
        }
    }
}

pub struct ReplicaConnection {
    stream: TcpStream,
}

impl ReplicaConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    async fn handshake(&mut self) -> Result<(), String> {
        write_response(&mut self.stream, format_resp_array("ping")).await?;

        self.stream
            .readable()
            .await
            .map_err(|e| format!("error code: {}", e))?;
        let mut input = [0; 512];
        let bytes_read = self
            .stream
            .read(&mut input)
            .await
            .map_err(|e| format!("error code: {}", e))?;
        if bytes_read == 0 {
            return Err("failed to read response to PING".to_string());
        }
        let _response = String::from_utf8(input.into()).map_err(|_| "invalid utf-8".to_string())?;

        Ok(())
    }
}

impl Connection for ReplicaConnection {
    async fn handle(&mut self, _context: Arc<Mutex<StorageContext>>) -> io::Result<()> {
        let _ = match self.handshake().await {
            Ok(_) => Ok(()),
            Err(err) => {
                // TODO: Exponential backoff retry
                self.stream
                    .write_all(format_error_simple_string(&err).as_bytes())
                    .await
            }
        };

        Ok(())
    }
}
