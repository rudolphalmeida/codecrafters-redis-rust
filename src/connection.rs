use std::{io, sync::Arc};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use crate::{context::StorageContext, format::format_error_simple_string, parser::parse_input};

pub trait Connection {
    async fn handle(&mut self, context: Arc<Mutex<StorageContext>>) -> io::Result<()>;
}

pub struct ClientConnection {
    socket: TcpStream,
}

impl ClientConnection {
    pub fn new(socket: TcpStream) -> Self {
        Self { socket }
    }

    async fn handle_client_loop(
        &mut self,
        context: Arc<Mutex<StorageContext>>,
    ) -> Result<(), String> {
        loop {
            self.socket
                .readable()
                .await
                .map_err(|e| format!("error code: {}", e))?;
            let mut input = [0; 512];
            let bytes_read = self
                .socket
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
            self.write_response(response).await?;
        }

        Ok(())
    }

    async fn write_response(&mut self, response: String) -> Result<(), String> {
        self.socket
            .writable()
            .await
            .map_err(|e| format!("error code: {}", e))?;
        self.socket
            .write_all(format!("{}\r\n", response).as_bytes())
            .await
            .map_err(|e| format!("error code: {}", e))
    }
}

impl Connection for ClientConnection {
    async fn handle(&mut self, context: Arc<Mutex<StorageContext>>) -> io::Result<()> {
        match self.handle_client_loop(context).await {
            Ok(_) => Ok(()),
            Err(err) => {
                self.socket
                    .write_all(format_error_simple_string(&err).as_bytes())
                    .await
            }
        }
    }
}
