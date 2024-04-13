use std::{
    io::{self, Error},
    sync::Arc,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use crate::{context::StorageContext, format::format_error_simple_string, parser::parse_input};

pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub async fn read(&mut self) -> io::Result<String> {
        let mut input = [0; 512];

        loop {
            self.stream.readable().await?;
            let bytes_read = self.stream.read(&mut input).await?;
            if bytes_read == 0 {
                continue;
            }

            return String::from_utf8(input[..bytes_read].into())
                .map_err(|_| Error::new(io::ErrorKind::InvalidData, "received invalid utf-8"));
        }
    }

    pub async fn write(&mut self, response: String) -> io::Result<()> {
        self.stream.writable().await?;
        self.stream
            .write_all(format!("{}\r\n", response).as_bytes())
            .await?;
        Ok(())
    }

    async fn handle_client_loop(
        &mut self,
        context: Arc<Mutex<StorageContext>>,
    ) -> Result<(), String> {
        loop {
            let input = self.read().await.map_err(|e| e.to_string())?;
            let command = parse_input(&input)?;
            let response = {
                let mut guard = context.lock().await;
                guard.execute_command(command)?
            };
            self.write(response)
                .await
                .map_err(|e| format!("error: {}", e))?;
        }
    }

    pub async fn handle(&mut self, context: Arc<Mutex<StorageContext>>) -> io::Result<()> {
        match self.handle_client_loop(context).await {
            Ok(_) => Ok(()),
            Err(err) => self.write(format_error_simple_string(&err)).await,
        }
    }
}
