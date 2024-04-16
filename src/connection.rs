use std::io;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

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

            return Ok(String::from_utf8_lossy(input[..bytes_read].into()).to_string());
        }
    }

    pub async fn write(&mut self, response: String) -> io::Result<()> {
        self.write_bytes(response.as_bytes()).await
    }

    pub async fn write_bytes(&mut self, response: &[u8]) -> io::Result<()> {
        self.stream.writable().await?;
        self.stream.write_all(response).await?;
        Ok(())
    }
}
