use std::{error::Error, io::ErrorKind};

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let _ = handle_connection(socket).await;
        });
    }
}

async fn handle_connection(mut socket: TcpStream) -> Result<(), Box<dyn Error>> {
    loop {
        socket.readable().await?;

        let mut buf = [0; 512];
        match socket.try_read(&mut buf) {
            Ok(0) => break,
            Ok(_) => {
                socket.writable().await?;
                socket.write("+PONG\r\n".as_bytes()).await?;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}
