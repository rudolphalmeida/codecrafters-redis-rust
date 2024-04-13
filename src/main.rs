use std::{env::Args, io, process::exit, sync::Arc};

use connection::{write_response, ClientConnection, Connection};
use format::format_resp_array;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use context::StorageContext;

mod connection;
mod context;
mod format;
mod parser;

#[tokio::main]
async fn main() -> Result<(), String> {
    let config = Config::from_args(std::env::args()).unwrap_or_else(|err| {
        eprintln!("Error: {}", err);
        exit(-1);
    });
    let context = Arc::new(Mutex::new(StorageContext::new(&config)));

    if let Some((ref ip, port)) = config.replica_of {
        let mut stream = TcpStream::connect(format!("{}:{}", ip, port))
            .await
            .unwrap();
        write_response(&mut stream, format_resp_array("ping"))
            .await
            .unwrap();

        stream
            .readable()
            .await
            .map_err(|e| format!("error code: {}", e))
            .unwrap();
        let mut input = [0; 512];
        let bytes_read = stream.read(&mut input).await.unwrap();
        if bytes_read == 0 {
            return Err("failed to read response to PING".to_string());
        }
        let _response = String::from_utf8(input.into()).unwrap();
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port))
        .await
        .unwrap();
    client_listener_loop(listener, context).await.unwrap();

    Ok(())
}

async fn client_listener_loop(
    listener: TcpListener,
    context: Arc<Mutex<StorageContext>>,
) -> io::Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        let context = Arc::clone(&context);
        tokio::spawn(async move {
            let _ = ClientConnection::new(socket).handle(context).await;
        });
    }
}

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
        port.parse::<u16>().map_err(|e| e.to_string())
    }

    fn read_replicaof(args: &mut dyn Iterator<Item = String>) -> Result<(String, u16), String> {
        let ip = args
            .next()
            .ok_or("expected ip address of master to follow --replicaof".to_string())?;
        let port = Config::read_port(args)?;
        Ok((ip, port))
    }
}
