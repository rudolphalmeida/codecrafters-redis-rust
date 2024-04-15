use std::{
    env::Args,
    io::{self, ErrorKind},
    process::exit,
    sync::Arc,
};

use connection::Connection;
use format::format_resp_array;
use tokio::net::{TcpListener, TcpStream};

use context::AppContext;

mod connection;
mod context;
mod format;
mod parser;

#[tokio::main]
async fn main() -> io::Result<()> {
    let config = Config::from_args(std::env::args()).unwrap_or_else(|err| {
        eprintln!("Error: {}", err);
        exit(-1);
    });
    let context = Arc::new(AppContext::new(&config));

    if let Some((ref ip, port)) = config.replica_of {
        let stream = TcpStream::connect(format!("{}:{}", ip, port)).await?;
        let mut connection = Connection::new(stream);
        connection.write(format_resp_array("ping")).await?;
        let response = connection.read().await?;
        if !response.to_lowercase().contains("pong") {
            return Err(io::Error::new(
                ErrorKind::ConnectionAborted,
                "invalid response from master",
            ));
        }
        connection
            .write(format_resp_array(&format!(
                "REPLCONF\nlistening-port\n{}",
                config.port
            )))
            .await?;
        let response = connection.read().await?;
        if !response.to_lowercase().contains("ok") {
            return Err(io::Error::new(
                ErrorKind::ConnectionAborted,
                "invalid response from master",
            ));
        }

        connection
            .write(format_resp_array("REPLCONF\ncapa\npsync2"))
            .await?;
        let response = connection.read().await?;
        if !response.to_lowercase().contains("ok") {
            return Err(io::Error::new(
                ErrorKind::ConnectionAborted,
                "invalid response from master",
            ));
        }

        connection.write(format_resp_array("PSYNC\n?\n-1")).await?;
        let response = connection.read().await?;
        println!("{}", response);
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;
    client_listener_loop(listener, context).await
}

async fn client_listener_loop(listener: TcpListener, context: Arc<AppContext>) -> io::Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        let context = Arc::clone(&context);
        tokio::spawn(async move {
            let mut connection = Connection::new(socket);
            let _ = context.handle(&mut connection).await;
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
