//! Run: `cargo run -p engine-server -- --listen 127.0.0.1:50051 --log info`

use std::net::SocketAddr;
use std::sync::Arc;

use engine_core::{Engine, EngineConfig};

fn parse_args() -> (SocketAddr, String) {
    let mut listen: SocketAddr = "127.0.0.1:50051".parse().unwrap();
    let mut log = "info".to_string();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                listen = args
                    .next()
                    .expect("--listen ADDR")
                    .parse()
                    .expect("valid socket address")
            }
            "--log" => log = args.next().expect("--log LEVEL"),
            "--help" | "-h" => {
                eprintln!("usage: engine-server [--listen 127.0.0.1:50051] [--log info]");
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument {other}");
                std::process::exit(2);
            }
        }
    }
    (listen, log)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (listen, log) = parse_args();
    tracing_subscriber::fmt().with_env_filter(log).init();
    let engine = Engine::with_connectors(EngineConfig::default(), engine_server::connectors())?;
    tracing::info!(%listen, version = Engine::version(), "engine sidecar listening");
    let runtime_engine = Arc::clone(&engine);
    runtime_engine.runtime().block_on(async move {
        let listener = tokio::net::TcpListener::bind(listen).await?;
        engine_server::serve(engine, listener, async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
