use anyhow::Result;
use simple_redis::{network, Backend};
use tokio::net::TcpListener;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";
    let listener = TcpListener::bind(addr).await?;
    info!("Simple Redis Server listening on {}", addr);
    let backend = Backend::new();
    loop {
        let (stream, s_addr) = listener.accept().await?;
        info!("Accepted connection from: {}", s_addr);
        let cloned_backend = backend.clone();
        tokio::spawn(async move {
            match network::stream_handler(stream, cloned_backend).await {
                Ok(_) => info!("Connection from {} exited", s_addr),
                Err(e) => warn!("Error handling connection {}: {:?}", s_addr, e),
            }
        });
    }
}
