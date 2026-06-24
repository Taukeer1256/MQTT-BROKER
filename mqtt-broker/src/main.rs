mod broker;
mod client;
mod packet;
mod router;

use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = "0.0.0.0:1883".parse()?;
    broker::Broker::new(addr).run().await?;

    Ok(())
}
