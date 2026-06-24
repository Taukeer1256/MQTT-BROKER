use std::net::SocketAddr;
use std::sync::Arc;

use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{error, info, instrument};

use crate::client::handle_connection;
use crate::router::Router;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct BrokerState {
    pub router: Mutex<Router>,
}

impl BrokerState {
    pub fn new() -> Self {
        Self {
            router: Mutex::new(Router::new()),
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }
}

impl Default for BrokerState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Broker {
    addr: SocketAddr,
    state: Arc<BrokerState>,
}

impl Broker {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            state: BrokerState::shared(),
        }
    }

    pub fn with_state(addr: SocketAddr, state: Arc<BrokerState>) -> Self {
        Self { addr, state }
    }

    pub fn state(&self) -> Arc<BrokerState> {
        Arc::clone(&self.state)
    }

    /// Bind the listener and spawn the accept loop, returning the bound address.
    pub async fn spawn_background(
        addr: SocketAddr,
    ) -> Result<(Arc<BrokerState>, SocketAddr), BrokerError> {
        let listener = TcpListener::bind(addr).await?;
        let bound = listener.local_addr()?;
        let state = BrokerState::shared();

        let state_clone = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = accept_loop(listener, state_clone).await {
                error!(error = %e, "broker accept loop failed");
            }
        });

        Ok((state, bound))
    }

    #[instrument(skip(self), fields(addr = %self.addr))]
    pub async fn run(self) -> Result<(), BrokerError> {
        let listener = TcpListener::bind(self.addr).await?;
        info!(addr = %listener.local_addr()?, "MQTT broker listening");
        accept_loop(listener, self.state).await
    }
}

async fn accept_loop(listener: TcpListener, state: Arc<BrokerState>) -> Result<(), BrokerError> {
    loop {
        let (stream, peer) = listener.accept().await?;
        info!(%peer, "accepted connection");
        let state = Arc::clone(&state);

        tokio::spawn(async move {
            handle_connection(stream, state).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn broker_binds_and_accepts() {
        let (_state, addr) = Broker::spawn_background("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        drop(stream);
    }
}
