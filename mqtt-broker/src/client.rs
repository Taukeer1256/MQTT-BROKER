use std::sync::Arc;

use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, info, instrument, warn};

use crate::broker::BrokerState;
use crate::packet::connect::{ConnectPacket, ConnAckPacket};
use crate::packet::publish::{PubAckPacket, PublishPacket};
use crate::packet::subscribe::{
    ping_resp, SubAckPacket, SubscribePacket, SUBACK_FAILURE, SUBACK_QOS0, SUBACK_QOS1,
};
use crate::packet::{MqttPacket, PacketError, PacketType, QoS};
use crate::router::RoutedMessage;

const READ_BUF_SIZE: usize = 4096;
const OUTBOUND_CHANNEL_SIZE: usize = 64;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("packet error: {0}")]
    Packet(#[from] PacketError),
    #[error("client disconnected")]
    Disconnected,
    #[error("protocol error: {0}")]
    Protocol(String),
}

pub struct ClientHandler {
    stream: TcpStream,
    broker: Arc<BrokerState>,
    client_id: Option<String>,
    outbound: Option<mpsc::Receiver<RoutedMessage>>,
    outbound_tx: mpsc::Sender<RoutedMessage>,
    read_buf: BytesMut,
}

impl ClientHandler {
    pub fn new(stream: TcpStream, broker: Arc<BrokerState>) -> Self {
        let (outbound_tx, outbound) = mpsc::channel(OUTBOUND_CHANNEL_SIZE);
        Self {
            stream,
            broker,
            client_id: None,
            outbound: Some(outbound),
            outbound_tx,
            read_buf: BytesMut::with_capacity(READ_BUF_SIZE),
        }
    }

    #[instrument(skip(self), fields(peer = ?self.stream.peer_addr().ok()))]
    pub async fn run(mut self) -> Result<(), ClientError> {
        let mut outbound = self.outbound.take().unwrap();
        loop {
            tokio::select! {
                read_result = self.read_packet() => {
                    match read_result? {
                        Some(()) => {}
                        None => break,
                    }
                }
                msg = outbound.recv() => {
                    match msg {
                        Some(routed) => self.send_routed_message(routed).await?,
                        None => break,
                    }
                }
            }
        }

        if let Some(ref id) = self.client_id {
            let mut router = self.broker.router.lock().await;
            router.remove_client(id);
            info!(client_id = %id, "client disconnected");
        }

        Ok(())
    }

    async fn read_packet(&mut self) -> Result<Option<()>, ClientError> {
        let n = self.stream.read_buf(&mut self.read_buf).await?;
        if n == 0 {
            return Ok(None);
        }

        loop {
            match MqttPacket::decode(&mut self.read_buf)? {
                Some(packet) => self.handle_packet(packet).await?,
                None => break,
            }
        }

        Ok(Some(()))
    }

    #[instrument(skip(self, packet))]
    async fn handle_packet(&mut self, packet: MqttPacket) -> Result<(), ClientError> {
        debug!(?packet.packet_type, "received packet");

        match packet.packet_type {
            PacketType::Connect => self.handle_connect(&packet).await?,
            PacketType::Publish => self.handle_publish(&packet).await?,
            PacketType::PubAck => self.handle_puback(&packet).await?,
            PacketType::Subscribe => self.handle_subscribe(&packet).await?,
            PacketType::PingReq => self.send_packet(ping_resp()).await?,
            PacketType::Disconnect => return Err(ClientError::Disconnected),
            other => warn!(?other, "unsupported packet type"),
        }

        Ok(())
    }

    async fn handle_connect(&mut self, packet: &MqttPacket) -> Result<(), ClientError> {
        let connect = ConnectPacket::decode(&packet.payload)?;

        if connect.protocol_name != "MQTT" || connect.protocol_level != 4 {
            return Err(ClientError::Protocol(format!(
                "unsupported protocol: {} v{}",
                connect.protocol_name, connect.protocol_level
            )));
        }

        let client_id = if connect.client_id.is_empty() {
            format!("anon-{}", uuid_simple())
        } else {
            connect.client_id.clone()
        };

        {
            let mut router = self.broker.router.lock().await;
            router.register_client(client_id.clone(), self.outbound_tx.clone());
        }

        self.client_id = Some(client_id.clone());
        info!(client_id = %client_id, keep_alive = connect.keep_alive, "client connected");

        let connack = ConnAckPacket::accepted(false);
        self.send_packet(connack.encode()).await?;

        Ok(())
    }

    async fn handle_publish(&mut self, packet: &MqttPacket) -> Result<(), ClientError> {
        let publish = PublishPacket::decode(packet.flags, &packet.payload)?;

        if publish.qos == QoS::AtLeastOnce {
            if let Some(id) = publish.packet_id {
                self.send_packet(PubAckPacket::new(id).encode()).await?;
            }
        }

        let mut router = self.broker.router.lock().await;
        router.publish(publish);

        Ok(())
    }

    async fn handle_puback(&mut self, _packet: &MqttPacket) -> Result<(), ClientError> {
        Ok(())
    }

    async fn handle_subscribe(&mut self, packet: &MqttPacket) -> Result<(), ClientError> {
        let subscribe = SubscribePacket::decode(packet.flags, &packet.payload)?;
        let client_id = self
            .client_id
            .as_ref()
            .ok_or_else(|| ClientError::Protocol("SUBSCRIBE before CONNECT".into()))?
            .clone();

        let mut return_codes = Vec::with_capacity(subscribe.subscriptions.len());

        {
            let mut router = self.broker.router.lock().await;
            for sub in &subscribe.subscriptions {
                let granted = router.subscribe(
                    &client_id,
                    sub.topic_filter.clone(),
                    sub.requested_qos,
                    self.outbound_tx.clone(),
                );
                let code = match granted {
                    QoS::AtMostOnce => SUBACK_QOS0,
                    QoS::AtLeastOnce => SUBACK_QOS1,
                    QoS::ExactlyOnce => SUBACK_FAILURE,
                };
                return_codes.push(code);

                for retained in router.retained_messages_matching(&sub.topic_filter) {
                    let msg = RoutedMessage {
                        publish: retained,
                        packet_id: None,
                    };
                    let _ = self.outbound_tx.try_send(msg);
                }
            }
        }

        let suback = SubAckPacket::new(subscribe.packet_id, return_codes);
        self.send_packet(suback.encode()).await?;

        Ok(())
    }

    async fn send_routed_message(&mut self, routed: RoutedMessage) -> Result<(), ClientError> {
        self.send_packet(routed.publish.encode()).await
    }

    async fn send_packet(&mut self, packet: MqttPacket) -> Result<(), ClientError> {
        let mut buf = BytesMut::new();
        packet.encode(&mut buf);
        self.stream.write_all(&buf).await?;
        Ok(())
    }
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", nanos)
}

/// Public helper for integration tests and benchmarks.
pub async fn handle_connection(stream: TcpStream, broker: Arc<BrokerState>) {
    let handler = ClientHandler::new(stream, broker);
    if let Err(e) = handler.run().await {
        debug!(error = %e, "connection closed");
    }
}
