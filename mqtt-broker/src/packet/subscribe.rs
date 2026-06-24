use bytes::{Buf, BufMut, BytesMut};

use super::{decode_string, PacketError, PacketType, QoS, MqttPacket};

pub const SUBACK_QOS0: u8 = 0x00;
pub const SUBACK_QOS1: u8 = 0x01;
pub const SUBACK_FAILURE: u8 = 0x80;

#[derive(Debug, Clone)]
pub struct Subscription {
    pub topic_filter: String,
    pub requested_qos: QoS,
}

#[derive(Debug, Clone)]
pub struct SubscribePacket {
    pub packet_id: u16,
    pub subscriptions: Vec<Subscription>,
}

impl SubscribePacket {
    pub fn decode(_flags: u8, payload: &[u8]) -> Result<Self, PacketError> {
        if payload.len() < 2 {
            return Err(PacketError::Incomplete {
                needed: 2,
                available: payload.len(),
            });
        }

        let mut buf = payload;
        let packet_id = buf.get_u16();
        let mut subscriptions = Vec::new();

        while !buf.is_empty() {
            let topic_filter = decode_string(&mut buf)?;
            if buf.is_empty() {
                return Err(PacketError::Incomplete {
                    needed: payload.len() + 1,
                    available: payload.len(),
                });
            }
            let qos = QoS::from_u8(buf.get_u8() & 0x03)?;
            subscriptions.push(Subscription {
                topic_filter,
                requested_qos: qos,
            });
        }

        Ok(Self {
            packet_id,
            subscriptions,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SubAckPacket {
    pub packet_id: u16,
    pub return_codes: Vec<u8>,
}

impl SubAckPacket {
    pub fn new(packet_id: u16, return_codes: Vec<u8>) -> Self {
        Self {
            packet_id,
            return_codes,
        }
    }

    pub fn encode(&self) -> MqttPacket {
        let mut payload = BytesMut::with_capacity(2 + self.return_codes.len());
        payload.put_u16(self.packet_id);
        for code in &self.return_codes {
            payload.put_u8(*code);
        }

        MqttPacket {
            packet_type: PacketType::SubAck,
            flags: 0,
            payload: payload.freeze(),
        }
    }
}

pub fn ping_resp() -> MqttPacket {
    MqttPacket {
        packet_type: PacketType::PingResp,
        flags: 0,
        payload: bytes::Bytes::new(),
    }
}
