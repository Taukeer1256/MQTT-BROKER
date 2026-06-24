use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{decode_string, encode_string, PacketError, PacketType, QoS, MqttPacket};

#[derive(Debug, Clone)]
pub struct PublishPacket {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: String,
    pub packet_id: Option<u16>,
    pub payload: Bytes,
}

impl PublishPacket {
    pub fn decode(flags: u8, payload: &[u8]) -> Result<Self, PacketError> {
        let dup = flags & 0x08 != 0;
        let qos = QoS::from_u8((flags >> 1) & 0x03)?;
        let retain = flags & 0x01 != 0;

        let mut buf = payload;
        let topic = decode_string(&mut buf)?;

        let packet_id = if qos != QoS::AtMostOnce {
            if buf.len() < 2 {
                return Err(PacketError::Incomplete {
                    needed: payload.len() + (2 - buf.len()),
                    available: payload.len(),
                });
            }
            Some(buf.get_u16())
        } else {
            None
        };

        let message = Bytes::copy_from_slice(buf);
        Ok(Self {
            dup,
            qos,
            retain,
            topic,
            packet_id,
            payload: message,
        })
    }

    pub fn encode(&self) -> MqttPacket {
        let mut payload = BytesMut::new();
        encode_string(&mut payload, &self.topic);
        if let Some(id) = self.packet_id {
            payload.put_u16(id);
        }
        payload.put_slice(&self.payload);

        let mut flags = 0u8;
        if self.dup {
            flags |= 0x08;
        }
        flags |= (self.qos.as_u8() & 0x03) << 1;
        if self.retain {
            flags |= 0x01;
        }

        MqttPacket {
            packet_type: PacketType::Publish,
            flags,
            payload: payload.freeze(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PubAckPacket {
    pub packet_id: u16,
}

impl PubAckPacket {
    pub fn new(packet_id: u16) -> Self {
        Self { packet_id }
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PacketError> {
        if payload.len() < 2 {
            return Err(PacketError::Incomplete {
                needed: 2,
                available: payload.len(),
            });
        }
        let mut buf = payload;
        Ok(Self {
            packet_id: buf.get_u16(),
        })
    }

    pub fn encode(&self) -> MqttPacket {
        let mut payload = BytesMut::with_capacity(2);
        payload.put_u16(self.packet_id);

        MqttPacket {
            packet_type: PacketType::PubAck,
            flags: 0,
            payload: payload.freeze(),
        }
    }
}
