use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

pub mod connect;
pub mod publish;
pub mod subscribe;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    ConnAck = 2,
    Publish = 3,
    PubAck = 4,
    Subscribe = 8,
    SubAck = 9,
    PingReq = 12,
    PingResp = 13,
    Disconnect = 14,
}

impl PacketType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Connect),
            2 => Some(Self::ConnAck),
            3 => Some(Self::Publish),
            4 => Some(Self::PubAck),
            8 => Some(Self::Subscribe),
            9 => Some(Self::SubAck),
            12 => Some(Self::PingReq),
            13 => Some(Self::PingResp),
            14 => Some(Self::Disconnect),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl QoS {
    pub fn from_u8(value: u8) -> Result<Self, PacketError> {
        match value {
            0 => Ok(Self::AtMostOnce),
            1 => Ok(Self::AtLeastOnce),
            2 => Err(PacketError::UnsupportedQoS),
            v => Err(PacketError::InvalidQoS(v)),
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Error)]
pub enum PacketError {
    #[error("incomplete packet: need {needed} bytes, have {available}")]
    Incomplete { needed: usize, available: usize },
    #[error("invalid remaining length encoding")]
    InvalidRemainingLength,
    #[error("unknown packet type: {0}")]
    UnknownPacketType(u8),
    #[error("invalid QoS level: {0}")]
    InvalidQoS(u8),
    #[error("QoS 2 is not supported")]
    UnsupportedQoS,
    #[error("invalid UTF-8 string")]
    InvalidString,
    #[error("protocol error: {0}")]
    Protocol(String),
}

pub struct MqttPacket {
    pub packet_type: PacketType,
    pub flags: u8,
    pub payload: Bytes,
}

impl MqttPacket {
    pub fn decode(buf: &mut BytesMut) -> Result<Option<Self>, PacketError> {
        if buf.is_empty() {
            return Ok(None);
        }

        let header_byte = buf[0];
        let packet_type = PacketType::from_u8(header_byte >> 4)
            .ok_or(PacketError::UnknownPacketType(header_byte >> 4))?;
        let flags = header_byte & 0x0F;

        let mut reader = &buf[1..];
        let (remaining_length, consumed) = decode_remaining_length(&mut reader)?;
        let header_size = 1 + consumed;
        let total_size = header_size + remaining_length;

        if buf.len() < total_size {
            return Ok(None);
        }

        let payload = buf.split_to(total_size).freeze().slice(header_size..total_size);

        Ok(Some(Self {
            packet_type,
            flags,
            payload,
        }))
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        let header_byte = ((self.packet_type as u8) << 4) | (self.flags & 0x0F);
        buf.put_u8(header_byte);
        encode_remaining_length(buf, self.payload.len());
        buf.put_slice(&self.payload);
    }
}

pub fn decode_remaining_length(buf: &mut &[u8]) -> Result<(usize, usize), PacketError> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    let mut consumed = 0usize;

    loop {
        if buf.is_empty() {
            return Err(PacketError::Incomplete {
                needed: consumed + 1,
                available: consumed,
            });
        }
        let byte = buf[0];
        buf.advance(1);
        consumed += 1;

        value += (byte as usize & 0x7F) * multiplier;
        if multiplier > 128 * 128 * 128 {
            return Err(PacketError::InvalidRemainingLength);
        }
        multiplier *= 128;

        if byte & 0x80 == 0 {
            break;
        }
    }

    Ok((value, consumed))
}

pub fn encode_remaining_length(buf: &mut BytesMut, length: usize) {
    let mut value = length;
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

pub fn decode_string(buf: &mut &[u8]) -> Result<String, PacketError> {
    if buf.len() < 2 {
        return Err(PacketError::Incomplete {
            needed: 2,
            available: buf.len(),
        });
    }
    let len = buf.get_u16() as usize;
    if buf.len() < len {
        return Err(PacketError::Incomplete {
            needed: 2 + len,
            available: buf.len() + 2,
        });
    }
    let s = std::str::from_utf8(&buf[..len]).map_err(|_| PacketError::InvalidString)?;
    let result = s.to_owned();
    buf.advance(len);
    Ok(result)
}

pub fn encode_string(buf: &mut BytesMut, s: &str) {
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_remaining_length() {
        for len in [0, 127, 128, 16383, 2097151] {
            let mut buf = BytesMut::new();
            encode_remaining_length(&mut buf, len);
            let mut slice = buf.as_ref();
            let (decoded, _) = decode_remaining_length(&mut slice).unwrap();
            assert_eq!(decoded, len);
        }
    }
}
