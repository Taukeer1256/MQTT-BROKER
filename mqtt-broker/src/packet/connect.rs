use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{decode_string, encode_string, PacketError, PacketType, MqttPacket};

pub const CONNACK_ACCEPTED: u8 = 0x00;

#[derive(Debug, Clone)]
pub struct ConnectPacket {
    pub protocol_name: String,
    pub protocol_level: u8,
    pub clean_session: bool,
    pub will_flag: bool,
    pub will_qos: u8,
    pub will_retain: bool,
    pub password_flag: bool,
    pub username_flag: bool,
    pub keep_alive: u16,
    pub client_id: String,
    pub will_topic: Option<String>,
    pub will_message: Option<Bytes>,
    pub username: Option<String>,
    pub password: Option<Bytes>,
}

impl ConnectPacket {
    pub fn decode(payload: &[u8]) -> Result<Self, PacketError> {
        let mut buf = payload;

        let protocol_name = decode_string(&mut buf)?;
        if buf.is_empty() {
            return Err(PacketError::Incomplete {
                needed: payload.len() + 1,
                available: payload.len(),
            });
        }
        let protocol_level = buf.get_u8();
        if buf.is_empty() {
            return Err(PacketError::Incomplete {
                needed: payload.len() + 1,
                available: payload.len(),
            });
        }
        let connect_flags = buf.get_u8();
        if buf.len() < 2 {
            return Err(PacketError::Incomplete {
                needed: payload.len() + (2 - buf.len()),
                available: payload.len(),
            });
        }
        let keep_alive = buf.get_u16();

        let clean_session = connect_flags & 0x02 != 0;
        let will_flag = connect_flags & 0x04 != 0;
        let will_qos = (connect_flags >> 3) & 0x03;
        let will_retain = connect_flags & 0x20 != 0;
        let password_flag = connect_flags & 0x40 != 0;
        let username_flag = connect_flags & 0x80 != 0;

        let client_id = decode_string(&mut buf)?;

        let will_topic = if will_flag {
            Some(decode_string(&mut buf)?)
        } else {
            None
        };

        let will_message = if will_flag {
            Some(decode_binary(&mut buf)?)
        } else {
            None
        };

        let username = if username_flag {
            Some(decode_string(&mut buf)?)
        } else {
            None
        };

        let password = if password_flag {
            Some(decode_binary(&mut buf)?)
        } else {
            None
        };

        Ok(Self {
            protocol_name,
            protocol_level,
            clean_session,
            will_flag,
            will_qos,
            will_retain,
            password_flag,
            username_flag,
            keep_alive,
            client_id,
            will_topic,
            will_message,
            username,
            password,
        })
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        encode_string(buf, &self.protocol_name);
        buf.put_u8(self.protocol_level);

        let mut flags = 0u8;
        if self.clean_session {
            flags |= 0x02;
        }
        if self.will_flag {
            flags |= 0x04;
            flags |= (self.will_qos & 0x03) << 3;
            if self.will_retain {
                flags |= 0x20;
            }
        }
        if self.password_flag {
            flags |= 0x40;
        }
        if self.username_flag {
            flags |= 0x80;
        }
        buf.put_u8(flags);
        buf.put_u16(self.keep_alive);

        encode_string(buf, &self.client_id);
        if let Some(ref topic) = self.will_topic {
            encode_string(buf, topic);
        }
        if let Some(ref msg) = self.will_message {
            encode_binary(buf, msg);
        }
        if let Some(ref user) = self.username {
            encode_string(buf, user);
        }
        if let Some(ref pass) = self.password {
            encode_binary(buf, pass);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ConnAckPacket {
    pub session_present: bool,
    pub return_code: u8,
}

impl ConnAckPacket {
    pub fn accepted(session_present: bool) -> Self {
        Self {
            session_present,
            return_code: CONNACK_ACCEPTED,
        }
    }

    pub fn encode(&self) -> MqttPacket {
        let mut payload = BytesMut::with_capacity(2);
        payload.put_u8(u8::from(self.session_present));
        payload.put_u8(self.return_code);

        MqttPacket {
            packet_type: PacketType::ConnAck,
            flags: 0,
            payload: payload.freeze(),
        }
    }
}

fn decode_binary(buf: &mut &[u8]) -> Result<Bytes, PacketError> {
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
    let data = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(data)
}

fn encode_binary(buf: &mut BytesMut, data: &[u8]) {
    buf.put_u16(data.len() as u16);
    buf.put_slice(data);
}
