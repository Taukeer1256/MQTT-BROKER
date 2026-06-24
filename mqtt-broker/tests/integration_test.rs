use std::time::Duration;

use bytes::{BufMut, BytesMut};
use mqtt_broker::broker::Broker;
use mqtt_broker::packet::connect::{ConnectPacket, CONNACK_ACCEPTED};
use mqtt_broker::packet::publish::PublishPacket;
use mqtt_broker::packet::{encode_string, MqttPacket, PacketType, QoS};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

fn encode_connect(client_id: &str) -> BytesMut {
    let mut payload = BytesMut::new();
    let connect = ConnectPacket {
        protocol_name: "MQTT".into(),
        protocol_level: 4,
        clean_session: true,
        will_flag: false,
        will_qos: 0,
        will_retain: false,
        password_flag: false,
        username_flag: false,
        keep_alive: 60,
        client_id: client_id.into(),
        will_topic: None,
        will_message: None,
        username: None,
        password: None,
    };
    connect.encode(&mut payload);

    let mut buf = BytesMut::new();
    MqttPacket {
        packet_type: PacketType::Connect,
        flags: 0,
        payload: payload.freeze(),
    }
    .encode(&mut buf);
    buf
}

fn encode_subscribe(packet_id: u16, topic: &str, qos: QoS) -> BytesMut {
    let mut payload = BytesMut::new();
    payload.put_u16(packet_id);
    encode_string(&mut payload, topic);
    payload.put_u8(qos.as_u8());

    let mut buf = BytesMut::new();
    MqttPacket {
        packet_type: PacketType::Subscribe,
        flags: 0x02,
        payload: payload.freeze(),
    }
    .encode(&mut buf);
    buf
}

fn encode_publish(topic: &str, payload_data: &[u8], qos: QoS, packet_id: Option<u16>) -> BytesMut {
    let publish = PublishPacket {
        dup: false,
        qos,
        retain: false,
        topic: topic.into(),
        packet_id,
        payload: payload_data.to_vec().into(),
    };
    let mqtt = publish.encode();
    let mut buf = BytesMut::new();
    mqtt.encode(&mut buf);
    buf
}

async fn read_mqtt_packet(stream: &mut TcpStream) -> Option<MqttPacket> {
    let mut buf = BytesMut::with_capacity(4096);
    loop {
        if let Ok(Some(packet)) = MqttPacket::decode(&mut buf) {
            return Some(packet);
        }
        let n = stream.read_buf(&mut buf).await.ok()?;
        if n == 0 {
            return None;
        }
    }
}

async fn spawn_test_broker() -> std::net::SocketAddr {
    let (_, addr) = Broker::spawn_background("127.0.0.1:0".parse().unwrap())
        .await
        .expect("broker should bind");
    addr
}

#[tokio::test]
async fn connect_and_receive_connack() {
    let addr = spawn_test_broker().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream.write_all(&encode_connect("test-client")).await.unwrap();

    let packet = timeout(Duration::from_secs(2), read_mqtt_packet(&mut stream))
        .await
        .expect("timed out waiting for CONNACK")
        .expect("connection closed");

    assert_eq!(packet.packet_type, PacketType::ConnAck);
    assert_eq!(packet.payload[1], CONNACK_ACCEPTED);
}

#[tokio::test]
async fn pub_sub_qos0_delivery() {
    let addr = spawn_test_broker().await;

    let mut publisher = TcpStream::connect(addr).await.unwrap();
    publisher
        .write_all(&encode_connect("publisher"))
        .await
        .unwrap();
    read_mqtt_packet(&mut publisher).await; // CONNACK

    let mut subscriber = TcpStream::connect(addr).await.unwrap();
    subscriber
        .write_all(&encode_connect("subscriber"))
        .await
        .unwrap();
    read_mqtt_packet(&mut subscriber).await; // CONNACK

    subscriber
        .write_all(&encode_subscribe(1, "test/topic", QoS::AtMostOnce))
        .await
        .unwrap();
    read_mqtt_packet(&mut subscriber).await; // SUBACK

    publisher
        .write_all(&encode_publish(
            "test/topic",
            b"hello mqtt",
            QoS::AtMostOnce,
            None,
        ))
        .await
        .unwrap();

    let delivered = timeout(Duration::from_secs(2), read_mqtt_packet(&mut subscriber))
        .await
        .expect("timed out waiting for PUBLISH")
        .expect("connection closed");

    assert_eq!(delivered.packet_type, PacketType::Publish);
    let publish = PublishPacket::decode(delivered.flags, &delivered.payload).unwrap();
    assert_eq!(publish.topic, "test/topic");
    assert_eq!(publish.payload.as_ref(), b"hello mqtt");
}

#[tokio::test]
async fn pub_sub_qos1_with_puback() {
    let addr = spawn_test_broker().await;

    let mut publisher = TcpStream::connect(addr).await.unwrap();
    publisher
        .write_all(&encode_connect("pub-qos1"))
        .await
        .unwrap();
    read_mqtt_packet(&mut publisher).await;

    let mut subscriber = TcpStream::connect(addr).await.unwrap();
    subscriber
        .write_all(&encode_connect("sub-qos1"))
        .await
        .unwrap();
    read_mqtt_packet(&mut subscriber).await;

    subscriber
        .write_all(&encode_subscribe(1, "qos1/topic", QoS::AtLeastOnce))
        .await
        .unwrap();
    read_mqtt_packet(&mut subscriber).await;

    publisher
        .write_all(&encode_publish(
            "qos1/topic",
            b"qos1 message",
            QoS::AtLeastOnce,
            Some(42),
        ))
        .await
        .unwrap();

    let puback = timeout(Duration::from_secs(2), read_mqtt_packet(&mut publisher))
        .await
        .expect("timed out waiting for PUBACK")
        .expect("connection closed");
    assert_eq!(puback.packet_type, PacketType::PubAck);

    let delivered = timeout(Duration::from_secs(2), read_mqtt_packet(&mut subscriber))
        .await
        .expect("timed out waiting for delivered PUBLISH")
        .expect("connection closed");
    assert_eq!(delivered.packet_type, PacketType::Publish);
    let publish = PublishPacket::decode(delivered.flags, &delivered.payload).unwrap();
    assert_eq!(publish.qos, QoS::AtLeastOnce);
    assert!(publish.packet_id.is_some());
}

#[tokio::test]
async fn ping_req_receives_ping_resp() {
    let addr = spawn_test_broker().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream.write_all(&encode_connect("ping-client")).await.unwrap();
    read_mqtt_packet(&mut stream).await;

    let mut ping = BytesMut::new();
    MqttPacket {
        packet_type: PacketType::PingReq,
        flags: 0,
        payload: bytes::Bytes::new(),
    }
    .encode(&mut ping);
    stream.write_all(&ping).await.unwrap();

    let resp = timeout(Duration::from_secs(2), read_mqtt_packet(&mut stream))
        .await
        .expect("timed out waiting for PINGRESP")
        .expect("connection closed");
    assert_eq!(resp.packet_type, PacketType::PingResp);
}

#[tokio::test]
async fn wildcard_subscription_matches() {
    let addr = spawn_test_broker().await;

    let mut publisher = TcpStream::connect(addr).await.unwrap();
    publisher.write_all(&encode_connect("wild-pub")).await.unwrap();
    read_mqtt_packet(&mut publisher).await;

    let mut subscriber = TcpStream::connect(addr).await.unwrap();
    subscriber
        .write_all(&encode_connect("wild-sub"))
        .await
        .unwrap();
    read_mqtt_packet(&mut subscriber).await;

    subscriber
        .write_all(&encode_subscribe(1, "sensors/+/temp", QoS::AtMostOnce))
        .await
        .unwrap();
    read_mqtt_packet(&mut subscriber).await;

    publisher
        .write_all(&encode_publish(
            "sensors/living/temp",
            b"21.0",
            QoS::AtMostOnce,
            None,
        ))
        .await
        .unwrap();

    let delivered = timeout(Duration::from_secs(2), read_mqtt_packet(&mut subscriber))
        .await
        .expect("timed out")
        .expect("connection closed");
    let publish = PublishPacket::decode(delivered.flags, &delivered.payload).unwrap();
    assert_eq!(publish.topic, "sensors/living/temp");
}
