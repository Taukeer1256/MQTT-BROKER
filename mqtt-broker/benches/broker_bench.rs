use std::net::SocketAddr;

use bytes::{BufMut, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use mqtt_broker::packet::connect::ConnectPacket;
use mqtt_broker::packet::publish::PublishPacket;
use mqtt_broker::packet::subscribe::SubscribePacket;
use mqtt_broker::packet::{MqttPacket, PacketType, QoS};
use mqtt_broker::router::{build_publish, topic_matches, Router};

fn bench_packet_decode_connect(c: &mut Criterion) {
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
        client_id: "bench-client".into(),
        will_topic: None,
        will_message: None,
        username: None,
        password: None,
    };
    connect.encode(&mut payload);

    let mut packet_buf = BytesMut::new();
    MqttPacket {
        packet_type: PacketType::Connect,
        flags: 0,
        payload: payload.freeze(),
    }
    .encode(&mut packet_buf);

    c.bench_function("decode_connect", |b| {
        b.iter(|| {
            let mut buf = packet_buf.clone();
            black_box(MqttPacket::decode(&mut buf).unwrap());
        });
    });
}

fn bench_packet_decode_publish(c: &mut Criterion) {
    let publish = build_publish("sensors/temperature", "22.5", QoS::AtMostOnce, false);
    let mqtt = publish.encode();
    let mut packet_buf = BytesMut::new();
    mqtt.encode(&mut packet_buf);

    c.bench_function("decode_publish", |b| {
        b.iter(|| {
            let mut buf = packet_buf.clone();
            let packet = MqttPacket::decode(&mut buf).unwrap().unwrap();
            black_box(PublishPacket::decode(packet.flags, &packet.payload).unwrap());
        });
    });
}

fn bench_topic_matching(c: &mut Criterion) {
    let mut group = c.benchmark_group("topic_matching");
    group.throughput(Throughput::Elements(1));

    group.bench_function("exact", |b| {
        b.iter(|| black_box(topic_matches("home/living/temp", "home/living/temp")));
    });

    group.bench_function("single_wildcard", |b| {
        b.iter(|| black_box(topic_matches("home/+/temp", "home/living/temp")));
    });

    group.bench_function("multi_wildcard", |b| {
        b.iter(|| black_box(topic_matches("home/#", "home/living/temp")));
    });

    group.finish();
}

fn bench_router_publish(c: &mut Criterion) {
    c.bench_function("router_publish_100_subs", |b| {
        let mut router = Router::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);

        for i in 0..100 {
            router.subscribe(
                &format!("client-{i}"),
                "sensors/#".into(),
                QoS::AtMostOnce,
                tx.clone(),
            );
        }

        let publish = build_publish("sensors/temperature", "22.5", QoS::AtMostOnce, false);

        b.iter(|| {
            black_box(router.publish(publish.clone()));
        });
    });
}

fn bench_subscribe_packet_decode(c: &mut Criterion) {
    let mut payload = BytesMut::new();
    payload.put_u16(1);
    payload.put_u16(8);
    payload.put_slice(b"sensors/#");
    payload.put_u8(1);

    let mut packet_buf = BytesMut::new();
    MqttPacket {
        packet_type: PacketType::Subscribe,
        flags: 0x02,
        payload: payload.freeze(),
    }
    .encode(&mut packet_buf);

    c.bench_function("decode_subscribe", |b| {
        b.iter(|| {
            let mut buf = packet_buf.clone();
            let packet = MqttPacket::decode(&mut buf).unwrap().unwrap();
            black_box(SubscribePacket::decode(packet.flags, &packet.payload).unwrap());
        });
    });
}

criterion_group!(
    benches,
    bench_packet_decode_connect,
    bench_packet_decode_publish,
    bench_subscribe_packet_decode,
    bench_topic_matching,
    bench_router_publish,
);
criterion_main!(benches);
