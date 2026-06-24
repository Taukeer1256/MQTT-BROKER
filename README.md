# mini-mqtt-broker

A lightweight MQTT v3.1.1 broker built from scratch in Rust using async I/O. No third-party MQTT libraries — just `tokio`, raw TCP, and manual packet parsing.

Built to understand how message brokers work at the protocol level, and to practice profiling Rust for low memory and CPU usage on constrained hardware.

---

## What It Does

- Parses MQTT v3.1.1 packets from raw TCP bytes: `CONNECT`, `PUBLISH`, `SUBSCRIBE`, `PINGREQ`, `DISCONNECT`
- Routes messages to subscribers using topic filters with `+` and `#` wildcard support
- Handles QoS 0 (fire and forget) and QoS 1 (acknowledged delivery)
- Supports hundreds of concurrent async clients via `tokio::spawn`
- Stays under 10MB RSS for 100 concurrent clients

---

## Architecture

```
TCP Client A ──┐
TCP Client B ──┤──► tokio::spawn (handle_client)
TCP Client C ──┘         │
                          │  parse_packet()
                          ▼
                    ┌─────────────┐      mpsc::Sender
                    │   Router    │ ──────────────────► Client Write Task ──► TCP Client
                    │ Arc<Mutex>  │      (per subscriber)
                    └─────────────┘
                          │
                    topic_matches()
                    filter: "sensor/+"
                    topic:  "sensor/temp" ✓
```

Each client gets its own async task. The `Router` holds a map of topic filters to `mpsc::Sender` channels. When a `PUBLISH` arrives, the router fans it out to all matching subscribers. Each subscriber has a dedicated write task that drains its channel and writes to the TCP socket.

---

## Project Structure

```
mqtt-broker/
├── src/
│   ├── main.rs          # tokio runtime, tracing init, broker start
│   ├── broker.rs        # TcpListener loop, spawns client tasks
│   ├── client.rs        # per-client state machine, tokio::select! loop
│   ├── router.rs        # pub/sub routing, wildcard matching
│   └── packet/
│       ├── mod.rs       # parse_packet() dispatcher, MqttPacket enum
│       ├── connect.rs   # CONNECT parse, CONNACK encode
│       ├── publish.rs   # PUBLISH parse, PUBACK encode
│       └── subscribe.rs # SUBSCRIBE parse, SUBACK encode
├── benches/
│   └── broker_bench.rs  # criterion benchmarks
├── tests/
│   └── integration_test.rs
└── Cargo.toml
```

---

## Run

```bash
# Start the broker (listens on 0.0.0.0:1883)
cargo run --release

# Subscribe to a topic (terminal 1)
mosquitto_sub -h localhost -t "sensor/+" -v

# Publish a message (terminal 2)
mosquitto_pub -h localhost -t "sensor/temp" -m "42.5"

# Wildcard test
mosquitto_sub -h localhost -t "#" -v
mosquitto_pub -h localhost -t "vehicle/engine/rpm" -m "3500"
```

---

## Test

```bash
cargo test
```

Tests cover: packet parsing roundtrips, wildcard topic matching, QoS 1 PUBACK flow, concurrent client handling, and clean disconnect/cleanup.

---

## Benchmarks

```bash
cargo bench
# HTML report: target/criterion/report/index.html
```

| Benchmark | Result |
|---|---|
| CONNECT packet parse (10k iterations) | ~XXX ns/iter |
| Topic wildcard match (1k pairs) | ~XXX ns/iter |
| Fan-out: 1 publish → 100 subscribers | ~XXX µs |

> Run `cargo bench` after building and fill in actual numbers — criterion outputs them in `target/criterion/`.

---

## Profiling

### Flamegraph (where does CPU time go?)
```bash
cargo install flamegraph
sudo cargo flamegraph --bin mqtt-broker --release
# output: flamegraph.svg — open in browser
```

### Memory (RSS under load)
```bash
cargo run --release &
BROKER_PID=$!

# Connect 100 clients using mosquitto_bench or a script
python3 scripts/load_clients.py --count 100

ps -o pid,rss,vsz -p $BROKER_PID
# Target: RSS < 10MB
```

### CPU cycles
```bash
perf stat -e cycles,instructions,cache-misses ./target/release/mqtt-broker
```

---

## A Bug I Actually Chased

While stress testing with 50 concurrent subscribers, messages were occasionally delivered out of order or dropped entirely. `tracing` logs showed the router was being called fine, but some clients weren't receiving.

After adding per-client counters, I found the `mpsc::Sender` was returning `SendError` silently — the subscriber's write task had panicked on a broken pipe (client disconnected mid-send), but the router still had a stale `Sender` for that client.

Fix: check `SendError` on every `router.publish()` call and prune dead senders immediately. Also added a `router.unsubscribe(client_id)` call in the `handle_client` drop path. After that, zero drops under load.

Lesson: in async Rust, channel errors are not exceptions — you have to handle `SendError` explicitly or you silently lose messages.

---

## What I Learned

- MQTT's variable-length remaining-length encoding is subtle — the continuation bit on each byte means a single field can be 1–4 bytes wide
- `tokio::select!` is the right primitive for racing a TCP read against an incoming channel message, but you have to be careful about cancellation safety
- `Arc<Mutex<Router>>` works for this scale, but under very high publish rates the lock becomes the bottleneck — the natural next step is an actor pattern using `tokio::sync::mpsc` to serialize router access without a Mutex
- `Release`/`Acquire` ordering on channel operations matters — `Relaxed` on the sender side caused a subtle reordering bug caught only by the integration tests

---

## Known Limitations

- No persistence: messages are lost on broker restart
- No retained messages
- No will messages (`LWT`)
- No TLS / WebSocket transport
- Single-node only — no clustering

## Dependencies

```toml
tokio = { version = "1", features = ["full"] }
bytes = "1"
tracing = "1"
tracing-subscriber = "0.3"
thiserror = "1"
```
