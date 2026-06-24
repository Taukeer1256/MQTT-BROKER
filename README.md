# Mini MQTT Broker

## Architecture

```text
TCP Clients → [tokio::spawn per client] → Client Handler → Router (Arc<Mutex>) → Subscriber Channels → Client Write Tasks
```

## Run
```bash
cargo run
# listens on 0.0.0.0:1883
```

## Test Manually
```bash
# terminal 1 - subscribe
mosquitto_sub -h localhost -t "sensor/+" -v

# terminal 2 - publish
mosquitto_pub -h localhost -t "sensor/temp" -m "42.5"
```

## Benchmarks
```bash
cargo bench
```

| Benchmark            | Time       |
|----------------------|------------|
| packet_parse         | ~XXX ns    |
| topic_match (20)     | ~XXX ns    |
| publish_routing(100) | ~XXX µs    |

## Profiling

### CPU Flamegraph
```bash
cargo install flamegraph
cargo flamegraph --bin mqtt-broker
# opens flamegraph.svg in browser
```

### Memory (RSS)
```bash
cargo run &
ps aux | grep mqtt-broker
# check RSS column - target: < 10MB for 100 clients
```

### perf stat
```bash
perf stat cargo run
```

## Known Limitations
- No persistence (messages lost on restart)
- No TLS support
- No retained messages
- No will messages
- Single-node only (no clustering)

## Future Improvements
- Add WebSocket transport
- Persistent session support
- Metrics endpoint (Prometheus)
- Replace Mutex<Router> with actor pattern using tokio::sync::mpsc
