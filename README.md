# Caminus CDC Engine

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](#) [![Rust Edition](https://img.shields.io/badge/rust-2024-orange)](#) [![License](https://img.shields.io/badge/license-MIT-blue)](#)

**Caminus** is a high-performance, distributed Change Data Capture (CDC) engine written in Rust. Built for ultra-low latency, zero-copy data serialization, and resilience, Caminus bypasses JVM overhead to stream billions of database mutation events seamlessly from transactional logs (PostgreSQL Logical Replication, Cassandra CommitLogs) to downstream sinks (Kafka, Redpanda, S3).

---

## Key Features

- **Pure-Rust Active-Standby Consensus**: Distributed Raft-like election engine for high-availability leader lease coordination.
- **Lock-Free DBLog Watermark Snapshotting**: Netflix DBLog algorithm interleaving table snapshot queries with live stream replication without locking tables.
- **WASM Single Message Transforms (SMTs)**: Sandboxed, high-speed WebAssembly inline transformations powered by Wasmtime.
- **Dynamic Schema Evolution & DLQ**: Versioned schema registry with `BACKWARD`/`FORWARD` compatibility checks and Dead Letter Queue (DLQ) poison pill isolation.
- **Multi-Tenant Partition Router**: Deterministic `KeyHash` and `TenantPrefix` partitioning preserving strict per-primary-key transactional ordering.
- **Adaptive Token-Bucket Rate Limiter**: Async backpressure traffic shaper protecting memory during downstream sink outages.
- **SIMD-Accelerated Serialization**: SIMD-json engine for high-throughput zero-copy JSON output.
- **Prometheus Observability**: Native HTTP metrics scraping endpoint on port `9000` exposing throughput, transformation latency, and replication lag.

---

## Quickstart

### 1. Build and Run Locally
```bash
# Clone repository
git clone https://github.com/ichbingautam/caminus.git
cd caminus

# Run unit and integration benchmark tests
cargo test

# Launch Caminus engine
cargo run --release
```

### 2. Docker Container Deployment
```bash
# Build multi-stage production image
docker build -t caminus:latest .

# Run container
docker run -p 9000:9000 caminus:latest
```

### 3. Kubernetes Deployment via Helm
```bash
helm upgrade --install caminus deploy/helm/caminus
```

---

## Administrative CLI Commands

Caminus includes operator administration utilities for inspecting system status and RocksDB checkpoints:

```bash
# Inspect system status
cargo run -- status

# Inspect stored replication offset checkpoint
cargo run -- inspect-offset postgres_users

# List registered schemas
cargo run -- schema-list postgres_users
```

---

## Architecture Documentation

For in-depth technical specifications and Mermaid pipeline sequence diagrams, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## License

Distributed under the MIT License.
