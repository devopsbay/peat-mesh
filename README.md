# peat-mesh

The networking layer that lets heterogeneous systems — servers, edge nodes, mobile devices — find each other, sync state, and coordinate across any transport. Part of the [Peat](https://github.com/defenseunicorns/peat) ecosystem.

## Overview

peat-mesh is the connective tissue between diverse systems in a Peat mesh. A Kubernetes pod running on a server, a Raspberry Pi in the field, and a phone running ATAK all use peat-mesh to discover peers, sync CRDT state, and route data — over QUIC, BLE, or both simultaneously.

Core capabilities:

- **Transport** - Pluggable transports (UDP bypass, BLE) with connection health monitoring and reconnection
- **Security** - Ed25519 identity, X25519 key exchange, ChaCha20-Poly1305 encryption, formation keys
- **Discovery** - mDNS and static peer discovery with hybrid strategies
- **Topology** - Dynamic topology management with partition detection and autonomous operation
- **Routing** - Mesh routing with data aggregation and deduplication
- **Storage** - Automerge CRDT backend with Iroh P2P sync, negentropy set reconciliation, redb persistence, streaming large-blob transfer, and per-collection CRDT compaction
- **Beacon** - Geographic beacon broadcasting and observation with geohash indexing
- **QoS** - Bandwidth management, TTL, retention policies, sync modes, and garbage collection
- **Broker** - Optional HTTP/WebSocket service broker (Axum-based)

## Installation

```toml
[dependencies]
peat-mesh = "0.3.2"
```

### Feature flags

| Feature | Description |
|---------|-------------|
| `automerge-backend` | Automerge CRDT storage with Iroh P2P sync and redb persistence |
| `bluetooth` | BLE mesh transport via [peat-btle](https://crates.io/crates/peat-btle) |
| `broker` | HTTP/WebSocket service broker (Axum) |
| `kubernetes` | Kubernetes peer discovery via EndpointSlice API ([guide](docs/kubernetes.md)) |
| `node` | All-in-one feature for the `peat-mesh-node` binary (includes broker, kubernetes, automerge-backend) |
| `lite-bridge` | UDP bridge for Peat-Lite embedded devices (OTA, telemetry) |

```toml
# Example with features
peat-mesh = { version = "0.1.0", features = ["automerge-backend", "bluetooth"] }
```

## Quick start

```rust
use peat_mesh::{PeatMeshBuilder, MeshConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mesh = PeatMeshBuilder::new()
        .with_config(MeshConfig::default())
        .build()
        .await?;

    // mesh is ready for peer discovery and data sync
    Ok(())
}
```

## Streaming Transfer

peat-mesh includes a streaming large-blob transfer module for bounded-memory file transfer across DDIL links. Transfers use O(chunk_size) memory regardless of blob size, with incremental SHA256 verification and resumable sessions via periodic checkpointing.

```rust
use peat_mesh::storage::{StreamingTransferConfig, TransferCheckpoint};

// Pre-built profiles for different network conditions
let config = StreamingTransferConfig::tactical(); // 256 KiB chunks, 30s checkpoint interval
// Also available: StreamingTransferConfig::datacenter() and ::edge()

// Resume from a previous checkpoint
let checkpoint = TransferCheckpoint::load("transfer-session.json")?;
```

| Profile | Chunk Size | Checkpoint Interval | Use Case |
|---------|-----------|-------------------|----------|
| `datacenter()` | 1 MiB | 60s | High-bandwidth, reliable links |
| `tactical()` | 256 KiB | 30s | Intermittent tactical networks |
| `edge()` | 64 KiB | 10s | Low-bandwidth edge/BTLE links |

## CRDT Compaction

Long-running nodes accumulate Automerge revision history that grows unbounded, eventually causing OOM on memory-constrained devices like ATAK/Android. Per-collection compaction addresses this by periodically discarding change history via `fork()` for collections that don't need it.

**Compaction is sync-mode-aware.** It only runs on `LatestOnly` collections (beacons, platforms, tracks, node_states, etc.) which already send full document state during sync. `FullHistory` collections (commands, audit_logs) are never compacted — their change history is needed for incremental delta sync.

```bash
# Enable with auto-derived safe collections
PEAT_COMPACTION_ENABLED=true

# Or specify explicit collections
PEAT_COMPACTION_ENABLED=true
PEAT_COMPACTION_COLLECTIONS=beacons,platforms,node_states

# Tune for mobile/tactical (more aggressive)
PEAT_COMPACTION_INTERVAL_SECS=60
PEAT_COMPACTION_THRESHOLD_BYTES=16384
```

Compaction is disabled by default. See the [Deployment Guide](docs/deployment.md#automerge-compaction) for full configuration details.

## Kubernetes Deployment

peat-mesh includes a binary target (`peat-mesh-node`), Dockerfile, and Helm chart for Kubernetes deployment.

```bash
# Build the binary
cargo build --release --bin peat-mesh-node --features node

# Build Docker image
docker build -t peat-mesh-node:latest -f deploy/Dockerfile .

# Deploy to k3d (local Kubernetes)
k3d cluster create peat-alpha
k3d image import peat-mesh-node:latest -c peat-alpha
helm install peat-mesh deploy/helm/peat-mesh \
  --set "formationSecret=$(openssl rand -base64 32)" \
  --set replicaCount=3
```

See the full [Deployment Guide](docs/deployment.md) for details on configuration, verification, cluster lifecycle, and multi-cluster federation testing.

## Development

```bash
# Build (default features)
cargo build

# Build with all optional features
cargo build --features automerge-backend,bluetooth,broker

# Run tests
cargo test

# Clippy
cargo clippy -- -D warnings
```

## CI

CI runs via [GOA](https://github.com/radicle-dev/goa) (GitOps Agent) on every Radicle patch:

1. **Format** - `cargo fmt --check`
2. **Clippy** - `cargo clippy -- -D warnings`
3. **Tests** - `cargo test`
4. **Feature builds** - `automerge-backend`, `bluetooth`, `broker`

Results are posted as patch reviews (accept/reject). Community patches require a delegate "ok-to-test" comment before CI runs.

### Manual CI

```bash
# Run the full CI pipeline locally
bash .goa
```

## Source

- **GitHub**: [peat-mesh](https://github.com/defenseunicorns/peat-mesh)
- **crates.io**: [peat-mesh](https://crates.io/crates/peat-mesh)

## Roadmap

**Phase 1 — Complete**
- P2P topology with QUIC/Iroh transport
- Automerge CRDT sync with negentropy reconciliation
- Ed25519 identity, X25519 key exchange, ChaCha20-Poly1305 encryption
- Certificate-based enrollment and tactical trust hierarchy ([ADR-0006](docs/adr/0006-membership-certificates-tactical-trust.md))
- Kubernetes deployment with EndpointSlice discovery ([ADR-0001](docs/adr/0001-kubernetes-istio-deployment.md))
- Streaming blob transfer with checkpoint/resume ([ADR-055](docs/adr/055-streaming-blob-transfer.md))
- mDNS and hybrid peer discovery ([ADR-0003](docs/adr/0003-peer-discovery-architecture.md))
- Graceful shutdown, lock ordering, operation timeouts

**Phase 2 — Planned**
- MLS group key agreement for forward secrecy ([ADR-0005](docs/adr/0005-e2e-encryption-key-management.md))
- DHT peer discovery via Iroh/Pkarr ([ADR-0010](docs/adr/0010-dht-peer-discovery.md))
- Full PACE transport failover wiring ([ADR-0004](docs/adr/0004-pluggable-transport-abstraction.md))
- Zarf/UDS integration patterns ([ADR-0008](docs/adr/0008-zarf-uds-integration.md))

**Phase 3 — Future**
- Multi-language SDK: Go, Python, Kotlin bindings ([ADR-0009](docs/adr/0009-sdk-integration.md))
- Signed CRDT operations for mutation attribution
- Formal benchmark suite

## License

Apache-2.0

## OTA Firmware Updates

peat-mesh nodes can push firmware updates to Peat-Lite ESP32 devices over UDP:

```bash
# Via HTTP API (when running as peat-mesh-node)
curl -X POST http://localhost:3000/api/v1/ota/<peer_id> \
  -F "firmware=@firmware.bin" -F "version=0.2.0"

# Check progress
curl http://localhost:3000/api/v1/ota/<peer_id>/status
```

The OTA sender implements stop-and-wait reliable transfer with SHA256 verification and optional Ed25519 signing. See [ADR-047](https://github.com/defenseunicorns/peat-mesh/blob/main/docs/adr/047-firmware-ota-distribution.md) for protocol details.
