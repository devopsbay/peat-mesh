# Deployment Guide

This guide covers building, containerizing, and deploying peat-mesh nodes to Kubernetes clusters using k3d for local development and testing. For code-level API details, see [kubernetes.md](kubernetes.md). For architectural context, see [ADR-0001](adr/0001-kubernetes-istio-deployment.md).

## Table of Contents

- [Binary target](#binary-target)
- [Docker image](#docker-image)
- [Helm chart](#helm-chart)
- [Local cluster with k3d](#local-cluster-with-k3d)
- [Verification](#verification)
- [Cluster lifecycle](#cluster-lifecycle)
- [Two-cluster federation](#two-cluster-federation)
- [Functional testing](#functional-testing)

## Binary target

The `peat-mesh-node` binary is a thin wrapper around the `PeatMesh` library that reads configuration from environment variables, sets up discovery, and serves the broker HTTP/WS API.

### Building

```bash
cargo build --release --bin peat-mesh-node --features node
```

The `node` meta-feature enables: `automerge-backend` + `broker` + `kubernetes` + `k8s-openapi/v1_32` + `tracing-subscriber`.

### Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PEAT_FORMATION_SECRET` | Yes | — | Base64-encoded formation secret (shared by all nodes in a formation) |
| `HOSTNAME` | No | `peat-mesh-0` | Pod name; used as deterministic keypair context (set automatically by K8s) |
| `PEAT_DISCOVERY` | No | `kubernetes` | Discovery mode: `kubernetes` or `mdns` |
| `PEAT_DATA_DIR` | No | `/data` | Persistent data directory (AutomergeStore uses `$PEAT_DATA_DIR/automerge/`) |
| `PEAT_BROKER_PORT` | No | `8081` | HTTP/WS broker listen port |
| `PEAT_IROH_BIND_PORT` | No | `11204` | Iroh QUIC (UDP) listen port |
| `PEAT_IROH_RELAY_URLS` | No | — | Comma-separated Iroh relay URLs for NAT traversal |
| `PEAT_COMPACTION_ENABLED` | No | `false` | Enable per-collection Automerge CRDT compaction |
| `PEAT_COMPACTION_INTERVAL_SECS` | No | `300` | Seconds between compaction sweeps (minimum: 10) |
| `PEAT_COMPACTION_THRESHOLD_BYTES` | No | `65536` | Only compact documents larger than this (bytes) |
| `PEAT_COMPACTION_COLLECTIONS` | No | — | Comma-separated collections to compact (empty = auto-derive from LatestOnly sync modes) |
| `RUST_LOG` | No | `info,peat_mesh=debug` | Tracing filter |

### Running locally

```bash
export PEAT_FORMATION_SECRET=$(openssl rand -base64 32)
export PEAT_DISCOVERY=mdns
./target/release/peat-mesh-node
```

The binary logs structured output via `tracing-subscriber` and shuts down gracefully on SIGTERM or SIGINT.

### Deterministic identity

Each node derives a stable Ed25519 keypair from the formation secret and its hostname using HKDF-SHA256. This means:

- Same `PEAT_FORMATION_SECRET` + same `HOSTNAME` = same `device_id` (survives pod restarts)
- Same secret + different hostname = different `device_id` (each pod gets a unique identity)
- No persistent storage needed for identity

## Docker image

### Building

From the peat-mesh repository root:

```bash
docker build -t peat-mesh-node:latest -f deploy/Dockerfile .
```

The multi-stage build uses:
- **Builder stage**: `rust:1.93-bookworm` with `clang` and `mold` (matching `.cargo/config.toml`)
- **Runtime stage**: `debian:bookworm-slim` with `ca-certificates`, `tini` (PID 1 signal handler), and `curl` (for debugging)

### Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 8081 | TCP | Broker HTTP/WS API |
| 11204 | UDP | Iroh QUIC sync |

### Image size

The runtime image is ~90MB (slim Debian base + statically-linked binary).

## Helm chart

The Helm chart is in `deploy/helm/peat-mesh/`.

### Resources created

| Resource | Purpose |
|----------|---------|
| **StatefulSet** | Stable pod names (`peat-mesh-0`, `-1`, ...) for deterministic keypair derivation |
| **Headless Service** (`clusterIP: None`) | Generates EndpointSlice resources that `KubernetesDiscovery` watches |
| **ServiceAccount** | Pod identity for RBAC |
| **Role** | `list` + `watch` on `endpointslices` in `discovery.k8s.io` |
| **RoleBinding** | Binds Role to ServiceAccount |
| **Secret** | Stores formation secret as a K8s Secret |

### Key design decisions

- **StatefulSet over Deployment**: Stable pod names (`HOSTNAME`) drive deterministic keypair derivation. No PVCs needed since identity is derived from seed + hostname.
- **Headless Service**: Required so that Kubernetes creates `EndpointSlice` resources. `KubernetesDiscovery` watches these with label selector `app=peat-mesh`.
- **RBAC**: Minimal — only EndpointSlice read access for peer discovery.

### Configuration (values.yaml)

```yaml
replicaCount: 3                    # Number of mesh nodes
image:
  repository: peat-mesh-node      # Docker image
  tag: latest
  pullPolicy: IfNotPresent
formationSecret: ""                # Base64-encoded formation secret (required)
discoveryMode: kubernetes          # "kubernetes" or "mdns"
brokerPort: 8081                   # Broker HTTP/WS port
irohBindPort: 11204                # Iroh QUIC UDP port
irohRelayUrls: ""                  # Comma-separated relay URLs
rustLog: "info,peat_mesh=debug"    # Tracing filter
compaction:
  enabled: false                   # Per-collection CRDT compaction (off by default)
  intervalSecs: 300                # Sweep interval (minimum: 10)
  thresholdBytes: 65536            # Only compact docs above this size
  collections: ""                  # Comma-separated collections (empty = auto-derive from LatestOnly)
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: 500m
    memory: 256Mi
```

### Installing

```bash
FORMATION_SECRET=$(openssl rand -base64 32)
helm install peat-mesh deploy/helm/peat-mesh \
  --set "formationSecret=$FORMATION_SECRET" \
  --set replicaCount=3
```

### Upgrading

```bash
helm upgrade peat-mesh deploy/helm/peat-mesh \
  --set "formationSecret=$FORMATION_SECRET" \
  --set replicaCount=5
```

### Uninstalling

```bash
helm uninstall peat-mesh
```

## Local cluster with k3d

[k3d](https://k3d.io/) runs lightweight k3s Kubernetes clusters inside Docker containers. It's ideal for local development and testing.

### Prerequisites

```bash
# kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl && mv kubectl ~/.local/bin/

# k3d
curl -sL https://github.com/k3d-io/k3d/releases/latest/download/k3d-linux-amd64 -o ~/.local/bin/k3d
chmod +x ~/.local/bin/k3d

# helm
curl -sL https://get.helm.sh/helm-v3.17.3-linux-amd64.tar.gz | tar xz -C /tmp
mv /tmp/linux-amd64/helm ~/.local/bin/
```

### Creating a cluster

```bash
k3d cluster create peat-alpha
```

### Importing the Docker image

k3d clusters can't pull from the local Docker daemon directly. Import the image:

```bash
k3d image import peat-mesh-node:latest -c peat-alpha
```

### Full deploy workflow

```bash
# Build
docker build -t peat-mesh-node:latest -f deploy/Dockerfile .

# Create cluster and import image
k3d cluster create peat-alpha
k3d image import peat-mesh-node:latest -c peat-alpha

# Deploy
FORMATION_SECRET=$(openssl rand -base64 32)
helm install peat-mesh deploy/helm/peat-mesh \
  --set "formationSecret=$FORMATION_SECRET" \
  --set replicaCount=3
```

## Verification

### Pod status

```bash
kubectl get pods -l app=peat-mesh
# NAME          READY   STATUS    RESTARTS   AGE
# peat-mesh-0   1/1     Running   0          30s
# peat-mesh-1   1/1     Running   0          25s
# peat-mesh-2   1/1     Running   0          20s
```

### Health and readiness

```bash
# Health (liveness)
kubectl exec peat-mesh-0 -- curl -s localhost:8081/api/v1/health
# {"status":"healthy","node_id":"peat-mesh-0"}

# Readiness
kubectl exec peat-mesh-0 -- curl -s localhost:8081/api/v1/ready
# {"ready":true,"node_id":"peat-mesh-0","checks":[]}
```

### Node info

```bash
kubectl exec peat-mesh-0 -- curl -s localhost:8081/api/v1/node
# {"node_id":"peat-mesh-0","uptime_secs":120,"version":"0.1.0"}
```

### Peer discovery and Iroh connections

Kubernetes EndpointSlice discovery feeds the `PeerConnector`, which registers each peer with Iroh's `StaticProvider`. Verify the Iroh-level connections and sync stack in the logs:

```bash
kubectl logs peat-mesh-0 | grep "Peer connected to Iroh"
# Peer connected to Iroh peer=peat-mesh-1 endpoint_id=0c48030ef6 addresses=[10.42.0.10:11204]
# Peer connected to Iroh peer=peat-mesh-2 endpoint_id=e0ae6e4c6a addresses=[10.42.0.11:11204]
```

The broker's `/api/v1/peers` endpoint reports mesh-level peers (PeatMesh topology). Iroh-level peer connections (used by the blob store and Automerge sync) are visible in the logs.

### Automerge sync stack

The node starts the full Automerge sync pipeline:

1. **AutomergeStore** opens a redb database at `$PEAT_DATA_DIR/automerge/`
2. **MeshSyncTransport** shares the Iroh endpoint with the blob store
3. **AutomergeSyncCoordinator** + **SyncChannelManager** handle delta sync
4. **SyncProtocolHandler** accepts incoming sync connections on `cap/automerge/1` ALPN
5. A **polling task** syncs all documents with all connected peers every 5 seconds

Verify the sync stack initialized:

```bash
kubectl logs peat-mesh-0 | grep -E "automerge|blob store ready"
# Opening redb database with cache_size=16777216 bytes
# Iroh blob store ready (blobs + automerge sync) iroh_endpoint_id=c25a10ed6e
```

### Automerge compaction

Background compaction prevents unbounded memory growth from CRDT revision history accumulation. This is critical for long-running nodes and memory-constrained devices (e.g., ATAK on Android).

Compaction uses Automerge's `fork()` to discard change history, keeping only the current document state. **This destroys the change history needed for incremental delta sync**, so compaction is only safe for `LatestOnly` sync-mode collections (which already send full state). `FullHistory` collections are never compacted.

**Compaction is disabled by default.** When enabled with no explicit collection list, it auto-derives the safe set from the `SyncModeRegistry` (all `LatestOnly` collections: beacons, platforms, tracks, nodes, cells, node_states, etc.).

**Enable with auto-derived collections (recommended):**

```bash
PEAT_COMPACTION_ENABLED=true
# Auto-derives from LatestOnly sync modes — no PEAT_COMPACTION_COLLECTIONS needed
```

**Explicit collection list:**

```bash
PEAT_COMPACTION_ENABLED=true
PEAT_COMPACTION_COLLECTIONS=beacons,platforms,node_states
```

**Aggressive for mobile/ATAK:**

```bash
PEAT_COMPACTION_ENABLED=true
PEAT_COMPACTION_INTERVAL_SECS=60
PEAT_COMPACTION_THRESHOLD_BYTES=16384
```

Verify compaction is running in the logs:

```bash
kubectl logs peat-mesh-0 | grep "compaction"
# Auto-derived compaction collections from LatestOnly sync modes collections=["beacons", "platforms", ...]
# Background compaction started (per-collection) interval_secs=300 threshold_bytes=65536 collections=[...]
# background compaction complete count=3 before=245760 after=12288
```

A warning is logged at startup if a compaction collection uses a non-LatestOnly sync mode.

### EndpointSlice

```bash
kubectl get endpointslice -l app=peat-mesh
# NAME              ADDRESSTYPE   PORTS        ENDPOINTS                         AGE
# peat-mesh-xxxxx   IPv4          11204,8081   10.42.0.9,10.42.0.10,10.42.0.11  30s
```

### Deterministic keypair verification

Delete a pod and verify the device_id is identical after restart:

```bash
# Before
kubectl logs peat-mesh-0 | grep "Mesh started"
# Mesh started node_id=peat-mesh-0 device_id=df0036ca6cfb08b2cc03214a1bdb4f24

kubectl delete pod peat-mesh-0
kubectl wait --for=condition=ready pod/peat-mesh-0 --timeout=30s

# After — same device_id
kubectl logs peat-mesh-0 | grep "Mesh started"
# Mesh started node_id=peat-mesh-0 device_id=df0036ca6cfb08b2cc03214a1bdb4f24
```

### Logs

```bash
# Single pod
kubectl logs peat-mesh-0

# All pods
kubectl logs -l app=peat-mesh

# Follow
kubectl logs -f peat-mesh-0
```

## Cluster lifecycle

k3d clusters can be stopped and started without losing state. This is useful for preserving test environments across sessions.

### Stopping a cluster (preserves state)

```bash
k3d cluster stop peat-alpha
```

This stops the Docker containers but preserves all Kubernetes state (pods, services, secrets, etc.). The cluster uses no resources while stopped.

### Starting a stopped cluster

```bash
k3d cluster start peat-alpha
```

All pods, services, and Helm releases resume exactly where they left off. Pod identities (device_id) remain stable since they're derived from the same formation secret + hostname.

### Listing clusters

```bash
k3d cluster list
# NAME         SERVERS   AGENTS   LOADBALANCER
# peat-alpha   1/1       0/0      true
```

### Deleting a cluster (destroys state)

```bash
k3d cluster delete peat-alpha
```

### Rebuilding after code changes

After modifying peat-mesh source code:

```bash
# Rebuild image
docker build -t peat-mesh-node:latest -f deploy/Dockerfile .

# Re-import into running cluster
k3d image import peat-mesh-node:latest -c peat-alpha

# Rolling restart to pick up new image
kubectl rollout restart statefulset peat-mesh
kubectl rollout status statefulset peat-mesh
```

## Two-cluster federation

Two k3d clusters on a shared Docker network can test cross-cluster communication.

### Setup

```bash
# Create shared Docker network
docker network create peat-federation

# Create two clusters on the shared network
k3d cluster create peat-alpha --network peat-federation
k3d cluster create peat-bravo --network peat-federation

# Build and import image to both
docker build -t peat-mesh-node:latest -f deploy/Dockerfile .
k3d image import peat-mesh-node:latest -c peat-alpha
k3d image import peat-mesh-node:latest -c peat-bravo

# Deploy with the SAME formation secret to both clusters
FORMATION_SECRET=$(openssl rand -base64 32)

kubectl config use-context k3d-peat-alpha
helm install peat-mesh deploy/helm/peat-mesh \
  --set "formationSecret=$FORMATION_SECRET" \
  --set replicaCount=2

kubectl config use-context k3d-peat-bravo
helm install peat-mesh deploy/helm/peat-mesh \
  --set "formationSecret=$FORMATION_SECRET" \
  --set replicaCount=2
```

### Cross-cluster connectivity

Both k3d clusters share the `peat-federation` Docker network, meaning their k3d server node containers can reach each other. To enable cross-cluster mesh traffic:

1. **Expose Iroh QUIC via NodePort** in each cluster so pods in one cluster can reach pods in another via the k3d server container IP + NodePort.

2. **Configure gateway pods** with `PEAT_STATIC_PEERS` pointing to remote cluster gateway addresses. Gateway pods use `HybridDiscovery` combining `KubernetesDiscovery` (local) + `StaticDiscovery` (remote).

3. **Find k3d server IPs** on the shared network:
   ```bash
   docker inspect k3d-peat-alpha-server-0 --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'
   docker inspect k3d-peat-bravo-server-0 --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'
   ```

### Stopping and resuming federation clusters

```bash
# Stop both (preserves state, uses no resources)
k3d cluster stop peat-alpha
k3d cluster stop peat-bravo

# Resume later
k3d cluster start peat-alpha
k3d cluster start peat-bravo
```

The `peat-federation` Docker network persists independently of the clusters.

### Cleanup

```bash
k3d cluster delete peat-alpha
k3d cluster delete peat-bravo
docker network rm peat-federation
```

## Functional testing

k3d clusters are well-suited for automated functional tests. The clusters can be scripted end-to-end:

### Single-cluster test script

```bash
#!/bin/bash
set -euo pipefail

CLUSTER="peat-test-$$"
SECRET=$(openssl rand -base64 32)

cleanup() { k3d cluster delete "$CLUSTER" 2>/dev/null || true; }
trap cleanup EXIT

# Setup
k3d cluster create "$CLUSTER" --no-lb
docker build -t peat-mesh-node:latest -f deploy/Dockerfile .
k3d image import peat-mesh-node:latest -c "$CLUSTER"
helm install peat-mesh deploy/helm/peat-mesh \
  --set "formationSecret=$SECRET" --set replicaCount=3 \
  --set image.pullPolicy=Never \
  --kube-context "k3d-$CLUSTER"

# Wait for pods
kubectl --context "k3d-$CLUSTER" rollout status statefulset/peat-mesh --timeout=120s

# Verify health
for i in 0 1 2; do
  STATUS=$(kubectl --context "k3d-$CLUSTER" exec "peat-mesh-$i" -- \
    curl -sf localhost:8081/api/v1/health | jq -r .status)
  [ "$STATUS" = "healthy" ] || { echo "FAIL: peat-mesh-$i unhealthy"; exit 1; }
done
echo "OK: All pods healthy"

# Verify peer discovery + Iroh connections
sleep 35  # K8s discovery polls every 30s
for i in 0 1 2; do
  PEERS=$(kubectl --context "k3d-$CLUSTER" logs "peat-mesh-$i" | \
    grep -c "Peer connected to Iroh" || true)
  [ "$PEERS" -ge 2 ] || { echo "FAIL: peat-mesh-$i has $PEERS Iroh peers (expected 2)"; exit 1; }
done
echo "OK: All pods discovered peers via Iroh"

# Verify Automerge sync stack initialized
for i in 0 1 2; do
  kubectl --context "k3d-$CLUSTER" logs "peat-mesh-$i" | \
    grep -q "blobs + automerge sync" || { echo "FAIL: peat-mesh-$i sync stack not initialized"; exit 1; }
done
echo "OK: Automerge sync stack active on all pods"

# Verify deterministic identity survives restart
BEFORE=$(kubectl --context "k3d-$CLUSTER" logs peat-mesh-0 | \
  grep "Mesh started" | grep -oP 'device_id=\S+')
kubectl --context "k3d-$CLUSTER" delete pod peat-mesh-0
kubectl --context "k3d-$CLUSTER" wait --for=condition=ready \
  pod/peat-mesh-0 --timeout=30s
AFTER=$(kubectl --context "k3d-$CLUSTER" logs peat-mesh-0 | \
  grep "Mesh started" | grep -oP 'device_id=\S+')
[ "$BEFORE" = "$AFTER" ] || { echo "FAIL: device_id changed after restart"; exit 1; }
echo "OK: Deterministic identity stable across restart"

# Verify no errors in logs
for i in 0 1 2; do
  ERRORS=$(kubectl --context "k3d-$CLUSTER" logs "peat-mesh-$i" | \
    grep -ci "error" || true)
  [ "$ERRORS" -eq 0 ] || echo "WARN: peat-mesh-$i has $ERRORS error lines"
done

echo "PASS: All functional checks passed"
```

### Cross-cluster test approach

To test execution from one cluster to another:

1. **Expose the broker service** from each cluster via k3d port mapping or NodePort
2. **Hit the broker API** from outside the cluster (or from a pod in the other cluster via the shared Docker network)
3. **Write a document** via the broker API on cluster A, verify it syncs to cluster B

```bash
# Create clusters with port mapping for broker access
k3d cluster create peat-alpha --network peat-federation \
  -p "8081:8081@server:0"
k3d cluster create peat-bravo --network peat-federation \
  -p "8082:8081@server:0"

# After deploy, test from host:
curl http://localhost:8081/api/v1/health   # cluster alpha
curl http://localhost:8082/api/v1/health   # cluster bravo
```

Within a single cluster, the Automerge sync coordinator automatically syncs documents between all pods over QUIC (`cap/automerge/1` ALPN). For cross-cluster sync, federation gateway pods need to bridge the two clusters via the pattern described in [ADR-0001 Section 6](adr/0001-kubernetes-istio-deployment.md#6-cluster-to-cluster-federation).
