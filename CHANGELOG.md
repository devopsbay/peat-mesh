# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.2] - 2026-04-05

### Added

- Per-collection, sync-mode-aware Automerge CRDT compaction to prevent
  unbounded memory growth on long-running nodes and mobile devices (peat#760).
  Compaction only runs on `LatestOnly` sync-mode collections, preserving
  change history needed for delta sync on `FullHistory` collections.
  Disabled by default; enable with `PEAT_COMPACTION_ENABLED=true`.

## [0.8.1] - 2026-04-03

### Fixed

- Formation auth handshake deadlock over iroh 0.97 connections (#759).
  `accept_bi()` requires the opener to write before the peer can accept the
  stream — the initiator now sends a `FORMATION_AUTH_VERSION` preamble byte
  immediately after `open_bi()`, unblocking the acceptor.

### Added

- Integration tests for formation auth handshake over live iroh connections
  (successful auth and wrong-key rejection).

## [0.8.0] - 2026-04-02

### Security

- **BREAKING**: Enforce mandatory formation credentials on all Iroh QUIC connections
  - `FormationKey` is now required (not optional) for `SyncProtocolHandler` and `MeshSyncTransport`
  - New `FormationEndpointHooks` gates ALL QUIC connections (sync, blobs) at transport level — only formation members accepted
  - New `FormationPeerSet` tracks known formation member EndpointIds, populated by discovery
  - Outgoing sync connections now run `respond_to_formation_auth()` HMAC challenge-response
  - Removed "warn-and-allow" certificate mode — certificates are always hard-enforced when configured
  - Removed `PEAT_REQUIRE_CERTIFICATES` env var — certificates always required when bundle is present

### Added

- `FormationPeerSet` type in `security::formation_peers` for thread-safe peer tracking
- `FormationEndpointHooks` in `storage::iroh_blob_store` for QUIC-level connection gating
- `MeshSyncTransport::connect_and_authenticate()` for authenticated outbound connections
- `SyncTransport::get_or_connect()` trait method with default fallback implementation
- `NetworkedIrohBlobStore::build_endpoint_with_formation_peers()` for production endpoint construction

### Changed

- `SyncProtocolHandler::new()` now requires `FormationKey` parameter (breaking)
- `SyncProtocolHandler::with_certificate_bundle()` no longer takes `require` parameter (breaking)
- `MeshSyncTransport::new()` now requires `FormationKey` parameter (breaking)
- `PeerConnector::new()` now requires `FormationPeerSet` parameter (breaking)
- `PeerConnector::with_certificate_bundle()` no longer takes `require` parameter (breaking)
- All sync connection sites use `get_or_connect()` instead of `get_connection()` for authenticated fallback

## [0.2.0] - 2026-02-20

### Added

- Extract sync modules from peat-protocol into peat-mesh (automerge_sync, sync_channel, sync_forwarding)
- Wire AutomergeSyncCoordinator into peat-mesh-node binary with 5s polling task
- Update deployment guide for Automerge sync stack

### Fixed

- Apply cargo fmt and clippy fixes for CI compliance

## [0.1.0] - 2026-02-18

### Added

- Initial extraction as standalone crate from peat-protocol
- Complete Peat-Lite CRDT encodings, query handler, and InMemoryBackend
- Kubernetes support: discovery, headless service, StatefulSet (ADR-0001)
- Kubernetes deployment infrastructure: Dockerfile, Helm chart, health probes (ADR-0001 Phase 2)
- HTTP/WS service broker with REST API and WebSocket event streaming
- Device keypair (Ed25519) and formation key security primitives
- mDNS, static, and Kubernetes peer discovery strategies
- Topology management with partition detection and autonomous operation
- Beacon broadcasting and observation for geographic mesh awareness
- GOA CI script for Radicle patches
- ADR-0001 through ADR-0009 transferred from peat repo
- README with CI and usage details

### Fixed

- CI pipe buffer deadlock (redirect verbose output to log file)
- Add crates.io metadata for discoverability
