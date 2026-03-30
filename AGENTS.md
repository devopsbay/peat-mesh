# Codex Project Guide - peat-mesh

## Project Overview

`peat-mesh` is a standalone mesh networking library with pluggable transport, discovery, topology management, and optional Automerge/Iroh synchronization.

## Build and Test

```bash
cargo build
cargo test
cargo build --features automerge-backend
cargo test --features automerge-backend
cargo build --features node
cargo fmt --check
cargo clippy -- -D warnings
```

## Key Areas

- `src/transport/`
- `src/discovery/`
- `src/topology/`
- `src/routing/`
- `src/storage/`
- `src/broker/`
