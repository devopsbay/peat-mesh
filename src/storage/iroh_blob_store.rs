//! Iroh blob store implementation (ADR-025)
//!
//! This module implements the `BlobStore` trait using iroh-blobs,
//! providing content-addressed blob storage with P2P mesh synchronization.
//!
//! # iroh-blobs Characteristics
//!
//! - Content-addressed storage using BLAKE3 hashes (32 bytes)
//! - Built on iroh's QUIC-based P2P networking
//! - Optimized for large file transfers with verified streaming
//! - No native metadata support (we use sidecar JSON files)
//!
//! # Usage
//!
//! ```ignore
//! use peat_mesh::storage::{IrohBlobStore, BlobStore, BlobMetadata};
//! use std::path::Path;
//!
//! let blob_store = IrohBlobStore::new_in_memory(blob_dir).await?;
//!
//! // Create blob from file
//! let token = blob_store.create_blob(
//!     Path::new("/models/yolov8.onnx"),
//!     BlobMetadata::with_name("yolov8.onnx")
//! ).await?;
//!
//! // Token can be shared via CRDT documents
//! // Other nodes can then fetch the blob
//! ```

use super::blob_traits::{BlobHandle, BlobHash, BlobMetadata, BlobProgress, BlobStore, BlobToken};
use anyhow::{Context, Result};
use iroh_blobs::{store::mem::MemStore, Hash};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::RwLock;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Sidecar metadata stored alongside blobs
///
/// Since iroh-blobs doesn't support native metadata, we store
/// metadata in JSON sidecar files: `{blob_dir}/{hash}.meta.json`
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SidecarMetadata {
    /// Original BlobMetadata
    metadata: BlobMetadata,
    /// Size in bytes
    size_bytes: u64,
    /// Creation timestamp (Unix seconds)
    created_at: u64,
}

impl SidecarMetadata {
    fn new(metadata: BlobMetadata, size_bytes: u64) -> Self {
        Self {
            metadata,
            size_bytes,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        }
    }
}

/// Iroh blob store implementing the BlobStore trait
///
/// Wraps iroh-blobs' in-memory store to provide backend-agnostic blob storage.
/// Blobs are content-addressed using BLAKE3 hashes.
///
/// # Metadata Storage
///
/// Since iroh-blobs doesn't support native metadata, we store metadata
/// in JSON sidecar files alongside the blob data.
pub struct IrohBlobStore {
    /// In-memory blob store from iroh-blobs
    store: MemStore,
    /// Cache of known blob tokens (hash -> token)
    token_cache: RwLock<HashMap<BlobHash, BlobToken>>,
    /// Directory for blob data exports and metadata sidecars
    blob_dir: PathBuf,
}

impl IrohBlobStore {
    /// Create a new Iroh blob store with in-memory storage
    ///
    /// # Arguments
    ///
    /// * `blob_dir` - Directory for exported blobs and metadata sidecars
    pub async fn new_in_memory(blob_dir: PathBuf) -> Result<Self> {
        // Create blob directory
        if let Err(e) = std::fs::create_dir_all(&blob_dir) {
            warn!("Failed to create blob directory {:?}: {}", blob_dir, e);
        }

        let store = MemStore::default();

        Ok(Self {
            store,
            token_cache: RwLock::new(HashMap::new()),
            blob_dir,
        })
    }

    /// Create with default temp directory
    pub async fn new_temp() -> Result<Self> {
        let blob_dir = std::env::temp_dir().join("peat_iroh_blobs");
        Self::new_in_memory(blob_dir).await
    }

    /// Get access to the underlying iroh-blobs store
    pub fn store(&self) -> &MemStore {
        &self.store
    }

    /// Get the blob directory path
    pub fn blob_dir(&self) -> &Path {
        &self.blob_dir
    }

    /// Convert iroh Hash to our BlobHash
    fn iroh_hash_to_blob_hash(hash: &Hash) -> BlobHash {
        BlobHash::from_hex(&hash.to_hex())
    }

    /// Convert our BlobHash to iroh Hash
    fn blob_hash_to_iroh_hash(hash: &BlobHash) -> Result<Hash> {
        Hash::from_str(hash.as_hex())
            .map_err(|e| anyhow::anyhow!("Invalid blob hash '{}': {}", hash.as_hex(), e))
    }

    /// Get the path for a blob's metadata sidecar file
    fn metadata_path(&self, hash: &BlobHash) -> PathBuf {
        self.blob_dir.join(format!("{}.meta.json", hash.as_hex()))
    }

    /// Get the local path for exported blob content
    fn local_blob_path(&self, hash: &BlobHash) -> PathBuf {
        self.blob_dir.join(hash.as_hex())
    }

    /// Save metadata sidecar file
    fn save_metadata(&self, hash: &BlobHash, metadata: &SidecarMetadata) -> Result<()> {
        let path = self.metadata_path(hash);
        let json = serde_json::to_string_pretty(metadata)?;
        std::fs::write(&path, json)
            .with_context(|| format!("Failed to write metadata to {:?}", path))?;
        Ok(())
    }

    /// Load metadata sidecar file
    fn load_metadata(&self, hash: &BlobHash) -> Option<SidecarMetadata> {
        let path = self.metadata_path(hash);
        if !path.exists() {
            return None;
        }
        std::fs::read_to_string(&path)
            .ok()
            .and_then(|json| serde_json::from_str(&json).ok())
    }

    /// Delete metadata sidecar file
    fn delete_metadata(&self, hash: &BlobHash) -> Result<()> {
        let path = self.metadata_path(hash);
        if path.exists() {
            std::fs::remove_file(&path)
                .with_context(|| format!("Failed to delete metadata at {:?}", path))?;
        }
        Ok(())
    }

    /// Cache a token for later lookup
    fn cache_token(&self, token: &BlobToken) {
        if let Ok(mut cache) = self.token_cache.write() {
            cache.insert(token.hash.clone(), token.clone());
        }
    }

    /// Export blob content to local filesystem
    async fn export_blob(&self, hash: &Hash) -> Result<PathBuf> {
        let blob_hash = Self::iroh_hash_to_blob_hash(hash);
        let local_path = self.local_blob_path(&blob_hash);

        // If already exported, return existing path
        if local_path.exists() {
            return Ok(local_path);
        }

        // Read blob content from store using get_bytes()
        let content = self
            .store
            .get_bytes(*hash)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get blob {}: {}", hash.to_hex(), e))?;

        // Write to local file
        std::fs::write(&local_path, &content)
            .with_context(|| format!("Failed to export blob to {:?}", local_path))?;

        Ok(local_path)
    }
}

#[async_trait::async_trait]
impl BlobStore for IrohBlobStore {
    async fn create_blob(&self, path: &Path, metadata: BlobMetadata) -> Result<BlobToken> {
        info!("Creating blob from file: {:?}", path);

        // Verify file exists
        if !path.exists() {
            return Err(anyhow::anyhow!("File not found: {:?}", path));
        }

        // Read file content
        let content = std::fs::read(path).with_context(|| format!("Failed to read {:?}", path))?;
        let size_bytes = content.len() as u64;

        // Add to iroh-blobs store using add_bytes()
        let tag = self.store.add_bytes(content).await?;
        let hash = tag.hash;

        // Build our token
        let token = BlobToken {
            hash: Self::iroh_hash_to_blob_hash(&hash),
            size_bytes,
            metadata: metadata.clone(),
        };

        // Save metadata sidecar
        let sidecar = SidecarMetadata::new(metadata, size_bytes);
        self.save_metadata(&token.hash, &sidecar)?;

        // Cache for later lookups
        self.cache_token(&token);

        debug!(
            "Created blob: hash={}, size={}",
            token.hash.as_hex(),
            token.size_bytes
        );

        Ok(token)
    }

    async fn create_blob_from_stream(
        &self,
        stream: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
        _expected_size: Option<u64>,
        metadata: BlobMetadata,
    ) -> Result<BlobToken> {
        use std::sync::atomic::{AtomicU64, Ordering};
        use tokio::io::AsyncWriteExt;

        static STREAM_COUNTER: AtomicU64 = AtomicU64::new(0);

        info!("Creating blob from stream");

        // Stream to temp file to limit peak memory to one copy
        let temp_path = self.blob_dir.join(format!(
            ".stream_import_{}",
            STREAM_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));

        {
            let mut file = tokio::fs::File::create(&temp_path)
                .await
                .with_context(|| format!("Failed to create temp file {:?}", temp_path))?;
            tokio::io::copy(stream, &mut file)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to stream blob to temp file: {}", e))?;
            file.flush().await?;
        }

        let result = self.create_blob(&temp_path, metadata).await;

        // Clean up temp file
        let _ = tokio::fs::remove_file(&temp_path).await;

        result
    }

    async fn create_blob_from_bytes(
        &self,
        data: &[u8],
        metadata: BlobMetadata,
    ) -> Result<BlobToken> {
        info!("Creating blob from {} bytes", data.len());

        let size_bytes = data.len() as u64;

        // Add to iroh-blobs store using add_bytes()
        let tag = self.store.add_bytes(data.to_vec()).await?;
        let hash = tag.hash;

        // Build our token
        let token = BlobToken {
            hash: Self::iroh_hash_to_blob_hash(&hash),
            size_bytes,
            metadata: metadata.clone(),
        };

        // Save metadata sidecar
        let sidecar = SidecarMetadata::new(metadata, size_bytes);
        self.save_metadata(&token.hash, &sidecar)?;

        // Cache for later lookups
        self.cache_token(&token);

        debug!(
            "Created blob from bytes: hash={}, size={}",
            token.hash.as_hex(),
            token.size_bytes
        );

        Ok(token)
    }

    async fn fetch_blob<F>(&self, token: &BlobToken, mut progress: F) -> Result<BlobHandle>
    where
        F: FnMut(BlobProgress) + Send + 'static,
    {
        info!("Fetching blob: hash={}", token.hash.as_hex());

        // Check if we already have it exported locally
        let local_path = self.local_blob_path(&token.hash);
        if local_path.exists() {
            debug!("Blob already exists locally at {:?}", local_path);
            progress(BlobProgress::Completed {
                local_path: local_path.clone(),
            });
            return Ok(BlobHandle::new(token.clone(), local_path));
        }

        // Send started event
        progress(BlobProgress::Started {
            total_bytes: token.size_bytes,
        });

        // Convert hash and check if in store using has()
        let iroh_hash = Self::blob_hash_to_iroh_hash(&token.hash)?;

        if self.store.has(iroh_hash).await? {
            // Blob is in our store, export it
            progress(BlobProgress::Downloading {
                downloaded_bytes: token.size_bytes / 2,
                total_bytes: token.size_bytes,
            });

            let export_path = self.export_blob(&iroh_hash).await?;

            progress(BlobProgress::Completed {
                local_path: export_path.clone(),
            });

            return Ok(BlobHandle::new(token.clone(), export_path));
        }

        // Blob not available locally
        // In Phase 1, remote fetch requires the P2P layer to be connected
        // and the blob to be announced. For now, return an error.
        progress(BlobProgress::Failed {
            error: format!(
                "Blob {} not available locally. Remote fetch requires P2P connectivity.",
                token.hash
            ),
        });

        Err(anyhow::anyhow!(
            "Blob {} not available locally. In Phase 1, ensure the blob is stored \
             on this node or connected via P2P to a node that has it.",
            token.hash.as_hex()
        ))
    }

    fn blob_exists_locally(&self, hash: &BlobHash) -> bool {
        // Check our local blob directory
        let local_path = self.local_blob_path(hash);
        if local_path.exists() {
            return true;
        }

        // Check cache
        if let Ok(cache) = self.token_cache.read() {
            if cache.contains_key(hash) {
                return true;
            }
        }

        // Check metadata sidecar (indicates we've seen this blob)
        self.metadata_path(hash).exists()
    }

    fn blob_info(&self, hash: &BlobHash) -> Option<BlobToken> {
        // Check cache first
        if let Ok(cache) = self.token_cache.read() {
            if let Some(token) = cache.get(hash) {
                return Some(token.clone());
            }
        }

        // Try loading from metadata sidecar
        if let Some(sidecar) = self.load_metadata(hash) {
            let token = BlobToken {
                hash: hash.clone(),
                size_bytes: sidecar.size_bytes,
                metadata: sidecar.metadata,
            };
            return Some(token);
        }

        None
    }

    async fn delete_blob(&self, hash: &BlobHash) -> Result<()> {
        info!("Deleting blob: hash={}", hash.as_hex());

        // Remove from local storage
        let local_path = self.local_blob_path(hash);
        if local_path.exists() {
            std::fs::remove_file(&local_path)
                .with_context(|| format!("Failed to delete local blob: {:?}", local_path))?;
        }

        // Remove metadata sidecar
        self.delete_metadata(hash)?;

        // Remove from cache
        if let Ok(mut cache) = self.token_cache.write() {
            cache.remove(hash);
        }

        // Note: We don't delete from the iroh-blobs store directly
        // as it may be shared or used for P2P sync. The in-memory store
        // will be garbage collected when no longer referenced.

        Ok(())
    }

    fn list_local_blobs(&self) -> Vec<BlobToken> {
        let mut tokens = Vec::new();

        // First, get tokens from cache
        if let Ok(cache) = self.token_cache.read() {
            tokens.extend(cache.values().cloned());
        }

        // Also scan metadata directory for any we might have missed
        if let Ok(entries) = std::fs::read_dir(&self.blob_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.ends_with(".meta.json") {
                        let hash_hex = filename.trim_end_matches(".meta.json");
                        let hash = BlobHash::from_hex(hash_hex);

                        // Skip if already in tokens
                        if tokens.iter().any(|t| t.hash == hash) {
                            continue;
                        }

                        // Load metadata and add to tokens
                        if let Some(sidecar) = self.load_metadata(&hash) {
                            tokens.push(BlobToken {
                                hash,
                                size_bytes: sidecar.size_bytes,
                                metadata: sidecar.metadata,
                            });
                        }
                    }
                }
            }
        }

        tokens
    }

    fn local_storage_bytes(&self) -> u64 {
        // Sum up sizes from cache (matches DittoBlobStore behavior)
        // This represents logical storage used by blobs we know about,
        // regardless of whether they've been exported to disk yet.
        if let Ok(cache) = self.token_cache.read() {
            if !cache.is_empty() {
                return cache.values().map(|t| t.size_bytes).sum();
            }
        }

        // Fallback: scan metadata sidecars for size info
        let mut total = 0u64;
        if let Ok(entries) = std::fs::read_dir(&self.blob_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.ends_with(".meta.json") {
                        let hash_hex = filename.trim_end_matches(".meta.json");
                        let hash = BlobHash::from_hex(hash_hex);
                        if let Some(sidecar) = self.load_metadata(&hash) {
                            total += sidecar.size_bytes;
                        }
                    }
                }
            }
        }

        total
    }
}

// ============================================================================
// NetworkedIrohBlobStore - P2P blob sync with iroh endpoint
// ============================================================================

use crate::config::IrohConfig;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointId, RelayMap, RelayMode, RelayUrl, SecretKey};
use iroh_blobs::BlobsProtocol;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock as TokioRwLock;

/// No-op endpoint hooks used as the default type parameter for [`build_endpoint`].
#[derive(Debug)]
struct NoopEndpointHooks;
impl iroh::endpoint::EndpointHooks for NoopEndpointHooks {}

/// Endpoint hooks that reject QUIC connections from peers not in the
/// [`FormationPeerSet`].
///
/// Enrollment ALPN (`peat/enroll/1`) is exempted because enrolling
/// devices are not yet formation members — enrollment has its own
/// token-based authentication.
///
/// All other ALPNs (sync, blobs, etc.) require the remote peer's
/// `EndpointId` to be present in the allowed set. Since EndpointIds
/// are derived from the formation secret via HKDF and QUIC TLS proves
/// private key possession, this is sufficient to gate access.
#[derive(Debug, Clone)]
pub struct FormationEndpointHooks {
    allowed_peers: crate::security::FormationPeerSet,
}

impl FormationEndpointHooks {
    pub fn new(allowed_peers: crate::security::FormationPeerSet) -> Self {
        Self { allowed_peers }
    }
}

impl iroh::endpoint::EndpointHooks for FormationEndpointHooks {
    fn after_handshake<'a>(
        &'a self,
        conn: &'a iroh::endpoint::ConnectionInfo,
    ) -> impl std::future::Future<Output = iroh::endpoint::AfterHandshakeOutcome> + Send + 'a {
        async move {
            // Enrollment ALPN is exempted — new devices need to enroll
            // before they become formation members.
            if conn.alpn() == super::enrollment_transport::CAP_ENROLLMENT_ALPN {
                return iroh::endpoint::AfterHandshakeOutcome::Accept;
            }

            let remote = conn.remote_id();
            if self.allowed_peers.contains(&remote) {
                tracing::debug!(
                    peer = %remote.fmt_short(),
                    alpn = %String::from_utf8_lossy(conn.alpn()),
                    "QUIC connection accepted (formation member)"
                );
                iroh::endpoint::AfterHandshakeOutcome::Accept
            } else {
                tracing::warn!(
                    peer = %remote.fmt_short(),
                    alpn = %String::from_utf8_lossy(conn.alpn()),
                    "QUIC connection REJECTED (not a formation member)"
                );
                iroh::endpoint::AfterHandshakeOutcome::Reject {
                    error_code: 3u32.into(),
                    reason: b"not a formation member".to_vec(),
                }
            }
        }
    }
}

// ============================================================================
// BlobPeerIndex - O(1) blob-to-peer resolution
// ============================================================================

/// Tracks which peers have which blobs for O(1) resolution.
///
/// Maintains bidirectional mappings between blob hashes and peer endpoints,
/// enabling efficient lookup in both directions without scanning.
///
/// # Thread Safety
///
/// This struct is intended to be wrapped in a `TokioRwLock` by its owner.
#[derive(Debug, Default)]
pub struct BlobPeerIndex {
    /// Reverse index: blob hash → set of peers that have it
    blob_to_peers: HashMap<BlobHash, HashSet<EndpointId>>,
    /// Forward index: peer → set of blob hashes they have
    peer_to_blobs: HashMap<EndpointId, HashSet<BlobHash>>,
}

impl BlobPeerIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a peer has a specific blob.
    pub fn advertise(&mut self, peer: EndpointId, hash: BlobHash) {
        self.blob_to_peers
            .entry(hash.clone())
            .or_default()
            .insert(peer);
        self.peer_to_blobs.entry(peer).or_default().insert(hash);
    }

    /// Remove all entries for a peer.
    pub fn remove_peer(&mut self, peer: &EndpointId) {
        if let Some(blobs) = self.peer_to_blobs.remove(peer) {
            for hash in &blobs {
                if let Some(peers) = self.blob_to_peers.get_mut(hash) {
                    peers.remove(peer);
                    if peers.is_empty() {
                        self.blob_to_peers.remove(hash);
                    }
                }
            }
        }
    }

    /// Get the set of peers known to have a specific blob.
    pub fn peers_with_blob(&self, hash: &BlobHash) -> Vec<EndpointId> {
        self.blob_to_peers
            .get(hash)
            .map(|peers| peers.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get the number of blobs known on a specific peer.
    pub fn peer_blob_count(&self, peer: &EndpointId) -> usize {
        self.peer_to_blobs
            .get(peer)
            .map(|blobs| blobs.len())
            .unwrap_or(0)
    }

    /// Total number of unique blob→peer mappings tracked.
    pub fn total_entries(&self) -> usize {
        self.blob_to_peers.values().map(|s| s.len()).sum()
    }
}

/// Networked Iroh blob store with P2P sync capabilities
///
/// Extends `IrohBlobStore` with:
/// - iroh `Router` accepting QUIC connections for blob transfer
/// - `BlobsProtocol` for serving blobs to peers
/// - `Downloader` for fetching from remote peers
///
/// # Architecture
///
/// ```text
/// NetworkedIrohBlobStore
///     ├─ IrohBlobStore (local storage)
///     ├─ Router (QUIC accept loop + Endpoint)
///     ├─ BlobsProtocol (incoming requests)
///     └─ known_peers (for remote fetch)
/// ```
///
/// # Usage
///
/// ```ignore
/// use peat_mesh::storage::NetworkedIrohBlobStore;
///
/// // Create with networking
/// let store = NetworkedIrohBlobStore::new(blob_dir).await?;
///
/// // Add known peer for remote fetch
/// store.add_peer(peer_endpoint_id).await;
///
/// // Create blob locally (available to peers via BlobsProtocol)
/// let token = store.create_blob_from_bytes(data, metadata).await?;
///
/// // Fetch blob (tries local first, then remote peers)
/// let handle = store.fetch_blob(&token, |progress| {}).await?;
/// ```
pub struct NetworkedIrohBlobStore {
    /// Local blob store
    local_store: IrohBlobStore,
    /// Iroh protocol router — accepts incoming QUIC connections and
    /// routes blobs ALPN to BlobsProtocol. Also owns the Endpoint.
    router: Router,
    /// BlobsProtocol for serving blobs
    blobs_protocol: BlobsProtocol,
    /// Known peers that may have blobs
    known_peers: TokioRwLock<Vec<EndpointId>>,
    /// Reverse index: blob hash → peers that have it, peer → blob hashes
    blob_peer_index: TokioRwLock<BlobPeerIndex>,
    /// In-memory address lookup for adding peer addresses at runtime
    memory_lookup: MemoryLookup,
    /// Timeout for graceful shutdown
    shutdown_timeout: Duration,
    /// Timeout for blob download operations from remote peers
    download_timeout: Duration,
}

impl NetworkedIrohBlobStore {
    /// Create a new networked blob store with default configuration.
    ///
    /// Binds an iroh endpoint on an ephemeral port with default relay servers.
    ///
    /// # Arguments
    ///
    /// * `blob_dir` - Directory for blob storage and metadata
    pub async fn new(blob_dir: PathBuf) -> Result<Arc<Self>> {
        Self::from_config(blob_dir, &IrohConfig::default()).await
    }

    /// Create with a specific bind address.
    pub async fn bind(blob_dir: PathBuf, bind_addr: std::net::SocketAddr) -> Result<Arc<Self>> {
        let config = IrohConfig {
            bind_addr: Some(bind_addr),
            ..Default::default()
        };
        Self::from_config(blob_dir, &config).await
    }

    /// Build an Iroh [`Endpoint`] and [`MemoryLookup`] from an [`IrohConfig`]
    /// **without** formation peer gating.
    ///
    /// **Warning:** This creates an unauthenticated endpoint. Production code
    /// must use [`build_endpoint_with_formation_peers`](Self::build_endpoint_with_formation_peers)
    /// to enforce mandatory credential authentication on all connections.
    pub async fn build_endpoint(config: &IrohConfig) -> Result<(Endpoint, MemoryLookup)> {
        Self::build_endpoint_with_hooks(config, None::<NoopEndpointHooks>).await
    }

    /// Build an Iroh [`Endpoint`] and [`MemoryLookup`] with
    /// [`FormationEndpointHooks`] that reject connections from unknown peers.
    ///
    /// This is the required entry point for production use. All QUIC
    /// connections (sync, blobs, etc.) are gated — only peers in the
    /// `formation_peers` set are accepted. Enrollment ALPN is exempted.
    pub async fn build_endpoint_with_formation_peers(
        config: &IrohConfig,
        formation_peers: crate::security::FormationPeerSet,
    ) -> Result<(Endpoint, MemoryLookup)> {
        let hooks = FormationEndpointHooks::new(formation_peers);
        Self::build_endpoint_with_hooks(config, Some(hooks)).await
    }

    /// Build an Iroh [`Endpoint`] and [`MemoryLookup`] with optional
    /// [`EndpointHooks`] for intercepting connection lifecycle events.
    ///
    /// Hooks allow callers to:
    /// - **`before_connect`**: inspect/reject outgoing connections before packets are sent
    /// - **`after_handshake`**: inspect/reject connections after TLS handshake completes
    ///
    /// [`EndpointHooks`]: iroh::endpoint::EndpointHooks
    /// [`MeshSyncTransport`]: super::mesh_sync_transport::MeshSyncTransport
    pub async fn build_endpoint_with_hooks(
        config: &IrohConfig,
        hooks: Option<impl iroh::endpoint::EndpointHooks + 'static>,
    ) -> Result<(Endpoint, MemoryLookup)> {
        let memory_lookup = MemoryLookup::new();

        let mut builder =
            Endpoint::builder(iroh::endpoint::presets::N0).address_lookup(memory_lookup.clone());

        // Install endpoint hooks if provided
        if let Some(hooks) = hooks {
            builder = builder.hooks(hooks);
        }

        // Apply deterministic secret key
        if let Some(key_bytes) = config.secret_key {
            builder = builder.secret_key(SecretKey::from_bytes(&key_bytes));
        }

        // Apply bind address
        if let Some(bind_addr) = config.bind_addr {
            builder = builder
                .bind_addr(bind_addr)
                .map_err(|e| anyhow::anyhow!("Invalid bind address: {}", e))?;
        }

        // Apply relay configuration
        if !config.relay_urls.is_empty() {
            let relay_map = RelayMap::from_iter(
                config
                    .relay_urls
                    .iter()
                    .filter_map(|url| url.parse::<RelayUrl>().ok()),
            );
            builder = builder.relay_mode(RelayMode::Custom(relay_map));
        }

        let endpoint = tokio::time::timeout(config.bind_timeout, builder.bind())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Iroh endpoint bind timed out after {:?}",
                    config.bind_timeout
                )
            })?
            .map_err(|e| anyhow::anyhow!("Failed to create iroh endpoint: {}", e))?;

        Ok((endpoint, memory_lookup))
    }

    /// Create a networked blob store from an [`IrohConfig`].
    ///
    /// Applies bind address and relay URL settings from the config when
    /// constructing the iroh endpoint.
    pub async fn from_config(blob_dir: PathBuf, config: &IrohConfig) -> Result<Arc<Self>> {
        let (endpoint, memory_lookup) = Self::build_endpoint(config).await?;
        Self::from_endpoint_with_protocols_with_timeouts(
            blob_dir,
            endpoint,
            memory_lookup,
            vec![],
            config.shutdown_timeout,
            config.download_timeout,
        )
        .await
    }

    /// Get the endpoint ID (public key) for this node
    pub fn endpoint_id(&self) -> EndpointId {
        self.router.endpoint().id()
    }

    /// Get a reference to the underlying endpoint
    pub fn endpoint(&self) -> &Endpoint {
        self.router.endpoint()
    }

    /// Get a reference to the blobs protocol handler
    pub fn blobs_protocol(&self) -> &BlobsProtocol {
        &self.blobs_protocol
    }

    /// Get a reference to the local store
    pub fn local_store(&self) -> &IrohBlobStore {
        &self.local_store
    }

    /// Get a reference to the in-memory address lookup provider
    pub fn memory_lookup(&self) -> &MemoryLookup {
        &self.memory_lookup
    }

    /// Create a networked blob store from a pre-built [`Endpoint`], optionally
    /// registering extra protocol handlers alongside the blobs ALPN.
    ///
    /// Use this when you need to share the endpoint with other protocols
    /// (e.g. Automerge sync on `CAP_AUTOMERGE_ALPN`).
    ///
    /// The Router's `spawn()` automatically updates the endpoint's ALPN list
    /// from all registered protocols, so there is no need to pre-configure
    /// ALPNs on the endpoint builder.
    ///
    /// Uses default timeout values. For custom timeouts, use
    /// [`from_endpoint_with_protocols_with_timeouts`](Self::from_endpoint_with_protocols_with_timeouts).
    pub async fn from_endpoint_with_protocols(
        blob_dir: PathBuf,
        endpoint: Endpoint,
        memory_lookup: MemoryLookup,
        extra_protocols: Vec<(&'static [u8], Box<dyn iroh::protocol::DynProtocolHandler>)>,
    ) -> Result<Arc<Self>> {
        let defaults = IrohConfig::default();
        Self::from_endpoint_with_protocols_with_timeouts(
            blob_dir,
            endpoint,
            memory_lookup,
            extra_protocols,
            defaults.shutdown_timeout,
            defaults.download_timeout,
        )
        .await
    }

    /// Like [`from_endpoint_with_protocols`](Self::from_endpoint_with_protocols)
    /// but with explicit timeout configuration.
    pub async fn from_endpoint_with_protocols_with_timeouts(
        blob_dir: PathBuf,
        endpoint: Endpoint,
        memory_lookup: MemoryLookup,
        extra_protocols: Vec<(&'static [u8], Box<dyn iroh::protocol::DynProtocolHandler>)>,
        shutdown_timeout: Duration,
        download_timeout: Duration,
    ) -> Result<Arc<Self>> {
        let local_store = IrohBlobStore::new_in_memory(blob_dir).await?;
        let blobs_protocol = BlobsProtocol::new(&local_store.store, None);

        let mut builder =
            Router::builder(endpoint).accept(iroh_blobs::ALPN, blobs_protocol.clone());

        for (alpn, handler) in extra_protocols {
            builder = builder.accept(alpn, handler);
        }

        let router = builder.spawn();

        Ok(Arc::new(Self {
            local_store,
            router,
            blobs_protocol,
            known_peers: TokioRwLock::new(Vec::new()),
            blob_peer_index: TokioRwLock::new(BlobPeerIndex::new()),
            memory_lookup,
            shutdown_timeout,
            download_timeout,
        }))
    }

    /// Gracefully shut down the protocol router and Iroh endpoint.
    ///
    /// Applies the configured `shutdown_timeout` (default 5s) to prevent
    /// hanging on stuck QUIC connections.
    pub async fn shutdown(&self) -> Result<()> {
        tokio::time::timeout(self.shutdown_timeout, self.router.shutdown())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Router shutdown timed out after {:?}",
                    self.shutdown_timeout
                )
            })?
            .map_err(|e| anyhow::anyhow!("Router shutdown error: {}", e))
    }

    /// Add a peer that may have blobs we want to fetch
    pub async fn add_peer(&self, peer_id: EndpointId) {
        let mut peers = self.known_peers.write().await;
        if !peers.contains(&peer_id) {
            peers.push(peer_id);
            debug!("Added peer {} for blob fetching", peer_id.fmt_short());
        }
    }

    /// Remove a peer from the known peers list and clean up its index entries.
    pub async fn remove_peer(&self, peer_id: &EndpointId) {
        let mut peers = self.known_peers.write().await;
        peers.retain(|p| p != peer_id);
        drop(peers);

        let mut index = self.blob_peer_index.write().await;
        index.remove_peer(peer_id);
    }

    /// List known peers
    pub async fn known_peers(&self) -> Vec<EndpointId> {
        self.known_peers.read().await.clone()
    }

    /// Record that a peer has a specific blob.
    ///
    /// Call this when you learn (via gossip, sync, or direct query) that
    /// a peer has a blob. Enables O(1) peer lookup in [`fetch_blob`](Self::fetch_blob).
    pub async fn advertise_blob(&self, peer: EndpointId, hash: BlobHash) {
        self.blob_peer_index.write().await.advertise(peer, hash);
    }

    /// Get peers known to have a specific blob.
    ///
    /// Returns an empty vec if no peer info is known for this blob.
    pub async fn peers_with_blob(&self, hash: &BlobHash) -> Vec<EndpointId> {
        self.blob_peer_index.read().await.peers_with_blob(hash)
    }

    /// Get a reference to the blob peer index.
    pub async fn blob_peer_index(&self) -> tokio::sync::RwLockReadGuard<'_, BlobPeerIndex> {
        self.blob_peer_index.read().await
    }

    /// Get the iroh-blobs Store API for advanced operations
    pub fn store_api(&self) -> &iroh_blobs::api::Store {
        &self.local_store.store
    }
}

#[async_trait::async_trait]
impl BlobStore for NetworkedIrohBlobStore {
    async fn create_blob(&self, path: &Path, metadata: BlobMetadata) -> Result<BlobToken> {
        // Delegate to local store - blob becomes available for P2P via BlobsProtocol
        self.local_store.create_blob(path, metadata).await
    }

    async fn create_blob_from_stream(
        &self,
        stream: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
        expected_size: Option<u64>,
        metadata: BlobMetadata,
    ) -> Result<BlobToken> {
        self.local_store
            .create_blob_from_stream(stream, expected_size, metadata)
            .await
    }

    async fn create_blob_from_bytes(
        &self,
        data: &[u8],
        metadata: BlobMetadata,
    ) -> Result<BlobToken> {
        // Delegate to local store
        self.local_store
            .create_blob_from_bytes(data, metadata)
            .await
    }

    async fn fetch_blob<F>(&self, token: &BlobToken, mut progress: F) -> Result<BlobHandle>
    where
        F: FnMut(BlobProgress) + Send + 'static,
    {
        info!(
            "NetworkedIrohBlobStore: Fetching blob {}",
            token.hash.as_hex()
        );

        // First, try local fetch
        let local_path = self.local_store.local_blob_path(&token.hash);
        if local_path.exists() {
            debug!("Blob exists locally at {:?}", local_path);
            progress(BlobProgress::Completed {
                local_path: local_path.clone(),
            });
            return Ok(BlobHandle::new(token.clone(), local_path));
        }

        // Check if in local store (not yet exported)
        let iroh_hash = IrohBlobStore::blob_hash_to_iroh_hash(&token.hash)?;
        if self.local_store.store.has(iroh_hash).await? {
            debug!("Blob in local store, exporting");
            return self.local_store.fetch_blob(token, progress).await;
        }

        // Not available locally - try to fetch from known peers
        progress(BlobProgress::Started {
            total_bytes: token.size_bytes,
        });

        // Build ordered peer list: indexed peers first, then remaining known peers
        let indexed_peers = self
            .blob_peer_index
            .read()
            .await
            .peers_with_blob(&token.hash);
        let all_peers = self.known_peers.read().await.clone();

        if all_peers.is_empty() {
            progress(BlobProgress::Failed {
                error: format!(
                    "Blob {} not available locally and no peers known",
                    token.hash
                ),
            });
            return Err(anyhow::anyhow!(
                "Blob {} not available locally and no peers configured for remote fetch",
                token.hash.as_hex()
            ));
        }

        // Prioritize peers from the index, then try the rest
        let mut ordered_peers = indexed_peers.clone();
        for peer in &all_peers {
            if !ordered_peers.contains(peer) {
                ordered_peers.push(*peer);
            }
        }

        if !indexed_peers.is_empty() {
            debug!(
                "Blob {} indexed on {} peers, {} total known",
                token.hash.as_hex(),
                indexed_peers.len(),
                all_peers.len()
            );
        }

        info!(
            "Attempting to fetch blob {} from {} peers",
            token.hash.as_hex(),
            ordered_peers.len()
        );

        // Use the downloader to fetch from peers with a connect timeout
        // matching the download timeout. The default 1s pool connect timeout
        // is too tight under load (e.g. parallel tests or busy systems).
        let pool_opts = iroh_blobs::util::connection_pool::Options {
            connect_timeout: self.download_timeout,
            ..Default::default()
        };
        let downloader = iroh_blobs::api::downloader::Downloader::new_with_opts(
            self.store_api(),
            self.router.endpoint(),
            pool_opts,
        );

        for peer_id in &ordered_peers {
            debug!(
                "Trying peer {} for blob {}",
                peer_id.fmt_short(),
                token.hash.as_hex()
            );

            progress(BlobProgress::Downloading {
                downloaded_bytes: 0,
                total_bytes: token.size_bytes,
            });

            // Try to download from this peer with timeout
            let download_result = tokio::time::timeout(
                self.download_timeout,
                downloader.download(iroh_hash, Some(*peer_id)),
            )
            .await;

            match download_result {
                Err(_elapsed) => {
                    warn!(
                        "Download from peer {} timed out after {:?}",
                        peer_id.fmt_short(),
                        self.download_timeout
                    );
                    continue;
                }
                Ok(Err(e)) => {
                    warn!(
                        "Failed to download from peer {}: {}",
                        peer_id.fmt_short(),
                        e
                    );
                    continue;
                }
                Ok(Ok(_)) => {
                    info!(
                        "Successfully downloaded blob {} from peer {}",
                        token.hash.as_hex(),
                        peer_id.fmt_short()
                    );

                    // Record in index for future lookups
                    self.blob_peer_index
                        .write()
                        .await
                        .advertise(*peer_id, token.hash.clone());

                    // Export to local filesystem
                    let export_path = self.local_store.export_blob(&iroh_hash).await?;

                    // Save metadata sidecar
                    let sidecar = SidecarMetadata::new(token.metadata.clone(), token.size_bytes);
                    self.local_store.save_metadata(&token.hash, &sidecar)?;

                    // Cache token
                    self.local_store.cache_token(token);

                    progress(BlobProgress::Completed {
                        local_path: export_path.clone(),
                    });

                    return Ok(BlobHandle::new(token.clone(), export_path));
                }
            }
        }

        // All peers failed
        progress(BlobProgress::Failed {
            error: format!(
                "Failed to fetch blob {} from all {} peers",
                token.hash,
                ordered_peers.len()
            ),
        });

        Err(anyhow::anyhow!(
            "Failed to fetch blob {} from any of {} known peers",
            token.hash.as_hex(),
            ordered_peers.len()
        ))
    }

    fn blob_exists_locally(&self, hash: &BlobHash) -> bool {
        self.local_store.blob_exists_locally(hash)
    }

    fn blob_info(&self, hash: &BlobHash) -> Option<BlobToken> {
        self.local_store.blob_info(hash)
    }

    async fn delete_blob(&self, hash: &BlobHash) -> Result<()> {
        self.local_store.delete_blob(hash).await
    }

    fn list_local_blobs(&self) -> Vec<BlobToken> {
        self.local_store.list_local_blobs()
    }

    fn local_storage_bytes(&self) -> u64 {
        self.local_store.local_storage_bytes()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_store() -> (IrohBlobStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = IrohBlobStore::new_in_memory(temp_dir.path().to_path_buf())
            .await
            .unwrap();
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_create_blob_from_bytes() {
        let (store, _temp) = create_test_store().await;

        let data = b"Hello, iroh-blobs!";
        let metadata = BlobMetadata::with_name("test.txt");

        let token = store.create_blob_from_bytes(data, metadata).await.unwrap();

        assert_eq!(token.size_bytes, data.len() as u64);
        assert_eq!(token.metadata.name, Some("test.txt".to_string()));
        assert!(!token.hash.as_hex().is_empty());
    }

    #[tokio::test]
    async fn test_create_blob_from_file() {
        let (store, temp_dir) = create_test_store().await;

        // Create a test file
        let test_file = temp_dir.path().join("test_input.txt");
        std::fs::write(&test_file, "File content for testing").unwrap();

        let metadata = BlobMetadata::with_name_and_type("test_input.txt", "text/plain");
        let token = store.create_blob(&test_file, metadata).await.unwrap();

        assert_eq!(token.size_bytes, 24); // "File content for testing".len()
        assert_eq!(token.metadata.name, Some("test_input.txt".to_string()));
        assert_eq!(token.metadata.content_type, Some("text/plain".to_string()));
    }

    #[tokio::test]
    async fn test_fetch_blob() {
        let (store, _temp) = create_test_store().await;

        // Create a blob first
        let data = b"Content to fetch";
        let metadata = BlobMetadata::with_name("fetch_test.bin");
        let token = store.create_blob_from_bytes(data, metadata).await.unwrap();

        // Fetch it back
        let handle = store.fetch_blob(&token, |_progress| {}).await.unwrap();

        assert!(handle.path.exists());
        let content = std::fs::read(&handle.path).unwrap();
        assert_eq!(content, data);
    }

    #[tokio::test]
    async fn test_blob_exists_locally() {
        let (store, _temp) = create_test_store().await;

        let data = b"Test data";
        let metadata = BlobMetadata::default();
        let token = store.create_blob_from_bytes(data, metadata).await.unwrap();

        assert!(store.blob_exists_locally(&token.hash));

        let unknown_hash =
            BlobHash::from_hex("0000000000000000000000000000000000000000000000000000000000000000");
        assert!(!store.blob_exists_locally(&unknown_hash));
    }

    #[tokio::test]
    async fn test_blob_info() {
        let (store, _temp) = create_test_store().await;

        let data = b"Info test";
        let metadata = BlobMetadata::with_name("info.dat").with_custom("version", "1.0");
        let token = store.create_blob_from_bytes(data, metadata).await.unwrap();

        let info = store.blob_info(&token.hash).unwrap();
        assert_eq!(info.size_bytes, token.size_bytes);
        assert_eq!(info.metadata.name, Some("info.dat".to_string()));
        assert_eq!(
            info.metadata.custom.get("version"),
            Some(&"1.0".to_string())
        );
    }

    #[tokio::test]
    async fn test_delete_blob() {
        let (store, _temp) = create_test_store().await;

        let data = b"To be deleted";
        let metadata = BlobMetadata::default();
        let token = store.create_blob_from_bytes(data, metadata).await.unwrap();

        // Export blob to local file first
        let _ = store.fetch_blob(&token, |_| {}).await.unwrap();

        assert!(store.blob_exists_locally(&token.hash));

        store.delete_blob(&token.hash).await.unwrap();

        // Should no longer exist locally
        let local_path = store.local_blob_path(&token.hash);
        assert!(!local_path.exists());
        assert!(store.blob_info(&token.hash).is_none());
    }

    #[tokio::test]
    async fn test_list_local_blobs() {
        let (store, _temp) = create_test_store().await;

        // Create multiple blobs
        let token1 = store
            .create_blob_from_bytes(b"Blob 1", BlobMetadata::with_name("one.txt"))
            .await
            .unwrap();
        let token2 = store
            .create_blob_from_bytes(b"Blob 2", BlobMetadata::with_name("two.txt"))
            .await
            .unwrap();
        let token3 = store
            .create_blob_from_bytes(b"Blob 3", BlobMetadata::with_name("three.txt"))
            .await
            .unwrap();

        let blobs = store.list_local_blobs();
        assert_eq!(blobs.len(), 3);

        let hashes: Vec<_> = blobs.iter().map(|t| t.hash.clone()).collect();
        assert!(hashes.contains(&token1.hash));
        assert!(hashes.contains(&token2.hash));
        assert!(hashes.contains(&token3.hash));
    }

    #[tokio::test]
    async fn test_local_storage_bytes() {
        let (store, _temp) = create_test_store().await;

        // Initially zero
        assert_eq!(store.local_storage_bytes(), 0);

        // Create blobs (no need to export them - cache tracks sizes)
        let data1 = b"Small";
        let _token1 = store
            .create_blob_from_bytes(data1, BlobMetadata::default())
            .await
            .unwrap();

        let data2 = b"Larger blob content";
        let _token2 = store
            .create_blob_from_bytes(data2, BlobMetadata::default())
            .await
            .unwrap();

        let total = store.local_storage_bytes();
        assert_eq!(total, (data1.len() + data2.len()) as u64);
    }

    #[tokio::test]
    async fn test_metadata_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let blob_dir = temp_dir.path().to_path_buf();

        // Create a store and add a blob
        let store1 = IrohBlobStore::new_in_memory(blob_dir.clone())
            .await
            .unwrap();

        let data = b"Persistent metadata test";
        let metadata = BlobMetadata::with_name("persist.txt").with_custom("key", "value");
        let token = store1.create_blob_from_bytes(data, metadata).await.unwrap();

        // Create a NEW store pointing to the same directory
        let store2 = IrohBlobStore::new_in_memory(blob_dir).await.unwrap();

        // Metadata should be loadable from the sidecar file
        let info = store2.blob_info(&token.hash).unwrap();
        assert_eq!(info.metadata.name, Some("persist.txt".to_string()));
        assert_eq!(info.metadata.custom.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_sidecar_metadata_serialization() {
        let metadata = BlobMetadata::with_name("test.onnx")
            .with_custom("version", "1.0")
            .with_custom("model_id", "yolov8");

        let sidecar = SidecarMetadata::new(metadata, 1024);

        let json = serde_json::to_string(&sidecar).unwrap();
        let parsed: SidecarMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.size_bytes, 1024);
        assert_eq!(parsed.metadata.name, Some("test.onnx".to_string()));
        assert_eq!(
            parsed.metadata.custom.get("version"),
            Some(&"1.0".to_string())
        );
    }

    #[tokio::test]
    async fn test_create_blob_from_stream() {
        let (store, _temp) = create_test_store().await;

        let data = b"Hello from a stream!";
        let mut reader: &[u8] = data;
        let metadata = BlobMetadata::with_name("streamed.txt");

        let token = store
            .create_blob_from_stream(&mut reader, Some(data.len() as u64), metadata)
            .await
            .unwrap();

        assert_eq!(token.size_bytes, data.len() as u64);
        assert_eq!(token.metadata.name, Some("streamed.txt".to_string()));

        // Verify content round-trips
        let handle = store.fetch_blob(&token, |_| {}).await.unwrap();
        let content = std::fs::read(&handle.path).unwrap();
        assert_eq!(content, data);
    }

    #[tokio::test]
    async fn test_create_blob_from_stream_unknown_size() {
        let (store, _temp) = create_test_store().await;

        let data = b"Stream with unknown size";
        let mut reader: &[u8] = data;

        let token = store
            .create_blob_from_stream(&mut reader, None, BlobMetadata::default())
            .await
            .unwrap();

        assert_eq!(token.size_bytes, data.len() as u64);
    }

    #[tokio::test]
    async fn test_open_read_stream() {
        use tokio::io::AsyncReadExt;

        let (store, _temp) = create_test_store().await;

        let data = b"Content for streaming read";
        let metadata = BlobMetadata::with_name("stream_read.bin");
        let token = store.create_blob_from_bytes(data, metadata).await.unwrap();

        // Fetch to make it available on disk
        let handle = store.fetch_blob(&token, |_| {}).await.unwrap();

        // Open as stream and read in chunks
        let mut stream = handle.open_read_stream().await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    // ---- BlobPeerIndex unit tests ----

    fn test_peer_id(seed: u8) -> EndpointId {
        SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn test_blob_peer_index_advertise_and_lookup() {
        let mut index = BlobPeerIndex::new();
        let peer_a = test_peer_id(1);
        let peer_b = test_peer_id(2);
        let hash = BlobHash::from_hex("abc123");

        index.advertise(peer_a, hash.clone());
        index.advertise(peer_b, hash.clone());

        let peers = index.peers_with_blob(&hash);
        assert_eq!(peers.len(), 2);
        assert!(peers.contains(&peer_a));
        assert!(peers.contains(&peer_b));
    }

    #[test]
    fn test_blob_peer_index_remove_peer() {
        let mut index = BlobPeerIndex::new();
        let peer_a = test_peer_id(1);
        let peer_b = test_peer_id(2);
        let hash1 = BlobHash::from_hex("abc");
        let hash2 = BlobHash::from_hex("def");

        index.advertise(peer_a, hash1.clone());
        index.advertise(peer_a, hash2.clone());
        index.advertise(peer_b, hash1.clone());

        assert_eq!(index.peer_blob_count(&peer_a), 2);
        assert_eq!(index.total_entries(), 3);

        index.remove_peer(&peer_a);

        assert_eq!(index.peer_blob_count(&peer_a), 0);
        assert_eq!(index.peers_with_blob(&hash1).len(), 1); // only peer_b
        assert!(index.peers_with_blob(&hash2).is_empty()); // peer_a was the only one
        assert_eq!(index.total_entries(), 1);
    }

    #[test]
    fn test_blob_peer_index_unknown_blob() {
        let index = BlobPeerIndex::new();
        let hash = BlobHash::from_hex("nonexistent");
        assert!(index.peers_with_blob(&hash).is_empty());
    }

    #[test]
    fn test_blob_peer_index_duplicate_advertise() {
        let mut index = BlobPeerIndex::new();
        let peer = test_peer_id(1);
        let hash = BlobHash::from_hex("abc");

        index.advertise(peer, hash.clone());
        index.advertise(peer, hash.clone()); // duplicate

        assert_eq!(index.peers_with_blob(&hash).len(), 1);
        assert_eq!(index.peer_blob_count(&peer), 1);
        assert_eq!(index.total_entries(), 1);
    }

    // ---- Integration tests ----

    /// After a successful P2P fetch, the blob→peer mapping should be recorded in the index.
    #[tokio::test]
    async fn test_p2p_fetch_populates_index() {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();

        // Build endpoints directly using empty_builder to avoid external
        // DNS/relay dependencies in tests.
        let memory_lookup_a = iroh::address_lookup::memory::MemoryLookup::new();
        let endpoint_a = Endpoint::empty_builder()
            .address_lookup(memory_lookup_a.clone())
            .secret_key(iroh::SecretKey::from_bytes(&[1u8; 32]))
            .bind()
            .await
            .unwrap();

        let memory_lookup_b = iroh::address_lookup::memory::MemoryLookup::new();
        let endpoint_b = Endpoint::empty_builder()
            .address_lookup(memory_lookup_b.clone())
            .secret_key(iroh::SecretKey::from_bytes(&[2u8; 32]))
            .bind()
            .await
            .unwrap();

        let store_a = NetworkedIrohBlobStore::from_endpoint_with_protocols(
            dir_a.path().to_path_buf(),
            endpoint_a,
            memory_lookup_a,
            vec![],
        )
        .await
        .unwrap();

        let store_b = NetworkedIrohBlobStore::from_endpoint_with_protocols(
            dir_b.path().to_path_buf(),
            endpoint_b,
            memory_lookup_b.clone(),
            vec![],
        )
        .await
        .unwrap();

        // Wire up discovery using the full endpoint address
        memory_lookup_b.add_endpoint_info(store_a.endpoint().addr());
        store_b.add_peer(store_a.endpoint_id()).await;

        // Index should be empty before fetch
        let token = store_a
            .create_blob_from_bytes(b"indexed blob", BlobMetadata::with_name("idx.bin"))
            .await
            .unwrap();
        assert!(store_b.peers_with_blob(&token.hash).await.is_empty());

        // Fetch — should populate the index
        let _handle = store_b.fetch_blob(&token, |_| {}).await.unwrap();

        let indexed_peers = store_b.peers_with_blob(&token.hash).await;
        assert_eq!(indexed_peers.len(), 1);
        assert_eq!(indexed_peers[0], store_a.endpoint_id());

        // Shutdown may time out under heavy parallel test load; ignore errors
        let _ = store_a.shutdown().await;
        let _ = store_b.shutdown().await;
    }

    /// Two `NetworkedIrohBlobStore` instances discover each other via
    /// `MemoryLookup`, one creates a blob, the other fetches it over QUIC.
    #[tokio::test]
    async fn test_p2p_blob_transfer() {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();

        // Build endpoints directly using empty_builder to avoid external
        // DNS/relay dependencies in tests.
        let memory_lookup_a = iroh::address_lookup::memory::MemoryLookup::new();
        let endpoint_a = Endpoint::empty_builder()
            .address_lookup(memory_lookup_a.clone())
            .secret_key(iroh::SecretKey::from_bytes(&[1u8; 32]))
            .bind()
            .await
            .unwrap();

        let memory_lookup_b = iroh::address_lookup::memory::MemoryLookup::new();
        let endpoint_b = Endpoint::empty_builder()
            .address_lookup(memory_lookup_b.clone())
            .secret_key(iroh::SecretKey::from_bytes(&[2u8; 32]))
            .bind()
            .await
            .unwrap();

        let store_a = NetworkedIrohBlobStore::from_endpoint_with_protocols(
            dir_a.path().to_path_buf(),
            endpoint_a,
            memory_lookup_a,
            vec![],
        )
        .await
        .unwrap();

        let store_b = NetworkedIrohBlobStore::from_endpoint_with_protocols(
            dir_b.path().to_path_buf(),
            endpoint_b,
            memory_lookup_b.clone(),
            vec![],
        )
        .await
        .unwrap();

        // Tell store_b about store_a's full endpoint address
        memory_lookup_b.add_endpoint_info(store_a.endpoint().addr());
        store_b.add_peer(store_a.endpoint_id()).await;

        // Create a blob on store_a
        let data = b"Hello from store A via P2P!";
        let metadata = BlobMetadata::with_name("p2p_test.txt");
        let token = store_a
            .create_blob_from_bytes(data, metadata)
            .await
            .unwrap();

        // Fetch the blob from store_b (should download from store_a over QUIC)
        let handle = store_b.fetch_blob(&token, |_| {}).await.unwrap();

        let content = std::fs::read(&handle.path).unwrap();
        assert_eq!(content, data);

        // Shutdown may time out under heavy parallel test load; ignore errors
        let _ = store_a.shutdown().await;
        let _ = store_b.shutdown().await;
    }

    // ---- EndpointHooks tests ----

    /// `build_endpoint_with_hooks(None)` behaves identically to `build_endpoint`.
    #[tokio::test]
    async fn test_build_endpoint_without_hooks() {
        let config = IrohConfig {
            secret_key: Some([42u8; 32]),
            ..Default::default()
        };
        let (endpoint, _lookup) = NetworkedIrohBlobStore::build_endpoint(&config)
            .await
            .unwrap();
        // Endpoint should have a valid ID derived from the secret key
        let id_str = endpoint.id().to_string();
        assert!(!id_str.is_empty());
        endpoint.close().await;
    }

    /// `EndpointHooks` fire on incoming connections when installed via the builder.
    #[tokio::test]
    async fn test_build_endpoint_with_custom_hooks() {
        use iroh::protocol::Router;
        use std::sync::atomic::{AtomicBool, Ordering};

        #[derive(Debug)]
        struct TrackingHooks {
            after_handshake_called: Arc<AtomicBool>,
        }
        impl iroh::endpoint::EndpointHooks for TrackingHooks {
            fn after_handshake<'a>(
                &'a self,
                _conn: &'a iroh::endpoint::ConnectionInfo,
            ) -> impl std::future::Future<Output = iroh::endpoint::AfterHandshakeOutcome> + Send + 'a
            {
                self.after_handshake_called.store(true, Ordering::SeqCst);
                async { iroh::endpoint::AfterHandshakeOutcome::Accept }
            }
        }

        let called = Arc::new(AtomicBool::new(false));
        let hooks = TrackingHooks {
            after_handshake_called: called.clone(),
        };

        // Build endpoints directly using empty_builder to avoid external
        // DNS/relay dependencies in tests.
        let lookup_a = iroh::address_lookup::memory::MemoryLookup::new();
        let endpoint_a = Endpoint::empty_builder()
            .address_lookup(lookup_a.clone())
            .secret_key(iroh::SecretKey::from_bytes(&[43u8; 32]))
            .hooks(hooks)
            .bind()
            .await
            .unwrap();

        // Minimal handler so Router accepts on this ALPN
        #[derive(Debug)]
        struct Noop;
        impl iroh::protocol::ProtocolHandler for Noop {
            async fn accept(
                &self,
                _conn: iroh::endpoint::Connection,
            ) -> Result<(), iroh::protocol::AcceptError> {
                Ok(())
            }
        }

        let _router_a = Router::builder(endpoint_a.clone())
            .accept(b"test/hook/1", Noop)
            .spawn();

        // Build a second endpoint and connect to the first to trigger the hook
        let lookup_b = iroh::address_lookup::memory::MemoryLookup::new();
        let endpoint_b = Endpoint::empty_builder()
            .address_lookup(lookup_b.clone())
            .secret_key(iroh::SecretKey::from_bytes(&[44u8; 32]))
            .bind()
            .await
            .unwrap();

        // Tell B about A's full endpoint address
        lookup_b.add_endpoint_info(endpoint_a.addr());

        // B connects to A — this triggers A's after_handshake hook
        let conn = endpoint_b
            .connect(endpoint_a.id(), b"test/hook/1")
            .await
            .unwrap();

        // Give the hook a moment to fire
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            called.load(Ordering::SeqCst),
            "after_handshake hook should have been called"
        );

        conn.close(0u32.into(), b"done");
        endpoint_a.close().await;
        endpoint_b.close().await;
    }
}
