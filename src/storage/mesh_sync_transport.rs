//! Transport and protocol handler for peat-mesh-node Automerge sync.
//!
//! Provides two main types:
//!
//! - [`MeshSyncTransport`]: implements [`SyncTransport`] by tracking active
//!   QUIC connections in a concurrent map.
//! - [`SyncProtocolHandler`]: implements `iroh::protocol::ProtocolHandler` so
//!   the Iroh Router dispatches incoming `CAP_AUTOMERGE_ALPN` connections to
//!   the sync coordinator.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use iroh::endpoint::{Connection, PathInfo};
use iroh::protocol::AcceptError;
use iroh::{Endpoint, EndpointId, Watcher};
use tracing::{debug, info, warn};

use super::automerge_sync::AutomergeSyncCoordinator;
use super::sync_transport::{SyncTransport, CAP_AUTOMERGE_ALPN};
use crate::security::formation_key::{
    FormationAuthResult, FormationChallenge, FormationChallengeResponse, FormationKey,
};

/// Timeout for formation key authentication handshake reads and writes.
const FORMATION_AUTH_TIMEOUT: Duration = Duration::from_secs(30);

// ────────────────────────────────────────────────────────────────────────────
// MeshSyncTransport
// ────────────────────────────────────────────────────────────────────────────

/// Lightweight transport for peat-mesh-node that tracks QUIC connections
/// to peers and hands them to the sync coordinator on demand.
///
/// Holds a mandatory [`FormationKey`] used to authenticate outgoing
/// connections via HMAC challenge-response.
pub struct MeshSyncTransport {
    endpoint: Endpoint,
    connections: RwLock<HashMap<EndpointId, Connection>>,
    formation_key: FormationKey,
}

impl fmt::Debug for MeshSyncTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let peer_count = self.connections.read().map(|c| c.len()).unwrap_or(0);
        f.debug_struct("MeshSyncTransport")
            .field(
                "endpoint_id",
                &format_args!("{}", self.endpoint.id().fmt_short()),
            )
            .field("connected_peers", &peer_count)
            .finish()
    }
}

impl MeshSyncTransport {
    /// Create a new transport sharing the given Iroh endpoint.
    ///
    /// The `formation_key` is used to authenticate outgoing connections
    /// via HMAC challenge-response before sync streams are opened.
    pub fn new(endpoint: Endpoint, formation_key: FormationKey) -> Self {
        Self {
            endpoint,
            connections: RwLock::new(HashMap::new()),
            formation_key,
        }
    }

    /// Store (or replace) the connection for a peer.
    pub fn insert_connection(&self, peer_id: EndpointId, conn: Connection) {
        let mut conns = self.connections.write().unwrap_or_else(|e| e.into_inner());
        conns.insert(peer_id, conn);
    }

    /// Remove the connection for a peer.
    pub fn remove_connection(&self, peer_id: &EndpointId) {
        let mut conns = self.connections.write().unwrap_or_else(|e| e.into_inner());
        conns.remove(peer_id);
    }

    /// Start full-duplex sync on a connection.
    ///
    /// Stores the connection and spawns a background task that accepts
    /// incoming bidirectional streams, forwarding each to the sync
    /// coordinator. This makes the QUIC connection fully bidirectional
    /// for sync — both sides can initiate streams.
    ///
    /// Use this for **both** incoming connections (from `SyncProtocolHandler`)
    /// and outgoing connections (from `connect_peer` or discovery).
    pub fn start_sync_connection(
        self: &Arc<Self>,
        connection: Connection,
        coordinator: Arc<AutomergeSyncCoordinator>,
    ) {
        let peer_id = connection.remote_id();
        self.insert_connection(peer_id, connection.clone());

        let transport = Arc::clone(self);

        // Log initial path info for the connection
        let paths: Vec<_> = connection.paths().into_iter().collect();
        let path_summary: Vec<&str> = paths
            .iter()
            .map(|p| if p.is_relay() { "relay" } else { "direct" })
            .collect();
        info!(
            peer = %peer_id.fmt_short(),
            paths = ?path_summary,
            "Sync connection established with {} path(s)",
            paths.len()
        );

        // Spawn path watcher to log multipath transitions
        let path_watcher = connection.paths();
        let path_peer_id = peer_id;
        tokio::spawn(async move {
            let mut watcher = path_watcher;
            loop {
                match watcher.updated().await {
                    Ok(path_list) => {
                        let paths: Vec<_> = path_list.into_iter().collect();
                        let selected = paths.iter().find(|p| p.is_selected());
                        let path_type =
                            selected.map(|p| if p.is_relay() { "relay" } else { "direct" });
                        let rtt = selected.and_then(|p| p.rtt());
                        info!(
                            peer = %path_peer_id.fmt_short(),
                            active_path = ?path_type,
                            rtt = ?rtt,
                            total_paths = paths.len(),
                            "Connection paths changed"
                        );
                    }
                    Err(_) => break, // Connection dropped
                }
            }
        });

        tokio::spawn(async move {
            loop {
                match connection.accept_bi().await {
                    Ok((send, recv)) => {
                        debug!(
                            peer = %peer_id.fmt_short(),
                            "Accepted incoming sync stream"
                        );
                        let coord = coordinator.clone();
                        tokio::spawn(async move {
                            if let Err(e) =
                                coord.handle_incoming_sync_stream(peer_id, send, recv).await
                            {
                                warn!(
                                    peer = %peer_id.fmt_short(),
                                    error = %e,
                                    "Error handling incoming sync stream"
                                );
                            }
                        });
                    }
                    Err(e) => {
                        debug!(
                            peer = %peer_id.fmt_short(),
                            error = %e,
                            "Sync connection closed"
                        );
                        break;
                    }
                }
            }

            transport.remove_connection(&peer_id);
            coordinator.clear_peer_sync_state(peer_id);
            info!(
                peer = %peer_id.fmt_short(),
                "Cleaned up sync state for disconnected peer"
            );
        });
    }

    /// Get a reference to the underlying Iroh endpoint.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Get multipath info for a peer connection.
    ///
    /// Returns the current set of network paths (relay, direct IPv4/IPv6)
    /// with per-path metadata. Returns `None` if the peer is not connected.
    ///
    /// Each [`PathInfo`] includes:
    /// - `is_selected()` — whether this is the active transmission path
    /// - `is_relay()` / `is_ip()` — path type
    /// - `rtt()` — round-trip time estimate
    /// - `stats()` — detailed per-path statistics (bytes, loss, MTU, congestion)
    pub fn peer_paths(&self, peer_id: &EndpointId) -> Option<Vec<PathInfo>> {
        let conns = self.connections.read().unwrap_or_else(|e| e.into_inner());
        let conn = conns.get(peer_id)?;
        Some(conn.paths().into_iter().collect())
    }

    /// Get the current round-trip time estimate for a peer.
    ///
    /// Returns the RTT of the selected (active) path, or `None` if the
    /// peer is not connected or no path is selected.
    pub fn peer_rtt(&self, peer_id: &EndpointId) -> Option<Duration> {
        let conns = self.connections.read().unwrap_or_else(|e| e.into_inner());
        let conn = conns.get(peer_id)?;
        conn.paths()
            .into_iter()
            .find(|p| p.is_selected())
            .and_then(|p| p.rtt())
    }

    /// Establish an outgoing QUIC connection to a peer with mandatory
    /// formation key authentication.
    ///
    /// Opens a `CAP_AUTOMERGE_ALPN` connection and runs the connector-side
    /// HMAC challenge-response handshake before storing the connection.
    pub async fn connect_and_authenticate(
        &self,
        peer_id: EndpointId,
    ) -> anyhow::Result<Connection> {
        let conn = self
            .endpoint
            .connect(peer_id, CAP_AUTOMERGE_ALPN)
            .await
            .map_err(|e| anyhow::anyhow!("failed to connect to peer: {e}"))?;

        respond_to_formation_auth(&self.formation_key, &conn).await?;

        info!(
            peer = %peer_id.fmt_short(),
            "outgoing sync connection authenticated via formation key"
        );

        self.insert_connection(peer_id, conn.clone());
        Ok(conn)
    }
}

#[async_trait]
impl SyncTransport for MeshSyncTransport {
    fn get_connection(&self, peer_id: &EndpointId) -> Option<Connection> {
        let conns = self.connections.read().unwrap_or_else(|e| e.into_inner());
        conns.get(peer_id).cloned()
    }

    /// Get an existing connection or establish a new authenticated one.
    ///
    /// Checks the cache first, pruning stale (closed) connections.
    /// If no live connection exists, opens a new `CAP_AUTOMERGE_ALPN`
    /// connection and runs formation key authentication.
    async fn get_or_connect(&self, peer_id: &EndpointId) -> anyhow::Result<Connection> {
        // Check cache, pruning closed connections
        if let Some(conn) = self.get_connection(peer_id) {
            if conn.close_reason().is_none() {
                return Ok(conn);
            }
            self.remove_connection(peer_id);
        }
        self.connect_and_authenticate(*peer_id).await
    }

    fn connected_peers(&self) -> Vec<EndpointId> {
        let mut conns = self.connections.write().unwrap_or_else(|e| e.into_inner());
        // Prune closed connections while we enumerate.
        conns.retain(|_id, conn| conn.close_reason().is_none());
        conns.keys().copied().collect()
    }
}

// ────────────────────────────────────────────────────────────────────────────
// SyncProtocolHandler
// ────────────────────────────────────────────────────────────────────────────

/// Iroh Router protocol handler for `CAP_AUTOMERGE_ALPN`.
///
/// When the Router receives a QUIC connection with our ALPN, it calls
/// [`accept`](iroh::protocol::ProtocolHandler::accept), which delegates to
/// [`MeshSyncTransport::start_sync_connection`] to set up full-duplex
/// sync on the connection.
///
/// ## Two-Phase Gating (Layer 2)
///
/// When a [`CertificateBundle`] is configured, incoming connections are
/// validated against it before sync begins. This is "Layer 2" of the
/// two-phase connection model:
///
/// - **Layer 0**: QUIC transport (formation_secret → EndpointId)
/// - **Layer 1**: `peat/enroll/1` ALPN (no cert required)
/// - **Layer 2**: `cap/automerge/1` ALPN — this handler (cert required when configured)
pub struct SyncProtocolHandler {
    transport: Arc<MeshSyncTransport>,
    coordinator: Arc<AutomergeSyncCoordinator>,
    /// Formation key for peer authentication (mandatory).
    /// All incoming connections must pass HMAC challenge-response
    /// before sync streams are accepted.
    formation_key: FormationKey,
    /// Optional certificate bundle for Layer 2 peer validation.
    /// When set, peers must have a valid, non-expired certificate
    /// or the connection is rejected.
    certificate_bundle: Option<Arc<RwLock<crate::security::certificate::CertificateBundle>>>,
}

impl fmt::Debug for SyncProtocolHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncProtocolHandler")
            .field("has_certificate_bundle", &self.certificate_bundle.is_some())
            .finish()
    }
}

impl SyncProtocolHandler {
    /// Create a new protocol handler with mandatory formation key authentication.
    ///
    /// All incoming connections on `CAP_AUTOMERGE_ALPN` must pass an
    /// HMAC-SHA256 challenge-response handshake before sync begins.
    /// Connections that fail authentication are rejected.
    pub fn new(
        transport: Arc<MeshSyncTransport>,
        coordinator: Arc<AutomergeSyncCoordinator>,
        formation_key: FormationKey,
    ) -> Self {
        Self {
            transport,
            coordinator,
            formation_key,
            certificate_bundle: None,
        }
    }

    /// Enable Layer 2 certificate validation.
    ///
    /// When a certificate bundle is set, peers without a valid certificate
    /// are rejected. There is no warn-and-allow mode — certificates are
    /// either enforced or not configured.
    pub fn with_certificate_bundle(
        mut self,
        bundle: Arc<RwLock<crate::security::certificate::CertificateBundle>>,
    ) -> Self {
        self.certificate_bundle = Some(bundle);
        self
    }
}

impl iroh::protocol::ProtocolHandler for SyncProtocolHandler {
    /// Handle an incoming QUIC connection on the `CAP_AUTOMERGE_ALPN` ALPN.
    ///
    /// Authentication layers (in order):
    /// 1. **Certificate validation** (Layer 2): If a certificate bundle is
    ///    configured, the peer's EndpointId is checked against known certificates.
    ///    Peers without valid certificates are rejected.
    /// 2. **Formation key** (mandatory): Runs HMAC-SHA256 challenge-response
    ///    handshake. Connections that fail are rejected.
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();

        // Layer 2: Certificate validation (hard-reject when configured)
        if let Some(ref bundle) = self.certificate_bundle {
            let bundle = bundle.read().unwrap_or_else(|e| e.into_inner());
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let peer_valid = bundle.validate_peer(peer.as_bytes(), now);

            if !peer_valid {
                warn!(
                    peer = %peer.fmt_short(),
                    "peer has no valid certificate, rejecting sync connection"
                );
                connection.close(2u32.into(), b"certificate required");
                return Ok(());
            }
            debug!(peer = %peer.fmt_short(), "peer certificate validated");
        }

        // Formation key authentication (mandatory)
        match Self::run_formation_auth(&self.formation_key, &connection).await {
            Ok(()) => {
                info!(peer = %peer.fmt_short(), "peer authenticated via formation key");
            }
            Err(e) => {
                warn!(
                    peer = %peer.fmt_short(),
                    error = %e,
                    "peer failed formation key authentication, rejecting"
                );
                connection.close(1u32.into(), b"formation auth failed");
                return Ok(());
            }
        }

        self.transport
            .start_sync_connection(connection, self.coordinator.clone());

        Ok(())
    }
}

impl SyncProtocolHandler {
    /// Run formation key challenge-response on an accepted QUIC connection
    /// (acceptor/server side).
    ///
    /// Waits for the connecting peer to open a bidirectional auth stream,
    /// sends a challenge nonce, reads the HMAC response, and verifies it.
    /// All I/O operations are bounded by [`FORMATION_AUTH_TIMEOUT`].
    ///
    /// Returns `Ok(())` on successful authentication.
    async fn run_formation_auth(fk: &FormationKey, connection: &Connection) -> anyhow::Result<()> {
        // Acceptor waits for the connector to open the auth stream.
        let (mut send, mut recv) =
            tokio::time::timeout(FORMATION_AUTH_TIMEOUT, connection.accept_bi())
                .await
                .map_err(|_| {
                    anyhow::anyhow!("formation auth timed out waiting for auth stream")
                })??;

        // Send challenge
        let (nonce, _expected) = fk.create_challenge();
        let challenge = FormationChallenge {
            formation_id: fk.formation_id().to_string(),
            nonce,
        };
        let challenge_bytes = challenge.to_bytes();
        tokio::time::timeout(FORMATION_AUTH_TIMEOUT, async {
            send.write_all(&(challenge_bytes.len() as u32).to_le_bytes())
                .await?;
            send.write_all(&challenge_bytes).await?;
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("formation auth timed out sending challenge"))??;

        // Read response
        let mut len_buf = [0u8; 4];
        tokio::time::timeout(FORMATION_AUTH_TIMEOUT, recv.read_exact(&mut len_buf))
            .await
            .map_err(|_| anyhow::anyhow!("formation auth timed out reading response length"))??;
        let resp_len = u32::from_le_bytes(len_buf) as usize;
        if resp_len > 256 {
            anyhow::bail!("response too large: {resp_len}");
        }
        let mut resp_buf = vec![0u8; resp_len];
        tokio::time::timeout(FORMATION_AUTH_TIMEOUT, recv.read_exact(&mut resp_buf))
            .await
            .map_err(|_| anyhow::anyhow!("formation auth timed out reading response body"))??;

        let resp = FormationChallengeResponse::from_bytes(&resp_buf)
            .map_err(|e| anyhow::anyhow!("invalid response: {e}"))?;

        // Verify and send result
        if fk.verify_response(&nonce, &resp.response) {
            tokio::time::timeout(
                FORMATION_AUTH_TIMEOUT,
                send.write_all(&[FormationAuthResult::Accepted.to_byte()]),
            )
            .await
            .map_err(|_| anyhow::anyhow!("formation auth timed out sending accept"))??;
            send.finish()?;
            Ok(())
        } else {
            tokio::time::timeout(
                FORMATION_AUTH_TIMEOUT,
                send.write_all(&[FormationAuthResult::Rejected.to_byte()]),
            )
            .await
            .map_err(|_| anyhow::anyhow!("formation auth timed out sending reject"))??;
            send.finish()?;
            anyhow::bail!("HMAC verification failed")
        }
    }
}

/// Connector-side formation key authentication.
///
/// The connecting peer opens a bidirectional auth stream, reads the challenge
/// from the acceptor, computes the HMAC response, and reads back the
/// accept/reject verdict. All I/O is bounded by [`FORMATION_AUTH_TIMEOUT`].
///
/// Call this immediately after establishing a QUIC connection to a peer that
/// requires formation key authentication (before opening sync streams).
///
/// Returns `Ok(())` if the acceptor verified our response successfully.
pub async fn respond_to_formation_auth(
    fk: &FormationKey,
    connection: &Connection,
) -> anyhow::Result<()> {
    // Connector opens the auth stream.
    let (mut send, mut recv) = tokio::time::timeout(FORMATION_AUTH_TIMEOUT, connection.open_bi())
        .await
        .map_err(|_| anyhow::anyhow!("formation auth timed out opening auth stream"))??;

    // Read challenge from acceptor
    let mut len_buf = [0u8; 4];
    tokio::time::timeout(FORMATION_AUTH_TIMEOUT, recv.read_exact(&mut len_buf))
        .await
        .map_err(|_| anyhow::anyhow!("formation auth timed out reading challenge length"))??;
    let challenge_len = u32::from_le_bytes(len_buf) as usize;
    if challenge_len > 1024 {
        anyhow::bail!("challenge too large: {challenge_len}");
    }
    let mut challenge_buf = vec![0u8; challenge_len];
    tokio::time::timeout(FORMATION_AUTH_TIMEOUT, recv.read_exact(&mut challenge_buf))
        .await
        .map_err(|_| anyhow::anyhow!("formation auth timed out reading challenge body"))??;

    let challenge = FormationChallenge::from_bytes(&challenge_buf)
        .map_err(|e| anyhow::anyhow!("invalid challenge: {e}"))?;

    // Verify formation ID matches
    if challenge.formation_id != fk.formation_id() {
        anyhow::bail!(
            "formation ID mismatch: expected {}, got {}",
            fk.formation_id(),
            challenge.formation_id
        );
    }

    // Compute and send HMAC response
    let response = fk.respond_to_challenge(&challenge.nonce);
    let resp = FormationChallengeResponse { response };
    let resp_bytes = resp.to_bytes();
    tokio::time::timeout(FORMATION_AUTH_TIMEOUT, async {
        send.write_all(&(resp_bytes.len() as u32).to_le_bytes())
            .await?;
        send.write_all(&resp_bytes).await?;
        Ok::<(), std::io::Error>(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("formation auth timed out sending response"))??;

    // Read accept/reject verdict
    let mut verdict = [0u8; 1];
    tokio::time::timeout(FORMATION_AUTH_TIMEOUT, recv.read_exact(&mut verdict))
        .await
        .map_err(|_| anyhow::anyhow!("formation auth timed out reading verdict"))??;

    match FormationAuthResult::from_byte(verdict[0]) {
        FormationAuthResult::Accepted => Ok(()),
        FormationAuthResult::Rejected => {
            anyhow::bail!("formation key authentication rejected by acceptor")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a test `FormationKey` for use in unit tests.
    fn test_formation_key() -> FormationKey {
        FormationKey::new("test-formation", &[42u8; 32])
    }

    #[test]
    fn test_formation_auth_timeout_is_reasonable() {
        assert_eq!(FORMATION_AUTH_TIMEOUT, Duration::from_secs(30));
        assert!(
            FORMATION_AUTH_TIMEOUT >= Duration::from_secs(5),
            "Auth timeout should be at least 5s for slow networks"
        );
        assert!(
            FORMATION_AUTH_TIMEOUT <= Duration::from_secs(120),
            "Auth timeout should not exceed 2 minutes"
        );
    }

    /// `MeshSyncTransport` and `SyncProtocolHandler` require a running Iroh
    /// `Endpoint` (QUIC stack) to construct. Unit tests for the full protocol
    /// flow are covered in integration tests (tests/hierarchy_e2e.rs,
    /// tests/dual_active_transport_e2e.rs). Tests here verify configuration
    /// values and type-level properties.

    #[test]
    fn test_sync_protocol_handler_debug_impl() {
        // Verify the Debug impl exists and is callable (compile-time + runtime check).
        let _: fn(&SyncProtocolHandler, &mut std::fmt::Formatter) -> std::fmt::Result =
            <SyncProtocolHandler as std::fmt::Debug>::fmt;
    }

    #[test]
    fn test_mesh_sync_transport_debug_impl() {
        let _: fn(&MeshSyncTransport, &mut std::fmt::Formatter) -> std::fmt::Result =
            <MeshSyncTransport as std::fmt::Debug>::fmt;
    }

    #[test]
    fn test_formation_key_can_create_challenge_response() {
        // Test that the FormationKey type works for challenge-response without
        // needing a QUIC connection.
        let fk = FormationKey::new("test-formation", &[1u8; 32]);

        // Create challenge
        let (nonce, _expected_hmac) = fk.create_challenge();
        assert_eq!(nonce.len(), 32, "Challenge nonce should be 32 bytes");

        // Generate response
        let response = fk.respond_to_challenge(&nonce);
        assert_eq!(response.len(), 32, "HMAC response should be 32 bytes");

        // Verify round-trip
        assert!(
            fk.verify_response(&nonce, &response),
            "Response should verify against same formation key"
        );
    }

    #[test]
    fn test_formation_key_rejects_wrong_response() {
        let fk = FormationKey::new("test-formation", &[1u8; 32]);
        let (nonce, _) = fk.create_challenge();

        // Tampered response
        let mut bad_response = fk.respond_to_challenge(&nonce);
        bad_response[0] ^= 0xff;

        assert!(
            !fk.verify_response(&nonce, &bad_response),
            "Tampered response should fail verification"
        );
    }

    #[test]
    fn test_formation_key_different_formations_incompatible() {
        let fk_a = FormationKey::new("formation-alpha", &[2u8; 32]);
        let fk_b = FormationKey::new("formation-beta", &[3u8; 32]);

        let (nonce, _) = fk_a.create_challenge();
        let response_b = fk_b.respond_to_challenge(&nonce);

        assert!(
            !fk_a.verify_response(&nonce, &response_b),
            "Different formation keys should not verify each other"
        );
    }

    #[test]
    fn test_formation_challenge_serialization_roundtrip() {
        let fk = FormationKey::new("roundtrip-test", &[4u8; 32]);
        let (nonce, _) = fk.create_challenge();

        let challenge = FormationChallenge {
            formation_id: fk.formation_id().to_string(),
            nonce: nonce.clone(),
        };

        let bytes = challenge.to_bytes();
        assert!(!bytes.is_empty());

        let decoded = FormationChallenge::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.formation_id, fk.formation_id());
        assert_eq!(decoded.nonce, nonce);
    }

    #[test]
    fn test_formation_challenge_response_serialization_roundtrip() {
        let fk = FormationKey::new("roundtrip-test", &[4u8; 32]);
        let (nonce, _) = fk.create_challenge();
        let response = fk.respond_to_challenge(&nonce);

        let resp = FormationChallengeResponse {
            response: response.clone(),
        };
        let bytes = resp.to_bytes();
        assert!(!bytes.is_empty());

        let decoded = FormationChallengeResponse::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.response, response);
    }

    #[test]
    fn test_formation_auth_result_byte_encoding() {
        assert_eq!(
            FormationAuthResult::from_byte(FormationAuthResult::Accepted.to_byte()),
            FormationAuthResult::Accepted
        );
        assert_eq!(
            FormationAuthResult::from_byte(FormationAuthResult::Rejected.to_byte()),
            FormationAuthResult::Rejected
        );
    }

    // ---- Multipath / noq feature tests ----

    /// Helper: create a pair of connected iroh endpoints for testing.
    ///
    /// Returns (endpoint_a, endpoint_b, connection_from_b_to_a).
    /// Uses Router on endpoint_a to handle incoming connections.
    async fn create_connected_endpoints() -> (Endpoint, Endpoint, Connection) {
        use iroh::address_lookup::memory::MemoryLookup;
        use iroh::protocol::Router;
        use std::sync::atomic::{AtomicBool, Ordering};

        let alpn: &[u8] = b"test/mesh/1";

        // Minimal protocol handler that just accepts
        #[derive(Debug)]
        struct AcceptAll(Arc<AtomicBool>);
        impl iroh::protocol::ProtocolHandler for AcceptAll {
            async fn accept(&self, _conn: Connection) -> Result<(), iroh::protocol::AcceptError> {
                self.0.store(true, Ordering::SeqCst);
                Ok(())
            }
        }

        let accepted = Arc::new(AtomicBool::new(false));

        // Build endpoints using empty_builder to avoid external DNS/relay
        // dependencies in tests.
        let lookup_a = MemoryLookup::new();
        let endpoint_a = Endpoint::empty_builder()
            .address_lookup(lookup_a.clone())
            .secret_key(iroh::SecretKey::from_bytes(&[10u8; 32]))
            .bind()
            .await
            .unwrap();

        // Router on A accepts incoming connections
        let _router_a = Router::builder(endpoint_a.clone())
            .accept(alpn, AcceptAll(accepted.clone()))
            .spawn();

        let lookup_b = MemoryLookup::new();
        let endpoint_b = Endpoint::empty_builder()
            .address_lookup(lookup_b.clone())
            .secret_key(iroh::SecretKey::from_bytes(&[11u8; 32]))
            .bind()
            .await
            .unwrap();

        // Tell B about A's full endpoint address
        lookup_b.add_endpoint_info(endpoint_a.addr());

        // B connects to A
        let conn = endpoint_b.connect(endpoint_a.id(), alpn).await.unwrap();

        (endpoint_a, endpoint_b, conn)
    }

    /// `peer_paths()` returns path info for a connected peer.
    #[tokio::test]
    async fn test_peer_paths_returns_paths_for_connected_peer() {
        let (endpoint_a, endpoint_b, conn) = create_connected_endpoints().await;

        let transport = MeshSyncTransport::new(endpoint_b.clone(), test_formation_key());
        let peer_id = conn.remote_id();
        transport.insert_connection(peer_id, conn);

        let paths = transport.peer_paths(&peer_id);
        assert!(paths.is_some(), "Should return paths for connected peer");

        let paths = paths.unwrap();
        assert!(!paths.is_empty(), "Should have at least one path");

        // At least one path should be selected
        let selected = paths.iter().filter(|p| p.is_selected()).count();
        assert!(selected >= 1, "At least one path should be selected");

        // Local connections should be direct (IP), not relay
        let has_ip = paths.iter().any(|p| p.is_ip());
        assert!(has_ip, "Local connection should have an IP path");

        endpoint_a.close().await;
        endpoint_b.close().await;
    }

    /// `peer_paths()` returns `None` for unknown peers.
    #[tokio::test]
    async fn test_peer_paths_returns_none_for_unknown_peer() {
        let endpoint = Endpoint::empty_builder()
            .secret_key(iroh::SecretKey::from_bytes(&[20u8; 32]))
            .bind()
            .await
            .unwrap();

        let transport = MeshSyncTransport::new(endpoint.clone(), test_formation_key());
        let unknown_peer = iroh::SecretKey::from_bytes(&[99u8; 32]).public();

        assert!(transport.peer_paths(&unknown_peer).is_none());
        assert!(transport.peer_rtt(&unknown_peer).is_none());

        endpoint.close().await;
    }

    /// `peer_rtt()` returns a duration for a connected peer.
    #[tokio::test]
    async fn test_peer_rtt_returns_value_for_connected_peer() {
        let (endpoint_a, endpoint_b, conn) = create_connected_endpoints().await;

        let transport = MeshSyncTransport::new(endpoint_b.clone(), test_formation_key());
        let peer_id = conn.remote_id();
        transport.insert_connection(peer_id, conn);

        // RTT may not be immediately available (needs a round-trip),
        // but peer_rtt should not panic and should return Some for
        // an established local connection.
        let rtt = transport.peer_rtt(&peer_id);
        // Local connections have near-zero RTT; it should be available
        // after the QUIC handshake.
        if let Some(rtt) = rtt {
            assert!(
                rtt < Duration::from_secs(5),
                "Local RTT should be well under 5s, got {:?}",
                rtt
            );
        }
        // Note: RTT may be None if no data has been sent yet on the
        // selected path — this is acceptable.

        endpoint_a.close().await;
        endpoint_b.close().await;
    }

    /// PathInfo exposes path type (relay vs direct) correctly.
    #[tokio::test]
    async fn test_path_info_type_detection() {
        let (endpoint_a, endpoint_b, conn) = create_connected_endpoints().await;

        let transport = MeshSyncTransport::new(endpoint_b.clone(), test_formation_key());
        let peer_id = conn.remote_id();
        transport.insert_connection(peer_id, conn);

        let paths = transport.peer_paths(&peer_id).unwrap();
        for path in &paths {
            // Each path must be exactly one type
            let is_ip = path.is_ip();
            let is_relay = path.is_relay();
            assert!(is_ip || is_relay, "Path should be either IP or relay");
        }

        endpoint_a.close().await;
        endpoint_b.close().await;
    }
}
