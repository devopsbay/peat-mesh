//! Transport and routing abstractions for sync operations
//!
//! These traits decouple the sync coordinator from concrete transport
//! implementations (e.g., IrohTransport) and routing strategies (e.g.,
//! HierarchicalRouter), enabling reuse across peat-mesh and peat-mesh-node.

use async_trait::async_trait;
use iroh::endpoint::Connection;
use iroh::EndpointId;

use super::automerge_sync::SyncDirection;

/// ALPN protocol identifier for Peat Protocol Automerge sync
pub const CAP_AUTOMERGE_ALPN: &[u8] = b"cap/automerge/1";

/// Transport abstraction for sync operations
///
/// Provides the minimal connection management surface needed by
/// `AutomergeSyncCoordinator` and `SyncChannelManager`.
#[async_trait]
pub trait SyncTransport: Send + Sync + 'static {
    /// Get an existing connection to a peer, if one exists.
    fn get_connection(&self, peer_id: &EndpointId) -> Option<Connection>;

    /// Get an existing connection or establish a new authenticated one.
    ///
    /// The default implementation simply delegates to [`get_connection`](Self::get_connection).
    /// [`MeshSyncTransport`](super::mesh_sync_transport::MeshSyncTransport)
    /// overrides this to establish outgoing connections with formation key
    /// authentication when no cached connection exists.
    async fn get_or_connect(&self, peer_id: &EndpointId) -> anyhow::Result<Connection> {
        self.get_connection(peer_id)
            .ok_or_else(|| anyhow::anyhow!("no connection to peer"))
    }

    /// List all currently connected peer IDs.
    fn connected_peers(&self) -> Vec<EndpointId>;
}

/// Routing abstraction for hierarchical sync direction
///
/// When present, the coordinator uses this to route sync messages based
/// on document direction (Upward, Downward, Lateral, Broadcast).
/// When absent, the coordinator broadcasts to all connected peers.
#[async_trait]
pub trait SyncRouter: Send + Sync + 'static {
    /// Get the peers to sync with for the given direction.
    ///
    /// `connected` is the current set of connected peers (from `SyncTransport::connected_peers()`).
    async fn get_targets(
        &self,
        direction: SyncDirection,
        connected: &[EndpointId],
    ) -> Vec<EndpointId>;

    /// Whether this node is the cell leader.
    async fn is_leader(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alpn_identifier_is_valid() {
        assert_eq!(CAP_AUTOMERGE_ALPN, b"cap/automerge/1");
        assert!(!CAP_AUTOMERGE_ALPN.is_empty());
        // ALPN identifiers must be ASCII and reasonably short
        assert!(CAP_AUTOMERGE_ALPN.len() < 256);
        assert!(CAP_AUTOMERGE_ALPN.iter().all(|b| b.is_ascii()));
    }

    #[test]
    fn test_alpn_contains_version() {
        let alpn_str = std::str::from_utf8(CAP_AUTOMERGE_ALPN).unwrap();
        assert!(
            alpn_str.contains("/1"),
            "ALPN should include version suffix"
        );
        assert!(
            alpn_str.starts_with("cap/"),
            "ALPN should start with protocol prefix"
        );
    }
}
