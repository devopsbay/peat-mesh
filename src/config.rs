//! Configuration types for the PeatMesh facade.

use crate::topology::TopologyConfig;
use crate::transport::TransportManagerConfig;
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the Iroh networking layer.
#[derive(Debug, Clone)]
pub struct IrohConfig {
    /// Explicit bind address for the Iroh endpoint.
    /// When `None`, Iroh picks an available port automatically.
    pub bind_addr: Option<std::net::SocketAddr>,
    /// Relay server URLs for NAT traversal.
    /// Empty means use Iroh's default relay infrastructure.
    pub relay_urls: Vec<String>,
    /// Deterministic Iroh identity seed (32 bytes).
    /// When `Some`, used as `SecretKey::from_bytes()` for the Iroh endpoint,
    /// giving the node a stable, reproducible identity.
    pub secret_key: Option<[u8; 32]>,
    /// Timeout for binding the QUIC endpoint (default: 10s).
    pub bind_timeout: Duration,
    /// Timeout for graceful shutdown (default: 5s).
    pub shutdown_timeout: Duration,
    /// Timeout for blob download operations from remote peers (default: 30s).
    pub download_timeout: Duration,
}

impl Default for IrohConfig {
    fn default() -> Self {
        Self {
            bind_addr: None,
            relay_urls: Vec::new(),
            secret_key: None,
            bind_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(5),
            download_timeout: Duration::from_secs(30),
        }
    }
}

/// Top-level configuration for PeatMesh.
///
/// Composes topology, discovery, and security settings into a single
/// configuration struct that drives the [`crate::mesh::PeatMesh`] facade.
#[derive(Debug, Clone, Default)]
pub struct MeshConfig {
    /// Node identifier. Auto-generated (UUID v4) if `None`.
    pub node_id: Option<String>,
    /// Optional path for persistent storage.
    pub storage_path: Option<PathBuf>,
    /// Topology formation configuration.
    pub topology: TopologyConfig,
    /// Peer discovery configuration.
    pub discovery: MeshDiscoveryConfig,
    /// Security configuration.
    pub security: SecurityConfig,
    /// Transport manager configuration for multi-transport selection.
    pub transport_manager: Option<TransportManagerConfig>,
    /// Iroh networking configuration.
    pub iroh: IrohConfig,
    /// Automerge CRDT compaction configuration.
    pub compaction: CompactionConfig,
}

/// Configuration for periodic Automerge document compaction.
///
/// Compaction discards CRDT revision history via `fork()`, keeping only the
/// current document state. This bounds memory growth on long-running nodes
/// at the cost of a full-state sync on the next peer exchange.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Enable automatic background compaction. Default: `false`.
    pub enabled: bool,
    /// How often the compaction sweep runs. Default: 5 minutes.
    /// Clamped to a minimum of 10 seconds to prevent busy-looping.
    pub interval: Duration,
    /// Only compact documents larger than this threshold. Default: 64 KiB.
    pub size_threshold_bytes: usize,
    /// Collections to compact. When empty and compaction is enabled,
    /// auto-derives from `SyncModeRegistry` (all `LatestOnly` collections).
    /// Only `LatestOnly` collections are safe to compact — compacting
    /// `FullHistory` collections destroys change history needed for delta sync.
    pub collections: Vec<String>,
}

/// Minimum compaction interval to prevent busy-looping (10 seconds).
const MIN_COMPACTION_INTERVAL: Duration = Duration::from_secs(10);

impl CompactionConfig {
    /// Returns the effective interval, clamped to at least 10 seconds.
    pub fn effective_interval(&self) -> Duration {
        self.interval.max(MIN_COMPACTION_INTERVAL)
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: Duration::from_secs(300),
            size_threshold_bytes: 64 * 1024,
            collections: Vec::new(),
        }
    }
}

/// Discovery settings for mesh peer discovery.
#[derive(Debug, Clone)]
pub struct MeshDiscoveryConfig {
    /// Enable mDNS-based peer discovery.
    pub mdns_enabled: bool,
    /// Service name advertised during discovery.
    pub service_name: String,
    /// Discovery broadcast interval.
    pub interval: Duration,
}

impl Default for MeshDiscoveryConfig {
    fn default() -> Self {
        Self {
            mdns_enabled: true,
            service_name: "peat-mesh".to_string(),
            interval: Duration::from_secs(30),
        }
    }
}

/// Security settings for mesh communications.
#[derive(Debug, Clone)]
pub struct SecurityConfig {
    /// Enable encryption for mesh communications.
    pub encryption_enabled: bool,
    /// Require cryptographic peer identity verification.
    pub require_peer_verification: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            encryption_enabled: true,
            require_peer_verification: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── MeshConfig defaults ──────────────────────────────────────

    #[test]
    fn test_mesh_config_default_node_id_is_none() {
        let cfg = MeshConfig::default();
        assert!(cfg.node_id.is_none());
    }

    #[test]
    fn test_mesh_config_default_storage_path_is_none() {
        let cfg = MeshConfig::default();
        assert!(cfg.storage_path.is_none());
    }

    #[test]
    fn test_mesh_config_default_topology() {
        let cfg = MeshConfig::default();
        // TopologyConfig default reevaluation_interval is 30s
        assert_eq!(
            cfg.topology.reevaluation_interval,
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn test_mesh_config_default_discovery() {
        let cfg = MeshConfig::default();
        assert!(cfg.discovery.mdns_enabled);
        assert_eq!(cfg.discovery.service_name, "peat-mesh");
    }

    #[test]
    fn test_mesh_config_default_security() {
        let cfg = MeshConfig::default();
        assert!(cfg.security.encryption_enabled);
        assert!(!cfg.security.require_peer_verification);
    }

    #[test]
    fn test_mesh_config_custom_values() {
        let cfg = MeshConfig {
            node_id: Some("custom-node".to_string()),
            storage_path: Some(PathBuf::from("/tmp/mesh")),
            discovery: MeshDiscoveryConfig {
                mdns_enabled: false,
                service_name: "my-mesh".to_string(),
                interval: Duration::from_secs(10),
            },
            security: SecurityConfig {
                encryption_enabled: false,
                require_peer_verification: true,
            },
            ..Default::default()
        };
        assert_eq!(cfg.node_id.as_deref(), Some("custom-node"));
        assert_eq!(
            cfg.storage_path.as_deref(),
            Some(std::path::Path::new("/tmp/mesh"))
        );
        assert!(!cfg.discovery.mdns_enabled);
        assert_eq!(cfg.discovery.service_name, "my-mesh");
        assert_eq!(cfg.discovery.interval, Duration::from_secs(10));
        assert!(!cfg.security.encryption_enabled);
        assert!(cfg.security.require_peer_verification);
    }

    #[test]
    fn test_mesh_config_clone() {
        let cfg = MeshConfig {
            node_id: Some("cloned".to_string()),
            ..Default::default()
        };
        let cloned = cfg.clone();
        assert_eq!(cloned.node_id, cfg.node_id);
    }

    #[test]
    fn test_mesh_config_debug() {
        let cfg = MeshConfig::default();
        let debug = format!("{:?}", cfg);
        assert!(debug.contains("MeshConfig"));
    }

    // ── MeshDiscoveryConfig defaults ─────────────────────────────

    #[test]
    fn test_discovery_config_default_mdns_enabled() {
        let cfg = MeshDiscoveryConfig::default();
        assert!(cfg.mdns_enabled);
    }

    #[test]
    fn test_discovery_config_default_service_name() {
        let cfg = MeshDiscoveryConfig::default();
        assert_eq!(cfg.service_name, "peat-mesh");
    }

    #[test]
    fn test_discovery_config_default_interval() {
        let cfg = MeshDiscoveryConfig::default();
        assert_eq!(cfg.interval, Duration::from_secs(30));
    }

    #[test]
    fn test_discovery_config_custom() {
        let cfg = MeshDiscoveryConfig {
            mdns_enabled: false,
            service_name: "custom".to_string(),
            interval: Duration::from_secs(5),
        };
        assert!(!cfg.mdns_enabled);
        assert_eq!(cfg.service_name, "custom");
        assert_eq!(cfg.interval, Duration::from_secs(5));
    }

    #[test]
    fn test_discovery_config_clone() {
        let cfg = MeshDiscoveryConfig::default();
        let cloned = cfg.clone();
        assert_eq!(cloned.service_name, cfg.service_name);
    }

    #[test]
    fn test_discovery_config_debug() {
        let cfg = MeshDiscoveryConfig::default();
        let debug = format!("{:?}", cfg);
        assert!(debug.contains("MeshDiscoveryConfig"));
    }

    // ── IrohConfig defaults ───────────────────────────────────────

    #[test]
    fn test_iroh_config_default() {
        let cfg = IrohConfig::default();
        assert!(cfg.bind_addr.is_none());
        assert!(cfg.relay_urls.is_empty());
        assert_eq!(cfg.bind_timeout, Duration::from_secs(10));
        assert_eq!(cfg.shutdown_timeout, Duration::from_secs(5));
        assert_eq!(cfg.download_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_iroh_config_custom_bind_addr() {
        let cfg = IrohConfig {
            bind_addr: Some("0.0.0.0:4433".parse().unwrap()),
            relay_urls: vec!["https://relay.example.com".to_string()],
            ..Default::default()
        };
        assert_eq!(cfg.bind_addr.unwrap().port(), 4433);
        assert_eq!(cfg.relay_urls.len(), 1);
    }

    #[test]
    fn test_iroh_config_secret_key() {
        let key = [42u8; 32];
        let cfg = IrohConfig {
            secret_key: Some(key),
            ..Default::default()
        };
        assert_eq!(cfg.secret_key.unwrap(), [42u8; 32]);
    }

    #[test]
    fn test_mesh_config_default_iroh() {
        let cfg = MeshConfig::default();
        assert!(cfg.iroh.bind_addr.is_none());
        assert!(cfg.iroh.relay_urls.is_empty());
    }

    // ── SecurityConfig defaults ──────────────────────────────────

    #[test]
    fn test_security_config_default_encryption_enabled() {
        let cfg = SecurityConfig::default();
        assert!(cfg.encryption_enabled);
    }

    #[test]
    fn test_security_config_default_peer_verification_disabled() {
        let cfg = SecurityConfig::default();
        assert!(!cfg.require_peer_verification);
    }

    #[test]
    fn test_security_config_custom() {
        let cfg = SecurityConfig {
            encryption_enabled: false,
            require_peer_verification: true,
        };
        assert!(!cfg.encryption_enabled);
        assert!(cfg.require_peer_verification);
    }

    #[test]
    fn test_security_config_clone() {
        let cfg = SecurityConfig {
            encryption_enabled: true,
            require_peer_verification: true,
        };
        let cloned = cfg.clone();
        assert_eq!(cloned.encryption_enabled, cfg.encryption_enabled);
        assert_eq!(
            cloned.require_peer_verification,
            cfg.require_peer_verification
        );
    }

    #[test]
    fn test_security_config_debug() {
        let cfg = SecurityConfig::default();
        let debug = format!("{:?}", cfg);
        assert!(debug.contains("SecurityConfig"));
    }

    // ── CompactionConfig defaults ────────────────────────────────

    #[test]
    fn test_compaction_config_default_disabled() {
        let cfg = CompactionConfig::default();
        assert!(!cfg.enabled);
    }

    #[test]
    fn test_compaction_config_default_interval() {
        let cfg = CompactionConfig::default();
        assert_eq!(cfg.interval, Duration::from_secs(300));
    }

    #[test]
    fn test_compaction_config_default_threshold() {
        let cfg = CompactionConfig::default();
        assert_eq!(cfg.size_threshold_bytes, 64 * 1024);
    }

    #[test]
    fn test_compaction_config_custom() {
        let cfg = CompactionConfig {
            enabled: true,
            interval: Duration::from_secs(60),
            size_threshold_bytes: 1024,
            collections: vec!["beacons".to_string()],
        };
        assert!(cfg.enabled);
        assert_eq!(cfg.interval, Duration::from_secs(60));
        assert_eq!(cfg.size_threshold_bytes, 1024);
        assert_eq!(cfg.collections.len(), 1);
    }

    #[test]
    fn test_compaction_config_clone() {
        let cfg = CompactionConfig {
            enabled: false,
            ..Default::default()
        };
        let cloned = cfg.clone();
        assert_eq!(cloned.enabled, cfg.enabled);
        assert_eq!(cloned.interval, cfg.interval);
    }

    #[test]
    fn test_compaction_config_debug() {
        let cfg = CompactionConfig::default();
        let debug = format!("{:?}", cfg);
        assert!(debug.contains("CompactionConfig"));
    }

    #[test]
    fn test_mesh_config_default_compaction() {
        let cfg = MeshConfig::default();
        assert!(!cfg.compaction.enabled);
        assert_eq!(cfg.compaction.interval, Duration::from_secs(300));
        assert!(cfg.compaction.collections.is_empty());
    }

    #[test]
    fn test_compaction_config_zero_interval_clamped() {
        let cfg = CompactionConfig {
            interval: Duration::from_secs(0),
            ..Default::default()
        };
        assert_eq!(cfg.effective_interval(), Duration::from_secs(10));
    }

    #[test]
    fn test_compaction_config_tiny_interval_clamped() {
        let cfg = CompactionConfig {
            interval: Duration::from_secs(3),
            ..Default::default()
        };
        assert_eq!(cfg.effective_interval(), Duration::from_secs(10));
    }

    #[test]
    fn test_compaction_config_large_interval_unchanged() {
        let cfg = CompactionConfig {
            interval: Duration::from_secs(600),
            ..Default::default()
        };
        assert_eq!(cfg.effective_interval(), Duration::from_secs(600));
    }

    #[test]
    fn test_compaction_config_exactly_min_interval() {
        let cfg = CompactionConfig {
            interval: Duration::from_secs(10),
            ..Default::default()
        };
        assert_eq!(cfg.effective_interval(), Duration::from_secs(10));
    }

    #[test]
    fn test_compaction_config_with_collections() {
        let cfg = CompactionConfig {
            enabled: true,
            collections: vec!["beacons".to_string(), "platforms".to_string()],
            ..Default::default()
        };
        assert!(cfg.enabled);
        assert_eq!(cfg.collections.len(), 2);
        assert_eq!(cfg.collections[0], "beacons");
    }
}
