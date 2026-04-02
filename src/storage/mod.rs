//! Storage backend trait abstractions and implementations
//!
//! Defines the core traits for the mesh storage layer, enabling runtime
//! backend selection between various storage implementations.

pub mod traits;

// Pure-generic storage types (no feature gate)
pub mod blob_traits;
pub mod geohash_index;
pub mod streaming_transfer;
pub mod ttl;

// Automerge backend storage (ADR-049 Phase 3)
#[cfg(feature = "automerge-backend")]
pub mod automerge_store;
#[cfg(feature = "automerge-backend")]
pub mod flow_control;
#[cfg(feature = "automerge-backend")]
pub mod iroh_blob_store;
#[cfg(feature = "automerge-backend")]
pub mod json_convert;
#[cfg(feature = "automerge-backend")]
pub mod negentropy_sync;
#[cfg(feature = "automerge-backend")]
pub mod partition_detection;
#[cfg(feature = "automerge-backend")]
pub mod query;
#[cfg(feature = "automerge-backend")]
pub mod sync_errors;
#[cfg(feature = "automerge-backend")]
pub mod sync_persistence;
#[cfg(feature = "automerge-backend")]
pub mod ttl_manager;
#[cfg(feature = "automerge-backend")]
pub mod typed_collection;

// Certificate CRDT store and enrollment protocol
#[cfg(feature = "automerge-backend")]
pub mod certificate_store;
#[cfg(feature = "automerge-backend")]
pub mod enrollment_transport;

// Sync coordination (extracted from peat-mesh)
#[cfg(feature = "automerge-backend")]
pub mod automerge_sync;
#[cfg(feature = "automerge-backend")]
pub mod mesh_sync_transport;
#[cfg(feature = "automerge-backend")]
pub mod sync_channel;
#[cfg(feature = "automerge-backend")]
pub mod sync_forwarding;
#[cfg(feature = "automerge-backend")]
pub mod sync_transport;

// Re-export key types (ungated)
pub use blob_traits::{
    BlobHandle, BlobHash, BlobMetadata, BlobProgress, BlobStorageSummary, BlobStore, BlobStoreExt,
    BlobToken, SharedBlobStore,
};
pub use geohash_index::GeohashIndex;
pub use streaming_transfer::{StreamingTransferConfig, TransferCheckpoint, TransferResult};
pub use traits::{Collection, DocumentPredicate, StorageBackend};
pub use ttl::{EvictionStrategy, OfflineRetentionPolicy, TtlConfig};

// Re-export key types (feature-gated)
#[cfg(feature = "automerge-backend")]
pub use automerge_store::AutomergeStore;
#[cfg(feature = "automerge-backend")]
pub use flow_control::{
    BoundedQueue, FlowControlConfig, FlowControlError, FlowControlStats, FlowController,
    PeerResourceTracker, SyncCooldownTracker, TokenBucket,
};
#[cfg(feature = "automerge-backend")]
pub use iroh_blob_store::{BlobPeerIndex, IrohBlobStore, NetworkedIrohBlobStore};
#[cfg(feature = "automerge-backend")]
pub use negentropy_sync::{NegentropyStats, NegentropySync, ReconcileResult, SyncItem};
#[cfg(feature = "automerge-backend")]
pub use partition_detection::{
    PartitionConfig, PartitionDetector, PartitionEvent, PeerHeartbeat, PeerPartitionState,
};
#[cfg(feature = "automerge-backend")]
pub use query::{extract_field, Query, SortOrder, Value};
#[cfg(feature = "automerge-backend")]
pub use sync_persistence::{
    Checkpoint, PersistedSyncState, PersistenceStats, SyncStatePersistence,
};
#[cfg(feature = "automerge-backend")]
pub use ttl_manager::TtlManager;
#[cfg(feature = "automerge-backend")]
pub use typed_collection::TypedCollection;

// Certificate store re-exports
#[cfg(feature = "automerge-backend")]
pub use certificate_store::{CertificateEntry, CertificateStore, RevocationEntry};

// Enrollment re-exports
#[cfg(feature = "automerge-backend")]
pub use enrollment_transport::{
    request_enrollment, EnrollmentProtocolHandler, CAP_ENROLLMENT_ALPN,
};

// Sync coordination re-exports
#[cfg(feature = "automerge-backend")]
pub use automerge_sync::{
    AutomergeSyncCoordinator, PeerSyncStats, ReceivedSyncPayload, SyncBatch, SyncDirection,
    SyncEntry, SyncMessageType, DEFAULT_SYNC_BATCH_TTL,
};
#[cfg(feature = "automerge-backend")]
pub use iroh_blob_store::FormationEndpointHooks;
#[cfg(feature = "automerge-backend")]
pub use mesh_sync_transport::{respond_to_formation_auth, MeshSyncTransport, SyncProtocolHandler};

// Re-export iroh multipath and endpoint types for downstream use
#[cfg(feature = "automerge-backend")]
pub mod iroh_types {
    //! Re-exported iroh types for multipath connection info and endpoint hooks.
    pub use iroh::endpoint::{Connection, EndpointHooks, PathId, PathInfo, PathStats, PathWatcher};
    pub use iroh::Watcher;
}
#[cfg(feature = "automerge-backend")]
pub use sync_channel::{ChannelManagerStats, ChannelState, SyncChannel, SyncChannelManager};
#[cfg(feature = "automerge-backend")]
pub use sync_forwarding::{ForwardingStats, SyncForwarder};
#[cfg(feature = "automerge-backend")]
pub use sync_transport::{SyncRouter, SyncTransport, CAP_AUTOMERGE_ALPN};
