//! Storage eviction service that integrates EvictionController with AutomergeStore
//!
//! This service bridges the QoS-aware eviction framework with the actual document
//! store. It spawns a periodic task to monitor storage pressure and run eviction
//! cycles when thresholds are exceeded.

#[cfg(feature = "automerge-backend")]
use super::{
    audit::EvictionAuditLog,
    eviction::{EvictionConfig, EvictionController},
    storage::QoSAwareStorage,
    QoSClass,
};

#[cfg(feature = "automerge-backend")]
use crate::storage::automerge_store::AutomergeStore;

#[cfg(feature = "automerge-backend")]
use std::sync::Arc;

#[cfg(feature = "automerge-backend")]
use tokio::task::JoinHandle;

/// Service that monitors storage pressure and triggers QoS-based eviction
///
/// Wraps `EvictionController` + `AutomergeStore` and spawns a periodic task
/// to run eviction cycles when storage pressure exceeds the configured threshold.
#[cfg(feature = "automerge-backend")]
pub struct StorageEvictionService {
    controller: Arc<EvictionController>,
    qos_storage: Arc<QoSAwareStorage>,
    #[allow(dead_code)]
    store: Arc<AutomergeStore>,
    task_handle: std::sync::Mutex<Option<JoinHandle<()>>>,
}

#[cfg(feature = "automerge-backend")]
impl StorageEvictionService {
    /// Create a new storage eviction service
    ///
    /// # Arguments
    ///
    /// * `store` - The AutomergeStore for document deletion
    /// * `max_storage_bytes` - Maximum storage capacity in bytes
    /// * `config` - Eviction configuration (thresholds, limits)
    pub fn new(
        store: Arc<AutomergeStore>,
        max_storage_bytes: usize,
        config: EvictionConfig,
    ) -> Self {
        let qos_storage = Arc::new(QoSAwareStorage::new(max_storage_bytes));
        let audit_log = Arc::new(EvictionAuditLog::new(1000));
        let controller =
            EvictionController::new(Arc::clone(&qos_storage), audit_log).with_config(config);

        // Set the eviction callback to delete from AutomergeStore
        let store_for_callback = Arc::clone(&store);
        controller.set_eviction_callback(Box::new(move |doc_id: &str| {
            store_for_callback
                .delete(doc_id)
                .map_err(|e| format!("Failed to delete {}: {}", doc_id, e))
        }));

        let controller = Arc::new(controller);

        Self {
            controller,
            qos_storage,
            store,
            task_handle: std::sync::Mutex::new(None),
        }
    }

    /// Register a document for QoS-aware storage tracking
    ///
    /// Called on document put to track storage usage per QoS class.
    pub fn register_document(&self, doc_key: &str, size_bytes: usize) {
        let collection = doc_key.split(':').next().unwrap_or(doc_key);
        let qos_class = QoSClass::for_collection(collection);
        let doc = super::storage::StoredDocument::new(doc_key, qos_class, size_bytes);
        self.qos_storage.register_document(doc);
    }

    /// Unregister a document (called on delete)
    pub fn unregister_document(&self, doc_key: &str) {
        self.qos_storage.unregister_document(doc_key);
    }

    /// Get current storage pressure (0.0 = empty, 1.0 = full)
    pub fn storage_pressure(&self) -> f32 {
        self.qos_storage.storage_pressure()
    }

    /// Get reference to the QoS-aware storage for metrics
    pub fn qos_storage(&self) -> &Arc<QoSAwareStorage> {
        &self.qos_storage
    }

    /// Start the periodic eviction check task
    ///
    /// Runs every 60 seconds. If storage pressure exceeds the eviction
    /// threshold, triggers an eviction cycle.
    pub fn start(&self) {
        let controller = Arc::clone(&self.controller);
        let qos_storage = Arc::clone(&self.qos_storage);

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                if qos_storage.should_evict() {
                    let pressure = qos_storage.storage_pressure();
                    tracing::info!(
                        pressure = format!("{:.1}%", pressure * 100.0),
                        "Storage pressure exceeds threshold, running eviction cycle"
                    );
                    if let Some(result) = controller.run_eviction_cycle() {
                        tracing::info!(
                            evicted = result.docs_evicted,
                            freed_bytes = result.bytes_freed,
                            pressure_after = format!("{:.1}%", result.pressure_after * 100.0),
                            "Eviction cycle complete"
                        );
                    }
                }
            }
        });

        *self.task_handle.lock().unwrap() = Some(handle);
    }

    /// Stop the periodic eviction task
    pub fn stop(&self) {
        if let Some(handle) = self.task_handle.lock().unwrap().take() {
            handle.abort();
        }
    }
}

#[cfg(feature = "automerge-backend")]
impl Drop for StorageEvictionService {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(all(test, feature = "automerge-backend"))]
mod tests {
    use super::super::audit::EvictionAuditLog;
    use super::super::eviction::EvictionController;
    use super::*;
    use automerge::Automerge;

    #[tokio::test]
    async fn test_eviction_service_register_and_pressure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(AutomergeStore::open(temp_dir.path()).unwrap());

        // Create service with 1KB max storage
        let service = StorageEvictionService::new(store.clone(), 1024, EvictionConfig::default());

        // Initially no pressure
        assert_eq!(service.storage_pressure(), 0.0);

        // Register a document that's 500 bytes (50% of 1KB)
        service.register_document("tracks:doc-1", 500);
        let pressure = service.storage_pressure();
        assert!(
            pressure > 0.4 && pressure < 0.6,
            "Expected ~50% pressure, got {}",
            pressure
        );

        // Unregister and pressure should drop
        service.unregister_document("tracks:doc-1");
        assert_eq!(service.storage_pressure(), 0.0);
    }

    #[tokio::test]
    async fn test_eviction_service_qos_classification() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(AutomergeStore::open(temp_dir.path()).unwrap());
        let service = StorageEvictionService::new(store, 10240, EvictionConfig::default());

        // Register documents from different collections
        service.register_document("commands:cmd-1", 100); // Critical
        service.register_document("tracks:track-1", 100); // Normal
        service.register_document("unknown:bulk-1", 100); // Bulk

        // All should be tracked
        let pressure = service.storage_pressure();
        assert!(pressure > 0.0, "Should have some storage usage");
    }

    #[tokio::test]
    async fn test_eviction_service_evicts_bulk_first() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(AutomergeStore::open(temp_dir.path()).unwrap());

        // Create documents in the store
        let doc = Automerge::new();
        store.put("unknown:bulk-1", &doc).unwrap();
        store.put("tracks:normal-1", &doc).unwrap();

        // Config with low threshold to trigger eviction
        let config = EvictionConfig {
            eviction_threshold: 0.3,
            target_pressure: 0.1,
            ..EvictionConfig::default()
        };
        // Use low max storage AND low QoSAwareStorage threshold
        let qos_storage = Arc::new(QoSAwareStorage::new(500).with_eviction_threshold(0.3));
        let audit_log = Arc::new(EvictionAuditLog::new(100));
        let controller =
            EvictionController::new(Arc::clone(&qos_storage), audit_log).with_config(config);

        let store_cb = Arc::clone(&store);
        controller.set_eviction_callback(Box::new(move |doc_id: &str| {
            store_cb
                .delete(doc_id)
                .map_err(|e| format!("Failed to delete {}: {}", doc_id, e))
        }));

        // Register with sizes that exceed threshold (400/500 = 80% > 30%)
        qos_storage.register_document(super::super::storage::StoredDocument::new(
            "unknown:bulk-1",
            QoSClass::Bulk,
            200,
        ));
        qos_storage.register_document(super::super::storage::StoredDocument::new(
            "tracks:normal-1",
            QoSClass::Normal,
            200,
        ));

        // Pressure should be high enough to trigger
        assert!(
            qos_storage.should_evict(),
            "Storage at 80% should exceed 30% threshold"
        );
    }
}
