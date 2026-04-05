//! TTL Manager for automatic document expiration
//!
//! This module provides automatic cleanup of expired documents based on Time-To-Live (TTL)
//! configuration. It is particularly critical for beacon documents which must expire after
//! 30 seconds to prevent stale position data from affecting cell formation.
//!
//! # Architecture
//!
//! ```text
//! Collection API
//!     ↓
//! upsert_with_ttl(key, data, ttl)
//!     ↓
//! TtlManager::set_ttl(key, ttl)
//!     ↓
//! BTreeMap<Instant, Vec<String>>
//!     ↓
//! Background cleanup task (every 10s)
//!     ↓
//! AutomergeStore::delete()
//! ```
//!
//! # Two-Layer TTL Strategy (ADR-002)
//!
//! **Storage Layer**: Tombstone TTL-based eviction
//! **Memory Layer**: Janitor cleanup for Automerge+Iroh (this module)
//!
//! # Usage Example
//!
//! ```ignore
//! let ttl_manager = TtlManager::new(store.clone(), config.clone());
//! ttl_manager.start_background_cleanup();
//!
//! // Set TTL for a beacon document
//! ttl_manager.set_ttl("beacons", "node-123", Duration::from_secs(30))?;
//!
//! // Document will be automatically deleted after 30 seconds
//! ```

use super::automerge_store::AutomergeStore;
use super::ttl::TtlConfig;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// TTL Manager for automatic document expiration
///
/// Tracks document expiry times and runs background cleanup task to remove
/// expired documents from the store.
pub struct TtlManager {
    /// Underlying Automerge store for document operations
    store: Arc<AutomergeStore>,

    /// TTL configuration
    config: TtlConfig,

    /// Expiry tracking: Instant → Vec<key>
    ///
    /// BTreeMap ensures efficient range queries for expired documents.
    /// Each expiry time maps to all document keys that expire at that time.
    /// Keys are in the format "collection/doc_id" (e.g., "beacons/node-123").
    expiry_map: Arc<RwLock<BTreeMap<Instant, Vec<String>>>>,

    /// Background cleanup task handle
    cleanup_task: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl TtlManager {
    /// Create a new TTL Manager
    ///
    /// # Arguments
    ///
    /// * `store` - AutomergeStore for document deletion
    /// * `config` - TTL configuration (beacon_ttl, position_ttl, etc.)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let store = AutomergeStore::new(iroh_transport.clone());
    /// let config = TtlConfig::tactical(); // 30s beacon TTL
    /// let ttl_manager = TtlManager::new(store, config);
    /// ```
    pub fn new(store: Arc<AutomergeStore>, config: TtlConfig) -> Self {
        Self {
            store,
            config,
            expiry_map: Arc::new(RwLock::new(BTreeMap::new())),
            cleanup_task: Arc::new(RwLock::new(None)),
        }
    }

    /// Schedule a document for expiration
    ///
    /// # Arguments
    ///
    /// * `key` - Full document key in format "collection/doc_id" (e.g., "beacons/node-123")
    /// * `ttl` - Time until expiration
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Beacon expires in 30 seconds
    /// ttl_manager.set_ttl("beacons/node-123", Duration::from_secs(30))?;
    /// ```
    pub fn set_ttl(&self, key: &str, ttl: Duration) -> Result<()> {
        let expiry_time = Instant::now() + ttl;

        let mut expiry_map = self.expiry_map.write().unwrap_or_else(|e| e.into_inner());
        expiry_map
            .entry(expiry_time)
            .or_default()
            .push(key.to_string());

        Ok(())
    }

    /// Remove all expired documents
    ///
    /// This method is called by the background cleanup task every 10 seconds.
    /// It finds all documents with expiry times <= now and deletes them.
    /// Documents are ordered according to the configured eviction strategy
    /// (OldestFirst, KeepLastN, etc.) before deletion.
    ///
    /// # Returns
    ///
    /// Number of documents cleaned up
    pub fn cleanup_expired(&self) -> Result<usize> {
        let now = Instant::now();
        let mut count = 0;

        // Get all expired entries (expiry_time <= now)
        let expired_keys = {
            let mut expiry_map = self.expiry_map.write().unwrap_or_else(|e| e.into_inner());

            // Split at first entry > now, take everything before
            let split_key = expiry_map
                .range(..=now)
                .next_back()
                .map(|(k, _)| *k)
                .unwrap_or(now);

            // Collect expired entries with their expiry times for strategy-aware ordering
            let expired: Vec<_> = expiry_map
                .range(..=split_key)
                .flat_map(|(expiry, keys)| keys.iter().map(move |k| (*expiry, k.clone())))
                .collect();

            // Remove from map
            expiry_map.retain(|&expiry_time, _| expiry_time > now);

            expired
        };

        // Apply eviction strategy ordering
        let ordered_keys = self.apply_eviction_strategy(expired_keys);

        // Delete each expired document
        for key in ordered_keys {
            self.store.delete(&key)?;
            count += 1;
        }

        Ok(count)
    }

    /// Order expired keys according to the configured eviction strategy
    fn apply_eviction_strategy(&self, mut expired: Vec<(Instant, String)>) -> Vec<String> {
        use super::ttl::EvictionStrategy;

        match self.config.evict_strategy {
            EvictionStrategy::OldestFirst => {
                // Sort by expiry time ascending (oldest expired first)
                expired.sort_by_key(|(expiry, _)| *expiry);
                expired.into_iter().map(|(_, key)| key).collect()
            }
            EvictionStrategy::KeepLastN(n) => {
                // Group by collection, keep last N per collection, evict the rest
                use std::collections::HashMap;
                let mut by_collection: HashMap<String, Vec<(Instant, String)>> = HashMap::new();
                for (expiry, key) in expired {
                    let collection = key.split('/').next().unwrap_or("").to_string();
                    by_collection
                        .entry(collection)
                        .or_default()
                        .push((expiry, key));
                }
                let mut to_delete = Vec::new();
                for (_collection, mut entries) in by_collection {
                    // Sort newest first, skip the last N, delete the rest
                    entries.sort_by_key(|(expiry, _)| std::cmp::Reverse(*expiry));
                    to_delete.extend(entries.into_iter().skip(n).map(|(_, key)| key));
                }
                to_delete
            }
            EvictionStrategy::StoragePressure { .. } | EvictionStrategy::None => {
                // No special ordering — delete all expired
                expired.into_iter().map(|(_, key)| key).collect()
            }
        }
    }

    /// Extend TTLs for all pending documents when the node is offline
    ///
    /// When no peers are connected, this extends remaining TTLs by the
    /// configured offline retention multiplier (offline_ttl / online_ttl ratio).
    /// This prevents premature eviction of data that can't be re-synced.
    ///
    /// Should be called periodically from the sync loop when `connected_peers()` is empty.
    pub fn extend_ttls_for_offline(&self) {
        let policy = match &self.config.offline_policy {
            Some(p) => p,
            None => return, // No offline policy configured
        };

        // Calculate extension factor: ratio of online to offline TTL
        // A ratio > 1 means we extend (online is longer than offline)
        // But when going offline, we want to retain longer, so use online/offline
        let online_secs = policy.online_ttl.as_secs_f64();
        let offline_secs = policy.offline_ttl.as_secs_f64();
        if offline_secs <= 0.0 || online_secs <= 0.0 {
            return;
        }
        // Extension factor: e.g., online=600s, offline=60s → factor=10x
        let factor = online_secs / offline_secs;
        if factor <= 1.0 {
            return; // Offline TTL is already >= online TTL, nothing to extend
        }

        let now = Instant::now();
        let mut expiry_map = self.expiry_map.write().unwrap_or_else(|e| e.into_inner());

        // Collect entries that haven't expired yet and extend them
        let entries: Vec<_> = expiry_map
            .iter()
            .filter(|(expiry, _)| **expiry > now)
            .flat_map(|(expiry, keys)| keys.iter().map(move |k| (*expiry, k.clone())))
            .collect();

        // Remove the old entries and re-insert with extended expiry
        expiry_map.clear();
        for (old_expiry, key) in entries {
            let remaining = old_expiry.duration_since(now);
            let extended = Duration::from_secs_f64(remaining.as_secs_f64() * factor);
            let new_expiry = now + extended;
            expiry_map.entry(new_expiry).or_default().push(key);
        }
    }

    /// Start background cleanup task
    ///
    /// Spawns a tokio task that runs cleanup_expired() every 10 seconds.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let ttl_manager = TtlManager::new(store, config);
    /// ttl_manager.start_background_cleanup();
    /// ```
    pub fn start_background_cleanup(&self) {
        let expiry_map = self.expiry_map.clone();
        let store = self.store.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));

            loop {
                interval.tick().await;

                // Run cleanup
                let now = Instant::now();
                let expired_docs = {
                    let mut expiry_map = expiry_map.write().unwrap_or_else(|e| e.into_inner());

                    // Get all expired entries
                    let split_key = expiry_map
                        .range(..=now)
                        .next_back()
                        .map(|(k, _)| *k)
                        .unwrap_or(now);

                    let expired: Vec<_> = expiry_map
                        .range(..=split_key)
                        .flat_map(|(_, docs)| docs.clone())
                        .collect();

                    // Remove from map
                    expiry_map.retain(|&expiry_time, _| expiry_time > now);

                    expired
                };

                // Delete expired documents
                for key in expired_docs {
                    if let Err(e) = store.delete(&key) {
                        eprintln!("TTL cleanup failed for {}: {}", key, e);
                    }
                }
            }
        });

        *self.cleanup_task.write().unwrap_or_else(|e| e.into_inner()) = Some(handle);
    }

    /// Stop background cleanup task
    pub fn stop_background_cleanup(&self) {
        if let Some(handle) = self
            .cleanup_task
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }
    }

    /// Get TTL configuration
    pub fn config(&self) -> &TtlConfig {
        &self.config
    }

    /// Get count of documents scheduled for expiration
    pub fn pending_count(&self) -> usize {
        self.expiry_map
            .read()
            .unwrap()
            .values()
            .map(|docs| docs.len())
            .sum()
    }
}

impl Drop for TtlManager {
    fn drop(&mut self) {
        self.stop_background_cleanup();
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ttl::EvictionStrategy;
    use automerge::Automerge;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_set_ttl() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = Arc::new(AutomergeStore::open(temp_dir.path())?);
        let config = TtlConfig::tactical();
        let ttl_manager = TtlManager::new(store, config);

        // Set TTL for a beacon
        ttl_manager.set_ttl("beacons/node-123", Duration::from_secs(30))?;

        // Verify it's tracked
        assert_eq!(ttl_manager.pending_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_expired() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = Arc::new(AutomergeStore::open(temp_dir.path())?);
        let config = TtlConfig::tactical();
        let ttl_manager = TtlManager::new(store.clone(), config);

        // Insert a test document
        let doc = Automerge::new();
        store.put("beacons/node-123", &doc)?;

        // Set very short TTL (100ms)
        ttl_manager.set_ttl("beacons/node-123", Duration::from_millis(100))?;

        // Wait for expiration
        sleep(Duration::from_millis(150)).await;

        // Run cleanup
        let count = ttl_manager.cleanup_expired()?;
        assert_eq!(count, 1);

        // Verify document is gone
        let result = store.get("beacons/node-123")?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_background_cleanup() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = Arc::new(AutomergeStore::open(temp_dir.path())?);
        let config = TtlConfig::tactical();
        let ttl_manager = TtlManager::new(store.clone(), config);

        // Insert test document
        let doc = Automerge::new();
        store.put("beacons/node-456", &doc)?;

        // Start background cleanup
        ttl_manager.start_background_cleanup();

        // Set very short TTL (1 second)
        ttl_manager.set_ttl("beacons/node-456", Duration::from_secs(1))?;

        // Wait for background cleanup (runs every 10s, but document expires in 1s)
        // We need to wait at least 11 seconds for cleanup to run
        sleep(Duration::from_secs(11)).await;

        // Verify document is gone
        let result = store.get("beacons/node-456")?;
        assert!(result.is_none());

        ttl_manager.stop_background_cleanup();

        Ok(())
    }

    #[tokio::test]
    async fn test_put_with_ttl_registers_expiry() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = Arc::new(AutomergeStore::open(temp_dir.path())?);
        let config = TtlConfig::tactical();
        let ttl_manager = TtlManager::new(store.clone(), config);

        // Create a document and store it with TTL via the integrated path
        let doc = Automerge::new();
        store.put("beacons/doc1", &doc)?;

        // Beacons have a TTL in tactical config (5 min), so set_ttl should register it
        let beacon_ttl = ttl_manager.config().get_collection_ttl("beacons").unwrap();
        ttl_manager.set_ttl("beacons/doc1", beacon_ttl)?;

        assert_eq!(ttl_manager.pending_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_put_with_ttl_no_ttl_collection() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = Arc::new(AutomergeStore::open(temp_dir.path())?);
        let config = TtlConfig::tactical();
        let ttl_manager = TtlManager::new(store.clone(), config);

        // hierarchical_commands has no TTL configured (returns None)
        let doc = Automerge::new();
        store.put("hierarchical_commands/doc1", &doc)?;

        // Only register TTL if the collection has one configured
        let collection_ttl = ttl_manager
            .config()
            .get_collection_ttl("hierarchical_commands");
        assert!(
            collection_ttl.is_none(),
            "hierarchical_commands should have no TTL"
        );

        // Since there's no TTL for this collection, nothing should be registered
        if let Some(ttl) = collection_ttl {
            ttl_manager.set_ttl("hierarchical_commands/doc1", ttl)?;
        }

        assert_eq!(ttl_manager.pending_count(), 0);

        Ok(())
    }

    #[test]
    fn test_tactical_preset_ttl_values() {
        let config = TtlConfig::tactical();

        assert_eq!(
            config.beacon_ttl,
            Duration::from_secs(300),
            "beacon_ttl should be 5 minutes"
        );
        assert_eq!(
            config.position_ttl,
            Duration::from_secs(600),
            "position_ttl should be 10 minutes"
        );
        assert_eq!(
            config.capability_ttl,
            Duration::from_secs(7200),
            "capability_ttl should be 2 hours"
        );
        assert_eq!(
            config.tombstone_ttl_hours, 168,
            "tombstone TTL should be 7 days (168 hours)"
        );
        assert!(matches!(
            config.evict_strategy,
            EvictionStrategy::OldestFirst
        ));
    }

    #[test]
    fn test_effective_ttl_returns_collection_ttl() {
        let config = TtlConfig::tactical();

        // Known collections should return their configured TTLs
        assert_eq!(
            config.get_collection_ttl("beacons"),
            Some(Duration::from_secs(300))
        );
        assert_eq!(
            config.get_collection_ttl("node_positions"),
            Some(Duration::from_secs(600))
        );
        assert_eq!(
            config.get_collection_ttl("capabilities"),
            Some(Duration::from_secs(7200))
        );
        assert_eq!(
            config.get_collection_ttl("cells"),
            Some(Duration::from_secs(3600))
        );

        // Collections without TTL should return None
        assert_eq!(config.get_collection_ttl("hierarchical_commands"), None);
        assert_eq!(config.get_collection_ttl("unknown_collection"), None);
    }

    #[tokio::test]
    async fn test_ttl_manager_with_automerge_store_cleanup() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = Arc::new(AutomergeStore::open(temp_dir.path())?);
        let config = TtlConfig::tactical();
        let ttl_manager = TtlManager::new(store.clone(), config);

        // Insert a document into the store
        let doc = Automerge::new();
        store.put("beacons/ephemeral1", &doc)?;

        // Verify document exists
        let result = store.get("beacons/ephemeral1")?;
        assert!(result.is_some(), "Document should exist before TTL expiry");

        // Set a very short TTL (100ms)
        ttl_manager.set_ttl("beacons/ephemeral1", Duration::from_millis(100))?;
        assert_eq!(ttl_manager.pending_count(), 1);

        // Wait for expiration
        sleep(Duration::from_millis(150)).await;

        // Run cleanup and verify the document was deleted
        let cleaned = ttl_manager.cleanup_expired()?;
        assert_eq!(cleaned, 1, "Should have cleaned up 1 expired document");

        // Verify the document is gone from the store
        let result = store.get("beacons/ephemeral1")?;
        assert!(
            result.is_none(),
            "Document should be deleted after TTL expiry and cleanup"
        );

        // Verify pending count is back to 0
        assert_eq!(ttl_manager.pending_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_eviction_strategy_keep_last_n() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = Arc::new(AutomergeStore::open(temp_dir.path())?);
        let config = TtlConfig::new().with_eviction(EvictionStrategy::KeepLastN(2));
        let ttl_manager = TtlManager::new(store.clone(), config);

        // Insert 5 beacon documents with staggered short TTLs
        for i in 0..5 {
            let doc = Automerge::new();
            store.put(&format!("beacons/node-{}", i), &doc)?;
            ttl_manager.set_ttl(
                &format!("beacons/node-{}", i),
                Duration::from_millis(50 + i * 10),
            )?;
        }

        // Wait for all to expire
        sleep(Duration::from_millis(200)).await;

        // Cleanup should keep last 2 (newest) and delete 3
        let count = ttl_manager.cleanup_expired()?;
        assert_eq!(count, 3, "KeepLastN(2) should delete 3 of 5 expired docs");

        Ok(())
    }

    #[tokio::test]
    async fn test_extend_ttls_for_offline() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = Arc::new(AutomergeStore::open(temp_dir.path())?);
        let config = TtlConfig::tactical(); // online_ttl=600s, offline_ttl=60s → 10x extension
        let ttl_manager = TtlManager::new(store.clone(), config);

        // Set a TTL that expires in 1 second
        let doc = Automerge::new();
        store.put("beacons/test-offline", &doc)?;
        ttl_manager.set_ttl("beacons/test-offline", Duration::from_secs(1))?;
        assert_eq!(ttl_manager.pending_count(), 1);

        // Extend TTLs (simulates going offline) — should multiply remaining by 10x
        ttl_manager.extend_ttls_for_offline();

        // Wait 1.5 seconds — original would have expired, extended should not
        sleep(Duration::from_millis(1500)).await;

        // Cleanup should find 0 expired (because TTL was extended)
        let count = ttl_manager.cleanup_expired()?;
        assert_eq!(count, 0, "Extended TTL should not have expired yet");
        assert_eq!(ttl_manager.pending_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_extend_ttls_no_offline_policy() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = Arc::new(AutomergeStore::open(temp_dir.path())?);
        let config = TtlConfig::long_duration(); // offline_policy = None
        let ttl_manager = TtlManager::new(store.clone(), config);

        ttl_manager.set_ttl("beacons/test", Duration::from_millis(100))?;

        // Should be a no-op when no offline policy configured
        ttl_manager.extend_ttls_for_offline();

        sleep(Duration::from_millis(150)).await;
        let count = ttl_manager.cleanup_expired()?;
        assert_eq!(
            count, 1,
            "Without offline policy, TTL should not be extended"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_documents_same_expiry() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store = Arc::new(AutomergeStore::open(temp_dir.path())?);
        let config = TtlConfig::tactical();
        let ttl_manager = TtlManager::new(store.clone(), config);

        // Insert multiple documents
        for i in 0..5 {
            let doc = Automerge::new();
            store.put(&format!("beacons/node-{}", i), &doc)?;
            ttl_manager.set_ttl(&format!("beacons/node-{}", i), Duration::from_millis(100))?;
        }

        assert_eq!(ttl_manager.pending_count(), 5);

        // Wait for expiration
        sleep(Duration::from_millis(150)).await;

        // Run cleanup
        let count = ttl_manager.cleanup_expired()?;
        assert_eq!(count, 5);

        // Verify all documents are gone
        for i in 0..5 {
            let result = store.get(&format!("beacons/node-{}", i))?;
            assert!(result.is_none());
        }

        Ok(())
    }
}
