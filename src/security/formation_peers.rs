//! Thread-safe set of known formation member EndpointIds.
//!
//! Used by [`FormationEndpointHooks`](crate::storage::FormationEndpointHooks)
//! to gate QUIC connections at the transport level, and by
//! [`PeerConnector`](crate::peer_connector::PeerConnector) to track
//! discovered formation members.

use iroh::EndpointId;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

/// A cloneable, thread-safe set of formation member EndpointIds.
///
/// Only peers whose EndpointId appears in this set are permitted to
/// establish QUIC connections (except on the enrollment ALPN).
#[derive(Clone, Debug)]
pub struct FormationPeerSet {
    inner: Arc<RwLock<HashSet<EndpointId>>>,
}

impl FormationPeerSet {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub fn insert(&self, peer: EndpointId) {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(peer);
    }

    pub fn remove(&self, peer: &EndpointId) {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(peer);
    }

    pub fn contains(&self, peer: &EndpointId) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(peer)
    }
}

impl Default for FormationPeerSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    fn test_endpoint_id(seed: u8) -> EndpointId {
        SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn insert_and_contains() {
        let set = FormationPeerSet::new();
        let id = test_endpoint_id(1);
        assert!(!set.contains(&id));
        set.insert(id);
        assert!(set.contains(&id));
    }

    #[test]
    fn remove() {
        let set = FormationPeerSet::new();
        let id = test_endpoint_id(2);
        set.insert(id);
        set.remove(&id);
        assert!(!set.contains(&id));
    }

    #[test]
    fn clone_shares_state() {
        let set = FormationPeerSet::new();
        let cloned = set.clone();
        let id = test_endpoint_id(3);
        set.insert(id);
        assert!(cloned.contains(&id));
    }
}
