//! Kubernetes-based peer discovery via EndpointSlice watching.
//!
//! Discovers peat-mesh peers running in a Kubernetes cluster by watching
//! `EndpointSlice` resources. Peer identity information (node_id, relay URLs)
//! is extracted from pod annotations with a configurable prefix.
//!
//! # Feature gate
//!
//! This module is only available when the `kubernetes` feature is enabled.

use super::{DiscoveryError, DiscoveryEvent, DiscoveryStrategy, PeerInfo, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Configuration for Kubernetes-based peer discovery.
#[derive(Debug, Clone)]
pub struct KubernetesDiscoveryConfig {
    /// Kubernetes namespace to watch. `None` reads from the service account
    /// mount (`/var/run/secrets/kubernetes.io/serviceaccount/namespace`)
    /// or falls back to `"default"`.
    pub namespace: Option<String>,
    /// Label selector for EndpointSlice resources.
    pub label_selector: String,
    /// Annotation prefix for extracting peer metadata (e.g. `"peat."`).
    pub annotation_prefix: String,
    /// Interval between re-list operations.
    pub poll_interval: Duration,
}

impl Default for KubernetesDiscoveryConfig {
    fn default() -> Self {
        Self {
            namespace: None,
            label_selector: "app=peat-mesh".to_string(),
            annotation_prefix: "peat.".to_string(),
            poll_interval: Duration::from_secs(30),
        }
    }
}

/// Kubernetes-based peer discovery strategy.
///
/// Watches `EndpointSlice` resources in a Kubernetes namespace and emits
/// `DiscoveryEvent`s as pods appear, disappear, or change.
pub struct KubernetesDiscovery {
    config: KubernetesDiscoveryConfig,
    #[allow(dead_code)]
    client: Option<kube::Client>,
    discovered: Arc<RwLock<HashMap<String, PeerInfo>>>,
    events_tx: mpsc::Sender<DiscoveryEvent>,
    events_rx: Option<mpsc::Receiver<DiscoveryEvent>>,
    running: Arc<RwLock<bool>>,
}

impl KubernetesDiscovery {
    /// Create a new Kubernetes discovery with the given configuration.
    pub fn new(config: KubernetesDiscoveryConfig) -> Self {
        let (events_tx, events_rx) = mpsc::channel(100);
        Self {
            config,
            client: None,
            discovered: Arc::new(RwLock::new(HashMap::new())),
            events_tx,
            events_rx: Some(events_rx),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Create a new Kubernetes discovery with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(KubernetesDiscoveryConfig::default())
    }

    /// Create a new Kubernetes discovery with an injected client (for testing).
    pub fn with_client(config: KubernetesDiscoveryConfig, client: kube::Client) -> Self {
        let (events_tx, events_rx) = mpsc::channel(100);
        Self {
            config,
            client: Some(client),
            discovered: Arc::new(RwLock::new(HashMap::new())),
            events_tx,
            events_rx: Some(events_rx),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Resolve the namespace from config, service account mount, or default.
    fn resolve_namespace(&self) -> String {
        if let Some(ref ns) = self.config.namespace {
            return ns.clone();
        }

        // Try reading from the service account mount
        match std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace") {
            Ok(ns) => ns.trim().to_string(),
            Err(_) => {
                debug!("No service account namespace found, using 'default'");
                "default".to_string()
            }
        }
    }

    /// Pick the transport port to advertise for discovered peers.
    ///
    /// peat-mesh-node exposes both the broker HTTP port and the Iroh QUIC port
    /// in a single Service. For peer-to-peer sync we must prefer the `iroh-quic`
    /// UDP port instead of whichever port happens to come first.
    fn select_port(ports: &Option<Vec<k8s_openapi::api::discovery::v1::EndpointPort>>) -> u16 {
        let Some(ports) = ports.as_ref() else {
            return 8080;
        };

        if let Some(port) = ports.iter().find_map(|p| {
            if p.name.as_deref() == Some("iroh-quic") {
                p.port.map(|v| v as u16)
            } else {
                None
            }
        }) {
            return port;
        }

        if let Some(port) = ports.iter().find_map(|p| {
            if p.protocol.as_deref() == Some("UDP") {
                p.port.map(|v| v as u16)
            } else {
                None
            }
        }) {
            return port;
        }

        ports.first().and_then(|p| p.port).unwrap_or(8080) as u16
    }

    /// Extract PeerInfo from an EndpointSlice's endpoints.
    pub fn extract_peers_from_endpoint_slice(
        endpoint_slice: &k8s_openapi::api::discovery::v1::EndpointSlice,
        annotation_prefix: &str,
    ) -> Vec<PeerInfo> {
        let mut peers = Vec::new();

        let endpoints = &endpoint_slice.endpoints;
        if endpoints.is_empty() {
            return peers;
        }

        for endpoint in endpoints {
            // Extract addresses
            let addresses = &endpoint.addresses;
            if addresses.is_empty() {
                continue;
            }

            // Try to get node_id from target_ref name or addresses
            let node_id = endpoint
                .target_ref
                .as_ref()
                .and_then(|tr| tr.name.clone())
                .unwrap_or_else(|| {
                    addresses
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_string())
                });

            // Parse addresses into SocketAddr (using port from the slice's ports)
            let port = Self::select_port(&endpoint_slice.ports);

            let socket_addrs: Vec<std::net::SocketAddr> = addresses
                .iter()
                .filter_map(|addr| {
                    addr.parse::<std::net::IpAddr>()
                        .ok()
                        .map(|ip| std::net::SocketAddr::new(ip, port))
                })
                .collect();

            if socket_addrs.is_empty() {
                continue;
            }

            // Extract metadata from annotations with the configured prefix
            let mut metadata = HashMap::new();
            let annotations = endpoint_slice.metadata.annotations.as_ref();

            if let Some(anns) = annotations {
                for (key, value) in anns {
                    if let Some(stripped) = key.strip_prefix(annotation_prefix) {
                        metadata.insert(stripped.to_string(), value.clone());
                    }
                }
            }

            // Check for relay URL in metadata
            let relay_url = metadata.remove("relay_url");

            let mut peer = PeerInfo::new(node_id, socket_addrs);
            if let Some(url) = relay_url {
                peer = peer.with_relay(url);
            }
            peer.metadata = metadata;

            peers.push(peer);
        }

        peers
    }
}

#[async_trait]
impl DiscoveryStrategy for KubernetesDiscovery {
    async fn start(&mut self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            warn!("Kubernetes discovery already running");
            return Ok(());
        }

        // Create client if not injected
        let client = match self.client.take() {
            Some(c) => c,
            None => kube::Client::try_default()
                .await
                .map_err(|e| DiscoveryError::KubernetesError(e.to_string()))?,
        };

        let namespace = self.resolve_namespace();
        info!(
            "Starting Kubernetes discovery in namespace '{}' with selector '{}'",
            namespace, self.config.label_selector
        );

        *running = true;
        drop(running);

        let discovered = self.discovered.clone();
        let events_tx = self.events_tx.clone();
        let running_flag = self.running.clone();
        let label_selector = self.config.label_selector.clone();
        let annotation_prefix = self.config.annotation_prefix.clone();
        let poll_interval = self.config.poll_interval;

        tokio::spawn(async move {
            use k8s_openapi::api::discovery::v1::EndpointSlice;
            use kube::api::{Api, ListParams};

            let api: Api<EndpointSlice> = Api::namespaced(client, &namespace);
            let lp = ListParams::default().labels(&label_selector);

            while *running_flag.read().await {
                match api.list(&lp).await {
                    Ok(list) => {
                        let mut current_peers: HashMap<String, PeerInfo> = HashMap::new();

                        for es in &list.items {
                            let peers = KubernetesDiscovery::extract_peers_from_endpoint_slice(
                                es,
                                &annotation_prefix,
                            );
                            for peer in peers {
                                current_peers.insert(peer.node_id.clone(), peer);
                            }
                        }

                        // Diff against discovered peers
                        let mut discovered_guard = discovered.write().await;

                        // Find new and updated peers
                        for (id, peer) in &current_peers {
                            if let Some(existing) = discovered_guard.get(id) {
                                if existing.addresses != peer.addresses
                                    || existing.metadata != peer.metadata
                                {
                                    if let Err(e) = events_tx
                                        .send(DiscoveryEvent::PeerUpdated(peer.clone()))
                                        .await
                                    {
                                        error!("Failed to send PeerUpdated event: {}", e);
                                    }
                                }
                            } else {
                                debug!("Discovered new Kubernetes peer: {}", id);
                                if let Err(e) = events_tx
                                    .send(DiscoveryEvent::PeerFound(peer.clone()))
                                    .await
                                {
                                    error!("Failed to send PeerFound event: {}", e);
                                }
                            }
                        }

                        // Find lost peers
                        let lost: Vec<String> = discovered_guard
                            .keys()
                            .filter(|id| !current_peers.contains_key(*id))
                            .cloned()
                            .collect();

                        for id in lost {
                            debug!("Lost Kubernetes peer: {}", id);
                            if let Err(e) =
                                events_tx.send(DiscoveryEvent::PeerLost(id.clone())).await
                            {
                                error!("Failed to send PeerLost event: {}", e);
                            }
                        }

                        *discovered_guard = current_peers;
                    }
                    Err(e) => {
                        error!("Failed to list EndpointSlices: {}", e);
                    }
                }

                tokio::time::sleep(poll_interval).await;
            }

            info!("Kubernetes discovery task stopped");
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }

        info!("Stopping Kubernetes discovery");
        *running = false;
        Ok(())
    }

    async fn discovered_peers(&self) -> Vec<PeerInfo> {
        self.discovered.read().await.values().cloned().collect()
    }

    fn event_stream(&mut self) -> Result<mpsc::Receiver<DiscoveryEvent>> {
        self.events_rx
            .take()
            .ok_or(DiscoveryError::EventStreamConsumed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::ObjectReference;
    use k8s_openapi::api::discovery::v1::{Endpoint, EndpointPort, EndpointSlice};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn make_endpoint_slice(
        endpoints: Vec<Endpoint>,
        ports: Vec<EndpointPort>,
        annotations: Option<std::collections::BTreeMap<String, String>>,
    ) -> EndpointSlice {
        EndpointSlice {
            metadata: ObjectMeta {
                annotations,
                ..Default::default()
            },
            address_type: "IPv4".to_string(),
            endpoints,
            ports: Some(ports),
        }
    }

    #[test]
    fn test_config_defaults() {
        let cfg = KubernetesDiscoveryConfig::default();
        assert!(cfg.namespace.is_none());
        assert_eq!(cfg.label_selector, "app=peat-mesh");
        assert_eq!(cfg.annotation_prefix, "peat.");
        assert_eq!(cfg.poll_interval, Duration::from_secs(30));
    }

    #[test]
    fn test_config_custom() {
        let cfg = KubernetesDiscoveryConfig {
            namespace: Some("production".to_string()),
            label_selector: "app=custom".to_string(),
            annotation_prefix: "mesh.".to_string(),
            poll_interval: Duration::from_secs(10),
        };
        assert_eq!(cfg.namespace.as_deref(), Some("production"));
        assert_eq!(cfg.label_selector, "app=custom");
        assert_eq!(cfg.poll_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_event_stream_consumed() {
        let mut discovery = KubernetesDiscovery::with_defaults();
        let _stream = discovery.event_stream().unwrap();

        let result = discovery.event_stream();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DiscoveryError::EventStreamConsumed
        ));
    }

    #[tokio::test]
    async fn test_discovered_peers_initially_empty() {
        let discovery = KubernetesDiscovery::with_defaults();
        let peers = discovery.discovered_peers().await;
        assert!(peers.is_empty());
    }

    #[tokio::test]
    async fn test_stop_when_not_started() {
        let mut discovery = KubernetesDiscovery::with_defaults();
        // Should succeed without error
        assert!(discovery.stop().await.is_ok());
    }

    #[test]
    fn test_extract_peers_from_endpoint_slice() {
        let endpoints = vec![Endpoint {
            addresses: vec!["10.0.0.1".to_string()],
            target_ref: Some(ObjectReference {
                name: Some("pod-alpha".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }];

        let ports = vec![EndpointPort {
            port: Some(8080),
            ..Default::default()
        }];

        let mut annotations = std::collections::BTreeMap::new();
        annotations.insert("peat.formation".to_string(), "alpha".to_string());

        let es = make_endpoint_slice(endpoints, ports, Some(annotations));
        let peers = KubernetesDiscovery::extract_peers_from_endpoint_slice(&es, "peat.");

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, "pod-alpha");
        assert_eq!(peers[0].addresses[0].to_string(), "10.0.0.1:8080");
        assert_eq!(
            peers[0].metadata.get("formation"),
            Some(&"alpha".to_string())
        );
    }

    #[test]
    fn test_extract_peers_multiple_endpoints() {
        let endpoints = vec![
            Endpoint {
                addresses: vec!["10.0.0.1".to_string()],
                target_ref: Some(ObjectReference {
                    name: Some("pod-a".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Endpoint {
                addresses: vec!["10.0.0.2".to_string()],
                target_ref: Some(ObjectReference {
                    name: Some("pod-b".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ];

        let ports = vec![EndpointPort {
            port: Some(9090),
            ..Default::default()
        }];

        let es = make_endpoint_slice(endpoints, ports, None);
        let peers = KubernetesDiscovery::extract_peers_from_endpoint_slice(&es, "peat.");

        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].node_id, "pod-a");
        assert_eq!(peers[1].node_id, "pod-b");
    }

    #[test]
    fn test_extract_peers_no_endpoints() {
        let es = EndpointSlice {
            metadata: ObjectMeta::default(),
            address_type: "IPv4".to_string(),
            endpoints: vec![],
            ports: None,
        };
        let peers = KubernetesDiscovery::extract_peers_from_endpoint_slice(&es, "peat.");
        assert!(peers.is_empty());
    }

    #[test]
    fn test_extract_peers_with_relay_url() {
        let endpoints = vec![Endpoint {
            addresses: vec!["10.0.0.5".to_string()],
            target_ref: Some(ObjectReference {
                name: Some("relay-pod".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }];

        let ports = vec![EndpointPort {
            port: Some(4433),
            ..Default::default()
        }];

        let mut annotations = std::collections::BTreeMap::new();
        annotations.insert(
            "peat.relay_url".to_string(),
            "https://relay.example.com".to_string(),
        );

        let es = make_endpoint_slice(endpoints, ports, Some(annotations));
        let peers = KubernetesDiscovery::extract_peers_from_endpoint_slice(&es, "peat.");

        assert_eq!(peers.len(), 1);
        assert_eq!(
            peers[0].relay_url,
            Some("https://relay.example.com".to_string())
        );
    }

    #[test]
    fn test_extract_peers_no_target_ref_uses_address() {
        let endpoints = vec![Endpoint {
            addresses: vec!["10.0.0.99".to_string()],
            target_ref: None,
            ..Default::default()
        }];

        let ports = vec![EndpointPort {
            port: Some(8080),
            ..Default::default()
        }];

        let es = make_endpoint_slice(endpoints, ports, None);
        let peers = KubernetesDiscovery::extract_peers_from_endpoint_slice(&es, "peat.");

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, "10.0.0.99");
    }

    #[test]
    fn test_resolve_namespace_from_config() {
        let config = KubernetesDiscoveryConfig {
            namespace: Some("my-namespace".to_string()),
            ..Default::default()
        };
        let discovery = KubernetesDiscovery::new(config);
        assert_eq!(discovery.resolve_namespace(), "my-namespace");
    }

    #[test]
    fn test_resolve_namespace_fallback_to_default() {
        let config = KubernetesDiscoveryConfig {
            namespace: None,
            ..Default::default()
        };
        let discovery = KubernetesDiscovery::new(config);
        // Outside a K8s cluster, should fall back to "default"
        let ns = discovery.resolve_namespace();
        // Will be "default" unless running in a pod
        assert!(!ns.is_empty());
    }
}
