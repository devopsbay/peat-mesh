//! peat-mesh-node — Kubernetes-ready mesh node binary.
//!
//! Reads configuration from environment variables, builds an `PeatMesh`
//! instance with deterministic keypair and Kubernetes discovery, and
//! serves the broker HTTP/WS API until SIGTERM/SIGINT.

use peat_mesh::broker::{Broker, BrokerConfig, OtaAppState};
use peat_mesh::config::{IrohConfig, MeshConfig};
use peat_mesh::discovery::{KubernetesDiscovery, KubernetesDiscoveryConfig};
use peat_mesh::mesh::PeatMeshBuilder;
use peat_mesh::peer_connector::PeerConnector;
use peat_mesh::qos::{
    eviction_service::StorageEvictionService, start_periodic_gc, DeletionPolicyRegistry,
    EvictionConfig, GarbageCollector, GcConfig,
};
use peat_mesh::security::{DeviceKeypair, FormationKey};
use peat_mesh::storage::{
    AutomergeStore, AutomergeSyncCoordinator, CertificateStore, EnrollmentProtocolHandler,
    MeshSyncTransport, NetworkedIrohBlobStore, SyncChannelManager, SyncProtocolHandler,
    SyncTransport, TtlConfig, TtlManager, CAP_AUTOMERGE_ALPN, CAP_ENROLLMENT_ALPN,
};
use peat_mesh::transport::{
    LiteMeshTransport, LiteMessageType, LiteTransportConfig, MeshTransport, OtaSender,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info, warn};

fn main() -> anyhow::Result<()> {
    // Install rustls crypto provider (required by kube's rustls-tls)
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Initialize tracing from RUST_LOG (default: info,peat_mesh=debug)
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info,peat_mesh=debug".to_string());
    tracing_subscriber::fmt().with_env_filter(filter).init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> anyhow::Result<()> {
    // ── Required env vars ────────────────────────────────────────
    let formation_secret = std::env::var("PEAT_FORMATION_SECRET")
        .map_err(|_| anyhow::anyhow!("PEAT_FORMATION_SECRET is required"))?;

    // ── Optional env vars ────────────────────────────────────────
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "peat-mesh-0".to_string());
    let discovery_mode =
        std::env::var("PEAT_DISCOVERY").unwrap_or_else(|_| "kubernetes".to_string());
    let broker_port: u16 = std::env::var("PEAT_BROKER_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8081);
    let iroh_bind_port: u16 = std::env::var("PEAT_IROH_BIND_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(11204);
    let relay_urls: Vec<String> = std::env::var("PEAT_IROH_RELAY_URLS")
        .ok()
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    // ── Certificate / enrollment env vars ──────────────────────
    let authority_key_hex = std::env::var("PEAT_AUTHORITY_KEY").ok();
    let require_certificates = std::env::var("PEAT_REQUIRE_CERTIFICATES")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let enrollment_tokens_raw = std::env::var("PEAT_ENROLLMENT_TOKENS").ok();

    info!(
        hostname = %hostname,
        discovery = %discovery_mode,
        broker_port = broker_port,
        iroh_bind_port = iroh_bind_port,
        certificates = authority_key_hex.is_some(),
        require_certificates,
        enrollment = enrollment_tokens_raw.is_some(),
        "Starting peat-mesh-node"
    );

    // ── Formation key ────────────────────────────────────────────
    let formation_key = FormationKey::from_base64("peat", &formation_secret)
        .map_err(|e| anyhow::anyhow!("Invalid PEAT_FORMATION_SECRET: {}", e))?;

    // ── Deterministic keypair from formation secret + hostname ───
    let seed = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        formation_secret.trim(),
    )
    .map_err(|e| anyhow::anyhow!("Invalid base64 in PEAT_FORMATION_SECRET: {}", e))?;

    // ── Derive deterministic Iroh secret key ─────────────────────
    let iroh_key = {
        use hkdf::Hkdf;
        use sha2::Sha256;
        let hk = Hkdf::<Sha256>::new(None, &seed);
        let mut key = [0u8; 32];
        hk.expand(format!("iroh:{}", hostname).as_bytes(), &mut key)
            .map_err(|e| anyhow::anyhow!("HKDF expand for Iroh key failed: {}", e))?;
        key
    };

    // ── Mesh config ──────────────────────────────────────────────
    let mesh_config = MeshConfig {
        node_id: Some(hostname.clone()),
        iroh: IrohConfig {
            bind_addr: Some(SocketAddr::from(([0, 0, 0, 0], iroh_bind_port))),
            relay_urls,
            secret_key: Some(iroh_key),
            ..Default::default()
        },
        ..Default::default()
    };

    // ── Discovery strategy ───────────────────────────────────────
    let mut discovery: Box<dyn peat_mesh::discovery::DiscoveryStrategy> =
        match discovery_mode.as_str() {
            "kubernetes" | "k8s" => {
                info!("Using Kubernetes EndpointSlice discovery");
                Box::new(KubernetesDiscovery::new(
                    KubernetesDiscoveryConfig::default(),
                ))
            }
            "mdns" => {
                info!("Using mDNS discovery");
                Box::new(
                    peat_mesh::discovery::MdnsDiscovery::new()
                        .map_err(|e| anyhow::anyhow!("mDNS discovery init failed: {}", e))?,
                )
            }
            other => {
                anyhow::bail!("Unknown PEAT_DISCOVERY mode: {}", other);
            }
        };

    // ── Take discovery event stream (must be before start) ───────
    let event_stream = discovery
        .event_stream()
        .map_err(|e| anyhow::anyhow!("Failed to get discovery event stream: {}", e))?;

    // ── Start discovery ──────────────────────────────────────────
    discovery
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start discovery: {}", e))?;
    info!("Discovery started");

    // ── Build Iroh endpoint ──────────────────────────────────────
    let (endpoint, memory_lookup) = NetworkedIrohBlobStore::build_endpoint(&mesh_config.iroh)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to build Iroh endpoint: {}", e))?;

    info!(
        iroh_endpoint_id = %endpoint.id().fmt_short(),
        "Iroh endpoint ready"
    );

    // ── Advertise via discovery ──────────────────────────────────
    if let Err(e) = discovery.advertise(&hostname, iroh_bind_port).await {
        warn!("Failed to advertise node via discovery: {}", e);
    }

    // ── Automerge document store ────────────────────────────────
    let data_dir = std::env::var("PEAT_DATA_DIR").unwrap_or_else(|_| "/data".to_string());
    let automerge_store = Arc::new(
        AutomergeStore::open(format!("{}/automerge", data_dir))
            .map_err(|e| anyhow::anyhow!("Failed to open AutomergeStore: {}", e))?,
    );

    // ── TTL manager ─────────────────────────────────────────────
    let ttl_config = match std::env::var("PEAT_TTL_PRESET").as_deref() {
        Ok("tactical") => TtlConfig::tactical(),
        Ok("long_duration") => TtlConfig::long_duration(),
        Ok("offline_node") => TtlConfig::offline_node(),
        _ => TtlConfig::default(),
    };
    let ttl_manager = Arc::new(TtlManager::new(automerge_store.clone(), ttl_config));
    ttl_manager.start_background_cleanup();
    info!(
        preset = std::env::var("PEAT_TTL_PRESET").unwrap_or_else(|_| "default".to_string()),
        "TTL manager started"
    );

    // ── Certificate store (ADR-0006) ────────────────────────────
    let cert_store: Option<Arc<CertificateStore>> = if let Some(ref auth_hex) = authority_key_hex {
        let auth_bytes = hex::decode(auth_hex)
            .map_err(|e| anyhow::anyhow!("Invalid PEAT_AUTHORITY_KEY hex: {}", e))?;
        if auth_bytes.len() != 32 {
            anyhow::bail!(
                "PEAT_AUTHORITY_KEY must be 32 bytes (64 hex chars), got {}",
                auth_bytes.len()
            );
        }
        let mut auth_key = [0u8; 32];
        auth_key.copy_from_slice(&auth_bytes);

        let store = Arc::new(CertificateStore::new(automerge_store.clone(), &[auth_key]));
        let loaded = store.load_all().unwrap_or(0);
        info!(authority = %auth_hex, loaded, "Certificate store initialized");
        Some(store)
    } else {
        None
    };

    let certificate_bundle = cert_store.as_ref().map(|cs| cs.bundle());

    // ── Garbage collector (ADR-034 Phase 3) ────────────────────
    let gc_policy_registry = Arc::new(DeletionPolicyRegistry::with_defaults());
    let gc = Arc::new(GarbageCollector::with_policy_registry(
        automerge_store.clone(),
        gc_policy_registry,
        GcConfig::default(), // 5 minute interval
    ));
    let gc_handle = start_periodic_gc(gc.clone());
    info!("Garbage collector started (interval=5m)");

    // ── Storage eviction service (PRD-005) ──────────────────────
    let max_storage_bytes: usize = std::env::var("PEAT_STORAGE_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(512 * 1024 * 1024); // Default: 512 MB
    let eviction_config = match std::env::var("PEAT_EVICTION_PRESET").as_deref() {
        Ok("aggressive") => EvictionConfig::aggressive(),
        Ok("conservative") => EvictionConfig::conservative(),
        _ => EvictionConfig::default(),
    };
    let eviction_service = Arc::new(StorageEvictionService::new(
        automerge_store.clone(),
        max_storage_bytes,
        eviction_config,
    ));
    eviction_service.start();
    info!(
        max_bytes = max_storage_bytes,
        "Storage eviction service started"
    );

    // ── Sync transport (shares endpoint with blob store) ────────
    let sync_transport = Arc::new(MeshSyncTransport::new(endpoint.clone()));

    // ── Sync coordinator ────────────────────────────────────────
    let coordinator = Arc::new(AutomergeSyncCoordinator::new(
        automerge_store.clone(),
        sync_transport.clone() as Arc<dyn SyncTransport>,
    ));

    // ── SyncChannelManager ──────────────────────────────────────
    let channel_manager = Arc::new(SyncChannelManager::new(
        sync_transport.clone() as Arc<dyn SyncTransport>,
        coordinator.clone(),
    ));
    coordinator.set_channel_manager(channel_manager);
    coordinator.set_ttl_manager(ttl_manager.clone());

    // ── Bandwidth allocation (PRD-004) ────────────────────────
    let bandwidth_bps: u64 = std::env::var("PEAT_BANDWIDTH_BPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000); // Default: 1 Mbps (tactical)
    let bandwidth_alloc = Arc::new(peat_mesh::qos::BandwidthAllocation::new(bandwidth_bps));
    coordinator.set_bandwidth_allocation(bandwidth_alloc);
    info!(
        bandwidth_bps = bandwidth_bps,
        "Bandwidth allocation configured"
    );

    // ── Sync mode overrides (PRD-003) ─────────────────────────
    // Format: PEAT_SYNC_MODE_OVERRIDES="collection=mode,..." where mode is
    // "latest_only", "full_history", or "windowed:SECONDS"
    if let Ok(overrides) = std::env::var("PEAT_SYNC_MODE_OVERRIDES") {
        use peat_mesh::qos::SyncMode;
        let registry = coordinator.sync_mode_registry();
        for entry in overrides.split(',') {
            let entry = entry.trim();
            if let Some((collection, mode_str)) = entry.split_once('=') {
                let mode = match mode_str.trim() {
                    "latest_only" => Some(SyncMode::LatestOnly),
                    "full_history" => Some(SyncMode::FullHistory),
                    s if s.starts_with("windowed:") => {
                        s[9..]
                            .parse::<u64>()
                            .ok()
                            .map(|secs| SyncMode::WindowedHistory {
                                window_seconds: secs,
                            })
                    }
                    _ => {
                        warn!(entry = entry, "Invalid sync mode override, skipping");
                        None
                    }
                };
                if let Some(mode) = mode {
                    registry.set(collection.trim(), mode);
                    info!(collection = collection.trim(), mode = %mode_str.trim(), "Sync mode override applied");
                }
            }
        }
    }

    // ── Sync protocol handler (for incoming QUIC connections) ───
    let mut sync_handler = SyncProtocolHandler::new(sync_transport.clone(), coordinator.clone());

    // Wire Layer 2 certificate gating if configured
    if let Some(ref bundle) = certificate_bundle {
        sync_handler = sync_handler.with_certificate_bundle(bundle.clone(), require_certificates);
        info!(
            require = require_certificates,
            "Layer 2 certificate gating enabled on sync protocol"
        );
    }

    // ── Enrollment protocol handler (Layer 1) ────────────────────
    let mut extra_protocols: Vec<(&'static [u8], Box<dyn iroh::protocol::DynProtocolHandler>)> =
        vec![(CAP_AUTOMERGE_ALPN, Box::new(sync_handler))];

    if let Some(ref tokens_raw) = enrollment_tokens_raw {
        let mesh_id = hostname.clone(); // Use hostname as mesh_id for now
        let authority_kp = DeviceKeypair::from_seed(&seed, "peat-mesh:authority-keypair")
            .map_err(|e| anyhow::anyhow!("Authority keypair derivation failed: {}", e))?;

        let mut enrollment_service = peat_mesh::security::StaticEnrollmentService::new(
            authority_kp,
            mesh_id,
            24 * 60 * 60 * 1000, // 24-hour validity
        );

        // Parse tokens: "token1=tactical,token2=edge"
        for entry in tokens_raw.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let parts: Vec<&str> = entry.splitn(2, '=').collect();
            let (token, tier) = if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                (parts[0], "tactical")
            };
            let mesh_tier = peat_mesh::security::MeshTier::from_str_name(tier)
                .unwrap_or(peat_mesh::security::MeshTier::Tactical);
            enrollment_service.add_token(
                token.as_bytes().to_vec(),
                mesh_tier,
                peat_mesh::security::certificate::permissions::STANDARD,
            );
            info!(token_prefix = &token[..token.len().min(4)], tier = %mesh_tier, "Registered enrollment token");
        }

        let enrollment_handler = EnrollmentProtocolHandler::new(Arc::new(enrollment_service));
        extra_protocols.push((CAP_ENROLLMENT_ALPN, Box::new(enrollment_handler)));
        info!("Enrollment ALPN (peat/enroll/1) enabled");
    }

    // ── Create networked blob store with protocols ───────────────
    let blob_dir = std::env::temp_dir().join(format!("peat_iroh_blobs_{}", hostname));
    let blob_store = NetworkedIrohBlobStore::from_endpoint_with_protocols(
        blob_dir,
        endpoint,
        memory_lookup,
        extra_protocols,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create networked blob store: {}", e))?;

    info!(
        iroh_endpoint_id = %blob_store.endpoint_id().fmt_short(),
        "Iroh blob store ready (blobs + automerge sync)"
    );

    // ── Build mesh ───────────────────────────────────────────────
    let mesh = PeatMeshBuilder::new(mesh_config)
        .with_device_keypair_from_seed(&seed, &hostname)
        .map_err(|e| anyhow::anyhow!("Keypair derivation failed: {}", e))?
        .with_formation_key(formation_key)
        .with_discovery(discovery)
        .build();

    mesh.start()
        .map_err(|e| anyhow::anyhow!("Failed to start mesh: {}", e))?;

    let device_id = mesh
        .device_keypair()
        .map(|kp| kp.device_id().to_hex())
        .unwrap_or_else(|| "unknown".to_string());
    info!(node_id = %mesh.node_id(), device_id = %device_id, "Mesh started");

    // ── Spawn PeerConnector ──────────────────────────────────────
    let mut connector = PeerConnector::new(seed.clone(), blob_store.clone());
    if let Some(ref bundle) = certificate_bundle {
        connector = connector.with_certificate_bundle(bundle.clone(), require_certificates);
    }
    let _connector_handle = connector.run(event_stream);

    // ── Spawn certificate hot-reload watcher ─────────────────────
    if let Some(ref cs) = cert_store {
        let cs_clone = cs.clone();
        tokio::spawn(async move {
            cs_clone.watch_and_reload().await;
        });
        info!("Certificate hot-reload watcher started");
    }

    // ── Spawn sync polling task ─────────────────────────────────
    // Periodically sync all documents with connected peers.
    // K8s pods are flat (no hierarchy), so broadcast to all is correct.
    let (sync_cancel_tx, mut sync_cancel_rx) = tokio::sync::watch::channel(false);
    let _sync_poll_handle = {
        let coordinator = coordinator.clone();
        let transport = sync_transport.clone();
        let ttl_for_sync = ttl_manager.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = sync_cancel_rx.changed() => {
                        info!("Sync polling task shutting down");
                        break;
                    }
                }
                let peers = transport.connected_peers();
                // When offline (no peers), extend TTLs to prevent premature eviction
                if peers.is_empty() {
                    ttl_for_sync.extend_ttls_for_offline();
                    continue;
                }
                for peer_id in peers {
                    if let Err(e) = coordinator.sync_all_documents_with_peer(peer_id).await {
                        warn!(
                            peer = %peer_id.fmt_short(),
                            error = %e,
                            "Failed to sync documents with peer"
                        );
                    }
                    // Exchange tombstones so deletions propagate (ADR-034)
                    if let Err(e) = coordinator.sync_tombstones_with_peer(peer_id).await {
                        warn!(
                            peer = %peer_id.fmt_short(),
                            error = %e,
                            "Failed to sync tombstones with peer"
                        );
                    }
                }
            }
        })
    };

    // ── Peat-Lite transport + OTA sender ───────────────────────────
    let lite_port: u16 = std::env::var("PEAT_LITE_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5555);

    let lite_config = LiteTransportConfig {
        listen_port: lite_port,
        broadcast_port: lite_port,
        ..Default::default()
    };

    // Use a node_id derived from hostname for the Lite transport
    let lite_node_id: u32 = {
        let mut hash: u32 = 0;
        for b in hostname.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(b as u32);
        }
        hash
    };

    let lite_transport = Arc::new(LiteMeshTransport::new(lite_config, lite_node_id));
    if let Err(e) = lite_transport.start().await {
        warn!(
            "Failed to start Lite transport: {} (OTA will be unavailable)",
            e
        );
    } else {
        info!(port = lite_port, "Peat-Lite transport started");
    }

    // Derive OTA signing keypair from formation secret
    let ota_keypair = DeviceKeypair::from_seed(&seed, "peat-ota-signing-v1")
        .map_err(|e| anyhow::anyhow!("OTA keypair derivation failed: {}", e))?;
    info!(
        ota_signing_pubkey = %hex::encode(ota_keypair.public_key_bytes()),
        "OTA signing keypair derived (use this pubkey for peat-lite builds)"
    );
    let ota_sender = Arc::new(OtaSender::new(
        lite_transport.clone(),
        Some(Arc::new(ota_keypair)),
    ));

    // Wire OTA callback: route OTA messages from Lite peers to OtaSender
    {
        let ota_sender_ref = ota_sender.clone();
        lite_transport.set_ota_callback(move |peer_id, msg_type, payload| {
            let sender = ota_sender_ref.clone();
            let peer = peer_id.to_string();
            let pl = payload.to_vec();
            // Spawn because the callback is synchronous but OtaSender methods are async
            tokio::spawn(async move {
                match msg_type {
                    LiteMessageType::OtaAccept => sender.handle_accept(&peer, &pl).await,
                    LiteMessageType::OtaAck => sender.handle_ack(&peer, &pl).await,
                    LiteMessageType::OtaResult => sender.handle_result(&peer, &pl).await,
                    LiteMessageType::OtaAbort => sender.handle_abort(&peer, &pl).await,
                    _ => {}
                }
            });
        });
    }

    // Spawn OTA sender tick task (retransmit/timeout management)
    let (ota_cancel_tx, mut ota_cancel_rx) = tokio::sync::watch::channel(false);
    let _ota_tick_handle = {
        let sender = ota_sender.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = ota_cancel_rx.changed() => {
                        info!("OTA tick task shutting down");
                        break;
                    }
                }
                sender.tick().await;
            }
        })
    };

    // ── Broker HTTP server ───────────────────────────────────────
    let mesh = Arc::new(mesh);
    let broker_config = BrokerConfig {
        bind_addr: SocketAddr::from(([0, 0, 0, 0], broker_port)),
        ..Default::default()
    };
    let store_adapter = peat_mesh::broker::StoreBrokerAdapter::new(automerge_store.clone());
    let composite_state = Arc::new(peat_mesh::broker::CompositeBrokerState::new(
        mesh.clone() as Arc<dyn peat_mesh::broker::state::MeshBrokerState>,
        store_adapter,
    ));

    let ota_app_state = Arc::new(OtaAppState {
        sender: ota_sender.clone(),
    });

    let broker = Broker::new(composite_state as Arc<dyn peat_mesh::broker::state::MeshBrokerState>)
        .with_config(broker_config);
    let router = broker.build_router_with_ota(ota_app_state);

    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], broker_port)))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind broker port {}: {}", broker_port, e))?;

    info!(addr = %listener.local_addr()?, "Broker listening");

    // ── Graceful shutdown on SIGTERM/SIGINT ───────────────────────
    let mesh_ref = mesh.clone();
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| anyhow::anyhow!("Broker server error: {}", e))?;

    info!("Shutting down...");
    let _ = sync_cancel_tx.send(true);
    let _ = ota_cancel_tx.send(true);
    ttl_manager.stop_background_cleanup();
    gc.stop();
    gc_handle.abort();
    if let Err(e) = lite_transport.stop().await {
        error!("Error stopping Lite transport: {}", e);
    }
    if let Err(e) = blob_store.shutdown().await {
        error!("Error shutting down Iroh router: {}", e);
    }
    if let Err(e) = mesh_ref.stop() {
        error!("Error stopping mesh: {}", e);
    }
    info!("peat-mesh-node stopped");

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received SIGINT"),
        _ = terminate => info!("Received SIGTERM"),
    }
}
