use crate::api::{api_routes, AppState};
use crate::config::ServerConfig;
use crate::envelope::LiveStreamEvent;
use crate::simulator::ForkSimulator;
use crate::source::StreamSourceManager;
use crate::state::LiveStateEngine;
use crate::ws::ws_routes;
use axum::Router;
use live_ws_core::adapters::SolanaRpcClient;
use live_ws_core::events::CoreStreamEvent;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

/// Real-time Axum HTTP and WebSocket broadcast server for Solana slots, forks,
/// candidate banks, and leader handoff windows.
pub struct LiveWsServer {
    config: ServerConfig,
    engine: Arc<RwLock<LiveStateEngine>>,
    broadcast_tx: broadcast::Sender<LiveStreamEvent>,
    ws_clients: Arc<AtomicU64>,
    source_manager: Arc<StreamSourceManager>,
    event_rx: Arc<Mutex<Option<mpsc::Receiver<CoreStreamEvent>>>>,
}

impl LiveWsServer {
    pub fn new(config: ServerConfig) -> Self {
        let broadcast_cap = config.ring_buffer_capacity.clamp(64, 16_384);
        let (broadcast_tx, _) = broadcast::channel(broadcast_cap);
        let engine = Arc::new(RwLock::new(LiveStateEngine::new(config.clone())));
        let ws_clients = Arc::new(AtomicU64::new(0));
        let (event_tx, event_rx) = mpsc::channel(2_048);
        let source_manager = Arc::new(StreamSourceManager::new(event_tx));

        Self {
            config,
            engine,
            broadcast_tx,
            ws_clients,
            source_manager,
            event_rx: Arc::new(Mutex::new(Some(event_rx))),
        }
    }

    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    pub fn engine(&self) -> Arc<RwLock<LiveStateEngine>> {
        self.engine.clone()
    }

    pub fn broadcast_sender(&self) -> broadcast::Sender<LiveStreamEvent> {
        self.broadcast_tx.clone()
    }

    /// Processes a single `CoreStreamEvent` into the state engine + replay ring
    /// and broadcasts the resulting `LiveStreamEvent` to all connected WebSocket clients.
    pub async fn inject_event(&self, event: CoreStreamEvent) -> LiveStreamEvent {
        let stream_event = self.engine.write().await.process_event(event);
        let _ = self.broadcast_tx.send(stream_event.clone());
        stream_event
    }

    /// Builds the combined Axum router (`/health`, `/api/v1/*`, `/ws`, `/api/v1/stream`).
    pub fn router(&self) -> Router {
        let rpc_client = if !self.config.rpc_url.is_empty() {
            Arc::new(RwLock::new(Some(Arc::new(SolanaRpcClient::new(
                self.config.rpc_url.clone(),
            )))))
        } else {
            Arc::new(RwLock::new(None))
        };

        let app_state = AppState {
            engine: self.engine.clone(),
            rpc_client,
            source_manager: Some(self.source_manager.clone()),
            simulator: Arc::new(ForkSimulator::new()),
            start_time: Instant::now(),
            config: self.config.clone(),
            ws_clients: self.ws_clients.clone(),
        };

        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);

        Router::new()
            .merge(api_routes(app_state.clone()))
            .merge(ws_routes(app_state, self.broadcast_tx.clone()))
            .layer(cors)
    }

    /// Starts the background ingestion pipeline without blocking on TCP bind.
    pub async fn start_pipeline(&self) -> Result<(), String> {
        self.source_manager
            .switch_source(
                &self.config.cluster,
                &self.config.source,
                &self.config.rpc_url,
                &self.config.ws_url,
                &self.config.validator_uds_path,
            )
            .await
            .map_err(|e| format!("Failed to start initial stream source: {}", e))?;

        if let Some(mut event_rx) = self.event_rx.lock().await.take() {
            let engine_clone = self.engine.clone();
            let broadcast_tx_clone = self.broadcast_tx.clone();
            tokio::spawn(async move {
                while let Some(core_event) = event_rx.recv().await {
                    let stream_event = engine_clone.write().await.process_event(core_event);
                    let _ = broadcast_tx_clone.send(stream_event);
                }
            });
        }

        Ok(())
    }

    /// Runs the full `solana-live-websocket` HTTP + WebSocket service until `Ctrl+C`.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr_str = format!("{}:{}", self.config.host, self.config.port);
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| format!("Invalid bind address '{}': {}", addr_str, e))?;

        info!("============================================================");
        info!("solana-live-websocket - starting live server");
        info!("============================================================");
        info!("Bind Address:        http://{}", addr);
        info!("Cluster:             {}", self.config.cluster);
        info!("Source:              {}", self.config.source);
        info!("RPC URL:             {}", self.config.rpc_url);
        info!("Replay Ring Buffer:  {} frames", self.config.ring_buffer_capacity);
        info!("WebSocket Stream:    ws://{}/ws", addr);
        info!("Snapshot Endpoint:   http://{}/api/v1/snapshot", addr);
        info!("============================================================");

        self.start_pipeline().await?;

        let app = self.router();
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        info!("solana-live-websocket listening on http://{}", addr);

        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;

        self.source_manager.stop().await;
        info!("solana-live-websocket stopped cleanly.");
        Ok(())
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutdown signal received, draining connections...");
}
