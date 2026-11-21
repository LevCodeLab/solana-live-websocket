use crate::config::ServerConfig;
use crate::simulator::{ForkSimulationRequest, ForkSimulator};
use crate::source::StreamSourceManager;
use crate::state::LiveStateEngine;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use live_ws_core::adapters::{ClusterConfig, SolanaRpcClient};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<RwLock<LiveStateEngine>>,
    pub rpc_client: Arc<RwLock<Option<Arc<SolanaRpcClient>>>>,
    pub source_manager: Option<Arc<StreamSourceManager>>,
    pub simulator: Arc<ForkSimulator>,
    pub start_time: Instant,
    pub config: ServerConfig,
    pub ws_clients: Arc<AtomicU64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
    pub version: String,
    pub uptime_seconds: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub status: String,
    pub cluster: String,
    pub source: String,
    pub environment: String,
    pub sequence: u64,
    pub coverage_score: u32,
    pub connected: bool,
    pub websocket_clients: usize,
}

#[derive(Debug, Default, Deserialize)]
pub struct ReplayQuery {
    pub since_seq: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
pub struct SlotsQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct SwitchSourceRequest {
    pub cluster: Option<String>,
    pub source: String,
    pub rpc_url: Option<String>,
    pub ws_url: Option<String>,
}

/// Builds all Axum REST endpoints for `solana-live-websocket`.
pub fn api_routes(state: AppState) -> Router {
    Router::new()
        .route("/health", get(get_health))
        .route("/api/v1/health", get(get_health))
        .route("/api/v1/status", get(get_status))
        .route("/api/v1/snapshot", get(get_snapshot))
        .route("/api/v1/slots", get(get_slots))
        .route("/api/v1/forks", get(get_forks))
        .route("/api/v1/leaders", get(get_leaders))
        .route("/api/v1/capabilities", get(get_capabilities))
        .route("/api/v1/replay", get(get_replay))
        .route("/api/v1/simulate", post(post_simulate))
        .route("/api/v1/source", post(post_switch_source))
        .with_state(state)
}

async fn get_health(State(state): State<AppState>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
        service: "solana-live-websocket".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: state.start_time.elapsed().as_secs(),
    })
}

async fn get_status(State(state): State<AppState>) -> impl IntoResponse {
    let ws_count = state.ws_clients.load(Ordering::Relaxed) as usize;
    let snapshot = state.engine.read().await.build_snapshot(ws_count);
    Json(StatusResponse {
        status: snapshot.status,
        cluster: snapshot.cluster,
        source: snapshot.source,
        environment: snapshot.environment,
        sequence: snapshot.sequence,
        coverage_score: snapshot.capabilities.coverage_score,
        connected: snapshot.network.connected,
        websocket_clients: ws_count,
    })
}

async fn get_snapshot(State(state): State<AppState>) -> impl IntoResponse {
    let ws_count = state.ws_clients.load(Ordering::Relaxed) as usize;
    let snapshot = state.engine.read().await.build_snapshot(ws_count);
    Json(snapshot)
}

async fn get_slots(
    Query(query): Query<SlotsQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let ws_count = state.ws_clients.load(Ordering::Relaxed) as usize;
    let limit = query.limit.unwrap_or(16).clamp(1, 128);
    let eng = state.engine.read().await;
    let snap = eng.build_snapshot(ws_count);
    let recent_slots = eng.recent_slots_summary(limit);
    Json(serde_json::json!({
        "current": snap.slot,
        "recent_slots": recent_slots,
    }))
}

async fn get_forks(State(state): State<AppState>) -> impl IntoResponse {
    let eng = state.engine.read().await;
    Json(eng.forks_view())
}

async fn get_leaders(State(state): State<AppState>) -> impl IntoResponse {
    let ws_count = state.ws_clients.load(Ordering::Relaxed) as usize;
    let snapshot = state.engine.read().await.build_snapshot(ws_count);
    Json(snapshot.leader)
}

async fn get_capabilities(State(state): State<AppState>) -> impl IntoResponse {
    let ws_count = state.ws_clients.load(Ordering::Relaxed) as usize;
    let eng = state.engine.read().await;
    let snapshot = eng.build_snapshot(ws_count);
    let detailed = eng.telemetry_capability_matrix();
    Json(serde_json::json!({
        "summary": snapshot.capabilities,
        "detailed": detailed,
    }))
}

async fn get_replay(
    Query(query): Query<ReplayQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let since_seq = query.since_seq.unwrap_or(0);
    let eng = state.engine.read().await;
    match eng.replay_since(since_seq) {
        Ok(events) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "ok",
                "since_seq": since_seq,
                "latest_seq": eng.current_sequence(),
                "count": events.len(),
                "events": events,
            })),
        ),
        Err(oldest) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "status": "lagged",
                "requested_seq": since_seq,
                "oldest_available_seq": oldest,
                "latest_seq": eng.current_sequence(),
                "message": "Requested sequence evicted from ring buffer; fetch /api/v1/snapshot",
            })),
        ),
    }
}

async fn post_simulate(
    State(state): State<AppState>,
    Json(req): Json<ForkSimulationRequest>,
) -> impl IntoResponse {
    Json(state.simulator.run_scenario(&req))
}

async fn post_switch_source(
    State(state): State<AppState>,
    Json(req): Json<SwitchSourceRequest>,
) -> impl IntoResponse {
    let cluster = req
        .cluster
        .unwrap_or_else(|| state.config.cluster.clone());
    let default_conf = match cluster.to_lowercase().as_str() {
        "devnet" => ClusterConfig::devnet(),
        "mainnet" | "mainnet-beta" => ClusterConfig::mainnet_beta(),
        "local" => ClusterConfig::local(),
        _ => ClusterConfig::testnet(),
    };

    let rpc_url = req.rpc_url.unwrap_or(default_conf.rpc_url);
    let ws_url = req.ws_url.unwrap_or(default_conf.ws_url);
    let source = req.source.to_lowercase();

    {
        let mut eng = state.engine.write().await;
        eng.update_cluster(
            cluster.clone(),
            source.clone(),
            rpc_url.clone(),
            ws_url.clone(),
        );
    }

    if let Some(ref mgr) = state.source_manager {
        if let Err(err) = mgr
            .switch_source(
                &cluster,
                &source,
                &rpc_url,
                &ws_url,
                &state.config.validator_uds_path,
            )
            .await
        {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "ok": false,
                    "error": err,
                })),
            );
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "cluster": cluster,
            "source": source,
        })),
    )
}
