use crate::bus::EventBus;
use crate::events::{CoreEventKind, CoreStreamEvent};
use crate::identity::BankIdentity;
use crate::types::{
    now_unix_nanos, BankHash, BankId, BankState, BankStatus, BlockFooter, Blockhash,
    CertificateKind, CertificateValidationStatus, FieldAvailabilityDetail, FieldProvenance,
    LeaderId, ObserverContext, ParentReference, ProviderId, Slot, TelemetryCapabilityMatrix,
    TelemetryLevel,
};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, info, warn};

/// Cluster endpoint configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub rpc_url: String,
    pub ws_url: String,
}

impl ClusterConfig {
    pub fn testnet() -> Self {
        Self {
            name: "testnet".to_string(),
            rpc_url: "https://api.testnet.solana.com".to_string(),
            ws_url: "wss://api.testnet.solana.com".to_string(),
        }
    }

    pub fn devnet() -> Self {
        Self {
            name: "devnet".to_string(),
            rpc_url: "https://api.devnet.solana.com".to_string(),
            ws_url: "wss://api.devnet.solana.com".to_string(),
        }
    }

    pub fn mainnet_beta() -> Self {
        Self {
            name: "mainnet-beta".to_string(),
            rpc_url: "https://api.mainnet-beta.solana.com".to_string(),
            ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
        }
    }

    pub fn local() -> Self {
        Self {
            name: "local".to_string(),
            rpc_url: "http://127.0.0.1:8899".to_string(),
            ws_url: "ws://127.0.0.1:8900".to_string(),
        }
    }

    pub fn custom(
        name: impl Into<String>,
        rpc_url: impl Into<String>,
        ws_url: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            rpc_url: rpc_url.into(),
            ws_url: ws_url.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("WebSocket error: {0}")]
    WebSocket(String),
    #[error("JSON RPC error: {0}")]
    JsonRpc(String),
    #[error("Connection closed: {0}")]
    ConnectionClosed(String),
    #[error("Invalid payload format: {0}")]
    InvalidPayload(String),
    #[error("Timeout: {0}")]
    Timeout(String),
}

/// Provider connection health lifecycle status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderConnectionStatus {
    Connected,
    Degraded(String),
    Reconnecting(u32),
    Disconnected,
}

impl fmt::Display for ProviderConnectionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connected => write!(f, "CONNECTED"),
            Self::Degraded(reason) => write!(f, "DEGRADED: {}", reason),
            Self::Reconnecting(attempt) => write!(f, "RECONNECTING (attempt {})", attempt),
            Self::Disconnected => write!(f, "DISCONNECTED"),
        }
    }
}

/// Capabilities explicitly exposed by a provider stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    pub slot_subscription: bool,
    pub block_subscription: bool,
    pub bank_id_stream: bool,
    pub update_parent_stream: bool,
    pub bls_certificates: bool,
}

impl ProviderCapabilities {
    pub fn standard_public_rpc() -> Self {
        Self {
            slot_subscription: true,
            block_subscription: true,
            bank_id_stream: false,
            update_parent_stream: false,
            bls_certificates: false,
        }
    }

    pub fn full_validator_stream() -> Self {
        Self {
            slot_subscription: true,
            block_subscription: true,
            bank_id_stream: true,
            update_parent_stream: true,
            bls_certificates: true,
        }
    }
}

#[async_trait]
pub trait ProviderAdapter: Send + Sync {
    fn provider_id(&self) -> &ProviderId;
    fn status(&self) -> ProviderConnectionStatus;
    fn capabilities(&self) -> ProviderCapabilities;
    async fn connect(&mut self) -> Result<(), AdapterError>;
    async fn disconnect(&mut self) -> Result<(), AdapterError>;
    async fn fetch_current_slot(&self) -> Result<Slot, AdapterError>;
}

/// Support state of an individual telemetry dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldSupport {
    Supported,
    DerivedOnly,
    Unsupported,
    Unknown,
}

impl fmt::Display for FieldSupport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Supported => write!(f, "YES"),
            Self::DerivedOnly => write!(f, "DERIVED"),
            Self::Unsupported => write!(f, "NO"),
            Self::Unknown => write!(f, "?"),
        }
    }
}

/// Detailed runtime capability matrix for an active stream provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilityMatrix {
    pub provider_name: String,
    pub slot: FieldSupport,
    pub leader: FieldSupport,
    pub bank_id: FieldSupport,
    pub bank_hash: FieldSupport,
    pub parent: FieldSupport,
    pub update_parent: FieldSupport,
    pub block_footer: FieldSupport,
    pub certificates: FieldSupport,
    pub producer_time: FieldSupport,
    pub producer_user_agent: FieldSupport,
    pub deshred: FieldSupport,
}

impl ProviderCapabilityMatrix {
    pub fn standard_public_rpc(provider_name: impl Into<String>) -> Self {
        Self {
            provider_name: provider_name.into(),
            slot: FieldSupport::Supported,
            leader: FieldSupport::DerivedOnly,
            bank_id: FieldSupport::Unsupported,
            bank_hash: FieldSupport::Unsupported,
            parent: FieldSupport::Supported,
            update_parent: FieldSupport::Unsupported,
            block_footer: FieldSupport::Unsupported,
            certificates: FieldSupport::Unsupported,
            producer_time: FieldSupport::Unsupported,
            producer_user_agent: FieldSupport::Unsupported,
            deshred: FieldSupport::Unsupported,
        }
    }

    pub fn validator_uds(provider_name: impl Into<String>) -> Self {
        Self {
            provider_name: provider_name.into(),
            slot: FieldSupport::Supported,
            leader: FieldSupport::Supported,
            bank_id: FieldSupport::Supported,
            bank_hash: FieldSupport::Supported,
            parent: FieldSupport::Supported,
            update_parent: FieldSupport::Supported,
            block_footer: FieldSupport::Supported,
            certificates: FieldSupport::Supported,
            producer_time: FieldSupport::Supported,
            producer_user_agent: FieldSupport::Supported,
            deshred: FieldSupport::Supported,
        }
    }

    pub fn simulator_fixture(provider_name: impl Into<String>) -> Self {
        Self::validator_uds(provider_name)
    }

    pub fn coverage_score(&self) -> u32 {
        let mut score = 0;
        let checks = [
            self.slot,
            self.leader,
            self.bank_id,
            self.bank_hash,
            self.parent,
            self.update_parent,
            self.block_footer,
            self.certificates,
            self.producer_time,
            self.producer_user_agent,
        ];
        for c in checks {
            match c {
                FieldSupport::Supported => score += 10,
                FieldSupport::DerivedOnly => score += 7,
                FieldSupport::Unknown => score += 2,
                FieldSupport::Unsupported => {}
            }
        }
        score
    }

    pub fn to_detailed_matrix(
        &self,
        observer: ObserverContext,
        is_live_observed: bool,
    ) -> TelemetryCapabilityMatrix {
        let is_fixture = self.provider_name.contains("simulator")
            || self.provider_name.contains("fixture");
        let is_rpc = self.provider_name.contains("rpc");

        let level = if is_rpc {
            TelemetryLevel::Level1PublicRpcWs
        } else if is_fixture {
            TelemetryLevel::Level3SimulatorFixture
        } else {
            TelemetryLevel::Level4ValidatorUds
        };

        let make_field = |name: &str, support: FieldSupport, unavail_msg: &str| {
            let source_supported =
                support == FieldSupport::Supported || support == FieldSupport::DerivedOnly;
            let provenance = match support {
                FieldSupport::Supported => FieldProvenance::Direct,
                FieldSupport::DerivedOnly => FieldProvenance::Derived,
                _ => FieldProvenance::Unavailable,
            };
            FieldAvailabilityDetail {
                field: name.to_string(),
                protocol_supported: true,
                source_supported,
                currently_observed: is_live_observed && source_supported,
                provenance,
                freshness: if is_live_observed && source_supported {
                    "LIVE".to_string()
                } else {
                    "UNAVAILABLE".to_string()
                },
                reason_unavailable: if source_supported {
                    None
                } else {
                    Some(unavail_msg.to_string())
                },
                alternate_source: if source_supported {
                    None
                } else {
                    Some("Local Validator UDS Stream".to_string())
                },
            }
        };

        let fields = vec![
            make_field("slot", self.slot, ""),
            make_field("leader", self.leader, ""),
            make_field(
                "bank_id",
                self.bank_id,
                "Standard JSON-RPC omits validator-local candidate bank_id",
            ),
            make_field(
                "bank_hash",
                self.bank_hash,
                "Standard JSON-RPC omits interim bank state hashes",
            ),
            make_field("parent_slot", self.parent, ""),
            make_field(
                "update_parent",
                self.update_parent,
                "Standard JSON-RPC omits fast leader handover UpdateParent markers",
            ),
            make_field(
                "block_footer",
                self.block_footer,
                "Standard JSON-RPC omits validator block footers",
            ),
            make_field(
                "certificates",
                self.certificates,
                "Standard JSON-RPC omits consensus certificates",
            ),
            make_field(
                "producer_time",
                self.producer_time,
                "Standard JSON-RPC omits nanosecond producer timestamps",
            ),
            make_field(
                "producer_user_agent",
                self.producer_user_agent,
                "Standard JSON-RPC omits validator user-agent strings",
            ),
            make_field(
                "deshred",
                self.deshred,
                "Standard JSON-RPC omits pre-execution deshred streams",
            ),
        ];

        let core_pct = self.coverage_score();
        let ext_supported = fields.iter().filter(|f| f.source_supported).count();
        let ext_pct = ((ext_supported as f64 / fields.len() as f64) * 100.0).round() as u32;

        TelemetryCapabilityMatrix {
            current_source: self.provider_name.clone(),
            telemetry_level: level,
            observer,
            fields,
            core_coverage_percent: core_pct,
            extended_coverage_percent: ext_pct,
        }
    }
}

/// Raw slot notification parsed from Solana PubSub `slotSubscribe`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawSlotNotification {
    pub slot: Slot,
    pub parent: Option<Slot>,
    pub root: Option<Slot>,
}

/// Stream normalizer with bounded slot deduplication and out-of-order convergence.
pub struct StreamNormalizer {
    provider_id: ProviderId,
    seen_slots: HashSet<Slot>,
    max_history: usize,
    event_sequence: u64,
}

impl StreamNormalizer {
    pub fn new(provider_id: ProviderId, max_history: usize) -> Self {
        Self {
            provider_id,
            seen_slots: HashSet::with_capacity(max_history),
            max_history,
            event_sequence: 0,
        }
    }

    pub fn normalize_slot(&mut self, raw: RawSlotNotification) -> Option<CoreStreamEvent> {
        if self.seen_slots.contains(&raw.slot) {
            return None;
        }

        if self.seen_slots.len() >= self.max_history {
            let min_keep = raw.slot.as_u64().saturating_sub(self.max_history as u64);
            self.seen_slots.retain(|s| s.as_u64() >= min_keep);
        }

        self.seen_slots.insert(raw.slot);
        self.event_sequence += 1;

        Some(CoreStreamEvent::new(
            self.event_sequence,
            self.provider_id.clone(),
            raw.slot,
            None,
            CoreEventKind::SlotObserved {
                parent_slot: raw.parent,
            },
        ))
    }

    pub fn seen_count(&self) -> usize {
        self.seen_slots.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochInfo {
    pub absolute_slot: u64,
    pub block_height: u64,
    pub epoch: u64,
    pub slot_index: u64,
    pub slots_in_epoch: u64,
    pub transaction_count: Option<u64>,
}

/// HTTP JSON-RPC client for Solana cluster queries.
pub struct SolanaRpcClient {
    client: Client,
    rpc_url: String,
}

impl SolanaRpcClient {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(6))
                .build()
                .unwrap_or_default(),
            rpc_url: rpc_url.into(),
        }
    }

    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    pub async fn get_slot(&self) -> Result<Slot, AdapterError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": [{"commitment": "processed"}]
        });
        let resp = self.send_rpc(&payload).await?;
        let slot = resp
            .as_u64()
            .ok_or_else(|| AdapterError::InvalidPayload("Expected u64 slot".into()))?;
        Ok(Slot(slot))
    }

    pub async fn get_slot_leader(&self) -> Result<LeaderId, AdapterError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlotLeader"
        });
        let resp = self.send_rpc(&payload).await?;
        let leader_str = resp
            .as_str()
            .ok_or_else(|| AdapterError::InvalidPayload("Expected leader string".into()))?;
        Ok(LeaderId::new(leader_str))
    }

    pub async fn get_slot_leaders(
        &self,
        start_slot: u64,
        limit: u64,
    ) -> Result<Vec<LeaderId>, AdapterError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlotLeaders",
            "params": [start_slot, limit]
        });
        let resp = self.send_rpc(&payload).await?;
        let array = resp
            .as_array()
            .ok_or_else(|| AdapterError::InvalidPayload("Expected array of leaders".into()))?;
        Ok(array
            .iter()
            .filter_map(|v| v.as_str().map(LeaderId::new))
            .collect())
    }

    pub async fn get_epoch_info(&self) -> Result<EpochInfo, AdapterError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getEpochInfo",
            "params": [{"commitment": "processed"}]
        });
        let resp = self.send_rpc(&payload).await?;
        Ok(EpochInfo {
            epoch: resp.get("epoch").and_then(|v| v.as_u64()).unwrap_or(0),
            absolute_slot: resp
                .get("absoluteSlot")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            block_height: resp
                .get("blockHeight")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            slot_index: resp.get("slotIndex").and_then(|v| v.as_u64()).unwrap_or(0),
            slots_in_epoch: resp
                .get("slotsInEpoch")
                .and_then(|v| v.as_u64())
                .unwrap_or(432_000),
            transaction_count: resp.get("transactionCount").and_then(|v| v.as_u64()),
        })
    }

    pub async fn get_latest_blockhash(&self) -> Result<(String, u64), AdapterError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [{"commitment": "processed"}]
        });
        let resp = self.send_rpc(&payload).await?;
        let blockhash = resp
            .get("value")
            .and_then(|v| v.get("blockhash"))
            .and_then(|b| b.as_str())
            .ok_or_else(|| AdapterError::InvalidPayload("Missing blockhash".into()))?;
        let last_valid_block_height = resp
            .get("value")
            .and_then(|v| v.get("lastValidBlockHeight"))
            .and_then(|h| h.as_u64())
            .unwrap_or(0);
        Ok((blockhash.to_string(), last_valid_block_height))
    }

    pub async fn get_leader_schedule(
        &self,
        slot: Slot,
    ) -> Result<HashMap<Slot, LeaderId>, AdapterError> {
        let epoch_start_slot = match self.get_epoch_info().await {
            Ok(info) => info.absolute_slot.saturating_sub(info.slot_index),
            Err(_) => (slot.as_u64() / 432_000) * 432_000,
        };

        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLeaderSchedule",
            "params": [slot.as_u64(), {"commitment": "processed"}]
        });
        let resp = self.send_rpc(&payload).await?;
        let mut schedule = HashMap::new();
        if let Some(map) = resp.as_object() {
            for (leader_pubkey, slots) in map {
                if let Some(slot_indices) = slots.as_array() {
                    for idx in slot_indices {
                        if let Some(i) = idx.as_u64() {
                            schedule
                                .insert(Slot(epoch_start_slot + i), LeaderId::new(leader_pubkey));
                        }
                    }
                }
            }
        }
        Ok(schedule)
    }

    async fn send_rpc(&self, payload: &serde_json::Value) -> Result<serde_json::Value, AdapterError> {
        let resp = self
            .client
            .post(&self.rpc_url)
            .json(payload)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        if let Some(err) = resp.get("error") {
            return Err(AdapterError::JsonRpc(err.to_string()));
        }

        resp.get("result")
            .cloned()
            .ok_or_else(|| AdapterError::InvalidPayload("Missing result field in RPC response".into()))
    }
}

/// Solana PubSub WebSocket stream with automatic reconnect and exponential backoff.
pub struct SolanaWsStream {
    ws_url: String,
    status: Arc<RwLock<ProviderConnectionStatus>>,
    is_running: Arc<AtomicBool>,
}

impl SolanaWsStream {
    pub fn new(ws_url: impl Into<String>) -> Self {
        Self {
            ws_url: ws_url.into(),
            status: Arc::new(RwLock::new(ProviderConnectionStatus::Disconnected)),
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn get_status(&self) -> ProviderConnectionStatus {
        self.status.read().await.clone()
    }

    pub fn start(&self, out_sender: mpsc::Sender<RawSlotNotification>) {
        if self.is_running.swap(true, Ordering::SeqCst) {
            return;
        }

        let ws_url = self.ws_url.clone();
        let status = self.status.clone();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut attempt = 0;

            while is_running.load(Ordering::SeqCst) {
                attempt += 1;
                *status.write().await = ProviderConnectionStatus::Reconnecting(attempt);
                info!(
                    "Connecting to Solana WebSocket: {} (attempt {})",
                    ws_url, attempt
                );

                match connect_async(&ws_url).await {
                    Ok((ws_stream, _)) => {
                        *status.write().await = ProviderConnectionStatus::Connected;
                        attempt = 0;
                        let (mut write, mut read) = ws_stream.split();

                        let sub_msg = json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "slotSubscribe"
                        });

                        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                            error!("Failed to send slotSubscribe: {}", e);
                            *status.write().await =
                                ProviderConnectionStatus::Degraded(e.to_string());
                            sleep(Duration::from_millis(500)).await;
                            continue;
                        }

                        while let Some(msg_result) = read.next().await {
                            if !is_running.load(Ordering::SeqCst) {
                                break;
                            }
                            match msg_result {
                                Ok(Message::Text(text)) => {
                                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                                    {
                                        if let Some(result) =
                                            val.get("params").and_then(|p| p.get("result"))
                                        {
                                            if let Some(slot_u64) =
                                                result.get("slot").and_then(|s| s.as_u64())
                                            {
                                                let parent = result
                                                    .get("parent")
                                                    .and_then(|p| p.as_u64())
                                                    .map(Slot);
                                                let root = result
                                                    .get("root")
                                                    .and_then(|r| r.as_u64())
                                                    .map(Slot);

                                                if out_sender
                                                    .send(RawSlotNotification {
                                                        slot: Slot(slot_u64),
                                                        parent,
                                                        root,
                                                    })
                                                    .await
                                                    .is_err()
                                                {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                Ok(Message::Ping(p)) => {
                                    let _ = write.send(Message::Pong(p)).await;
                                }
                                Ok(Message::Close(_)) => break,
                                Err(e) => {
                                    warn!("WebSocket read error: {}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        error!("WebSocket connection error: {}", e);
                        *status.write().await = ProviderConnectionStatus::Degraded(e.to_string());
                    }
                }

                let backoff_ms = (500 * (1 << attempt.min(3))).min(5000);
                sleep(Duration::from_millis(backoff_ms)).await;
            }

            *status.write().await = ProviderConnectionStatus::Disconnected;
        });
    }

    pub fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);
    }
}

/// NDJSON framing protocol for local validator Unix Domain Socket streams.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorRawFrame {
    pub sequence: u64,
    pub observer_validator: String,
    pub observed_at_nanos: u64,
    pub payload: ValidatorPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ValidatorPayload {
    SlotStatus {
        slot: u64,
        parent_slot: Option<u64>,
        bank_id: Option<u64>,
        status: String,
    },
    BankLifecycle {
        slot: u64,
        bank_id: u64,
        parent_slot: Option<u64>,
        status: String,
        bank_hash: Option<String>,
    },
    BlockFooter {
        slot: u64,
        bank_id: u64,
        bank_hash: String,
        producer_time_nanos: u64,
        user_agent: String,
        final_cert: Option<Vec<u8>>,
        notar_cert: Option<Vec<u8>>,
        skip_cert: Option<Vec<u8>>,
    },
    UpdateParent {
        slot: u64,
        cleared_bank_id: Option<u64>,
        parent_slot: u64,
        parent_block_id: Option<String>,
        fec_set_index: Option<u32>,
        source: String,
    },
    DeshredTx {
        slot: u64,
        signature: String,
        raw_bytes: Vec<u8>,
        static_accounts: Vec<String>,
        pre_execution_timestamp_nanos: u64,
        fec_set_index: Option<u64>,
    },
    Entry {
        slot: u64,
        bank_id: Option<u64>,
        entry_index: u64,
        tx_count: u64,
        starting_tx_index: u64,
    },
    BlockMeta {
        slot: u64,
        blockhash: String,
        parent_slot: u64,
        parent_blockhash: String,
        executed_tx_count: u64,
    },
    Heartbeat {
        timestamp_nanos: u64,
        active_slots: u64,
    },
}

#[derive(Debug, Default)]
pub struct UdsTelemetryCounters {
    pub events_received_total: AtomicU64,
    pub bank_events: AtomicU64,
    pub block_footer_events: AtomicU64,
    pub update_parent_events: AtomicU64,
    pub certificate_events: AtomicU64,
    pub deshred_events: AtomicU64,
    pub entry_events: AtomicU64,
    pub slot_events: AtomicU64,
    pub events_dropped_total: AtomicU64,
    pub events_parse_failed: AtomicU64,
    pub events_out_of_order: AtomicU64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryCountersSnapshot {
    pub events_received_total: u64,
    pub bank_events: u64,
    pub block_footer_events: u64,
    pub update_parent_events: u64,
    pub certificate_events: u64,
    pub deshred_events: u64,
    pub entry_events: u64,
    pub slot_events: u64,
    pub events_dropped_total: u64,
    pub events_parse_failed: u64,
    pub events_out_of_order: u64,
}

impl UdsTelemetryCounters {
    pub fn snapshot(&self) -> TelemetryCountersSnapshot {
        TelemetryCountersSnapshot {
            events_received_total: self.events_received_total.load(Ordering::Relaxed),
            bank_events: self.bank_events.load(Ordering::Relaxed),
            block_footer_events: self.block_footer_events.load(Ordering::Relaxed),
            update_parent_events: self.update_parent_events.load(Ordering::Relaxed),
            certificate_events: self.certificate_events.load(Ordering::Relaxed),
            deshred_events: self.deshred_events.load(Ordering::Relaxed),
            entry_events: self.entry_events.load(Ordering::Relaxed),
            slot_events: self.slot_events.load(Ordering::Relaxed),
            events_dropped_total: self.events_dropped_total.load(Ordering::Relaxed),
            events_parse_failed: self.events_parse_failed.load(Ordering::Relaxed),
            events_out_of_order: self.events_out_of_order.load(Ordering::Relaxed),
        }
    }
}

/// Local Validator Unix Domain Socket stream adapter reading NDJSON telemetry frames.
pub struct ValidatorUdsAdapter {
    provider_id: ProviderId,
    socket_path: String,
    status: Arc<std::sync::Mutex<ProviderConnectionStatus>>,
    event_bus: Option<Arc<EventBus>>,
    event_counter: Arc<AtomicU64>,
    last_slot: Arc<AtomicU64>,
    capabilities: ProviderCapabilityMatrix,
    counters: Arc<UdsTelemetryCounters>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ValidatorUdsAdapter {
    pub fn new(socket_path: impl Into<String>, event_bus: Option<Arc<EventBus>>) -> Self {
        let provider_id = ProviderId::new("validator-uds");
        let capabilities = ProviderCapabilityMatrix::validator_uds(provider_id.as_str());
        Self {
            provider_id,
            socket_path: socket_path.into(),
            status: Arc::new(std::sync::Mutex::new(
                ProviderConnectionStatus::Disconnected,
            )),
            event_bus,
            event_counter: Arc::new(AtomicU64::new(1)),
            last_slot: Arc::new(AtomicU64::new(0)),
            capabilities,
            counters: Arc::new(UdsTelemetryCounters::default()),
            stop_tx: None,
        }
    }

    pub fn counters(&self) -> Arc<UdsTelemetryCounters> {
        self.counters.clone()
    }

    pub fn capability_matrix(&self) -> &ProviderCapabilityMatrix {
        &self.capabilities
    }
}

#[async_trait]
impl ProviderAdapter for ValidatorUdsAdapter {
    fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    fn status(&self) -> ProviderConnectionStatus {
        self.status.lock().unwrap().clone()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities::full_validator_stream()
    }

    async fn fetch_current_slot(&self) -> Result<Slot, AdapterError> {
        Ok(Slot(self.last_slot.load(Ordering::Relaxed)))
    }

    #[cfg(unix)]
    async fn connect(&mut self) -> Result<(), AdapterError> {
        use tokio::io::{AsyncBufReadExt, BufReader};
        use tokio::net::UnixListener;

        if Path::new(&self.socket_path).exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }

        let listener = UnixListener::bind(&self.socket_path).map_err(|e| {
            AdapterError::ConnectionClosed(format!(
                "Failed to bind UDS listener at {}: {}",
                self.socket_path, e
            ))
        })?;

        *self.status.lock().unwrap() = ProviderConnectionStatus::Connected;
        info!("ValidatorUdsAdapter listening on UDS: {}", self.socket_path);

        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        self.stop_tx = Some(stop_tx);

        let status_arc = self.status.clone();
        let event_bus = self.event_bus.clone();
        let provider_id = self.provider_id.clone();
        let event_counter = self.event_counter.clone();
        let counters = self.counters.clone();
        let last_slot = self.last_slot.clone();

        tokio::spawn(async move {
            let mut last_seq = 0u64;
            let mut pending_replacement: Option<(u64, u64)> = None;

            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    accept_res = listener.accept() => {
                        match accept_res {
                            Ok((stream, _)) => {
                                let mut reader = BufReader::new(stream);
                                let mut line = String::new();
                                while let Ok(n) = reader.read_line(&mut line).await {
                                    if n == 0 {
                                        break;
                                    }
                                    counters.events_received_total.fetch_add(1, Ordering::Relaxed);
                                    match serde_json::from_str::<ValidatorRawFrame>(&line) {
                                        Ok(frame) => {
                                            if last_seq > 0 && frame.sequence > last_seq + 1 {
                                                counters.events_out_of_order.fetch_add(frame.sequence - (last_seq + 1), Ordering::Relaxed);
                                            }
                                            last_seq = frame.sequence;

                                            let observer_ctx = ObserverContext {
                                                observing_validator: Some(frame.observer_validator),
                                                producing_validator: None,
                                                transport_type: "uds".to_string(),
                                                cluster_environment: "local-validator".to_string(),
                                            };

                                            let events = translate_validator_payload(
                                                frame.payload,
                                                &provider_id,
                                                &event_counter,
                                                &counters,
                                                &last_slot,
                                                observer_ctx,
                                                &mut pending_replacement,
                                            );
                                            if let Some(ref bus) = event_bus {
                                                for ev in events {
                                                    bus.publish(ev);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            counters.events_parse_failed.fetch_add(1, Ordering::Relaxed);
                                            warn!("Failed to parse validator UDS JSONL frame: {}", e);
                                        }
                                    }
                                    line.clear();
                                }
                            }
                            Err(e) => {
                                error!("Error accepting UDS connection: {}", e);
                                sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                }
            }
            *status_arc.lock().unwrap() = ProviderConnectionStatus::Disconnected;
        });

        Ok(())
    }

    #[cfg(not(unix))]
    async fn connect(&mut self) -> Result<(), AdapterError> {
        Err(AdapterError::ConnectionClosed(
            "Unix Domain Sockets require a Unix platform".to_string(),
        ))
    }

    async fn disconnect(&mut self) -> Result<(), AdapterError> {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if Path::new(&self.socket_path).exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
        *self.status.lock().unwrap() = ProviderConnectionStatus::Disconnected;
        Ok(())
    }
}

pub fn translate_validator_payload(
    payload: ValidatorPayload,
    provider: &ProviderId,
    counter: &Arc<AtomicU64>,
    counters: &Arc<UdsTelemetryCounters>,
    last_slot: &Arc<AtomicU64>,
    observer_ctx: ObserverContext,
    pending_replacement: &mut Option<(u64, u64)>,
) -> Vec<CoreStreamEvent> {
    let mut events = Vec::new();
    match payload {
        ValidatorPayload::SlotStatus {
            slot,
            parent_slot,
            bank_id,
            status,
        } => {
            counters.slot_events.fetch_add(1, Ordering::Relaxed);
            last_slot.store(slot, Ordering::Relaxed);
            let s = Slot(slot);
            events.push(
                CoreStreamEvent::new(
                    counter.fetch_add(1, Ordering::Relaxed),
                    provider.clone(),
                    s,
                    None,
                    CoreEventKind::SlotObserved {
                        parent_slot: parent_slot.map(Slot),
                    },
                )
                .with_observer(observer_ctx.clone()),
            );
            events.push(
                CoreStreamEvent::new(
                    counter.fetch_add(1, Ordering::Relaxed),
                    provider.clone(),
                    s,
                    None,
                    CoreEventKind::SlotStatusChanged {
                        slot: s,
                        status,
                        bank_id: bank_id.map(BankId),
                    },
                )
                .with_observer(observer_ctx),
            );
        }
        ValidatorPayload::BankLifecycle {
            slot,
            bank_id,
            parent_slot,
            status,
            bank_hash,
        } => {
            counters.bank_events.fetch_add(1, Ordering::Relaxed);
            let s = Slot(slot);
            let b_id = BankId(bank_id);
            let parent_ref = parent_slot
                .map(|ps| ParentReference::SlotOnly(Slot(ps)))
                .unwrap_or(ParentReference::Unknown);
            let b_hash = bank_hash.map(Blockhash::new);
            let ident = BankIdentity::new(provider.clone(), Some(b_id), s, b_hash.clone());

            let bank_status = match status.as_str() {
                "Created" | "CreatedBank" => BankStatus::CreatedBank,
                "Processed" => BankStatus::Processed,
                "Confirmed" => BankStatus::Confirmed,
                "Rooted" => BankStatus::Rooted,
                "Dead" => BankStatus::Dead,
                _ => BankStatus::Unknown,
            };
            let bank_state = match status.as_str() {
                "Confirmed" | "Rooted" => BankState::Canonical,
                "Dead" | "Abandoned" => BankState::Abandoned,
                _ => BankState::Observed,
            };

            events.push(
                CoreStreamEvent::new(
                    counter.fetch_add(1, Ordering::Relaxed),
                    provider.clone(),
                    s,
                    b_hash,
                    CoreEventKind::BankObserved {
                        identity: ident.clone(),
                        parent: parent_ref,
                        state: bank_state,
                    },
                )
                .with_observer(observer_ctx.clone()),
            );
            events.push(
                CoreStreamEvent::new(
                    counter.fetch_add(1, Ordering::Relaxed),
                    provider.clone(),
                    s,
                    None,
                    CoreEventKind::BankStatusChanged {
                        identity: ident.clone(),
                        status: bank_status,
                    },
                )
                .with_observer(observer_ctx.clone()),
            );

            if let Some(cleared) = pending_replacement.take() {
                if cleared.0 == slot {
                    let old_ident =
                        BankIdentity::new(provider.clone(), Some(BankId(cleared.1)), s, None);
                    events.push(
                        CoreStreamEvent::new(
                            counter.fetch_add(1, Ordering::Relaxed),
                            provider.clone(),
                            s,
                            None,
                            CoreEventKind::ReplacementBank {
                                old_identity: old_ident,
                                replacement_identity: ident,
                                slot: s,
                            },
                        )
                        .with_observer(observer_ctx),
                    );
                }
            }
        }
        ValidatorPayload::BlockFooter {
            slot,
            bank_id,
            bank_hash,
            producer_time_nanos,
            user_agent,
            final_cert,
            notar_cert,
            skip_cert,
        } => {
            counters.block_footer_events.fetch_add(1, Ordering::Relaxed);
            let s = Slot(slot);
            let received_nanos = now_unix_nanos();
            let footer = BlockFooter {
                slot: s,
                bank_id: Some(BankId(bank_id)),
                bank_hash: Some(BankHash::new(bank_hash)),
                block_producer_time_nanos: Some(producer_time_nanos),
                block_user_agent: Some(user_agent),
                block_final_cert: final_cert.clone(),
                skip_reward_cert: skip_cert,
                notar_reward_cert: notar_cert,
                raw_payload: None,
                received_at_nanos: received_nanos,
            };
            events.push(
                CoreStreamEvent::new(
                    counter.fetch_add(1, Ordering::Relaxed),
                    provider.clone(),
                    s,
                    None,
                    CoreEventKind::BlockFooterObserved { footer },
                )
                .with_observer(observer_ctx.clone()),
            );

            let interval = (received_nanos as i64) - (producer_time_nanos as i64);
            events.push(
                CoreStreamEvent::new(
                    counter.fetch_add(1, Ordering::Relaxed),
                    provider.clone(),
                    s,
                    None,
                    CoreEventKind::ProducerTimingObserved {
                        slot: s,
                        producer_time_nanos,
                        received_at_nanos: received_nanos,
                        interval_nanos: interval,
                    },
                )
                .with_observer(observer_ctx.clone()),
            );

            if let Some(cert_bytes) = final_cert {
                counters.certificate_events.fetch_add(1, Ordering::Relaxed);
                let raw_len = cert_bytes.len();
                events.push(
                    CoreStreamEvent::new(
                        counter.fetch_add(1, Ordering::Relaxed),
                        provider.clone(),
                        s,
                        None,
                        CoreEventKind::CertificateObserved {
                            slot: s,
                            kind: CertificateKind::FinalCert,
                            raw_len,
                            raw_bytes: Some(cert_bytes),
                            block_id: None,
                            validation_status: Some(CertificateValidationStatus::ParsedUnverified),
                            stake_percent: Some(80.0),
                        },
                    )
                    .with_observer(observer_ctx),
                );
            }
        }
        ValidatorPayload::UpdateParent {
            slot,
            cleared_bank_id,
            parent_slot,
            parent_block_id,
            fec_set_index,
            source,
        } => {
            counters.update_parent_events.fetch_add(1, Ordering::Relaxed);
            if let Some(bid) = cleared_bank_id {
                *pending_replacement = Some((slot, bid));
            }
            events.push(
                CoreStreamEvent::new(
                    counter.fetch_add(1, Ordering::Relaxed),
                    provider.clone(),
                    Slot(slot),
                    None,
                    CoreEventKind::UpdateParent {
                        slot: Slot(slot),
                        cleared_bank_id: cleared_bank_id.map(BankId),
                        parent_slot: Slot(parent_slot),
                        parent_block_id,
                        fec_set_index: fec_set_index.map(|i| i as u64),
                        source,
                    },
                )
                .with_observer(observer_ctx),
            );
        }
        ValidatorPayload::DeshredTx {
            slot,
            signature,
            raw_bytes,
            static_accounts,
            pre_execution_timestamp_nanos,
            fec_set_index,
        } => {
            counters.deshred_events.fetch_add(1, Ordering::Relaxed);
            events.push(
                CoreStreamEvent::new(
                    counter.fetch_add(1, Ordering::Relaxed),
                    provider.clone(),
                    Slot(slot),
                    None,
                    CoreEventKind::DeshredObserved {
                        slot: Slot(slot),
                        signature,
                        raw_tx_bytes: Some(raw_bytes),
                        pre_execution_timestamp_nanos: Some(pre_execution_timestamp_nanos),
                        static_accounts,
                        fec_set_index,
                    },
                )
                .with_observer(observer_ctx),
            );
        }
        ValidatorPayload::Entry {
            slot,
            bank_id,
            entry_index,
            tx_count,
            ..
        } => {
            counters.entry_events.fetch_add(1, Ordering::Relaxed);
            events.push(
                CoreStreamEvent::new(
                    counter.fetch_add(1, Ordering::Relaxed),
                    provider.clone(),
                    Slot(slot),
                    None,
                    CoreEventKind::EntryObserved {
                        slot: Slot(slot),
                        bank_id: bank_id.map(BankId),
                        entry_index,
                        tx_count,
                    },
                )
                .with_observer(observer_ctx),
            );
        }
        ValidatorPayload::BlockMeta {
            slot,
            blockhash,
            parent_slot,
            parent_blockhash,
            ..
        } => {
            let b_hash = Blockhash::new(blockhash);
            events.push(
                CoreStreamEvent::new(
                    counter.fetch_add(1, Ordering::Relaxed),
                    provider.clone(),
                    Slot(slot),
                    Some(b_hash.clone()),
                    CoreEventKind::BlockMetaObserved {
                        slot: Slot(slot),
                        blockhash: b_hash,
                        parent_slot: Slot(parent_slot),
                        parent_blockhash: Blockhash::new(parent_blockhash),
                        bank_id: None,
                    },
                )
                .with_observer(observer_ctx),
            );
        }
        ValidatorPayload::Heartbeat { .. } => {}
    }
    events
}

/// Deterministic offline simulator / fixture stream adapter for instant local testing.
pub struct SimulatorFixtureAdapter {
    provider_id: ProviderId,
    event_counter: Arc<AtomicU64>,
    slot_cursor: Arc<AtomicU64>,
    capabilities: ProviderCapabilityMatrix,
}

impl SimulatorFixtureAdapter {
    pub fn new() -> Self {
        let provider_id = ProviderId::new("offline-simulator");
        let capabilities = ProviderCapabilityMatrix::simulator_fixture(provider_id.as_str());
        Self {
            provider_id,
            event_counter: Arc::new(AtomicU64::new(1)),
            slot_cursor: Arc::new(AtomicU64::new(1_000_000)),
            capabilities,
        }
    }

    pub fn capability_matrix(&self) -> &ProviderCapabilityMatrix {
        &self.capabilities
    }

    /// Emits a deterministic full-fidelity slot and fork lifecycle batch advancing the slot cursor.
    pub fn emit_slot_batch(&self) -> Vec<CoreStreamEvent> {
        let mut events = Vec::new();
        let raw_slot = self.slot_cursor.fetch_add(1, Ordering::Relaxed);
        let slot = Slot(raw_slot);
        let parent_slot = Slot(raw_slot.saturating_sub(1));
        let provider = self.provider_id.clone();

        let leader_alpha = LeaderId::new("ValidatorAlpha11111111111111111111111111111");
        let leader_beta = LeaderId::new("ValidatorBeta111111111111111111111111111111");
        let active_leader = if (raw_slot / 4) % 2 == 0 {
            leader_alpha.clone()
        } else {
            leader_beta.clone()
        };
        let next_leader = if ((raw_slot + 1) / 4) % 2 == 0 {
            leader_alpha.clone()
        } else {
            leader_beta.clone()
        };

        // 1. SlotObserved
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider.clone(),
            slot,
            None,
            CoreEventKind::SlotObserved {
                parent_slot: Some(parent_slot),
            },
        ));

        // 2. LeaderScheduleObserved & LeaderObserved
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider.clone(),
            slot,
            None,
            CoreEventKind::LeaderScheduleObserved {
                starting_slot: slot,
                leaders: vec![
                    active_leader.clone(),
                    next_leader,
                    leader_alpha,
                    leader_beta,
                ],
            },
        ));
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider.clone(),
            slot,
            None,
            CoreEventKind::LeaderObserved {
                leader: active_leader,
            },
        ));

        // 3. Candidate Bank A created
        let bank_a_hash = Blockhash::new(format!("CandidateHashA_{}", raw_slot));
        let bank_a_ident = BankIdentity::new(
            provider.clone(),
            Some(BankId(1)),
            slot,
            Some(bank_a_hash.clone()),
        );
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider.clone(),
            slot,
            Some(bank_a_hash),
            CoreEventKind::BankCreated {
                identity: bank_a_ident.clone(),
                parent: ParentReference::SlotOnly(parent_slot),
            },
        ));

        // 4. EntryObserved on Bank A
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider.clone(),
            slot,
            None,
            CoreEventKind::EntryObserved {
                slot,
                bank_id: Some(BankId(1)),
                entry_index: 0,
                tx_count: 180,
            },
        ));

        // 5. UpdateParent clears Bank A and redirects lineage
        let parent_hash_str = format!("ParentBlockhash_{}", parent_slot.as_u64());
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider.clone(),
            slot,
            None,
            CoreEventKind::UpdateParent {
                slot,
                cleared_bank_id: Some(BankId(1)),
                parent_slot,
                parent_block_id: Some(parent_hash_str.clone()),
                fec_set_index: Some(0),
                source: "simulator_handover".to_string(),
            },
        ));

        // 6. Replacement Bank B created on authoritative parent
        let bank_b_hash = Blockhash::new(format!("CanonicalHashB_{}", raw_slot));
        let bank_b_ident = BankIdentity::new(
            provider.clone(),
            Some(BankId(2)),
            slot,
            Some(bank_b_hash.clone()),
        );
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider.clone(),
            slot,
            Some(bank_b_hash.clone()),
            CoreEventKind::BankCreated {
                identity: bank_b_ident.clone(),
                parent: ParentReference::KnownBlockhash(Blockhash::new(parent_hash_str)),
            },
        ));

        // 7. ReplacementBank correlation
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider.clone(),
            slot,
            None,
            CoreEventKind::ReplacementBank {
                old_identity: bank_a_ident,
                replacement_identity: bank_b_ident.clone(),
                slot,
            },
        ));

        // 8. BlockFooterObserved + CertificateObserved
        let mut cert_bytes = vec![0x7A; 48];
        cert_bytes.extend_from_slice(&[0b1111_1111, 0b1111_1111]);

        let footer = BlockFooter {
            slot,
            bank_id: Some(BankId(2)),
            bank_hash: Some(BankHash::new(format!("BankStateHash_{}", raw_slot))),
            block_producer_time_nanos: Some(1_700_000_000_123_456_000),
            block_user_agent: Some("agave-v2.1.0-simulator".to_string()),
            block_final_cert: Some(cert_bytes.clone()),
            skip_reward_cert: None,
            notar_reward_cert: Some(vec![0x3C; 48]),
            raw_payload: None,
            received_at_nanos: 1_700_000_000_125_000_000,
        };
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider.clone(),
            slot,
            Some(bank_b_hash.clone()),
            CoreEventKind::BlockFooterObserved { footer },
        ));
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider.clone(),
            slot,
            None,
            CoreEventKind::CertificateObserved {
                slot,
                kind: CertificateKind::FinalCert,
                raw_len: cert_bytes.len(),
                raw_bytes: Some(cert_bytes),
                block_id: Some(bank_b_hash.0.clone()),
                validation_status: Some(CertificateValidationStatus::ParsedUnverified),
                stake_percent: Some(84.2),
            },
        ));

        // 9. CanonicalObserved + FinalizedObserved
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider.clone(),
            slot,
            Some(bank_b_hash.clone()),
            CoreEventKind::CanonicalObserved {
                canonical_identity: bank_b_ident.clone(),
            },
        ));
        events.push(CoreStreamEvent::new(
            self.event_counter.fetch_add(1, Ordering::Relaxed),
            provider,
            slot,
            Some(bank_b_hash),
            CoreEventKind::FinalizedObserved {
                identity: bank_b_ident,
                cert_type: "BLS_FAST_PATH_CERT".to_string(),
                latency_ms: Some(112),
            },
        ));

        events
    }
}

impl Default for SimulatorFixtureAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duplicate_slots_deduplicated_and_out_of_order_accepted() {
        let mut normalizer = StreamNormalizer::new(ProviderId::new("test-provider"), 100);

        let ev1 = normalizer.normalize_slot(RawSlotNotification {
            slot: Slot(502),
            parent: Some(Slot(501)),
            root: Some(Slot(470)),
        });
        assert!(ev1.is_some());

        // Duplicate slot 502 dropped
        let dup = normalizer.normalize_slot(RawSlotNotification {
            slot: Slot(502),
            parent: Some(Slot(501)),
            root: Some(Slot(470)),
        });
        assert!(dup.is_none());

        // Out-of-order slot 501 accepted cleanly
        let ooo = normalizer.normalize_slot(RawSlotNotification {
            slot: Slot(501),
            parent: Some(Slot(500)),
            root: None,
        });
        assert!(ooo.is_some());
        assert_eq!(normalizer.seen_count(), 2);
    }

    #[test]
    fn test_simulator_fixture_batch_coverage_and_events() {
        let adapter = SimulatorFixtureAdapter::new();
        assert_eq!(adapter.capability_matrix().coverage_score(), 100);
        let batch = adapter.emit_slot_batch();
        assert!(batch.len() >= 10);
        assert!(batch
            .iter()
            .any(|e| matches!(e.kind, CoreEventKind::UpdateParent { .. })));
        assert!(batch
            .iter()
            .any(|e| matches!(e.kind, CoreEventKind::ReplacementBank { .. })));
    }
}
