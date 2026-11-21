use crate::snapshot::LiveClusterSnapshot;
use live_ws_core::events::{CoreEventKind, CoreStreamEvent};
use live_ws_core::types::{now_unix_ms, ObserverContext};
use serde::{Deserialize, Serialize};

/// Field-level provenance tag attached to outbound WebSocket events and snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventProvenance {
    DIRECT,
    DERIVED,
    ESTIMATED,
    UNAVAILABLE,
}

/// Normalized WebSocket event payload with monotonic service sequence ID and provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveStreamEvent {
    pub schema_version: u32,
    pub sequence: u64,
    pub event_id: u64,
    pub cluster: String,
    pub source: String,
    pub environment: String,
    pub observed_at_ms: u64,
    pub received_at_ms: u64,
    pub slot: u64,
    pub bank_id: Option<u64>,
    pub blockhash: Option<String>,
    pub parent_slot: Option<u64>,
    pub parent_blockhash: Option<String>,
    pub provenance: EventProvenance,
    pub event_type: String,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub observer: Option<ObserverContext>,
}

/// Top-level WebSocket wire envelope emitted on `/ws` and `/api/v1/stream`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum StreamEnvelope {
    /// Sent immediately on WebSocket upgrade so new clients start from exact cluster state.
    Snapshot {
        sequence: u64,
        snapshot: LiveClusterSnapshot,
    },
    /// Monotonically sequenced live or replayed stream event.
    Event {
        event: LiveStreamEvent,
    },
    /// Emitted after replaying buffered events from the ring buffer for `?since_seq=`.
    ReplayComplete {
        since_seq: u64,
        replayed_count: usize,
        latest_seq: u64,
    },
    /// Sent when a client requests a sequence older than the ring buffer or lags behind broadcast.
    Lagged {
        requested_seq: Option<u64>,
        oldest_available_seq: u64,
        latest_seq: u64,
        dropped_events: u64,
        message: String,
        snapshot: LiveClusterSnapshot,
    },
    /// Periodic server liveness frame carrying current sequence and slot.
    Heartbeat {
        sequence: u64,
        slot: u64,
        timestamp_ms: u64,
        ws_clients: usize,
    },
    /// Application-level pong reply to client ping control frame.
    Pong {
        timestamp_ms: u64,
    },
}

/// Inbound JSON control message sent by WebSocket clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientControlMessage {
    Hello {
        client_id: Option<String>,
        since_seq: Option<u64>,
    },
    Replay {
        since_seq: u64,
    },
    Ping {
        timestamp_ms: Option<u64>,
    },
    Pong,
}

impl LiveStreamEvent {
    /// Converts a `CoreStreamEvent` into a wire-ready `LiveStreamEvent`.
    pub fn from_core_event(
        sequence: u64,
        cluster: &str,
        source: &str,
        environment: &str,
        event: &CoreStreamEvent,
    ) -> Self {
        let is_fixture = environment == "fixture";
        let default_prov = if is_fixture {
            EventProvenance::DERIVED
        } else {
            EventProvenance::DIRECT
        };

        let (event_type, bank_id, parent_slot, parent_blockhash, provenance, payload) =
            match &event.kind {
                CoreEventKind::SlotObserved { parent_slot } => (
                    "slot_update".to_string(),
                    None,
                    parent_slot.map(|s| s.as_u64()),
                    None,
                    default_prov,
                    serde_json::json!({
                        "slot": event.slot.as_u64(),
                        "parent_slot": parent_slot.map(|s| s.as_u64()),
                    }),
                ),
                CoreEventKind::SlotStatusChanged {
                    slot,
                    status,
                    bank_id,
                } => (
                    "slot_status".to_string(),
                    bank_id.as_ref().map(|b| b.0),
                    None,
                    None,
                    default_prov,
                    serde_json::json!({
                        "slot": slot.as_u64(),
                        "status": status,
                        "bank_id": bank_id.as_ref().map(|b| b.0),
                    }),
                ),
                CoreEventKind::LeaderObserved { leader } => (
                    "leader_handoff".to_string(),
                    None,
                    None,
                    None,
                    EventProvenance::DERIVED,
                    serde_json::json!({
                        "slot": event.slot.as_u64(),
                        "leader": leader.as_str(),
                    }),
                ),
                CoreEventKind::LeaderScheduleObserved {
                    starting_slot,
                    leaders,
                } => (
                    "leader_schedule".to_string(),
                    None,
                    None,
                    None,
                    EventProvenance::DERIVED,
                    serde_json::json!({
                        "starting_slot": starting_slot.as_u64(),
                        "count": leaders.len(),
                        "sample_leader": leaders.first().map(|l| l.as_str()).unwrap_or(""),
                    }),
                ),
                CoreEventKind::BankCreated { identity, parent } => (
                    "bank_created".to_string(),
                    identity.bank_id.as_ref().map(|b| b.0),
                    None,
                    None,
                    default_prov,
                    serde_json::json!({
                        "bank_id": identity.bank_id.as_ref().map(|b| b.0),
                        "slot": identity.slot.as_u64(),
                        "parent": format!("{:?}", parent),
                    }),
                ),
                CoreEventKind::BankObserved {
                    identity,
                    parent,
                    state,
                } => (
                    "fork_detected".to_string(),
                    identity.bank_id.as_ref().map(|b| b.0),
                    None,
                    None,
                    default_prov,
                    serde_json::json!({
                        "bank_id": identity.bank_id.as_ref().map(|b| b.0),
                        "slot": identity.slot.as_u64(),
                        "parent": format!("{:?}", parent),
                        "state": state.to_string(),
                    }),
                ),
                CoreEventKind::BankStatusChanged { identity, status } => (
                    "bank_status".to_string(),
                    identity.bank_id.as_ref().map(|b| b.0),
                    None,
                    None,
                    default_prov,
                    serde_json::json!({
                        "bank_id": identity.bank_id.as_ref().map(|b| b.0),
                        "status": status.to_string(),
                    }),
                ),
                CoreEventKind::UpdateParent {
                    slot,
                    cleared_bank_id,
                    parent_slot,
                    parent_block_id,
                    fec_set_index,
                    source,
                } => (
                    "update_parent".to_string(),
                    cleared_bank_id.as_ref().map(|b| b.0),
                    Some(parent_slot.as_u64()),
                    parent_block_id.clone(),
                    default_prov,
                    serde_json::json!({
                        "slot": slot.as_u64(),
                        "cleared_bank_id": cleared_bank_id.as_ref().map(|b| b.0),
                        "parent_slot": parent_slot.as_u64(),
                        "parent_block_id": parent_block_id,
                        "fec_set_index": fec_set_index,
                        "source": source,
                    }),
                ),
                CoreEventKind::ReplacementBank {
                    old_identity,
                    replacement_identity,
                    slot,
                } => (
                    "bank_replaced".to_string(),
                    replacement_identity.bank_id.as_ref().map(|b| b.0),
                    None,
                    None,
                    EventProvenance::DERIVED,
                    serde_json::json!({
                        "slot": slot.as_u64(),
                        "old_bank_id": old_identity.bank_id.as_ref().map(|b| b.0),
                        "replacement_bank_id": replacement_identity.bank_id.as_ref().map(|b| b.0),
                    }),
                ),
                CoreEventKind::BlockObserved {
                    blockhash,
                    parent_blockhash,
                    tx_count,
                } => (
                    "block_sealed".to_string(),
                    None,
                    None,
                    parent_blockhash.as_ref().map(|h| h.0.clone()),
                    EventProvenance::DIRECT,
                    serde_json::json!({
                        "slot": event.slot.as_u64(),
                        "blockhash": blockhash.0,
                        "parent_blockhash": parent_blockhash.as_ref().map(|h| &h.0),
                        "tx_count": tx_count,
                    }),
                ),
                CoreEventKind::BlockFooterObserved { footer } => (
                    "block_footer".to_string(),
                    footer.bank_id.as_ref().map(|b| b.0),
                    None,
                    None,
                    default_prov,
                    serde_json::json!({
                        "slot": footer.slot.as_u64(),
                        "bank_id": footer.bank_id.as_ref().map(|b| b.0),
                        "bank_hash": footer.bank_hash.as_ref().map(|h| &h.0),
                        "producer_time_nanos": footer.block_producer_time_nanos,
                        "user_agent": footer.block_user_agent,
                        "has_final_cert": footer.block_final_cert.is_some(),
                        "has_notar_cert": footer.notar_reward_cert.is_some(),
                    }),
                ),
                CoreEventKind::CertificateObserved {
                    slot,
                    kind,
                    raw_len,
                    raw_bytes,
                    block_id,
                    validation_status,
                    stake_percent,
                } => (
                    "certificate_observed".to_string(),
                    None,
                    None,
                    None,
                    default_prov,
                    serde_json::json!({
                        "slot": slot.as_u64(),
                        "kind": kind.to_string(),
                        "raw_len": raw_len,
                        "has_raw_bytes": raw_bytes.is_some(),
                        "block_id": block_id,
                        "validation_status": validation_status.map(|s| s.to_string()),
                        "stake_percent": stake_percent,
                    }),
                ),
                CoreEventKind::CanonicalObserved { canonical_identity } => (
                    "canonical_changed".to_string(),
                    canonical_identity.bank_id.as_ref().map(|b| b.0),
                    None,
                    None,
                    EventProvenance::DERIVED,
                    serde_json::json!({
                        "slot": canonical_identity.slot.as_u64(),
                        "bank_id": canonical_identity.bank_id.as_ref().map(|b| b.0),
                        "blockhash": canonical_identity.blockhash.as_ref().map(|h| &h.0),
                    }),
                ),
                CoreEventKind::FinalizedObserved {
                    identity,
                    cert_type,
                    latency_ms,
                } => (
                    "finality_changed".to_string(),
                    identity.bank_id.as_ref().map(|b| b.0),
                    None,
                    None,
                    default_prov,
                    serde_json::json!({
                        "slot": identity.slot.as_u64(),
                        "bank_id": identity.bank_id.as_ref().map(|b| b.0),
                        "cert_type": cert_type,
                        "latency_ms": latency_ms,
                    }),
                ),
                CoreEventKind::DeshredObserved {
                    slot,
                    signature,
                    raw_tx_bytes,
                    pre_execution_timestamp_nanos,
                    static_accounts,
                    fec_set_index,
                } => (
                    "deshred_tx".to_string(),
                    None,
                    None,
                    None,
                    default_prov,
                    serde_json::json!({
                        "slot": slot.as_u64(),
                        "signature": signature,
                        "raw_tx_len": raw_tx_bytes.as_ref().map(|b| b.len()),
                        "pre_execution_timestamp_nanos": pre_execution_timestamp_nanos,
                        "static_accounts": static_accounts,
                        "fec_set_index": fec_set_index,
                    }),
                ),
                CoreEventKind::ProducerTimingObserved {
                    slot,
                    producer_time_nanos,
                    received_at_nanos,
                    interval_nanos,
                } => (
                    "producer_timing".to_string(),
                    None,
                    None,
                    None,
                    default_prov,
                    serde_json::json!({
                        "slot": slot.as_u64(),
                        "producer_time_nanos": producer_time_nanos,
                        "received_at_nanos": received_at_nanos,
                        "interval_nanos": interval_nanos,
                        "interval_ms": (*interval_nanos as f64) / 1_000_000.0,
                    }),
                ),
                CoreEventKind::ProviderConnected { provider } => (
                    "stream_connected".to_string(),
                    None,
                    None,
                    None,
                    EventProvenance::DIRECT,
                    serde_json::json!({
                        "provider": provider.as_str(),
                    }),
                ),
                CoreEventKind::ProviderDisconnected { provider, reason } => (
                    "stream_disconnected".to_string(),
                    None,
                    None,
                    None,
                    EventProvenance::DIRECT,
                    serde_json::json!({
                        "provider": provider.as_str(),
                        "reason": reason,
                    }),
                ),
                other => (
                    "consensus_event".to_string(),
                    None,
                    None,
                    None,
                    EventProvenance::DERIVED,
                    serde_json::json!({
                        "detail": format!("{:?}", other),
                    }),
                ),
            };

        Self {
            schema_version: 1,
            sequence,
            event_id: event.event_id,
            cluster: cluster.to_string(),
            source: source.to_string(),
            environment: environment.to_string(),
            observed_at_ms: event.received_time_ms,
            received_at_ms: now_unix_ms(),
            slot: event.slot.as_u64(),
            bank_id,
            blockhash: event.blockhash.as_ref().map(|h| h.0.clone()),
            parent_slot,
            parent_blockhash,
            provenance,
            event_type,
            payload,
            observer: event.observer.clone(),
        }
    }
}
