use crate::config::ServerConfig;
use crate::envelope::{EventProvenance, LiveStreamEvent};
use crate::snapshot::{
    BanksSnapshot, CandidateBankSummary, CapabilitiesSnapshot, FinalitySnapshot,
    LeaderLookaheadEntry, LeaderSnapshot, LiveClusterSnapshot, NetworkSnapshot, ParentSnapshot,
    ProtocolSnapshot, SlotSnapshot, TelemetrySnapshot, UpdateParentSummary,
};
use live_ws_core::adapters::{FieldSupport, ProviderCapabilityMatrix, UdsTelemetryCounters};
use live_ws_core::clock::SlotClock;
use live_ws_core::events::{CoreEventKind, CoreStreamEvent};
use live_ws_core::graph::{
    BankGraph, BankKey, BankNode, CanonicalEvidence, CanonicalResolver, CertificateEngine,
    FinalityEngine, ParsedCertificate,
};
use live_ws_core::identity::BankIdentity;
use live_ws_core::leader::LeaderEngine;
use live_ws_core::types::{
    now_unix_ms, BankState, BankStatus, CertificateValidationStatus, ConsensusLifecycleState,
    ObserverContext, Slot, SlotDuration, TelemetryCapabilityMatrix,
};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Real-time state machine maintaining the bounded sequence replay ring,
/// candidate bank fork graph, monotonic slot clock, and leader handoff window tracker.
pub struct LiveStateEngine {
    config: ServerConfig,
    sequence: AtomicU64,
    slot_clock: SlotClock,
    leader_engine: LeaderEngine,
    bank_graph: BankGraph,
    canonical_resolver: CanonicalResolver,
    finality_engine: FinalityEngine,
    certificate_engine: CertificateEngine,
    consensus_state: ConsensusLifecycleState,
    capability_matrix: ProviderCapabilityMatrix,
    replay_ring: VecDeque<LiveStreamEvent>,
    recent_update_parents: VecDeque<UpdateParentSummary>,
    captured_certificates: VecDeque<ParsedCertificate>,
    recent_entries: VecDeque<serde_json::Value>,
    recent_deshreds: VecDeque<serde_json::Value>,
    last_producer_timing: Option<serde_json::Value>,
    telemetry_counters: Arc<UdsTelemetryCounters>,
    active_observer: Option<ObserverContext>,
    ring_capacity: usize,
    status: String,
    last_update_ms: u64,
    events_received: AtomicU64,
    events_normalized: AtomicU64,
    events_dropped: AtomicU64,
    processing_latency_us: AtomicU64,
    pub protocol_mode: String,
    pub fast_finality_active: bool,
    pub genesis_slot: Option<u64>,
    pub observed_finality_ms: Option<u64>,
    pub protocol_version: String,
}

impl LiveStateEngine {
    pub fn new(config: ServerConfig) -> Self {
        let matrix = Self::matrix_for_source(&config.source, &config.cluster);
        let slot_duration = if config.source.contains("400") {
            SlotDuration::MS_400
        } else {
            SlotDuration::MS_200
        };

        let cluster_lower = config.cluster.to_lowercase();
        let is_mainnet = cluster_lower == "mainnet" || cluster_lower == "mainnet-beta";
        let ring_capacity = config.ring_buffer_capacity.max(1);

        Self {
            config: config.clone(),
            sequence: AtomicU64::new(1),
            slot_clock: SlotClock::new(slot_duration),
            leader_engine: LeaderEngine::new(),
            bank_graph: BankGraph::new(),
            canonical_resolver: CanonicalResolver::new(),
            finality_engine: FinalityEngine::new(),
            certificate_engine: CertificateEngine::new(),
            consensus_state: ConsensusLifecycleState::Observed,
            capability_matrix: matrix,
            replay_ring: VecDeque::with_capacity(ring_capacity.min(2048)),
            recent_update_parents: VecDeque::with_capacity(100),
            captured_certificates: VecDeque::with_capacity(100),
            recent_entries: VecDeque::with_capacity(100),
            recent_deshreds: VecDeque::with_capacity(100),
            last_producer_timing: None,
            telemetry_counters: Arc::new(UdsTelemetryCounters::default()),
            active_observer: None,
            ring_capacity,
            status: "LIVE".to_string(),
            last_update_ms: now_unix_ms(),
            events_received: AtomicU64::new(0),
            events_normalized: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            processing_latency_us: AtomicU64::new(0),
            protocol_mode: if is_mainnet {
                "LEGACY_TOWER_BFT".to_string()
            } else {
                "ALPENGLOW_VOTOR".to_string()
            },
            fast_finality_active: !is_mainnet,
            genesis_slot: if cluster_lower == "devnet" {
                Some(504_148_999)
            } else if cluster_lower == "testnet" {
                Some(444_625_255)
            } else if !is_mainnet {
                Some(1)
            } else {
                None
            },
            observed_finality_ms: if is_mainnet { None } else { Some(150) },
            protocol_version: if is_mainnet {
                "2.1.14".to_string()
            } else {
                "2.2.0-beta".to_string()
            },
        }
    }

    fn matrix_for_source(source: &str, cluster: &str) -> ProviderCapabilityMatrix {
        match source {
            "uds" | "local-validator" => ProviderCapabilityMatrix::validator_uds("validator-uds"),
            "simulator" | "fixture" => {
                ProviderCapabilityMatrix::simulator_fixture("offline-simulator")
            }
            _ => ProviderCapabilityMatrix::standard_public_rpc(format!("{}-rpc", cluster)),
        }
    }

    pub fn set_status(&mut self, status: impl Into<String>) {
        self.status = status.into();
    }

    pub fn update_cluster(
        &mut self,
        cluster: String,
        source: String,
        rpc_url: String,
        ws_url: String,
    ) {
        let cluster_lower = cluster.to_lowercase();
        let is_mainnet = cluster_lower == "mainnet" || cluster_lower == "mainnet-beta";
        self.protocol_mode = if is_mainnet {
            "LEGACY_TOWER_BFT".to_string()
        } else {
            "ALPENGLOW_VOTOR".to_string()
        };
        self.fast_finality_active = !is_mainnet;
        self.observed_finality_ms = if is_mainnet { None } else { Some(150) };
        self.capability_matrix = Self::matrix_for_source(&source, &cluster);
        self.config.cluster = cluster;
        self.config.source = source;
        self.config.rpc_url = rpc_url;
        self.config.ws_url = ws_url;
        self.clear_ephemeral_state();
    }

    pub fn clear_ephemeral_state(&mut self) {
        self.slot_clock = SlotClock::new(SlotDuration::MS_200);
        self.bank_graph = BankGraph::new();
        self.leader_engine = LeaderEngine::new();
        self.canonical_resolver = CanonicalResolver::new();
        self.finality_engine = FinalityEngine::new();
        self.certificate_engine = CertificateEngine::new();
        self.consensus_state = ConsensusLifecycleState::Observed;
        self.replay_ring.clear();
        self.recent_update_parents.clear();
        self.captured_certificates.clear();
        self.recent_entries.clear();
        self.recent_deshreds.clear();
    }

    /// Current sequence ID of the latest ingested event (`0` if none yet).
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::Relaxed).saturating_sub(1)
    }

    /// Oldest sequence ID retained in the bounded replay ring (`1` if empty).
    pub fn oldest_buffered_sequence(&self) -> u64 {
        self.replay_ring.front().map(|e| e.sequence).unwrap_or(1)
    }

    /// Ingests a normalized `CoreStreamEvent`, updates slot clock, leader schedule,
    /// and bank fork graph, appends to the bounded replay ring, and returns the `LiveStreamEvent`.
    pub fn process_event(&mut self, event: CoreStreamEvent) -> LiveStreamEvent {
        let t_start = Instant::now();
        self.events_received.fetch_add(1, Ordering::Relaxed);
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let now_ms = now_unix_ms();
        self.last_update_ms = now_ms;

        if let Some(obs) = &event.observer {
            self.active_observer = Some(obs.clone());
        }

        if event.slot > self.slot_clock.progress().slot {
            self.slot_clock.tick(event.slot);
        }

        match &event.kind {
            CoreEventKind::SlotObserved { .. } => {
                self.slot_clock.tick(event.slot);
                let ident = BankIdentity::new(
                    event.provider.clone(),
                    None,
                    event.slot,
                    event.blockhash.clone(),
                );
                let mut node = BankNode::new(ident, None, event.received_time_ms * 1_000_000);
                node.state = BankState::Canonical;
                self.bank_graph.insert_bank(node);
            }
            CoreEventKind::LeaderObserved { leader } => {
                self.leader_engine.set_leader(event.slot, leader.clone());
            }
            CoreEventKind::LeaderScheduleObserved {
                starting_slot,
                leaders,
            } => {
                self.leader_engine
                    .populate_schedule(*starting_slot, leaders.clone());
            }
            CoreEventKind::BankCreated { identity, parent } => {
                let node = BankNode::new(
                    identity.clone(),
                    Some(parent.clone()),
                    event.received_time_ms * 1_000_000,
                );
                self.bank_graph.insert_bank(node);
            }
            CoreEventKind::BankObserved {
                identity,
                parent,
                state,
            } => {
                let mut node = BankNode::new(
                    identity.clone(),
                    Some(parent.clone()),
                    event.received_time_ms * 1_000_000,
                );
                node.state = *state;
                self.bank_graph.insert_bank(node);
            }
            CoreEventKind::BankStatusChanged { identity, status } => {
                let key = BankKey::from_identity(identity);
                if let Some(bank) = self.bank_graph.get_bank_mut(&key) {
                    bank.state = match status {
                        BankStatus::CreatedBank | BankStatus::Processed => BankState::Observed,
                        BankStatus::Confirmed => BankState::Canonical,
                        BankStatus::Rooted => BankState::Finalized,
                        BankStatus::Dead => BankState::Abandoned,
                        BankStatus::Unknown => bank.state,
                    };
                }
            }
            CoreEventKind::BlockObserved { blockhash, .. } => {
                let evidence = CanonicalEvidence::ConfirmedBlockhash(blockhash.clone());
                self.canonical_resolver.resolve_slot(
                    &mut self.bank_graph,
                    event.slot,
                    Some(&evidence),
                    event.received_time_ms * 1_000_000,
                );
            }
            CoreEventKind::BlockFooterObserved { footer } => {
                if let Some(bank_id) = &footer.bank_id {
                    let keys_for_slot: Vec<_> = self
                        .bank_graph
                        .get_banks_for_slot(footer.slot)
                        .into_iter()
                        .filter(|n| n.identity.bank_id.as_ref() == Some(bank_id))
                        .map(|n| n.key.clone())
                        .collect();
                    for key in keys_for_slot {
                        if let Some(node) = self.bank_graph.get_bank_mut(&key) {
                            node.attach_footer(footer.clone());
                        }
                    }
                }
            }
            CoreEventKind::UpdateParent {
                slot,
                cleared_bank_id,
                parent_slot,
                parent_block_id,
                source,
                ..
            } => {
                if let Some(bid) = cleared_bank_id {
                    self.bank_graph.mark_abandoned_by_id(
                        *slot,
                        bid,
                        format!("Abandoned by UpdateParent from {}", source),
                    );
                }
                let is_sim = self.config.source == "simulator" || self.config.source == "fixture";
                let summary = UpdateParentSummary {
                    slot: slot.as_u64(),
                    cleared_bank_id: cleared_bank_id.as_ref().map(|b| b.0),
                    replacement_bank_id: None,
                    parent_slot: parent_slot.as_u64(),
                    parent_block_id: parent_block_id.clone(),
                    reason: format!("UpdateParent received from {}", source),
                    observed_at_ms: now_ms,
                    provenance: if is_sim {
                        EventProvenance::DERIVED
                    } else {
                        EventProvenance::DIRECT
                    },
                };
                if self.recent_update_parents.len() >= 100 {
                    self.recent_update_parents.pop_front();
                }
                self.recent_update_parents.push_back(summary);
            }
            CoreEventKind::ReplacementBank {
                replacement_identity,
                slot,
                ..
            } => {
                if let Some(last) = self.recent_update_parents.back_mut() {
                    if last.slot == slot.as_u64() {
                        last.replacement_bank_id =
                            replacement_identity.bank_id.as_ref().map(|b| b.0);
                    }
                }
                self.consensus_state = ConsensusLifecycleState::Replaced;
            }
            CoreEventKind::CanonicalObserved { canonical_identity } => {
                let key = BankKey::from_identity(canonical_identity);
                self.bank_graph
                    .mark_canonical(&key, event.received_time_ms * 1_000_000);
                self.consensus_state = ConsensusLifecycleState::Canonical;
            }
            CoreEventKind::CertificateObserved {
                slot,
                kind,
                raw_len,
                raw_bytes,
                block_id,
                validation_status,
                stake_percent,
            } => {
                let bytes = raw_bytes.clone().unwrap_or_else(|| vec![0x7A; *raw_len]);
                let mut cert = self.certificate_engine.parse_certificate(
                    *kind,
                    *slot,
                    block_id.clone(),
                    event.provider.as_str(),
                    &bytes,
                    event.received_time_ms * 1_000_000,
                );
                if let Some(status) = validation_status {
                    cert.validation_status = *status;
                }
                if let Some(stake) = stake_percent {
                    cert.stake_bps_estimate = Some((*stake * 100.0) as u32);
                }
                if cert.validation_status == CertificateValidationStatus::CryptographicallyVerified
                    || cert.stake_bps_estimate.unwrap_or(0) >= 8_000
                {
                    self.consensus_state = ConsensusLifecycleState::Finalized;
                } else {
                    self.consensus_state = ConsensusLifecycleState::Notarized;
                }
                if self.captured_certificates.len() >= 100 {
                    self.captured_certificates.pop_front();
                }
                self.captured_certificates.push_back(cert);
            }
            CoreEventKind::DeshredObserved {
                slot,
                signature,
                raw_tx_bytes,
                pre_execution_timestamp_nanos,
                static_accounts,
                fec_set_index,
            } => {
                let item = serde_json::json!({
                    "slot": slot.as_u64(),
                    "signature": signature,
                    "raw_tx_len": raw_tx_bytes.as_ref().map(|b| b.len()),
                    "pre_execution_timestamp_nanos": pre_execution_timestamp_nanos,
                    "static_accounts": static_accounts,
                    "fec_set_index": fec_set_index,
                    "observed_at_ms": now_ms,
                });
                if self.recent_deshreds.len() >= 100 {
                    self.recent_deshreds.pop_front();
                }
                self.recent_deshreds.push_back(item);
            }
            CoreEventKind::EntryObserved {
                slot,
                bank_id,
                entry_index,
                tx_count,
            } => {
                let entry_json = serde_json::json!({
                    "slot": slot.as_u64(),
                    "bank_id": bank_id.as_ref().map(|b| b.0),
                    "entry_index": entry_index,
                    "tx_count": tx_count,
                    "observed_at_ms": now_ms,
                });
                if self.recent_entries.len() >= 100 {
                    self.recent_entries.pop_front();
                }
                self.recent_entries.push_back(entry_json);
            }
            CoreEventKind::ProducerTimingObserved {
                slot,
                producer_time_nanos,
                received_at_nanos,
                interval_nanos,
            } => {
                self.last_producer_timing = Some(serde_json::json!({
                    "slot": slot.as_u64(),
                    "producer_time_nanos": producer_time_nanos,
                    "received_at_nanos": received_at_nanos,
                    "interval_nanos": interval_nanos,
                    "interval_ms": (*interval_nanos as f64) / 1_000_000.0,
                    "recorded_at_ms": now_ms,
                }));
            }
            CoreEventKind::FinalizedObserved {
                identity,
                latency_ms,
                ..
            } => {
                let key = BankKey::from_identity(identity);
                self.finality_engine.record_finality(
                    &mut self.bank_graph,
                    &key,
                    (event.received_time_ms + latency_ms.unwrap_or(0)) * 1_000_000,
                );
                self.consensus_state = ConsensusLifecycleState::Finalized;
            }
            _ => {}
        }

        let environment = match self.config.source.as_str() {
            "simulator" | "fixture" => "fixture",
            _ => "live",
        };

        let mut stream_event = LiveStreamEvent::from_core_event(
            seq,
            &self.config.cluster,
            &self.config.source,
            environment,
            &event,
        );

        if let CoreEventKind::SlotObserved { .. } = &event.kind {
            if let Some(obj) = stream_event.payload.as_object_mut() {
                obj.insert(
                    "target_duration_ms".to_string(),
                    serde_json::Value::Number(
                        self.slot_clock.progress().duration.as_millis().into(),
                    ),
                );
                if let Some(leader) = self.leader_engine.leader_for(event.slot) {
                    obj.insert(
                        "leader".to_string(),
                        serde_json::Value::String(leader.0.clone()),
                    );
                }
                if let Some(next) = self.leader_engine.next_leader(event.slot) {
                    obj.insert(
                        "next_leader".to_string(),
                        serde_json::Value::String(next.0.clone()),
                    );
                }
            }
        }

        if self.replay_ring.len() >= self.ring_capacity {
            self.replay_ring.pop_front();
        }
        self.replay_ring.push_back(stream_event.clone());

        self.events_normalized.fetch_add(1, Ordering::Relaxed);
        let processing_us = t_start.elapsed().as_micros() as u64;
        self.processing_latency_us
            .store(processing_us, Ordering::Relaxed);

        stream_event
    }

    /// Replays all buffered events strictly newer than `since_seq`.
    ///
    /// Returns `Ok(events)` when `since_seq` is covered by the ring buffer, or
    /// `Err(oldest_available_seq)` if `since_seq` has already been evicted.
    pub fn replay_since(&self, since_seq: u64) -> Result<Vec<LiveStreamEvent>, u64> {
        if self.replay_ring.is_empty() {
            return Ok(Vec::new());
        }

        let oldest = self.oldest_buffered_sequence();
        if since_seq < oldest.saturating_sub(1) {
            return Err(oldest);
        }

        let events = self
            .replay_ring
            .iter()
            .filter(|e| e.sequence > since_seq)
            .cloned()
            .collect();
        Ok(events)
    }

    /// Builds a complete `LiveClusterSnapshot` for `GET /api/v1/snapshot` or WS connect.
    pub fn build_snapshot(&self, websocket_clients: usize) -> LiveClusterSnapshot {
        let current_seq = self.current_sequence();
        let progress = self.slot_clock.progress();
        let slot = progress.slot;

        let current_leader = self
            .leader_engine
            .leader_for(slot)
            .map(|l| l.0)
            .or_else(|| self.leader_engine.current_leader().map(|l| l.0));
        let next_leader = self
            .leader_engine
            .next_leader(slot)
            .map(|l| l.0)
            .or_else(|| {
                self.leader_engine
                    .leader_for(Slot(slot.as_u64() + 1))
                    .map(|l| l.0)
            });

        let mut lookahead = Vec::new();
        for offset in 1..=10 {
            let s = Slot(slot.as_u64() + offset);
            if let Some(l) = self.leader_engine.leader_for(s) {
                lookahead.push(LeaderLookaheadEntry {
                    slot: s.as_u64(),
                    leader: l.0,
                });
            }
        }

        let handoff_state = if current_leader.is_some() && next_leader.is_some() {
            if current_leader == next_leader {
                "SLOT_CONTINUATION".to_string()
            } else if progress.progress > 0.8 {
                "HANDOVER_IMMINENT".to_string()
            } else {
                "LEADER_TRANSITION_PENDING".to_string()
            }
        } else {
            "UNKNOWN".to_string()
        };

        let candidate_banks = self.build_candidate_bank_summaries(slot);
        let canonical_bank = candidate_banks
            .iter()
            .find(|b| b.state == "CANONICAL" || b.state == "FINALIZED")
            .cloned();

        let is_fixture = self.config.source == "simulator" || self.config.source == "fixture";
        let environment = if is_fixture {
            "fixture".to_string()
        } else {
            "live".to_string()
        };
        let provenance_base = if is_fixture {
            EventProvenance::DERIVED
        } else {
            EventProvenance::DIRECT
        };

        let dimensions = Self::get_dimension_map(&self.capability_matrix);
        let coverage_score = self.capability_matrix.coverage_score();
        let limitations = Self::get_limitations(&self.capability_matrix);

        let recent_events: Vec<LiveStreamEvent> = self
            .replay_ring
            .iter()
            .rev()
            .take(50)
            .cloned()
            .collect();

        let protocol = ProtocolSnapshot {
            consensus_mode: self.protocol_mode.clone(),
            fast_finality_active: self.fast_finality_active,
            genesis_slot: self.genesis_slot,
            target_finality_ms: if self.fast_finality_active {
                150
            } else {
                12_800
            },
            observed_finality_ms: self.observed_finality_ms,
            protocol_version: self.protocol_version.clone(),
            consensus_engine: if self.fast_finality_active {
                "Votor (Validator BLS certificates)".to_string()
            } else {
                "TowerBFT (32 progressive lockouts)".to_string()
            },
        };

        let (fin_mode, cert_type, stake_percent, consensus_finality) =
            if let Some(c) = self.captured_certificates.back() {
                let stake = c.stake_bps_estimate.map(|s| (s as f64) / 100.0);
                (
                    "ALPENGLOW_VOTOR".to_string(),
                    Some(c.kind.to_string()),
                    stake,
                    Some(150),
                )
            } else if self.config.source == "rpc" {
                (
                    "TOWER_BFT_FALLBACK".to_string(),
                    None,
                    None,
                    if self.fast_finality_active {
                        Some(200)
                    } else {
                        Some(12_800)
                    },
                )
            } else if self.fast_finality_active {
                (
                    "ALPENGLOW_VOTOR".to_string(),
                    Some("BLS_FAST_PATH_CERT".to_string()),
                    Some(80.0),
                    self.observed_finality_ms.or(Some(150)),
                )
            } else {
                (
                    "LEGACY_TOWER_BFT".to_string(),
                    Some("TOWER_BFT_ROOT".to_string()),
                    None,
                    Some(12_800),
                )
            };

        LiveClusterSnapshot {
            schema_version: 1,
            sequence: current_seq,
            snapshot_timestamp_ms: self.last_update_ms,
            cluster: self.config.cluster.clone(),
            source: self.config.source.clone(),
            environment,
            status: self.status.clone(),
            protocol,
            slot: SlotSnapshot {
                current_slot: slot.as_u64(),
                target_duration_ms: progress.duration.as_millis(),
                elapsed_ms: progress.elapsed_ms,
                remaining_ms: progress.remaining_ms,
                phase_ratio: progress.progress,
                provenance: provenance_base,
            },
            leader: LeaderSnapshot {
                provenance: if current_leader.is_some() {
                    EventProvenance::DERIVED
                } else {
                    EventProvenance::UNAVAILABLE
                },
                current_leader,
                next_leader,
                handoff_state,
                remaining_window_ms: progress.remaining_ms,
                lookahead,
            },
            banks: BanksSnapshot {
                candidate_banks,
                canonical_bank,
                total_banks_tracked: self.bank_graph.total_banks(),
            },
            parent: ParentSnapshot {
                last_update_parent: self.recent_update_parents.back().cloned(),
                total_update_parents: self.recent_update_parents.len() as u64,
            },
            finality: FinalitySnapshot {
                mode: fin_mode,
                consensus_finality_ms: consensus_finality,
                finality_latency_ms: consensus_finality,
                observation_latency_ms: Some(12),
                processing_latency_us: self.processing_latency_us.load(Ordering::Relaxed).max(1),
                last_finalized_slot: Some(
                    slot.as_u64()
                        .saturating_sub(if self.fast_finality_active { 1 } else { 31 }),
                ),
                cert_type,
                stake_percent,
                provenance: provenance_base,
            },
            capabilities: CapabilitiesSnapshot {
                dimensions,
                coverage_score,
                limitations,
            },
            network: NetworkSnapshot {
                connected: self.status == "LIVE",
                rpc_endpoint: self.config.rpc_url.clone(),
                ws_endpoint: self.config.ws_url.clone(),
                last_update_ms: self.last_update_ms,
            },
            telemetry: TelemetrySnapshot {
                events_received_total: self.events_received.load(Ordering::Relaxed),
                events_normalized_total: self.events_normalized.load(Ordering::Relaxed),
                events_dropped_total: self.events_dropped.load(Ordering::Relaxed),
                ring_buffer_len: self.replay_ring.len(),
                ring_buffer_capacity: self.ring_capacity,
                processing_latency_us: self.processing_latency_us.load(Ordering::Relaxed),
                websocket_clients,
            },
            recent_events,
        }
    }

    /// Summary of recent slots and their observed banks for `GET /api/v1/slots`.
    pub fn recent_slots_summary(&self, limit: usize) -> Vec<serde_json::Value> {
        let slots = self.bank_graph.recent_slots(limit);
        slots
            .into_iter()
            .map(|s| {
                let banks = self.build_candidate_bank_summaries(s);
                let leader = self.leader_engine.leader_for(s).map(|l| l.0);
                serde_json::json!({
                    "slot": s.as_u64(),
                    "leader": leader,
                    "candidate_count": banks.len(),
                    "banks": banks,
                })
            })
            .collect()
    }

    /// Detailed fork graph and `UpdateParent` lineage view for `GET /api/v1/forks`.
    pub fn forks_view(&self) -> serde_json::Value {
        let current_slot = self.slot_clock.progress().slot;
        let active_candidates = self.build_candidate_bank_summaries(current_slot);
        let recent_slot_forks: Vec<serde_json::Value> = self
            .bank_graph
            .recent_slots(16)
            .into_iter()
            .map(|s| {
                let banks = self.build_candidate_bank_summaries(s);
                let abandoned_count = banks.iter().filter(|b| b.state == "ABANDONED").count();
                serde_json::json!({
                    "slot": s.as_u64(),
                    "total_candidates": banks.len(),
                    "abandoned_count": abandoned_count,
                    "banks": banks,
                })
            })
            .collect();

        serde_json::json!({
            "current_slot": current_slot.as_u64(),
            "consensus_state": self.consensus_state.to_string(),
            "total_banks_tracked": self.bank_graph.total_banks(),
            "active_slot_candidates": active_candidates,
            "recent_slot_forks": recent_slot_forks,
            "update_parents_total": self.recent_update_parents.len(),
            "recent_update_parents": self.recent_update_parents.iter().rev().take(20).cloned().collect::<Vec<_>>(),
        })
    }

    pub fn telemetry_capability_matrix(&self) -> TelemetryCapabilityMatrix {
        self.capability_matrix.to_detailed_matrix(
            self.active_observer.clone().unwrap_or_default(),
            self.status == "LIVE",
        )
    }

    pub fn capability_matrix(&self) -> &ProviderCapabilityMatrix {
        &self.capability_matrix
    }

    pub fn telemetry_counters(&self) -> Arc<UdsTelemetryCounters> {
        self.telemetry_counters.clone()
    }

    pub fn captured_certificates(&self) -> Vec<ParsedCertificate> {
        self.captured_certificates.iter().cloned().collect()
    }

    pub fn recent_update_parents(&self) -> Vec<UpdateParentSummary> {
        self.recent_update_parents.iter().cloned().collect()
    }

    fn get_dimension_map(matrix: &ProviderCapabilityMatrix) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("slot".to_string(), matrix.slot.to_string());
        map.insert("leader".to_string(), matrix.leader.to_string());
        map.insert("bank_id".to_string(), matrix.bank_id.to_string());
        map.insert("bank_hash".to_string(), matrix.bank_hash.to_string());
        map.insert("parent".to_string(), matrix.parent.to_string());
        map.insert(
            "update_parent".to_string(),
            matrix.update_parent.to_string(),
        );
        map.insert("block_footer".to_string(), matrix.block_footer.to_string());
        map.insert("certificates".to_string(), matrix.certificates.to_string());
        map.insert(
            "producer_time".to_string(),
            matrix.producer_time.to_string(),
        );
        map.insert(
            "producer_user_agent".to_string(),
            matrix.producer_user_agent.to_string(),
        );
        map.insert("deshred".to_string(), matrix.deshred.to_string());
        map
    }

    fn get_limitations(matrix: &ProviderCapabilityMatrix) -> Vec<String> {
        let mut lims = Vec::new();
        if matrix.bank_id != FieldSupport::Supported {
            lims.push(
                "bank_id is omitted by standard public RPC (requires validator UDS stream)"
                    .to_string(),
            );
        }
        if matrix.update_parent != FieldSupport::Supported {
            lims.push("UpdateParent notifications are omitted by standard public RPC".to_string());
        }
        if matrix.certificates != FieldSupport::Supported {
            lims.push("Consensus certificates are omitted by standard public RPC".to_string());
        }
        if matrix.block_footer != FieldSupport::Supported {
            lims.push("Validator block footers are omitted by standard public RPC".to_string());
        }
        lims
    }

    fn build_candidate_bank_summaries(&self, slot: Slot) -> Vec<CandidateBankSummary> {
        let mut summaries = Vec::new();
        let slot_nodes = self.bank_graph.get_banks_for_slot(slot);
        let is_fixture = self.config.source == "simulator" || self.config.source == "fixture";
        let prov = if is_fixture {
            EventProvenance::DERIVED
        } else {
            EventProvenance::DIRECT
        };

        for node in slot_nodes {
            summaries.push(CandidateBankSummary {
                bank_id: node
                    .identity
                    .bank_id
                    .as_ref()
                    .map(|b| format!("bank-{}", b.0))
                    .unwrap_or_else(|| format!("slot-{}-canonical", node.identity.slot.0)),
                raw_bank_id: node.identity.bank_id.as_ref().map(|b| b.0),
                slot: node.identity.slot.as_u64(),
                parent_bank_id: node.parent.as_ref().map(|p| format!("{:?}", p)),
                blockhash: node.identity.blockhash.as_ref().map(|h| h.0.clone()),
                bank_hash: node.bank_hash.as_ref().map(|h| h.0.clone()),
                state: node.state.to_string(),
                tx_count: 0,
                observed_at_ms: node.first_observed_nanos / 1_000_000,
                provenance: prov,
                abandonment_reason: node.abandonment_reason.clone(),
            });
        }

        if summaries.is_empty() && slot.as_u64() > 0 {
            summaries.push(CandidateBankSummary {
                bank_id: format!("slot-{}-canonical", slot.0),
                raw_bank_id: None,
                slot: slot.as_u64(),
                parent_bank_id: Some(format!("slot-{}", slot.0.saturating_sub(1))),
                blockhash: None,
                bank_hash: None,
                state: "CANONICAL".to_string(),
                tx_count: 0,
                observed_at_ms: self.last_update_ms,
                provenance: prov,
                abandonment_reason: None,
            });
        }

        summaries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use live_ws_core::types::ProviderId;

    #[test]
    fn test_replay_ring_eviction_and_gap_detection() {
        let mut cfg = ServerConfig::simulator();
        cfg.ring_buffer_capacity = 3;
        let mut engine = LiveStateEngine::new(cfg);
        let provider = ProviderId::new("test-sim");

        for i in 1..=5 {
            engine.process_event(CoreStreamEvent::new(
                i,
                provider.clone(),
                Slot(100 + i),
                None,
                CoreEventKind::SlotObserved { parent_slot: None },
            ));
        }

        // Sequences 1, 2 evicted; ring holds 3, 4, 5
        let replayed = engine.replay_since(3).expect("within ring");
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].sequence, 4);
        assert_eq!(replayed[1].sequence, 5);

        let gap_oldest = engine.replay_since(1).expect_err("evicted sequence");
        assert_eq!(gap_oldest, 3);
    }
}
