use crate::identity::BankIdentity;
use crate::types::{
    BankHash, BankId, BankState, BankStatus, BlockFooter, Blockhash, CertificateKind,
    CertificateValidationStatus, ParentReference, ProviderId, Slot,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Unique identifier for a bank node within the in-memory `BankGraph`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BankKey {
    pub provider_id: ProviderId,
    pub slot: Slot,
    pub bank_id: Option<BankId>,
    pub blockhash: Option<Blockhash>,
}

impl BankKey {
    pub fn from_identity(identity: &BankIdentity) -> Self {
        Self {
            provider_id: identity.provider_id.clone(),
            slot: identity.slot,
            bank_id: identity.bank_id.clone(),
            blockhash: identity.blockhash.clone(),
        }
    }
}

/// Candidate or confirmed bank in the Solana cluster fork graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankNode {
    pub key: BankKey,
    pub identity: BankIdentity,
    pub parent: Option<ParentReference>,
    pub node_identity: Option<String>,
    pub bank_hash: Option<BankHash>,
    pub producer_time_nanos: Option<u64>,
    pub producer_user_agent: Option<String>,
    pub bank_status: BankStatus,
    pub footer: Option<BlockFooter>,
    pub first_observed_nanos: u64,
    pub state: BankState,
    pub abandonment_reason: Option<String>,
    pub canonical_at_nanos: Option<u64>,
    pub finalized_at_nanos: Option<u64>,
}

impl BankNode {
    pub fn new(
        identity: BankIdentity,
        parent: Option<ParentReference>,
        observed_nanos: u64,
    ) -> Self {
        let key = BankKey::from_identity(&identity);
        Self {
            key,
            identity,
            parent,
            node_identity: None,
            bank_hash: None,
            producer_time_nanos: None,
            producer_user_agent: None,
            bank_status: BankStatus::Unknown,
            footer: None,
            first_observed_nanos: observed_nanos,
            state: BankState::Observed,
            abandonment_reason: None,
            canonical_at_nanos: None,
            finalized_at_nanos: None,
        }
    }

    pub fn attach_footer(&mut self, footer: BlockFooter) {
        if footer.bank_hash.is_some() {
            self.bank_hash = footer.bank_hash.clone();
        }
        if footer.block_producer_time_nanos.is_some() {
            self.producer_time_nanos = footer.block_producer_time_nanos;
        }
        if footer.block_user_agent.is_some() {
            self.producer_user_agent = footer.block_user_agent.clone();
        }
        self.footer = Some(footer);
    }

    pub fn update_bank_status(&mut self, status: BankStatus) {
        self.bank_status = status;
        match status {
            BankStatus::Rooted => {
                self.state = BankState::Finalized;
            }
            BankStatus::Dead => {
                self.state = BankState::Abandoned;
                if self.abandonment_reason.is_none() {
                    self.abandonment_reason = Some("Bank marked Dead by validator".to_string());
                }
            }
            _ => {}
        }
    }
}

/// Directed acyclic graph of candidate, abandoned, canonical, and finalized banks.
#[derive(Debug, Default, Clone)]
pub struct BankGraph {
    slot_index: BTreeMap<Slot, Vec<BankKey>>,
    nodes: HashMap<BankKey, BankNode>,
    blockhash_index: HashMap<Blockhash, Vec<BankKey>>,
}

impl BankGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_bank(&mut self, node: BankNode) -> BankKey {
        let key = node.key.clone();

        if let Some(existing) = self.nodes.get_mut(&key) {
            if existing.parent.is_none() && node.parent.is_some() {
                existing.parent = node.parent;
            }
            if existing.identity.blockhash.is_none() && node.identity.blockhash.is_some() {
                existing.identity.blockhash = node.identity.blockhash.clone();
                existing.key.blockhash = node.identity.blockhash.clone();
                if let Some(ref hash) = existing.identity.blockhash {
                    self.blockhash_index
                        .entry(hash.clone())
                        .or_default()
                        .push(key.clone());
                }
            }
            return key;
        }

        let slot = node.identity.slot;
        if let Some(ref hash) = node.identity.blockhash {
            self.blockhash_index
                .entry(hash.clone())
                .or_default()
                .push(key.clone());
        }

        self.slot_index.entry(slot).or_default().push(key.clone());
        self.nodes.insert(key.clone(), node);

        key
    }

    pub fn get_bank(&self, key: &BankKey) -> Option<&BankNode> {
        self.nodes.get(key)
    }

    pub fn get_bank_mut(&mut self, key: &BankKey) -> Option<&mut BankNode> {
        self.nodes.get_mut(key)
    }

    pub fn get_banks_for_slot(&self, slot: Slot) -> Vec<&BankNode> {
        self.slot_index
            .get(&slot)
            .map(|keys| keys.iter().filter_map(|k| self.nodes.get(k)).collect())
            .unwrap_or_default()
    }

    pub fn recent_slots(&self, limit: usize) -> Vec<Slot> {
        self.slot_index
            .keys()
            .rev()
            .take(limit)
            .copied()
            .collect()
    }

    pub fn total_banks(&self) -> usize {
        self.nodes.len()
    }

    pub fn mark_canonical(&mut self, key: &BankKey, timestamp_nanos: u64) -> bool {
        if let Some(node) = self.nodes.get_mut(key) {
            node.state = BankState::Canonical;
            node.canonical_at_nanos = Some(timestamp_nanos);
            true
        } else {
            false
        }
    }

    pub fn mark_abandoned(&mut self, key: &BankKey, reason: impl Into<String>) -> bool {
        if let Some(node) = self.nodes.get_mut(key) {
            node.state = BankState::Abandoned;
            node.abandonment_reason = Some(reason.into());
            true
        } else {
            false
        }
    }

    pub fn mark_abandoned_by_id(
        &mut self,
        slot: Slot,
        bank_id: &BankId,
        reason: impl Into<String>,
    ) -> bool {
        let reason_str = reason.into();
        let mut found = false;
        if let Some(keys) = self.slot_index.get(&slot).cloned() {
            for key in keys {
                if let Some(node) = self.nodes.get_mut(&key) {
                    if node.identity.bank_id.as_ref() == Some(bank_id) {
                        node.state = BankState::Abandoned;
                        node.abandonment_reason = Some(reason_str.clone());
                        found = true;
                    }
                }
            }
        }
        found
    }

    pub fn mark_finalized(&mut self, key: &BankKey, timestamp_nanos: u64) -> bool {
        if let Some(node) = self.nodes.get_mut(key) {
            node.state = BankState::Finalized;
            node.finalized_at_nanos = Some(timestamp_nanos);
            true
        } else {
            false
        }
    }

    pub fn find_by_blockhash(&self, hash: &Blockhash) -> Vec<&BankNode> {
        self.blockhash_index
            .get(hash)
            .map(|keys| keys.iter().filter_map(|k| self.nodes.get(k)).collect())
            .unwrap_or_default()
    }

    pub fn prune_older_than(&mut self, min_slot: Slot) -> usize {
        let mut pruned = 0;
        let old_slots: Vec<Slot> = self.slot_index.range(..min_slot).map(|(s, _)| *s).collect();

        for s in old_slots {
            if let Some(keys) = self.slot_index.remove(&s) {
                for k in keys {
                    if let Some(node) = self.nodes.remove(&k) {
                        pruned += 1;
                        if let Some(ref hash) = node.identity.blockhash {
                            if let Some(hashes) = self.blockhash_index.get_mut(hash) {
                                hashes.retain(|existing_k| existing_k != &k);
                            }
                        }
                    }
                }
            }
        }
        pruned
    }
}

/// Cryptographic or commitment evidence supporting canonical fork resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CanonicalEvidence {
    ConfirmedBlockhash(Blockhash),
    FinalizedRoot(Blockhash),
    CertifiedChildLineage(Blockhash),
}

/// Outcome of resolving the canonical bank for a slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CanonicalResolution {
    Resolved(BankKey),
    Ambiguous { slot: Slot, candidates_count: usize },
    NoCandidates(Slot),
}

/// Evidence-based canonical fork resolver. Never guesses or assumes first-seen is canonical.
#[derive(Debug, Default)]
pub struct CanonicalResolver;

impl CanonicalResolver {
    pub fn new() -> Self {
        Self
    }

    pub fn resolve_slot(
        &self,
        graph: &mut BankGraph,
        slot: Slot,
        evidence: Option<&CanonicalEvidence>,
        timestamp_nanos: u64,
    ) -> CanonicalResolution {
        let bank_summaries: Vec<(BankKey, Option<Blockhash>, BankState)> = graph
            .get_banks_for_slot(slot)
            .into_iter()
            .map(|b| (b.key.clone(), b.identity.blockhash.clone(), b.state))
            .collect();

        if bank_summaries.is_empty() {
            return CanonicalResolution::NoCandidates(slot);
        }

        if let Some(ev) = evidence {
            let target_hash = match ev {
                CanonicalEvidence::ConfirmedBlockhash(h) => h,
                CanonicalEvidence::FinalizedRoot(h) => h,
                CanonicalEvidence::CertifiedChildLineage(h) => h,
            };

            let matching_keys: Vec<BankKey> = bank_summaries
                .iter()
                .filter(|(_, hash, _)| hash.as_ref() == Some(target_hash))
                .map(|(key, _, _)| key.clone())
                .collect();

            if let Some(canonical_key) = matching_keys.first() {
                graph.mark_canonical(canonical_key, timestamp_nanos);

                let other_keys: Vec<BankKey> = bank_summaries
                    .iter()
                    .filter(|(key, _, _)| key != canonical_key)
                    .map(|(key, _, _)| key.clone())
                    .collect();

                for other_key in other_keys {
                    graph.mark_abandoned(
                        &other_key,
                        format!(
                            "Competing candidate abandoned in favor of confirmed canonical blockhash {}",
                            target_hash
                        ),
                    );
                }

                return CanonicalResolution::Resolved(canonical_key.clone());
            }
        }

        let certified: Vec<BankKey> = bank_summaries
            .iter()
            .filter(|(_, _, state)| *state == BankState::Canonical || *state == BankState::Finalized)
            .map(|(key, _, _)| key.clone())
            .collect();

        if let Some(key) = certified.first() {
            return CanonicalResolution::Resolved(key.clone());
        }

        CanonicalResolution::Ambiguous {
            slot,
            candidates_count: bank_summaries.len(),
        }
    }
}

/// Statistical latency summary for measured finality transitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySummary {
    pub sample_count: usize,
    pub min_us: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
    pub classification: &'static str,
}

/// Recorded finality transition for a bank node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalityRecord {
    pub slot: Slot,
    pub bank_key: BankKey,
    pub blockhash: Option<Blockhash>,
    pub observed_nanos: u64,
    pub canonical_nanos: Option<u64>,
    pub finalized_nanos: u64,
    pub canonical_latency_us: Option<u64>,
    pub finality_latency_us: u64,
}

/// Finality latency tracker backed by explicit root commitments or certificates.
#[derive(Debug, Default)]
pub struct FinalityEngine {
    records: Vec<FinalityRecord>,
    canonical_latencies_us: Vec<u64>,
    finality_latencies_us: Vec<u64>,
}

impl FinalityEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_finality(
        &mut self,
        graph: &mut BankGraph,
        bank_key: &BankKey,
        finalized_nanos: u64,
    ) -> Option<FinalityRecord> {
        let node = graph.get_bank_mut(bank_key)?;

        node.state = BankState::Finalized;
        node.finalized_at_nanos = Some(finalized_nanos);

        let observed_nanos = node.first_observed_nanos;
        let canonical_nanos = node.canonical_at_nanos;

        let finality_latency_us = finalized_nanos.saturating_sub(observed_nanos) / 1000;
        let canonical_latency_us =
            canonical_nanos.map(|c| c.saturating_sub(observed_nanos) / 1000);

        let record = FinalityRecord {
            slot: node.identity.slot,
            bank_key: bank_key.clone(),
            blockhash: node.identity.blockhash.clone(),
            observed_nanos,
            canonical_nanos,
            finalized_nanos,
            canonical_latency_us,
            finality_latency_us,
        };

        if let Some(c_us) = canonical_latency_us {
            self.canonical_latencies_us.push(c_us);
        }
        self.finality_latencies_us.push(finality_latency_us);
        self.records.push(record.clone());

        Some(record)
    }

    pub fn finalized_count(&self) -> usize {
        self.records.len()
    }

    pub fn finality_latency_summary(&self) -> Option<LatencySummary> {
        Self::compute_summary(&self.finality_latencies_us)
    }

    pub fn canonical_latency_summary(&self) -> Option<LatencySummary> {
        Self::compute_summary(&self.canonical_latencies_us)
    }

    fn compute_summary(samples: &[u64]) -> Option<LatencySummary> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let n = sorted.len();
        Some(LatencySummary {
            sample_count: n,
            min_us: sorted[0],
            p50_us: sorted[(n * 50) / 100],
            p90_us: sorted[((n * 90) / 100).min(n - 1)],
            p95_us: sorted[((n * 95) / 100).min(n - 1)],
            p99_us: sorted[((n * 99) / 100).min(n - 1)],
            max_us: sorted[n - 1],
            classification: "MEASURED",
        })
    }
}

/// Structural decode state of an observed consensus certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CertificateDecodeStatus {
    Decoded,
    EmptyPayload,
    TooShort,
}

/// Parsed consensus certificate metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedCertificate {
    pub kind: CertificateKind,
    pub slot: Slot,
    pub block_id: Option<String>,
    pub provider: String,
    pub raw_len: usize,
    pub participant_count_estimate: usize,
    pub stake_bps_estimate: Option<u32>,
    pub decode_status: CertificateDecodeStatus,
    pub validation_status: CertificateValidationStatus,
    pub observed_at_nanos: u64,
}

/// Lightweight structural certificate parser for block footer certificates.
#[derive(Debug, Default)]
pub struct CertificateEngine;

impl CertificateEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_certificate(
        &self,
        kind: CertificateKind,
        slot: Slot,
        block_id: Option<String>,
        provider: &str,
        raw_bytes: &[u8],
        observed_at_nanos: u64,
    ) -> ParsedCertificate {
        let raw_len = raw_bytes.len();
        let (decode_status, validation_status, participant_count_estimate) = if raw_len == 0 {
            (
                CertificateDecodeStatus::EmptyPayload,
                CertificateValidationStatus::Malformed,
                0,
            )
        } else if raw_len < 48 {
            (
                CertificateDecodeStatus::TooShort,
                CertificateValidationStatus::Malformed,
                0,
            )
        } else {
            let bitmap_bytes = if raw_len > 48 { &raw_bytes[48..] } else { &[] };
            let bits: usize = bitmap_bytes.iter().map(|b| b.count_ones() as usize).sum();
            (
                CertificateDecodeStatus::Decoded,
                CertificateValidationStatus::ParsedUnverified,
                bits.max(1),
            )
        };

        ParsedCertificate {
            kind,
            slot,
            block_id,
            provider: provider.to_string(),
            raw_len,
            participant_count_estimate,
            stake_bps_estimate: None,
            decode_status,
            validation_status,
            observed_at_nanos,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_never_resolves_first_seen_as_canonical_without_evidence() {
        let mut graph = BankGraph::new();
        let provider = ProviderId::new("testnet-rpc");
        let slot = Slot(500);

        let ident = BankIdentity::new(
            provider,
            Some(BankId(1)),
            slot,
            Some(Blockhash::new("UnconfirmedHash1")),
        );
        graph.insert_bank(BankNode::new(ident, None, 10_000));

        let resolver = CanonicalResolver::new();
        let resolution = resolver.resolve_slot(&mut graph, slot, None, 15_000);

        assert!(matches!(
            resolution,
            CanonicalResolution::Ambiguous {
                slot: Slot(500),
                candidates_count: 1
            }
        ));
        assert_eq!(graph.get_banks_for_slot(slot)[0].state, BankState::Observed);
    }

    #[test]
    fn test_resolves_canonical_and_abandons_competing_forks() {
        let mut graph = BankGraph::new();
        let provider = ProviderId::new("testnet-rpc");
        let slot = Slot(500);

        let confirmed_hash = Blockhash::new("ConfirmedBlockhashX");
        let orphan_hash = Blockhash::new("OrphanBlockhashY");

        let key_a = graph.insert_bank(BankNode::new(
            BankIdentity::new(
                provider.clone(),
                Some(BankId(1)),
                slot,
                Some(confirmed_hash.clone()),
            ),
            None,
            10_000,
        ));
        let key_b = graph.insert_bank(BankNode::new(
            BankIdentity::new(provider, Some(BankId(2)), slot, Some(orphan_hash)),
            None,
            10_005,
        ));

        let resolver = CanonicalResolver::new();
        let evidence = CanonicalEvidence::ConfirmedBlockhash(confirmed_hash);
        let resolution = resolver.resolve_slot(&mut graph, slot, Some(&evidence), 20_000);

        assert_eq!(resolution, CanonicalResolution::Resolved(key_a.clone()));
        assert_eq!(graph.get_bank(&key_a).unwrap().state, BankState::Canonical);
        assert_eq!(graph.get_bank(&key_b).unwrap().state, BankState::Abandoned);
    }
}
