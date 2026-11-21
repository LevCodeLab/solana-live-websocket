use crate::identity::BankIdentity;
use crate::types::{
    now_unix_ms, BankId, BankState, BankStatus, BlockFooter, Blockhash, CertificateKind,
    CertificateValidationStatus, LeaderId, ObserverContext, ParentReference, ProviderId, Slot,
};
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Normalized internal consensus stream event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreStreamEvent {
    pub event_id: u64,
    pub provider: ProviderId,
    pub slot: Slot,
    pub blockhash: Option<Blockhash>,
    pub received_time_ms: u64,
    #[serde(skip, default = "Instant::now")]
    pub received_instant: Instant,
    pub kind: CoreEventKind,
    #[serde(default)]
    pub observer: Option<ObserverContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoreEventKind {
    SlotObserved {
        parent_slot: Option<Slot>,
    },
    SlotStatusChanged {
        slot: Slot,
        status: String,
        bank_id: Option<BankId>,
    },
    LeaderObserved {
        leader: LeaderId,
    },
    LeaderScheduleObserved {
        starting_slot: Slot,
        leaders: Vec<LeaderId>,
    },
    BankObserved {
        identity: BankIdentity,
        parent: ParentReference,
        state: BankState,
    },
    BankCreated {
        identity: BankIdentity,
        parent: ParentReference,
    },
    BankStatusChanged {
        identity: BankIdentity,
        status: BankStatus,
    },
    EntryObserved {
        slot: Slot,
        bank_id: Option<BankId>,
        entry_index: u64,
        tx_count: u64,
    },
    ParentChanged {
        abandoned_bank: BankIdentity,
        new_parent: ParentReference,
        reason: String,
    },
    UpdateParent {
        slot: Slot,
        cleared_bank_id: Option<BankId>,
        parent_slot: Slot,
        parent_block_id: Option<String>,
        fec_set_index: Option<u64>,
        source: String,
    },
    BankAbandoned {
        identity: BankIdentity,
        reason: String,
    },
    ReplacementBank {
        old_identity: BankIdentity,
        replacement_identity: BankIdentity,
        slot: Slot,
    },
    BlockObserved {
        blockhash: Blockhash,
        parent_blockhash: Option<Blockhash>,
        tx_count: u64,
    },
    BlockMetaObserved {
        slot: Slot,
        blockhash: Blockhash,
        parent_slot: Slot,
        parent_blockhash: Blockhash,
        bank_id: Option<BankId>,
    },
    BlockFooterObserved {
        footer: BlockFooter,
    },
    CertificateObserved {
        slot: Slot,
        kind: CertificateKind,
        raw_len: usize,
        #[serde(default)]
        raw_bytes: Option<Vec<u8>>,
        #[serde(default)]
        block_id: Option<String>,
        #[serde(default)]
        validation_status: Option<CertificateValidationStatus>,
        #[serde(default)]
        stake_percent: Option<f64>,
    },
    CanonicalObserved {
        canonical_identity: BankIdentity,
    },
    FinalizedObserved {
        identity: BankIdentity,
        cert_type: String,
        latency_ms: Option<u64>,
    },
    DeshredObserved {
        slot: Slot,
        signature: String,
        #[serde(default)]
        raw_tx_bytes: Option<Vec<u8>>,
        #[serde(default)]
        pre_execution_timestamp_nanos: Option<u64>,
        #[serde(default)]
        static_accounts: Vec<String>,
        #[serde(default)]
        fec_set_index: Option<u64>,
    },
    ProducerTimingObserved {
        slot: Slot,
        producer_time_nanos: u64,
        received_at_nanos: u64,
        interval_nanos: i64,
    },
    ProviderConnected {
        provider: ProviderId,
    },
    ProviderDisconnected {
        provider: ProviderId,
        reason: String,
    },
}

impl CoreStreamEvent {
    pub fn new(
        event_id: u64,
        provider: ProviderId,
        slot: Slot,
        blockhash: Option<Blockhash>,
        kind: CoreEventKind,
    ) -> Self {
        Self {
            event_id,
            provider,
            slot,
            blockhash,
            received_time_ms: now_unix_ms(),
            received_instant: Instant::now(),
            kind,
            observer: None,
        }
    }

    pub fn with_observer(mut self, observer: ObserverContext) -> Self {
        self.observer = Some(observer);
        self
    }
}
