//! Core slot clock, bank fork graph, leader lookahead, protocol detector,
//! and RPC / PubSub WebSocket / Unix Domain Socket stream adapters.

pub mod adapters;
pub mod bus;
pub mod clock;
pub mod detector;
pub mod events;
pub mod graph;
pub mod identity;
pub mod leader;
pub mod types;

/// Crate schema version for normalized internal stream frames.
pub const CORE_SCHEMA_VERSION: u32 = 1;

pub use bus::EventBus;
pub use clock::{SlotClock, SlotProgress};
pub use events::{CoreEventKind, CoreStreamEvent};
pub use graph::{
    BankGraph, BankKey, BankNode, CanonicalEvidence, CanonicalResolution, CanonicalResolver,
    CertificateEngine, FinalityEngine, ParsedCertificate,
};
pub use identity::BankIdentity;
pub use leader::LeaderEngine;
pub use types::{
    BankHash, BankId, BankState, BankStatus, BlockFooter, Blockhash, CandidateId, CertificateKind,
    ConsensusLifecycleState, ConsensusMode, FieldProvenance, HandoffState, LeaderId,
    ObserverContext, ParentReference, ProtocolProfile, ProviderId, Slot, SlotDuration,
    TelemetryCapabilityMatrix,
};
