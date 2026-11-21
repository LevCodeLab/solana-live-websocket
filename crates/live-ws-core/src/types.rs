use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Current wall-clock time in milliseconds since UNIX_EPOCH.
pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Current wall-clock time in nanoseconds since UNIX_EPOCH.
pub fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Converts UNIX timestamp seconds into `(year, month, day, hour, minute, second)` in UTC.
pub fn civil_utc_from_unix_secs(secs: u64) -> (i32, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let hour = (rem / 3600) as u32;
    let minute = ((rem % 3600) / 60) as u32;
    let second = (rem % 60) as u32;

    // Howard Hinnant civil_from_days algorithm
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = (if m <= 2 { y + 1 } else { y }) as i32;

    (year, m, d, hour, minute, second)
}

/// Formats UNIX seconds as RFC-3339 / ISO-8601 UTC string (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn format_unix_secs_iso8601(secs: u64) -> String {
    let (y, m, d, hh, mm, ss) = civil_utc_from_unix_secs(secs);
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, hh, mm, ss)
}

/// Returns current UTC timestamp in ISO-8601 format (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn utc_iso8601_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format_unix_secs_iso8601(secs)
}

/// Returns current UTC timestamp in compact file-safe format (`YYYYMMDD_HHMMSS`).
pub fn utc_compact_stamp_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let (y, m, d, hh, mm, ss) = civil_utc_from_unix_secs(secs);
    format!("{:04}{:02}{:02}_{:02}{:02}{:02}", y, m, d, hh, mm, ss)
}

/// Monotonically increasing slot number in the Solana cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub struct Slot(pub u64);

impl Slot {
    pub const ZERO: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub fn prev(self) -> Option<Self> {
        self.0.checked_sub(1).map(Self)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Slot {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

/// Cryptographic hash identifying a sealed block on Solana.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Blockhash(pub String);

impl Blockhash {
    pub fn new(hash: impl Into<String>) -> Self {
        Self(hash.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Blockhash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Cryptographic state accumulator of accounts delta in the validator bank.
/// Distinct from `Blockhash` and modeled separately.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BankHash(pub String);

impl BankHash {
    pub fn new(hash: impl Into<String>) -> Self {
        Self(hash.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BankHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Validator-local bank identifier.
/// Scoped strictly to a single validator observer node instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BankId(pub u64);

impl fmt::Display for BankId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bank-{}", self.0)
    }
}

/// Synthetic candidate identifier used only when a provider omits `bank_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CandidateId(pub String);

impl CandidateId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CandidateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "candidate-{}", self.0)
    }
}

/// Explicit provenance tracking for every normalized field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldProvenance {
    /// Observed directly on the wire stream (UDS, WebSocket, RPC).
    Direct,
    /// Derived deterministically from protocol rules (e.g. leader schedule).
    Derived,
    /// Reconstructed via multi-signal correlation.
    Inferred,
    /// Approximated via monotonic clock timing.
    Estimated,
    /// Omitted by the active upstream provider.
    Unavailable,
}

impl fmt::Display for FieldProvenance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct => write!(f, "DIRECT"),
            Self::Derived => write!(f, "DERIVED"),
            Self::Inferred => write!(f, "INFERRED"),
            Self::Estimated => write!(f, "ESTIMATED"),
            Self::Unavailable => write!(f, "UNAVAILABLE"),
        }
    }
}

/// Raw bank-scoped lifecycle status from validator telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum BankStatus {
    CreatedBank,
    Processed,
    Confirmed,
    Rooted,
    Dead,
    #[default]
    Unknown,
}

impl fmt::Display for BankStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreatedBank => write!(f, "CreatedBank"),
            Self::Processed => write!(f, "Processed"),
            Self::Confirmed => write!(f, "Confirmed"),
            Self::Rooted => write!(f, "Rooted"),
            Self::Dead => write!(f, "Dead"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Consensus certificate category emitted in block footers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CertificateKind {
    FinalCert,
    SkipRewardCert,
    NotarRewardCert,
    FinalizeFast,
    FinalizeSlow,
    Notarize,
    NotarizeFallback,
    Skip,
    Genesis,
    Unknown,
}

impl fmt::Display for CertificateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FinalCert => write!(f, "FINAL_CERT"),
            Self::SkipRewardCert => write!(f, "SKIP_REWARD_CERT"),
            Self::NotarRewardCert => write!(f, "NOTAR_REWARD_CERT"),
            Self::FinalizeFast => write!(f, "FINALIZE_FAST"),
            Self::FinalizeSlow => write!(f, "FINALIZE_SLOW"),
            Self::Notarize => write!(f, "NOTARIZE"),
            Self::NotarizeFallback => write!(f, "NOTARIZE_FALLBACK"),
            Self::Skip => write!(f, "SKIP"),
            Self::Genesis => write!(f, "GENESIS"),
            Self::Unknown => write!(f, "UNKNOWN_CERTIFICATE_TYPE"),
        }
    }
}

/// Block footer metadata emitted at the end of block execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BlockFooter {
    pub slot: Slot,
    pub bank_id: Option<BankId>,
    pub bank_hash: Option<BankHash>,
    pub block_producer_time_nanos: Option<u64>,
    pub block_user_agent: Option<String>,
    pub block_final_cert: Option<Vec<u8>>,
    pub skip_reward_cert: Option<Vec<u8>>,
    pub notar_reward_cert: Option<Vec<u8>>,
    pub raw_payload: Option<Vec<u8>>,
    pub received_at_nanos: u64,
}

/// Identifier for an upstream ingestion provider (e.g. "testnet-rpc", "devnet-ws", "validator-uds").
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProviderId(pub String);

impl ProviderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Validator identity scheduled as slot leader.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaderId(pub String);

impl LeaderId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LeaderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Slot duration supporting SIMD-0525 staged reductions (400ms down to 200ms).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDuration(pub Duration);

impl SlotDuration {
    pub const MS_400: Self = Self(Duration::from_millis(400));
    pub const MS_350: Self = Self(Duration::from_millis(350));
    pub const MS_300: Self = Self(Duration::from_millis(300));
    pub const MS_250: Self = Self(Duration::from_millis(250));
    pub const MS_200: Self = Self(Duration::from_millis(200));

    pub fn from_millis(ms: u64) -> Self {
        Self(Duration::from_millis(ms))
    }

    pub fn as_millis(&self) -> u64 {
        self.0.as_millis() as u64
    }

    pub fn as_duration(&self) -> Duration {
        self.0
    }
}

impl Default for SlotDuration {
    fn default() -> Self {
        Self::MS_200
    }
}

impl fmt::Display for SlotDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.as_millis())
    }
}

/// State of a candidate bank in `BankGraph`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BankState {
    Observed,
    Canonical,
    Abandoned,
    Finalized,
}

impl fmt::Display for BankState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Observed => write!(f, "OBSERVED"),
            Self::Canonical => write!(f, "CANONICAL"),
            Self::Abandoned => write!(f, "ABANDONED"),
            Self::Finalized => write!(f, "FINALIZED"),
        }
    }
}

/// Parent reference of a bank or block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParentReference {
    KnownBlockhash(Blockhash),
    SlotOnly(Slot),
    Genesis,
    Unknown,
}

/// Leader handoff state across slot boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HandoffState {
    Building,
    ParentReady,
    Handover,
    Unknown,
}

impl fmt::Display for HandoffState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Building => write!(f, "BUILDING"),
            Self::ParentReady => write!(f, "PARENT_READY"),
            Self::Handover => write!(f, "HANDOVER"),
            Self::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// Consensus mode detected on the active cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusMode {
    LegacyTowerBFT,
    AlpenglowVotor,
    Unknown,
}

impl fmt::Display for ConsensusMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LegacyTowerBFT => write!(f, "LEGACY_TOWER_BFT"),
            Self::AlpenglowVotor => write!(f, "ALPENGLOW_VOTOR"),
            Self::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// Protocol operational profile detected dynamically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolProfile {
    pub consensus_mode: ConsensusMode,
    pub target_slot_duration: SlotDuration,
    pub alpenglow_genesis_cert_present: bool,
    pub multi_bank_enabled: bool,
    pub update_parent_enabled: bool,
}

impl Default for ProtocolProfile {
    fn default() -> Self {
        Self {
            consensus_mode: ConsensusMode::Unknown,
            target_slot_duration: SlotDuration::MS_200,
            alpenglow_genesis_cert_present: false,
            multi_bank_enabled: false,
            update_parent_enabled: false,
        }
    }
}

/// Operational telemetry level hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TelemetryLevel {
    Level0PublicRpc,
    Level1PublicRpcWs,
    Level2GrpcStream,
    Level3SimulatorFixture,
    Level4ValidatorUds,
}

impl fmt::Display for TelemetryLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Level0PublicRpc => write!(f, "LEVEL 0 (PUBLIC RPC)"),
            Self::Level1PublicRpcWs => write!(f, "LEVEL 1 (RPC + WS)"),
            Self::Level2GrpcStream => write!(f, "LEVEL 2 (GRPC STREAM)"),
            Self::Level3SimulatorFixture => write!(f, "LEVEL 3 (SIMULATOR FIXTURE)"),
            Self::Level4ValidatorUds => write!(f, "LEVEL 4 (VALIDATOR UDS)"),
        }
    }
}

/// Disambiguation of the observing validator node from the producing leader node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct ObserverContext {
    pub observing_validator: Option<String>,
    pub producing_validator: Option<String>,
    pub transport_type: String,
    pub cluster_environment: String,
}

/// Verification state of a consensus certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CertificateValidationStatus {
    RawObserved,
    Parsed,
    StructurallyValid,
    ParsedUnverified,
    CryptographicallyVerified,
    Malformed,
}

impl fmt::Display for CertificateValidationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RawObserved => write!(f, "RAW_OBSERVED"),
            Self::Parsed => write!(f, "PARSED"),
            Self::StructurallyValid => write!(f, "STRUCTURALLY_VALID"),
            Self::ParsedUnverified => write!(f, "PARSED_UNVERIFIED"),
            Self::CryptographicallyVerified => write!(f, "CRYPTOGRAPHICALLY_VERIFIED"),
            Self::Malformed => write!(f, "MALFORMED"),
        }
    }
}

/// Availability and diagnostic status for an individual telemetry field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldAvailabilityDetail {
    pub field: String,
    pub protocol_supported: bool,
    pub source_supported: bool,
    pub currently_observed: bool,
    pub provenance: FieldProvenance,
    pub freshness: String,
    pub reason_unavailable: Option<String>,
    pub alternate_source: Option<String>,
}

/// Comprehensive runtime telemetry capability matrix across core and extended fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryCapabilityMatrix {
    pub current_source: String,
    pub telemetry_level: TelemetryLevel,
    pub observer: ObserverContext,
    pub fields: Vec<FieldAvailabilityDetail>,
    pub core_coverage_percent: u32,
    pub extended_coverage_percent: u32,
}

/// Consensus lifecycle state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum ConsensusLifecycleState {
    #[default]
    Observed,
    Processed,
    Candidate,
    Canonical,
    Notarized,
    Finalized,
    Rooted,
    Abandoned,
    Replaced,
    Skipped,
}

impl fmt::Display for ConsensusLifecycleState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Observed => write!(f, "OBSERVED"),
            Self::Processed => write!(f, "PROCESSED"),
            Self::Candidate => write!(f, "CANDIDATE"),
            Self::Canonical => write!(f, "CANONICAL"),
            Self::Notarized => write!(f, "NOTARIZED"),
            Self::Finalized => write!(f, "FINALIZED"),
            Self::Rooted => write!(f, "ROOTED"),
            Self::Abandoned => write!(f, "ABANDONED"),
            Self::Replaced => write!(f, "REPLACED"),
            Self::Skipped => write!(f, "SKIPPED"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utc_iso8601_formatting() {
        assert_eq!(format_unix_secs_iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_unix_secs_iso8601(1_700_000_000), "2023-11-14T22:13:20Z");
    }
}
