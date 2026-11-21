use crate::types::{BankId, Blockhash, ProviderId, Slot};
use serde::{Deserialize, Serialize};

/// Bank and Block Identity scoped to its upstream provider.
///
/// Architectural invariant:
/// `bank_id` is strictly local to `provider_id` (a single validator observer).
/// Cross-provider reconciliation requires `(slot, blockhash)` matching.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BankIdentity {
    pub provider_id: ProviderId,
    pub bank_id: Option<BankId>,
    pub slot: Slot,
    pub blockhash: Option<Blockhash>,
}

impl BankIdentity {
    pub fn new(
        provider_id: ProviderId,
        bank_id: Option<BankId>,
        slot: Slot,
        blockhash: Option<Blockhash>,
    ) -> Self {
        Self {
            provider_id,
            bank_id,
            slot,
            blockhash,
        }
    }

    /// Determines if two bank observations refer to the exact same physical block.
    pub fn correlates_with(&self, other: &Self) -> bool {
        if self.slot != other.slot {
            return false;
        }

        if self.provider_id == other.provider_id {
            if let (Some(a), Some(b)) = (&self.bank_id, &other.bank_id) {
                if a == b {
                    return true;
                }
            }
        }

        match (&self.blockhash, &other.blockhash) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_bank_id_across_different_providers_not_same_bank() {
        let provider_a = ProviderId::new("provider-alpha");
        let provider_b = ProviderId::new("provider-beta");

        let bank_a = BankIdentity::new(
            provider_a,
            Some(BankId(7)),
            Slot(1000),
            Some(Blockhash::new("BlockhashAlpha")),
        );

        let bank_b = BankIdentity::new(
            provider_b,
            Some(BankId(7)),
            Slot(1000),
            Some(Blockhash::new("BlockhashBeta")),
        );

        assert!(!bank_a.correlates_with(&bank_b));
        assert_ne!(bank_a, bank_b);
    }

    #[test]
    fn test_cross_provider_correlation_via_blockhash() {
        let provider_a = ProviderId::new("provider-alpha");
        let provider_b = ProviderId::new("provider-beta");

        let bank_a = BankIdentity::new(
            provider_a,
            Some(BankId(12)),
            Slot(500),
            Some(Blockhash::new("CanonicalBlockhash42")),
        );

        let bank_b = BankIdentity::new(
            provider_b,
            Some(BankId(99)),
            Slot(500),
            Some(Blockhash::new("CanonicalBlockhash42")),
        );

        assert!(bank_a.correlates_with(&bank_b));
    }

    #[test]
    fn test_different_slots_never_correlate() {
        let provider = ProviderId::new("provider-alpha");
        let bank_a = BankIdentity::new(
            provider.clone(),
            Some(BankId(1)),
            Slot(100),
            Some(Blockhash::new("Hash")),
        );
        let bank_b = BankIdentity::new(
            provider,
            Some(BankId(1)),
            Slot(101),
            Some(Blockhash::new("Hash")),
        );
        assert!(!bank_a.correlates_with(&bank_b));
    }
}
