//! Deterministic fork, candidate bank, and leader handoff simulator.
//!
//! Generates reproducible multi-bank fork scenarios, `UpdateParent` lineage switches,
//! and leader handoff window transitions using synthetic validator identities.

use serde::{Deserialize, Serialize};

pub const VALIDATOR_ALPHA: &str = "ValidatorAlpha11111111111111111111111111111";
pub const VALIDATOR_BETA: &str = "ValidatorBeta111111111111111111111111111111";
pub const VALIDATOR_GAMMA: &str = "ValidatorGamma11111111111111111111111111111";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SimulatedBankNode {
    pub bank_id: String,
    pub bank_hash: Option<String>,
    pub blockhash: String,
    pub parent_slot: u64,
    pub state: String,
    pub is_followed: bool,
    pub provenance: String,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedLeaderWindow {
    pub current_leader: String,
    pub next_leader: String,
    pub remaining_ms: u64,
    pub slot_duration_ms: u64,
    pub handoff_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedParentLineage {
    pub parent_slot: u64,
    pub active_bank_id: String,
    pub cleared_bank_id: Option<String>,
    pub update_parent_occurred: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkSimulationRequest {
    pub scenario: String,
    pub seed: Option<u64>,
    pub bank_count: Option<usize>,
    pub followed_bank_id: Option<String>,
    pub late_window: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkSimulationResponse {
    pub scenario_id: String,
    pub scenario_type: String,
    pub mode: String,
    pub slot: u64,
    pub banks: Vec<SimulatedBankNode>,
    pub leader: SimulatedLeaderWindow,
    pub parent: SimulatedParentLineage,
    pub summary: String,
}

#[derive(Default, Clone)]
pub struct ForkSimulator;

impl ForkSimulator {
    pub fn new() -> Self {
        Self
    }

    /// Runs a deterministic fork or leader handoff scenario.
    pub fn run_scenario(&self, req: &ForkSimulationRequest) -> ForkSimulationResponse {
        let seed = req.seed.unwrap_or(42);
        let slot = 450_000_000 + (seed % 100);

        match req.scenario.to_lowercase().as_str() {
            "multiple_banks" => self.sim_multiple_banks(
                slot,
                req.bank_count.unwrap_or(3),
                req.followed_bank_id.as_deref(),
            ),
            "abandon_bank" | "ghost_fork" => {
                self.sim_abandon_bank(slot, req.followed_bank_id.as_deref())
            }
            "leader_handoff" => self.sim_leader_handoff(slot, req.late_window.unwrap_or(true)),
            _ => self.sim_update_parent(slot),
        }
    }

    fn sim_multiple_banks(
        &self,
        slot: u64,
        count: usize,
        followed: Option<&str>,
    ) -> ForkSimulationResponse {
        let count = count.clamp(2, 4);
        let selected = followed.unwrap_or("bank-B");
        let letters = ["A", "B", "C", "D"];
        let banks = letters
            .iter()
            .take(count)
            .map(|letter| {
                let bank_id = format!("bank-{}", letter);
                SimulatedBankNode {
                    is_followed: bank_id == selected,
                    bank_id,
                    bank_hash: Some(format!("BankHash_{}_{}", letter, slot)),
                    blockhash: format!("SynthBlockhash_{}_{}", letter, slot % 1000),
                    parent_slot: slot - 1,
                    state: "CANDIDATE".to_string(),
                    provenance: "DERIVED".to_string(),
                    notes: format!("Candidate fork {} anchored to slot {}", letter, slot - 1),
                }
            })
            .collect();

        ForkSimulationResponse {
            scenario_id: format!("sim-multibanks-{}", slot),
            scenario_type: "multiple_banks".to_string(),
            mode: "SIMULATION".to_string(),
            slot,
            banks,
            leader: SimulatedLeaderWindow {
                current_leader: VALIDATOR_ALPHA.to_string(),
                next_leader: VALIDATOR_BETA.to_string(),
                remaining_ms: 160,
                slot_duration_ms: 200,
                handoff_state: "ACTIVE".to_string(),
            },
            parent: SimulatedParentLineage {
                parent_slot: slot - 1,
                active_bank_id: selected.to_string(),
                cleared_bank_id: None,
                update_parent_occurred: false,
                reason: None,
            },
            summary: format!(
                "Slot {} carries {} competing candidate banks; {} currently tracked.",
                slot, count, selected
            ),
        }
    }

    fn sim_abandon_bank(&self, slot: u64, followed: Option<&str>) -> ForkSimulationResponse {
        let abandoned = followed.unwrap_or("bank-A");
        let canonical = if abandoned == "bank-A" {
            "bank-B"
        } else {
            "bank-A"
        };

        let banks = vec![
            SimulatedBankNode {
                bank_id: "bank-A".to_string(),
                bank_hash: Some(format!("BankHash_A_{}", slot)),
                blockhash: format!("SynthBlockhash_A_{}", slot % 1000),
                parent_slot: slot - 1,
                state: if canonical == "bank-A" {
                    "CANONICAL".to_string()
                } else {
                    "ABANDONED".to_string()
                },
                is_followed: abandoned == "bank-A",
                provenance: "DERIVED".to_string(),
                notes: if canonical == "bank-A" {
                    "Promoted to canonical fork".to_string()
                } else {
                    "Abandoned by validator fork switch".to_string()
                },
            },
            SimulatedBankNode {
                bank_id: "bank-B".to_string(),
                bank_hash: Some(format!("BankHash_B_{}", slot)),
                blockhash: format!("SynthBlockhash_B_{}", slot % 1000),
                parent_slot: slot - 1,
                state: if canonical == "bank-B" {
                    "CANONICAL".to_string()
                } else {
                    "ABANDONED".to_string()
                },
                is_followed: abandoned == "bank-B",
                provenance: "DERIVED".to_string(),
                notes: if canonical == "bank-B" {
                    "Promoted to canonical fork".to_string()
                } else {
                    "Abandoned by validator fork switch".to_string()
                },
            },
        ];

        ForkSimulationResponse {
            scenario_id: format!("sim-abandon-{}", slot),
            scenario_type: "abandon_bank".to_string(),
            mode: "SIMULATION".to_string(),
            slot,
            banks,
            leader: SimulatedLeaderWindow {
                current_leader: VALIDATOR_ALPHA.to_string(),
                next_leader: VALIDATOR_BETA.to_string(),
                remaining_ms: 140,
                slot_duration_ms: 200,
                handoff_state: "ACTIVE".to_string(),
            },
            parent: SimulatedParentLineage {
                parent_slot: slot - 1,
                active_bank_id: canonical.to_string(),
                cleared_bank_id: Some(abandoned.to_string()),
                update_parent_occurred: true,
                reason: Some("Candidate bank abandoned in favor of canonical fork".to_string()),
            },
            summary: format!(
                "{} was abandoned; {} became canonical on slot {}.",
                abandoned, canonical, slot
            ),
        }
    }

    fn sim_update_parent(&self, slot: u64) -> ForkSimulationResponse {
        let banks = vec![
            SimulatedBankNode {
                bank_id: "bank-1".to_string(),
                bank_hash: Some(format!("BankHash_1_{}", slot)),
                blockhash: format!("OldParentBranch_{}", slot % 100),
                parent_slot: slot - 2,
                state: "ABANDONED".to_string(),
                is_followed: false,
                provenance: "DERIVED".to_string(),
                notes: "Cleared by SIMD-0337 UpdateParent marker".to_string(),
            },
            SimulatedBankNode {
                bank_id: "bank-2".to_string(),
                bank_hash: Some(format!("BankHash_2_{}", slot)),
                blockhash: format!("FreshParentBranch_{}", slot % 100),
                parent_slot: slot - 1,
                state: "CANONICAL".to_string(),
                is_followed: true,
                provenance: "DERIVED".to_string(),
                notes: "Replacement bank anchored to certified parent slot".to_string(),
            },
        ];

        ForkSimulationResponse {
            scenario_id: format!("sim-update-parent-{}", slot),
            scenario_type: "update_parent".to_string(),
            mode: "SIMULATION".to_string(),
            slot,
            banks,
            leader: SimulatedLeaderWindow {
                current_leader: VALIDATOR_ALPHA.to_string(),
                next_leader: VALIDATOR_BETA.to_string(),
                remaining_ms: 135,
                slot_duration_ms: 200,
                handoff_state: "ACTIVE".to_string(),
            },
            parent: SimulatedParentLineage {
                parent_slot: slot - 1,
                active_bank_id: "bank-2".to_string(),
                cleared_bank_id: Some("bank-1".to_string()),
                update_parent_occurred: true,
                reason: Some("SIMD-0337 fast leader handover parent switch".to_string()),
            },
            summary: format!(
                "UpdateParent cleared bank-1 on slot {} and promoted replacement bank-2.",
                slot
            ),
        }
    }

    fn sim_leader_handoff(&self, slot: u64, late_window: bool) -> ForkSimulationResponse {
        let remaining_ms = if late_window { 18 } else { 165 };
        let handoff_state = if late_window {
            "HANDOVER_IMMINENT"
        } else {
            "ACTIVE"
        };

        ForkSimulationResponse {
            scenario_id: format!("sim-handoff-{}", slot),
            scenario_type: "leader_handoff".to_string(),
            mode: "SIMULATION".to_string(),
            slot,
            banks: vec![SimulatedBankNode {
                bank_id: "bank-active".to_string(),
                bank_hash: Some(format!("BankHash_Active_{}", slot)),
                blockhash: format!("LeaderTip_{}", slot % 100),
                parent_slot: slot - 1,
                state: "CANONICAL".to_string(),
                is_followed: true,
                provenance: "DERIVED".to_string(),
                notes: "Current leader active candidate bank".to_string(),
            }],
            leader: SimulatedLeaderWindow {
                current_leader: VALIDATOR_BETA.to_string(),
                next_leader: VALIDATOR_GAMMA.to_string(),
                remaining_ms,
                slot_duration_ms: 200,
                handoff_state: handoff_state.to_string(),
            },
            parent: SimulatedParentLineage {
                parent_slot: slot - 1,
                active_bank_id: "bank-active".to_string(),
                cleared_bank_id: None,
                update_parent_occurred: false,
                reason: None,
            },
            summary: format!(
                "Leader window on slot {} has {}ms left (state: {}).",
                slot, remaining_ms, handoff_state
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fork_simulator_deterministic_across_runs() {
        let sim = ForkSimulator::new();
        let req = ForkSimulationRequest {
            scenario: "update_parent".to_string(),
            seed: Some(7),
            bank_count: None,
            followed_bank_id: None,
            late_window: None,
        };
        let a = sim.run_scenario(&req);
        let b = sim.run_scenario(&req);
        assert_eq!(a.slot, b.slot);
        assert_eq!(a.banks, b.banks);
        assert_eq!(a.parent.cleared_bank_id, Some("bank-1".to_string()));
    }
}
