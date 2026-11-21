use crate::types::{LeaderId, Slot};
use std::collections::BTreeMap;

/// Warm in-memory cache of slot leader assignments and handoff transitions.
#[derive(Debug, Default, Clone)]
pub struct LeaderEngine {
    schedule: BTreeMap<Slot, LeaderId>,
    current_leader: Option<(Slot, LeaderId)>,
}

impl LeaderEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_leader(&mut self, slot: Slot, leader: LeaderId) {
        self.schedule.insert(slot, leader.clone());
        self.current_leader = Some((slot, leader));
    }

    pub fn populate_schedule(&mut self, starting_slot: Slot, leaders: Vec<LeaderId>) {
        for (idx, leader) in leaders.into_iter().enumerate() {
            let slot = Slot(starting_slot.as_u64().saturating_add(idx as u64));
            self.schedule.insert(slot, leader);
        }
    }

    pub fn leader_for(&self, slot: Slot) -> Option<LeaderId> {
        self.schedule.get(&slot).cloned()
    }

    pub fn current_leader(&self) -> Option<LeaderId> {
        self.current_leader.as_ref().map(|(_, l)| l.clone())
    }

    pub fn next_leader(&self, current_slot: Slot) -> Option<LeaderId> {
        self.schedule.get(&current_slot.next()).cloned()
    }

    pub fn is_leader_transition(&self, slot_a: Slot, slot_b: Slot) -> bool {
        match (self.leader_for(slot_a), self.leader_for(slot_b)) {
            (Some(a), Some(b)) => a != b,
            _ => false,
        }
    }

    pub fn prune_older_than(&mut self, min_slot: Slot) {
        self.schedule.retain(|s, _| *s >= min_slot);
    }

    pub fn cached_slots_count(&self) -> usize {
        self.schedule.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leader_engine_warm_lookup_and_transition() {
        let mut engine = LeaderEngine::new();
        let slot = Slot(100);

        let leader1 = LeaderId::new("ValidatorAlpha11111111111111111111111111111");
        let leader2 = LeaderId::new("ValidatorBeta111111111111111111111111111111");

        engine.set_leader(slot, leader1.clone());
        engine.set_leader(slot.next(), leader2.clone());

        assert_eq!(engine.leader_for(slot), Some(leader1));
        assert_eq!(engine.next_leader(slot), Some(leader2.clone()));
        assert!(engine.is_leader_transition(slot, slot.next()));

        engine.prune_older_than(Slot(101));
        assert!(engine.leader_for(slot).is_none());
        assert_eq!(engine.leader_for(slot.next()), Some(leader2));
    }
}
