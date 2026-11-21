use crate::events::CoreStreamEvent;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

/// High-throughput, non-blocking internal EventBus backed by a bounded ring broadcast channel.
#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<Arc<CoreStreamEvent>>,
    event_counter: Arc<AtomicU64>,
}

impl EventBus {
    pub const DEFAULT_CAPACITY: usize = 4096;

    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            event_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn next_event_id(&self) -> u64 {
        self.event_counter.fetch_add(1, Ordering::Relaxed)
    }

    pub fn publish(&self, event: CoreStreamEvent) -> usize {
        let arc_event = Arc::new(event);
        self.sender.send(arc_event).unwrap_or(0)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<CoreStreamEvent>> {
        self.sender.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(Self::DEFAULT_CAPACITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::CoreEventKind;
    use crate::types::{ProviderId, Slot};

    #[tokio::test]
    async fn test_event_bus_publish_subscribe() {
        let bus = EventBus::new(32);
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();

        let event = CoreStreamEvent::new(
            bus.next_event_id(),
            ProviderId::new("test-provider"),
            Slot(100),
            None,
            CoreEventKind::SlotObserved { parent_slot: None },
        );

        let count = bus.publish(event);
        assert_eq!(count, 2);

        let received1 = rx1.recv().await.unwrap();
        assert_eq!(received1.slot, Slot(100));

        let received2 = rx2.recv().await.unwrap();
        assert_eq!(received2.slot, Slot(100));
    }
}
