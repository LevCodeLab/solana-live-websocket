use async_trait::async_trait;
use live_ws_core::adapters::{
    ProviderAdapter, ProviderCapabilityMatrix, RawSlotNotification, SimulatorFixtureAdapter,
    SolanaRpcClient, SolanaWsStream, StreamNormalizer, ValidatorUdsAdapter,
};
use live_ws_core::bus::EventBus;
use live_ws_core::events::{CoreEventKind, CoreStreamEvent};
use live_ws_core::identity::BankIdentity;
use live_ws_core::types::{LeaderId, ProviderId, Slot};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

/// Uniform async event producer interface across live RPC/WS, UDS, and simulator streams.
#[async_trait]
pub trait LiveStreamSource: Send + Sync {
    fn source_name(&self) -> &str;
    fn environment(&self) -> &str;
    fn capability_matrix(&self) -> ProviderCapabilityMatrix;
    async fn start(&mut self, event_tx: mpsc::Sender<CoreStreamEvent>) -> Result<(), String>;
    async fn stop(&mut self) -> Result<(), String>;
    fn is_connected(&self) -> bool;
}

/// Live Solana JSON-RPC + PubSub WebSocket slot and leader schedule stream.
pub struct RpcPubSubSource {
    cluster_name: String,
    rpc_url: String,
    ws_url: String,
    running: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl RpcPubSubSource {
    pub fn new(
        cluster_name: impl Into<String>,
        rpc_url: impl Into<String>,
        ws_url: impl Into<String>,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            rpc_url: rpc_url.into(),
            ws_url: ws_url.into(),
            running: Arc::new(AtomicBool::new(false)),
            connected: Arc::new(AtomicBool::new(false)),
            stop_tx: None,
        }
    }
}

#[async_trait]
impl LiveStreamSource for RpcPubSubSource {
    fn source_name(&self) -> &str {
        "rpc"
    }

    fn environment(&self) -> &str {
        "live"
    }

    fn capability_matrix(&self) -> ProviderCapabilityMatrix {
        ProviderCapabilityMatrix::standard_public_rpc(format!("{}-rpc", self.cluster_name))
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    async fn start(&mut self, event_tx: mpsc::Sender<CoreStreamEvent>) -> Result<(), String> {
        info!("Starting RpcPubSubSource for cluster: {}", self.cluster_name);
        self.running.store(true, Ordering::Relaxed);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
        self.stop_tx = Some(stop_tx);

        let rpc_client = Arc::new(SolanaRpcClient::new(self.rpc_url.clone()));
        let ws_stream = SolanaWsStream::new(self.ws_url.clone());
        let (raw_tx, mut raw_rx) = mpsc::channel::<RawSlotNotification>(100);
        ws_stream.start(raw_tx);

        let provider_id = ProviderId::new(format!("{}-rpc", self.cluster_name));
        let mut normalizer = StreamNormalizer::new(provider_id.clone(), 5000);
        let connected_flag = self.connected.clone();
        let running_flag = self.running.clone();

        tokio::spawn(async move {
            connected_flag.store(true, Ordering::Relaxed);
            let mut schedule_map: BTreeMap<u64, LeaderId> = BTreeMap::new();
            let mut max_scheduled_slot: u64 = 0;

            let mut init_slot = None;
            for _ in 0..3 {
                if let Ok(s) = rpc_client.get_slot().await {
                    init_slot = Some(s);
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            }

            if let Some(slot) = init_slot {
                if let Ok(leaders) = rpc_client.get_slot_leaders(slot.as_u64(), 500).await {
                    max_scheduled_slot = slot.as_u64() + leaders.len() as u64;
                    for (idx, lead) in leaders.iter().enumerate() {
                        schedule_map.insert(slot.as_u64() + idx as u64, lead.clone());
                    }
                    let _ = event_tx
                        .send(CoreStreamEvent::new(
                            1,
                            provider_id.clone(),
                            slot,
                            None,
                            CoreEventKind::LeaderScheduleObserved {
                                starting_slot: slot,
                                leaders: leaders.clone(),
                            },
                        ))
                        .await;
                    if let Some(leader) = leaders.first() {
                        let _ = event_tx
                            .send(CoreStreamEvent::new(
                                2,
                                provider_id.clone(),
                                slot,
                                None,
                                CoreEventKind::LeaderObserved {
                                    leader: leader.clone(),
                                },
                            ))
                            .await;
                    }
                }
            } else if let Ok(direct_leader) = rpc_client.get_slot_leader().await {
                let _ = event_tx
                    .send(CoreStreamEvent::new(
                        2,
                        provider_id.clone(),
                        Slot(0),
                        None,
                        CoreEventKind::LeaderObserved {
                            leader: direct_leader,
                        },
                    ))
                    .await;
            }

            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        ws_stream.stop();
                        break;
                    }
                    raw_msg = raw_rx.recv() => {
                        match raw_msg {
                            Some(raw) => {
                                let root_opt = raw.root;
                                if let Some(event) = normalizer.normalize_slot(raw) {
                                    let slot = event.slot;
                                    let slot_u64 = slot.as_u64();
                                    if event_tx.send(event).await.is_err() {
                                        break;
                                    }

                                    if let Some(root_slot) = root_opt {
                                        let root_delta_slots = slot_u64.saturating_sub(root_slot.as_u64());
                                        let root_latency_ms = root_delta_slots * 200;
                                        let _ = event_tx.send(CoreStreamEvent::new(
                                            3,
                                            provider_id.clone(),
                                            root_slot,
                                            None,
                                            CoreEventKind::FinalizedObserved {
                                                identity: BankIdentity::new(
                                                    provider_id.clone(),
                                                    None,
                                                    root_slot,
                                                    None,
                                                ),
                                                cert_type: "TOWER_BFT_ROOT".to_string(),
                                                latency_ms: Some(root_latency_ms),
                                            },
                                        )).await;
                                    }

                                    if slot_u64 + 100 >= max_scheduled_slot {
                                        if let Ok(leaders) = rpc_client.get_slot_leaders(slot_u64, 500).await {
                                            max_scheduled_slot = slot_u64 + leaders.len() as u64;
                                            for (idx, lead) in leaders.iter().enumerate() {
                                                schedule_map.insert(slot_u64 + idx as u64, lead.clone());
                                            }
                                            let _ = event_tx.send(CoreStreamEvent::new(
                                                1,
                                                provider_id.clone(),
                                                Slot(slot_u64),
                                                None,
                                                CoreEventKind::LeaderScheduleObserved {
                                                    starting_slot: Slot(slot_u64),
                                                    leaders,
                                                },
                                            )).await;
                                        }
                                    }

                                    if let Some(lead) = schedule_map.get(&slot_u64) {
                                        let _ = event_tx.send(CoreStreamEvent::new(
                                            2,
                                            provider_id.clone(),
                                            slot,
                                            None,
                                            CoreEventKind::LeaderObserved {
                                                leader: lead.clone(),
                                            },
                                        )).await;
                                    }

                                    let min_keep = slot_u64.saturating_sub(500);
                                    schedule_map.retain(|&s, _| s >= min_keep);
                                }
                            }
                            None => {
                                warn!("Upstream Solana WebSocket stream ended");
                                connected_flag.store(false, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                }
            }

            running_flag.store(false, Ordering::Relaxed);
            connected_flag.store(false, Ordering::Relaxed);
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), String> {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        self.running.store(false, Ordering::Relaxed);
        self.connected.store(false, Ordering::Relaxed);
        Ok(())
    }
}

/// Local validator Unix Domain Socket stream source (`/tmp/solana_validator_stream.sock`).
pub struct ValidatorUdsSource {
    socket_path: String,
    running: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ValidatorUdsSource {
    pub fn new(socket_path: impl Into<String>) -> Self {
        Self {
            socket_path: socket_path.into(),
            running: Arc::new(AtomicBool::new(false)),
            connected: Arc::new(AtomicBool::new(false)),
            stop_tx: None,
        }
    }
}

#[async_trait]
impl LiveStreamSource for ValidatorUdsSource {
    fn source_name(&self) -> &str {
        "uds"
    }

    fn environment(&self) -> &str {
        "live"
    }

    fn capability_matrix(&self) -> ProviderCapabilityMatrix {
        ProviderCapabilityMatrix::validator_uds("validator-uds")
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    async fn start(&mut self, event_tx: mpsc::Sender<CoreStreamEvent>) -> Result<(), String> {
        info!("Starting ValidatorUdsSource on socket: {}", self.socket_path);
        self.running.store(true, Ordering::Relaxed);
        self.connected.store(true, Ordering::Relaxed);

        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
        self.stop_tx = Some(stop_tx);

        let event_bus = Arc::new(EventBus::new(65_536));
        let mut bus_rx = event_bus.subscribe();

        let mut adapter = ValidatorUdsAdapter::new(&self.socket_path, Some(event_bus));
        adapter
            .connect()
            .await
            .map_err(|e| format!("Failed to bind validator UDS adapter: {}", e))?;

        let running_flag = self.running.clone();
        let connected_flag = self.connected.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        let _ = adapter.disconnect().await;
                        break;
                    }
                    ev_res = bus_rx.recv() => {
                        match ev_res {
                            Ok(ev) => {
                                if event_tx.send((*ev).clone()).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => {
                                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                            }
                        }
                    }
                }
            }
            running_flag.store(false, Ordering::Relaxed);
            connected_flag.store(false, Ordering::Relaxed);
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), String> {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        self.running.store(false, Ordering::Relaxed);
        self.connected.store(false, Ordering::Relaxed);
        Ok(())
    }
}

/// Deterministic offline simulator source for zero-network local testing.
pub struct OfflineSimulatorSource {
    tick_ms: u64,
    running: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl OfflineSimulatorSource {
    pub fn new(tick_ms: u64) -> Self {
        Self {
            tick_ms: tick_ms.max(10),
            running: Arc::new(AtomicBool::new(false)),
            connected: Arc::new(AtomicBool::new(false)),
            stop_tx: None,
        }
    }
}

impl Default for OfflineSimulatorSource {
    fn default() -> Self {
        Self::new(200)
    }
}

#[async_trait]
impl LiveStreamSource for OfflineSimulatorSource {
    fn source_name(&self) -> &str {
        "simulator"
    }

    fn environment(&self) -> &str {
        "fixture"
    }

    fn capability_matrix(&self) -> ProviderCapabilityMatrix {
        ProviderCapabilityMatrix::simulator_fixture("offline-simulator")
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    async fn start(&mut self, event_tx: mpsc::Sender<CoreStreamEvent>) -> Result<(), String> {
        info!("Starting OfflineSimulatorSource (tick={}ms)", self.tick_ms);
        self.running.store(true, Ordering::Relaxed);
        self.connected.store(true, Ordering::Relaxed);

        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
        self.stop_tx = Some(stop_tx);

        let tick_ms = self.tick_ms;
        let running_flag = self.running.clone();
        let connected_flag = self.connected.clone();
        let adapter = SimulatorFixtureAdapter::new();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(tick_ms));
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    _ = interval.tick() => {
                        let batch = adapter.emit_slot_batch();
                        for ev in batch {
                            if event_tx.send(ev).await.is_err() {
                                running_flag.store(false, Ordering::Relaxed);
                                connected_flag.store(false, Ordering::Relaxed);
                                return;
                            }
                        }
                    }
                }
            }
            running_flag.store(false, Ordering::Relaxed);
            connected_flag.store(false, Ordering::Relaxed);
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), String> {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        self.running.store(false, Ordering::Relaxed);
        self.connected.store(false, Ordering::Relaxed);
        Ok(())
    }
}

/// Manages active stream sources and supports hot-switching between RPC/WS, UDS, and simulator.
pub struct StreamSourceManager {
    current_source: RwLock<Option<Box<dyn LiveStreamSource>>>,
    event_tx: mpsc::Sender<CoreStreamEvent>,
}

impl StreamSourceManager {
    pub fn new(event_tx: mpsc::Sender<CoreStreamEvent>) -> Self {
        Self {
            current_source: RwLock::new(None),
            event_tx,
        }
    }

    pub async fn switch_source(
        &self,
        cluster_name: &str,
        source_type: &str,
        rpc_url: &str,
        ws_url: &str,
        uds_path: &str,
    ) -> Result<(), String> {
        let mut source_lock = self.current_source.write().await;
        if let Some(mut old_source) = source_lock.take() {
            let _ = old_source.stop().await;
        }

        let mut new_source: Box<dyn LiveStreamSource> = match source_type {
            "uds" | "local-validator" => Box::new(ValidatorUdsSource::new(uds_path)),
            "simulator" | "fixture" => Box::new(OfflineSimulatorSource::default()),
            _ => Box::new(RpcPubSubSource::new(cluster_name, rpc_url, ws_url)),
        };

        new_source.start(self.event_tx.clone()).await?;
        *source_lock = Some(new_source);
        Ok(())
    }

    pub async fn stop(&self) {
        let mut source_lock = self.current_source.write().await;
        if let Some(mut s) = source_lock.take() {
            let _ = s.stop().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_offline_simulator_source_emits_events_and_stops() {
        let (tx, mut rx) = mpsc::channel(64);
        let mgr = StreamSourceManager::new(tx);
        mgr.switch_source("testnet", "simulator", "", "", "/tmp/unused.sock")
            .await
            .expect("start simulator");

        let ev = tokio::time::timeout(tokio::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("timeout")
            .expect("event");
        assert!(ev.slot.as_u64() >= 1_000_000);
        mgr.stop().await;
    }
}
