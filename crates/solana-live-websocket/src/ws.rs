use crate::api::AppState;
use crate::envelope::{ClientControlMessage, LiveStreamEvent, StreamEnvelope};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query,
    },
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use live_ws_core::types::now_unix_ms;
use serde::Deserialize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{info, warn};

#[derive(Debug, Default, Deserialize)]
pub struct WsStreamQuery {
    pub since_seq: Option<u64>,
    pub snapshot: Option<bool>,
}

pub fn ws_routes(
    state: AppState,
    broadcast_tx: broadcast::Sender<LiveStreamEvent>,
) -> Router {
    let state_ws = state.clone();
    let tx_ws = broadcast_tx.clone();
    let state_v1 = state;
    let tx_v1 = broadcast_tx;

    Router::new()
        .route(
            "/ws",
            get(
                move |Query(query): Query<WsStreamQuery>, ws: WebSocketUpgrade| {
                    let st = state_ws.clone();
                    let tx = tx_ws.clone();
                    async move { ws.on_upgrade(move |socket| handle_socket(socket, st, tx, query)) }
                },
            ),
        )
        .route(
            "/api/v1/stream",
            get(
                move |Query(query): Query<WsStreamQuery>, ws: WebSocketUpgrade| {
                    let st = state_v1.clone();
                    let tx = tx_v1.clone();
                    async move { ws.on_upgrade(move |socket| handle_socket(socket, st, tx, query)) }
                },
            ),
        )
}

async fn handle_socket(
    socket: WebSocket,
    state: AppState,
    broadcast_tx: broadcast::Sender<LiveStreamEvent>,
    query: WsStreamQuery,
) {
    let prev_clients = state.ws_clients.fetch_add(1, Ordering::Relaxed) as usize;
    if prev_clients >= state.config.max_ws_clients {
        state.ws_clients.fetch_sub(1, Ordering::Relaxed);
        let (mut sender, _) = socket.split();
        let _ = sender.send(Message::Close(None)).await;
        return;
    }

    // Subscribe before taking snapshot/replay so zero events slip between initial sync and live stream.
    let mut broadcast_rx = broadcast_tx.subscribe();
    let (mut sender, mut receiver) = socket.split();

    info!(
        "WebSocket client connected (active={}, since_seq={:?})",
        prev_clients + 1,
        query.since_seq
    );

    let mut highest_sent_seq: u64 = 0;
    let send_initial_snapshot = query.snapshot.unwrap_or(true);

    if send_initial_snapshot {
        let ws_count = state.ws_clients.load(Ordering::Relaxed) as usize;
        let snap = state.engine.read().await.build_snapshot(ws_count);
        // Only advance deduplication watermark if no explicit replay was requested.
        if query.since_seq.is_none() {
            highest_sent_seq = snap.sequence;
        }
        let env = StreamEnvelope::Snapshot {
            sequence: snap.sequence,
            snapshot: snap,
        };
        if send_envelope(&mut sender, &env).await.is_err() {
            state.ws_clients.fetch_sub(1, Ordering::Relaxed);
            return;
        }
    }

    if let Some(since_seq) = query.since_seq {
        if replay_or_recover(&state, &mut sender, since_seq, &mut highest_sent_seq)
            .await
            .is_err()
        {
            state.ws_clients.fetch_sub(1, Ordering::Relaxed);
            return;
        }
    }

    let heartbeat_ms = state.config.heartbeat_interval_ms.max(100);
    let mut ping_interval = tokio::time::interval(Duration::from_millis(heartbeat_ms));
    ping_interval.tick().await;

    loop {
        tokio::select! {
            event_res = broadcast_rx.recv() => {
                match event_res {
                    Ok(event) => {
                        if event.sequence <= highest_sent_seq {
                            continue;
                        }
                        highest_sent_seq = event.sequence;
                        let env = StreamEnvelope::Event { event };
                        if send_envelope(&mut sender, &env).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(dropped)) => {
                        warn!("WebSocket client lagged, sending recovery snapshot (dropped={})", dropped);
                        let ws_count = state.ws_clients.load(Ordering::Relaxed) as usize;
                        let (oldest, snap) = {
                            let eng = state.engine.read().await;
                            (eng.oldest_buffered_sequence(), eng.build_snapshot(ws_count))
                        };
                        highest_sent_seq = snap.sequence;
                        let recovery = StreamEnvelope::Lagged {
                            requested_seq: None,
                            oldest_available_seq: oldest,
                            latest_seq: snap.sequence,
                            dropped_events: dropped,
                            message: format!(
                                "Client lagged by {} broadcast frames; fresh cluster snapshot attached",
                                dropped
                            ),
                            snapshot: snap,
                        };
                        if send_envelope(&mut sender, &recovery).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }

            client_msg = receiver.next() => {
                match client_msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(parsed) = serde_json::from_str::<ClientControlMessage>(&text) {
                            match parsed {
                                ClientControlMessage::Hello { since_seq: Some(seq), .. }
                                | ClientControlMessage::Replay { since_seq: seq } => {
                                    if replay_or_recover(&state, &mut sender, seq, &mut highest_sent_seq).await.is_err() {
                                        break;
                                    }
                                }
                                ClientControlMessage::Ping { timestamp_ms } => {
                                    let pong = StreamEnvelope::Pong {
                                        timestamp_ms: timestamp_ms.unwrap_or_else(now_unix_ms),
                                    };
                                    if send_envelope(&mut sender, &pong).await.is_err() {
                                        break;
                                    }
                                }
                                ClientControlMessage::Hello { since_seq: None, .. }
                                | ClientControlMessage::Pong => {}
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if sender.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }

            _ = ping_interval.tick() => {
                let ws_count = state.ws_clients.load(Ordering::Relaxed) as usize;
                let (seq, slot) = {
                    let eng = state.engine.read().await;
                    let snap = eng.build_snapshot(ws_count);
                    (snap.sequence, snap.slot.current_slot)
                };
                let hb = StreamEnvelope::Heartbeat {
                    sequence: seq,
                    slot,
                    timestamp_ms: now_unix_ms(),
                    ws_clients: ws_count,
                };
                if send_envelope(&mut sender, &hb).await.is_err() {
                    break;
                }
            }
        }
    }

    state.ws_clients.fetch_sub(1, Ordering::Relaxed);
    info!("WebSocket client disconnected");
}

async fn replay_or_recover(
    state: &AppState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    since_seq: u64,
    highest_sent_seq: &mut u64,
) -> Result<(), ()> {
    let ws_count = state.ws_clients.load(Ordering::Relaxed) as usize;
    let outcome = {
        let eng = state.engine.read().await;
        match eng.replay_since(since_seq) {
            Ok(events) => Ok((events, eng.current_sequence())),
            Err(oldest) => Err((oldest, eng.build_snapshot(ws_count))),
        }
    };

    match outcome {
        Ok((replayed_events, latest_seq)) => {
            let count = replayed_events.len();
            for rev in replayed_events {
                if rev.sequence > *highest_sent_seq {
                    *highest_sent_seq = rev.sequence;
                }
                let env = StreamEnvelope::Event { event: rev };
                send_envelope(sender, &env).await?;
            }
            if latest_seq > *highest_sent_seq {
                *highest_sent_seq = latest_seq;
            }
            let done = StreamEnvelope::ReplayComplete {
                since_seq,
                replayed_count: count,
                latest_seq,
            };
            send_envelope(sender, &done).await
        }
        Err((oldest, snap)) => {
            *highest_sent_seq = snap.sequence;
            let dropped = oldest.saturating_sub(since_seq.saturating_add(1));
            let gap = StreamEnvelope::Lagged {
                requested_seq: Some(since_seq),
                oldest_available_seq: oldest,
                latest_seq: snap.sequence,
                dropped_events: dropped,
                message: format!(
                    "Requested since_seq={} is older than ring buffer oldest={}; attached full snapshot",
                    since_seq, oldest
                ),
                snapshot: snap,
            };
            send_envelope(sender, &gap).await
        }
    }
}

async fn send_envelope(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    envelope: &StreamEnvelope,
) -> Result<(), ()> {
    let json = serde_json::to_string(envelope).map_err(|_| ())?;
    sender.send(Message::Text(json)).await.map_err(|_| ())
}
