//! Quickstart example: spins up an in-memory `LiveWsServer` on a random localhost port,
//! connects a WebSocket client to `/ws`, prints live slot + fork + leader frames,
//! and reconnects with `?since_seq=` to replay missed frames from the ring buffer.

use futures_util::StreamExt;
use live_ws_core::adapters::SimulatorFixtureAdapter;
use solana_live_websocket::{LiveWsServer, ServerConfig, StreamEnvelope};
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = Arc::new(LiveWsServer::new(ServerConfig::simulator()));
    let fixture = SimulatorFixtureAdapter::new();

    // Seed one initial batch before any client connects
    for ev in fixture.emit_slot_batch() {
        server.inject_event(ev).await;
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let router = server.router();
    let server_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    println!("== connected to ws://{}/ws ==", addr);
    let (mut ws, _) = connect_async(format!("ws://{}/ws", addr)).await?;

    // 1. Read snapshot-on-connect frame
    if let Some(Ok(Message::Text(txt))) = timeout(Duration::from_secs(2), ws.next()).await? {
        if let StreamEnvelope::Snapshot { sequence, snapshot } = serde_json::from_str(&txt)? {
            println!(
                "[snapshot] seq={} slot={} leader={:?} tracked_banks={}",
                sequence,
                snapshot.slot.current_slot,
                snapshot.leader.current_leader,
                snapshot.banks.total_banks_tracked
            );
        }
    }

    // 2. Push a second live slot batch while client is connected
    let batch2 = fixture.emit_slot_batch();
    let batch2_len = batch2.len();
    for ev in batch2 {
        server.inject_event(ev).await;
    }

    let mut last_seen_seq = 0u64;
    for _ in 0..4 {
        if let Some(Ok(Message::Text(txt))) = timeout(Duration::from_secs(2), ws.next()).await? {
            if let StreamEnvelope::Event { event } = serde_json::from_str(&txt)? {
                last_seen_seq = event.sequence;
                println!(
                    "[live]     seq={} slot={} type={} bank={:?}",
                    event.sequence, event.slot, event.event_type, event.bank_id
                );
            }
        }
    }
    let _ = ws.close(None).await;

    // 3. Reconnect with ?since_seq= to replay remaining frames without missing a beat
    let reconnect_url = format!(
        "ws://{}/ws?since_seq={}&snapshot=false",
        addr, last_seen_seq
    );
    println!("== reconnecting with {} (expected {} remaining frames) ==", reconnect_url, batch2_len - 4);
    let (mut ws_resume, _) = connect_async(&reconnect_url).await?;

    while let Some(Ok(Message::Text(txt))) = timeout(Duration::from_secs(2), ws_resume.next()).await? {
        match serde_json::from_str::<StreamEnvelope>(&txt)? {
            StreamEnvelope::Event { event } => {
                println!(
                    "[replay]   seq={} slot={} type={}",
                    event.sequence, event.slot, event.event_type
                );
            }
            StreamEnvelope::ReplayComplete {
                since_seq,
                replayed_count,
                latest_seq,
            } => {
                println!(
                    "[done]     replayed {} frames from seq={} up to latest_seq={}",
                    replayed_count, since_seq, latest_seq
                );
                break;
            }
            _ => {}
        }
    }

    let _ = ws_resume.close(None).await;
    server_handle.abort();
    Ok(())
}
