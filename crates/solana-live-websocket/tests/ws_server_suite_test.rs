use axum::{body::Body, http::Request};
use futures_util::{SinkExt, StreamExt};
use live_ws_core::events::{CoreEventKind, CoreStreamEvent};
use live_ws_core::types::{LeaderId, ProviderId, Slot};
use solana_live_websocket::{
    ClientControlMessage, EventProvenance, LiveStateEngine, LiveWsServer, ServerConfig,
    StreamEnvelope,
};
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tower::ServiceExt;

#[tokio::test]
async fn test_rest_endpoints_health_snapshot_slots_forks_leaders_capabilities() {
    let server = LiveWsServer::new(ServerConfig::simulator());
    let provider = ProviderId::new("offline-simulator");

    server
        .inject_event(CoreStreamEvent::new(
            1,
            provider.clone(),
            Slot(900_001),
            None,
            CoreEventKind::SlotObserved {
                parent_slot: Some(Slot(900_000)),
            },
        ))
        .await;
    server
        .inject_event(CoreStreamEvent::new(
            2,
            provider,
            Slot(900_001),
            None,
            CoreEventKind::LeaderObserved {
                leader: LeaderId::new("ValidatorAlpha11111111111111111111111111111"),
            },
        ))
        .await;

    let router = server.router();

    for path in [
        "/health",
        "/api/v1/health",
        "/api/v1/status",
        "/api/v1/snapshot",
        "/api/v1/slots",
        "/api/v1/forks",
        "/api/v1/leaders",
        "/api/v1/capabilities",
        "/api/v1/replay?since_seq=1",
    ] {
        let req = Request::builder().uri(path).body(Body::empty()).unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::OK,
            "endpoint failed: {}",
            path
        );
    }
}

#[tokio::test]
async fn test_public_rpc_capability_score_and_provenance() {
    let mut cfg = ServerConfig::default();
    cfg.source = "rpc".to_string();
    cfg.cluster = "testnet".to_string();
    let mut engine = LiveStateEngine::new(cfg);

    let s = engine.process_event(CoreStreamEvent::new(
        1,
        ProviderId::new("testnet-rpc"),
        Slot(2_000),
        None,
        CoreEventKind::SlotObserved {
            parent_slot: Some(Slot(1_999)),
        },
    ));

    assert_eq!(s.provenance, EventProvenance::DIRECT);
    assert_eq!(s.event_type, "slot_update");

    let snap = engine.build_snapshot(0);
    assert_eq!(snap.capabilities.coverage_score, 27);
    assert!(snap.capabilities.limitations.len() >= 3);
}

#[tokio::test]
async fn test_websocket_snapshot_on_connect_live_events_and_ping_pong() {
    let mut cfg = ServerConfig::simulator();
    cfg.heartbeat_interval_ms = 250;
    let server = Arc::new(LiveWsServer::new(cfg));

    // Seed one slot prior to WebSocket client connecting
    server
        .inject_event(CoreStreamEvent::new(
            1,
            ProviderId::new("offline-simulator"),
            Slot(42_000),
            None,
            CoreEventKind::SlotObserved {
                parent_slot: Some(Slot(41_999)),
            },
        ))
        .await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server.router();
    let server_task = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let ws_url = format!("ws://{}/ws", addr);
    let (mut ws_stream, _) = connect_async(&ws_url).await.expect("connect /ws");

    // 1. First frame on connect must be Snapshot at sequence 1
    let first_msg = timeout(Duration::from_secs(2), ws_stream.next())
        .await
        .expect("snapshot timeout")
        .expect("stream open")
        .expect("ws text");
    let Message::Text(first_txt) = first_msg else {
        panic!("expected text frame");
    };
    let snap_env: StreamEnvelope = serde_json::from_str(&first_txt).expect("parse envelope");
    match snap_env {
        StreamEnvelope::Snapshot { sequence, snapshot } => {
            assert_eq!(sequence, 1);
            assert_eq!(snapshot.slot.current_slot, 42_000);
        }
        other => panic!("expected Snapshot, got {:?}", other),
    }

    // 2. Inject a live slot + leader event and verify real-time push
    server
        .inject_event(CoreStreamEvent::new(
            2,
            ProviderId::new("offline-simulator"),
            Slot(42_001),
            None,
            CoreEventKind::LeaderObserved {
                leader: LeaderId::new("ValidatorBeta111111111111111111111111111111"),
            },
        ))
        .await;

    let live_msg = timeout(Duration::from_secs(2), ws_stream.next())
        .await
        .expect("live event timeout")
        .expect("stream open")
        .expect("ws text");
    let Message::Text(live_txt) = live_msg else {
        panic!("expected text frame");
    };
    let live_env: StreamEnvelope = serde_json::from_str(&live_txt).expect("parse live event");
    match live_env {
        StreamEnvelope::Event { event } => {
            assert_eq!(event.sequence, 2);
            assert_eq!(event.slot, 42_001);
            assert_eq!(event.event_type, "leader_handoff");
        }
        other => panic!("expected Event, got {:?}", other),
    }

    // 3. Send application Ping control frame and verify Pong response
    let ping_json = serde_json::to_string(&ClientControlMessage::Ping {
        timestamp_ms: Some(123_456),
    })
    .unwrap();
    ws_stream.send(Message::Text(ping_json)).await.unwrap();

    let mut saw_pong = false;
    for _ in 0..4 {
        let next_frame = timeout(Duration::from_secs(2), ws_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(txt) = next_frame {
            if let Ok(StreamEnvelope::Pong { timestamp_ms }) =
                serde_json::from_str::<StreamEnvelope>(&txt)
            {
                assert_eq!(timestamp_ms, 123_456);
                saw_pong = true;
                break;
            }
        }
    }
    assert!(saw_pong, "expected Pong response from WebSocket server");

    let _ = ws_stream.close(None).await;
    server_task.abort();
}
