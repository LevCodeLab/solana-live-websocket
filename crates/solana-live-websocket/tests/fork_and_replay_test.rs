use futures_util::StreamExt;
use live_ws_core::events::{CoreEventKind, CoreStreamEvent};
use live_ws_core::identity::BankIdentity;
use live_ws_core::types::{
    BankId, Blockhash, CertificateKind, CertificateValidationStatus, ParentReference, ProviderId,
    Slot,
};
use solana_live_websocket::{LiveStateEngine, LiveWsServer, ServerConfig, StreamEnvelope};
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[test]
fn test_candidate_fork_update_parent_and_replacement_bank_lifecycle() {
    let mut engine = LiveStateEngine::new(ServerConfig::simulator());
    let provider = ProviderId::new("offline-simulator");
    let slot = Slot(750_000);

    // 1. Candidate bank-1 created on slot 750_000
    let ident_a = BankIdentity::new(
        provider.clone(),
        Some(BankId(1)),
        slot,
        Some(Blockhash::new("CandidateForkA")),
    );
    engine.process_event(CoreStreamEvent::new(
        1,
        provider.clone(),
        slot,
        Some(Blockhash::new("CandidateForkA")),
        CoreEventKind::BankCreated {
            identity: ident_a.clone(),
            parent: ParentReference::SlotOnly(Slot(749_999)),
        },
    ));

    // 2. UpdateParent clears bank-1
    let up = engine.process_event(CoreStreamEvent::new(
        2,
        provider.clone(),
        slot,
        None,
        CoreEventKind::UpdateParent {
            slot,
            cleared_bank_id: Some(BankId(1)),
            parent_slot: Slot(749_999),
            parent_block_id: Some("AuthoritativeParent_749999".to_string()),
            fec_set_index: Some(0),
            source: "fast_handover".to_string(),
        },
    ));
    assert_eq!(up.event_type, "update_parent");
    assert_eq!(up.bank_id, Some(1));

    // 3. Replacement bank-2 created and correlated
    let ident_b = BankIdentity::new(
        provider.clone(),
        Some(BankId(2)),
        slot,
        Some(Blockhash::new("CanonicalForkB")),
    );
    engine.process_event(CoreStreamEvent::new(
        3,
        provider.clone(),
        slot,
        Some(Blockhash::new("CanonicalForkB")),
        CoreEventKind::BankCreated {
            identity: ident_b.clone(),
            parent: ParentReference::KnownBlockhash(Blockhash::new("AuthoritativeParent_749999")),
        },
    ));
    engine.process_event(CoreStreamEvent::new(
        4,
        provider.clone(),
        slot,
        None,
        CoreEventKind::ReplacementBank {
            old_identity: ident_a,
            replacement_identity: ident_b.clone(),
            slot,
        },
    ));

    // 4. Certificate + canonical resolution
    engine.process_event(CoreStreamEvent::new(
        5,
        provider.clone(),
        slot,
        None,
        CoreEventKind::CertificateObserved {
            slot,
            kind: CertificateKind::FinalCert,
            raw_len: 50,
            raw_bytes: Some(vec![0x7A; 50]),
            block_id: Some("CanonicalForkB".to_string()),
            validation_status: Some(CertificateValidationStatus::ParsedUnverified),
            stake_percent: Some(86.5),
        },
    ));
    let can_ev = engine.process_event(CoreStreamEvent::new(
        6,
        provider,
        slot,
        Some(Blockhash::new("CanonicalForkB")),
        CoreEventKind::CanonicalObserved {
            canonical_identity: ident_b,
        },
    ));
    assert_eq!(can_ev.event_type, "canonical_changed");

    let snap = engine.build_snapshot(0);
    assert_eq!(snap.parent.total_update_parents, 1);
    let last_up = snap.parent.last_update_parent.expect("update_parent tracked");
    assert_eq!(last_up.cleared_bank_id, Some(1));
    assert_eq!(last_up.replacement_bank_id, Some(2));
    assert_eq!(snap.finality.stake_percent, Some(86.5));

    // Verify bank-1 is ABANDONED and bank-2 is CANONICAL in snapshot
    let b1 = snap
        .banks
        .candidate_banks
        .iter()
        .find(|b| b.raw_bank_id == Some(1))
        .expect("bank-1 retained for forensics");
    let b2 = snap
        .banks
        .candidate_banks
        .iter()
        .find(|b| b.raw_bank_id == Some(2))
        .expect("bank-2 present");
    assert_eq!(b1.state, "ABANDONED");
    assert_eq!(b2.state, "CANONICAL");
}

#[tokio::test]
async fn test_websocket_since_seq_replay_and_lagged_client_recovery() {
    let mut cfg = ServerConfig::simulator();
    cfg.ring_buffer_capacity = 4; // Keep only latest 4 events in replay ring
    let server = Arc::new(LiveWsServer::new(cfg));
    let provider = ProviderId::new("offline-simulator");

    // Inject 6 events (sequences 1..=6); ring capacity=4 retains sequences 3, 4, 5, 6
    for i in 1..=6u64 {
        server
            .inject_event(CoreStreamEvent::new(
                i,
                provider.clone(),
                Slot(50_000 + i),
                None,
                CoreEventKind::SlotObserved {
                    parent_slot: Some(Slot(49_999 + i)),
                },
            ))
            .await;
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server.router();
    let server_task = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // Case A: Reconnect with ?since_seq=4&snapshot=false -> expects events 5, 6 + ReplayComplete
    let url_replay = format!("ws://{}/api/v1/stream?since_seq=4&snapshot=false", addr);
    let (mut ws_replay, _) = connect_async(&url_replay).await.unwrap();

    let mut replayed_seqs = Vec::new();
    let mut replay_done = false;
    for _ in 0..3 {
        let msg = timeout(Duration::from_secs(2), ws_replay.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Text(txt) = msg else {
            panic!("expected text");
        };
        match serde_json::from_str::<StreamEnvelope>(&txt).unwrap() {
            StreamEnvelope::Event { event } => replayed_seqs.push(event.sequence),
            StreamEnvelope::ReplayComplete {
                since_seq,
                replayed_count,
                latest_seq,
            } => {
                assert_eq!(since_seq, 4);
                assert_eq!(replayed_count, 2);
                assert_eq!(latest_seq, 6);
                replay_done = true;
            }
            other => panic!("unexpected frame during replay: {:?}", other),
        }
    }
    assert_eq!(replayed_seqs, vec![5, 6]);
    assert!(replay_done);
    let _ = ws_replay.close(None).await;

    // Case B: Reconnect with evicted ?since_seq=1&snapshot=false -> expects Lagged recovery frame!
    let url_lagged = format!("ws://{}/ws?since_seq=1&snapshot=false", addr);
    let (mut ws_lagged, _) = connect_async(&url_lagged).await.unwrap();

    let lag_msg = timeout(Duration::from_secs(2), ws_lagged.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Text(lag_txt) = lag_msg else {
        panic!("expected text");
    };
    match serde_json::from_str::<StreamEnvelope>(&lag_txt).unwrap() {
        StreamEnvelope::Lagged {
            requested_seq,
            oldest_available_seq,
            latest_seq,
            snapshot,
            ..
        } => {
            assert_eq!(requested_seq, Some(1));
            assert_eq!(oldest_available_seq, 3);
            assert_eq!(latest_seq, 6);
            assert_eq!(snapshot.slot.current_slot, 50_006);
        }
        other => panic!("expected Lagged recovery frame, got {:?}", other),
    }

    let _ = ws_lagged.close(None).await;
    server_task.abort();
}
