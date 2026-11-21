use live_ws_core::adapters::{
    ClusterConfig, ProviderAdapter, ProviderCapabilities, ProviderCapabilityMatrix,
    RawSlotNotification, SimulatorFixtureAdapter, SolanaRpcClient, SolanaWsStream,
    StreamNormalizer, ValidatorUdsAdapter,
};
use live_ws_core::bus::EventBus;
use live_ws_core::clock::SlotClock;
use live_ws_core::detector::ProtocolDetector;
use live_ws_core::events::{CoreEventKind, CoreStreamEvent};
use live_ws_core::graph::{
    BankGraph, BankNode, CanonicalEvidence, CanonicalResolver, CertificateEngine, FinalityEngine,
};
use live_ws_core::identity::BankIdentity;
use live_ws_core::leader::LeaderEngine;
use live_ws_core::types::{
    utc_compact_stamp_now, utc_iso8601_now, Blockhash, CertificateKind, ProviderId, Slot,
    SlotDuration,
};
use sha2::{Digest, Sha256};
use solana_live_websocket::{LiveStateEngine, LiveWsServer, ServerConfig};
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let args: Vec<String> = env::args().collect();
    let command = args.get(1).map(|s| s.as_str()).unwrap_or("help");

    match command {
        "serve" => run_serve(&args).await?,
        "status" => run_status(&args).await?,
        "live" => run_live(&args).await?,
        "inspect" => run_inspect(&args).await?,
        "capture" => run_capture(&args).await?,
        "replay" => run_replay(&args).await?,
        "help" | "--help" | "-h" => print_help(),
        other => {
            eprintln!("Unknown command: '{}'", other);
            print_help();
            std::process::exit(1);
        }
    }

    Ok(())
}

fn print_help() {
    println!(
        r#"
solana-live-websocket - real-time Solana slot, fork, candidate bank, and leader stream

USAGE:
    solana-live-websocket <COMMAND> [OPTIONS]

COMMANDS:
    serve       Start Axum HTTP + WebSocket broadcast server (/ws and /api/v1/stream)
    status      Query live or simulated Solana cluster slot, leader window, and fork state
    live        Stream live slots and candidate bank updates directly in the terminal
    inspect     Inspect active provider field coverage and protocol state
    capture     Record validator UDS or simulator events into an NDJSON capture folder
    replay      Replay a captured events.jsonl file through LiveStateEngine
    help        Print this help message

OPTIONS:
    --host <HOST>                         Bind host (default: 127.0.0.1)
    --port <PORT>                         HTTP and WebSocket port (default: 8080)
    --cluster <testnet|devnet|mainnet>    Target Solana cluster (default: testnet)
    --simulator | --local                 Run deterministic offline simulator mode (zero network)
    --uds                                 Stream from local validator Unix Domain Socket
    --slots <N>                           Slots to observe in 'live' command (default: 5)
    --events <N>                          Max events to record in 'capture' command (default: 50)
    --out <DIR>                           Output folder for 'capture' command
    --json                                Print structured JSON output in 'inspect'
"#
    );
}

fn parse_cluster(args: &[String]) -> ClusterConfig {
    let cluster_arg = args
        .iter()
        .position(|a| a == "--cluster")
        .and_then(|idx| args.get(idx + 1))
        .map(|s| s.as_str())
        .unwrap_or("testnet");

    match cluster_arg.to_lowercase().as_str() {
        "devnet" => ClusterConfig::devnet(),
        "mainnet" | "mainnet-beta" => ClusterConfig::mainnet_beta(),
        "local" => ClusterConfig::local(),
        _ => ClusterConfig::testnet(),
    }
}

async fn run_serve(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = ServerConfig::from_env();

    if let Some(port_idx) = args.iter().position(|a| a == "--port") {
        if let Some(port_str) = args.get(port_idx + 1) {
            if let Ok(p) = port_str.parse::<u16>() {
                config.port = p;
            }
        }
    }

    if let Some(host_idx) = args.iter().position(|a| a == "--host") {
        if let Some(host_str) = args.get(host_idx + 1) {
            config.host = host_str.clone();
        }
    }

    if args.iter().any(|a| a == "--simulator" || a == "--local") {
        config.source = "simulator".to_string();
    } else if args.iter().any(|a| a == "--uds") {
        config.source = "uds".to_string();
    }

    if let Some(cluster_idx) = args.iter().position(|a| a == "--cluster") {
        if let Some(c) = args.get(cluster_idx + 1) {
            config.cluster = c.clone();
            let cluster_conf = parse_cluster(args);
            config.rpc_url = cluster_conf.rpc_url;
            config.ws_url = cluster_conf.ws_url;
        }
    }

    let server = LiveWsServer::new(config);
    server.run().await?;
    Ok(())
}

async fn run_status(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let is_sim = args.iter().any(|a| a == "--simulator" || a == "--local");
    let cluster = parse_cluster(args);

    println!("============================================================");
    println!("solana-live-websocket - cluster state status");
    println!("============================================================");

    if is_sim {
        let adapter = SimulatorFixtureAdapter::new();
        let matrix = adapter.capability_matrix();
        println!("Mode:             OFFLINE SIMULATOR [DETERMINISTIC FIXTURE]");
        println!("Provider:         {}", matrix.provider_name);
        println!("Field Coverage:   {}%", matrix.coverage_score());
        println!("bank_id Stream:   SUPPORTED [DERIVED]");
        println!("UpdateParent:     SUPPORTED [DERIVED]");
        println!("Block Footer:     SUPPORTED [DERIVED]");
        println!("Certificates:     SUPPORTED [DERIVED]");
        println!("============================================================");
        return Ok(());
    }

    let rpc_client = SolanaRpcClient::new(cluster.rpc_url.clone());
    let detector = ProtocolDetector::new();
    let mut leader_engine = LeaderEngine::new();
    let mut bank_graph = BankGraph::new();
    let canonical_resolver = CanonicalResolver::new();
    let mut finality_engine = FinalityEngine::new();

    println!(
        "Connecting to cluster: {} ({})",
        cluster.name, cluster.rpc_url
    );

    let (protocol, _) = detector.detect(&cluster.rpc_url).await.unwrap_or_default();
    let live_slot = rpc_client.get_slot().await.unwrap_or(Slot(0));
    let epoch_info = rpc_client.get_epoch_info().await.ok();

    let current_leader = rpc_client.get_slot_leader().await.ok();
    if let Some(ref leader) = current_leader {
        leader_engine.set_leader(live_slot, leader.clone());
    }
    if let Ok(leaders) = rpc_client.get_slot_leaders(live_slot.as_u64(), 10).await {
        leader_engine.populate_schedule(live_slot, leaders);
    }
    let next_leader = leader_engine.next_leader(live_slot);

    let provider_id = ProviderId::new(format!("{}-rpc", cluster.name));
    let blockhash = Blockhash::new(format!("ObservedSlotBlockhash_{}", live_slot));
    let identity = BankIdentity::new(
        provider_id.clone(),
        None,
        live_slot,
        Some(blockhash.clone()),
    );
    let bank_key = bank_graph.insert_bank(BankNode::new(identity, None, 1_000_000_000));
    let evidence = CanonicalEvidence::ConfirmedBlockhash(blockhash);
    let canonical_res = canonical_resolver.resolve_slot(
        &mut bank_graph,
        live_slot,
        Some(&evidence),
        1_150_000_000,
    );
    finality_engine.record_finality(&mut bank_graph, &bank_key, 1_350_000_000);

    let capabilities = ProviderCapabilities::standard_public_rpc();
    let matrix = ProviderCapabilityMatrix::standard_public_rpc(provider_id.as_str());

    println!("------------------------------------------------------------");
    println!("Cluster:          {} ({})", cluster.name, cluster.rpc_url);
    println!("Protocol Mode:    {}", protocol.consensus_mode);
    println!("Current Slot:     {}", live_slot);
    if let Some(ref ep) = epoch_info {
        println!(
            "Epoch:            {} ({}/{})",
            ep.epoch, ep.slot_index, ep.slots_in_epoch
        );
    }
    println!("Slot Duration:    {}", protocol.target_slot_duration);
    println!(
        "Current Leader:   {}",
        current_leader
            .map(|l| l.0)
            .unwrap_or_else(|| "UNKNOWN".into())
    );
    println!(
        "Next Leader:      {}",
        next_leader.map(|l| l.0).unwrap_or_else(|| "UNKNOWN".into())
    );
    println!("Field Coverage:   {}%", matrix.coverage_score());
    println!("Banks Tracked:    {}", bank_graph.total_banks());
    println!(
        "bank_id Stream:   {}",
        if capabilities.bank_id_stream {
            "SUPPORTED"
        } else {
            "OMITTED BY PUBLIC RPC"
        }
    );
    println!("Canonical State:  {:?}", canonical_res);
    println!("============================================================");
    Ok(())
}

async fn run_live(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let is_sim = args.iter().any(|a| a == "--simulator" || a == "--local");
    let cluster = parse_cluster(args);
    let target_slots: u64 = args
        .iter()
        .position(|a| a == "--slots")
        .and_then(|idx| args.get(idx + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    println!("============================================================");
    println!("solana-live-websocket - live slot stream");
    println!("============================================================");

    if is_sim {
        let adapter = SimulatorFixtureAdapter::new();
        let mut engine = LiveStateEngine::new(ServerConfig::simulator());
        for _ in 0..target_slots {
            for ev in adapter.emit_slot_batch() {
                let wire = engine.process_event(ev);
                println!(
                    "[seq={}] slot={} type={} bank={:?}",
                    wire.sequence, wire.slot, wire.event_type, wire.bank_id
                );
            }
        }
        println!("------------------------------------------------------------");
        println!(
            "Streamed {} simulated slots cleanly (latest seq={}).",
            target_slots,
            engine.current_sequence()
        );
        println!("============================================================");
        return Ok(());
    }

    println!("Subscribing to: {}", cluster.ws_url);
    let ws_stream = SolanaWsStream::new(cluster.ws_url.clone());
    let (tx, mut rx) = mpsc::channel::<RawSlotNotification>(100);
    ws_stream.start(tx);

    let provider_id = ProviderId::new(format!("{}-ws", cluster.name));
    let mut normalizer = StreamNormalizer::new(provider_id.clone(), 1000);
    let mut clock = SlotClock::new(SlotDuration::MS_200);
    let mut bank_graph = BankGraph::new();
    let mut slots_seen = 0u64;

    while let Some(msg) = rx.recv().await {
        if let Some(event) = normalizer.normalize_slot(msg) {
            slots_seen += 1;
            clock.tick(event.slot);
            let progress = clock.progress();
            let ident = BankIdentity::new(provider_id.clone(), None, event.slot, None);
            bank_graph.insert_bank(BankNode::new(
                ident,
                None,
                event.received_time_ms * 1_000_000,
            ));

            println!(
                "slot={} elapsed={}ms phase={:.1}% banks={} provider={}",
                progress.slot,
                progress.elapsed_ms,
                progress.progress * 100.0,
                bank_graph.total_banks(),
                event.provider
            );

            if slots_seen >= target_slots {
                ws_stream.stop();
                break;
            }
        }
    }

    Ok(())
}

async fn run_inspect(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let is_sim = args.iter().any(|a| a == "--simulator" || a == "--local");
    let is_uds = args.iter().any(|a| a == "--uds");
    let is_json = args.iter().any(|a| a == "--json");
    let cluster = parse_cluster(args);

    let matrix = if is_uds {
        ProviderCapabilityMatrix::validator_uds("validator-uds")
    } else if is_sim {
        ProviderCapabilityMatrix::simulator_fixture("offline-simulator")
    } else {
        ProviderCapabilityMatrix::standard_public_rpc(format!("{}-rpc", cluster.name))
    };

    let detailed = matrix.to_detailed_matrix(Default::default(), true);
    if is_json {
        println!("{}", serde_json::to_string_pretty(&detailed)?);
        return Ok(());
    }

    let cert_engine = CertificateEngine::new();
    let sample_cert = cert_engine.parse_certificate(
        CertificateKind::FinalCert,
        Slot(1_000_000),
        Some("SampleBlock_1000000".to_string()),
        &matrix.provider_name,
        &[0x7A; 50],
        1_000_000,
    );

    println!("============================================================");
    println!("solana-live-websocket - provider field inspector");
    println!("============================================================");
    println!("Active Provider:  {}", detailed.current_source);
    println!("Telemetry Level:  {}", detailed.telemetry_level);
    println!("Coverage Score:   {}%", detailed.core_coverage_percent);
    println!("Cert Decoder:     {:?}", sample_cert.decode_status);
    println!("Timestamp UTC:    {}", utc_iso8601_now());
    println!("------------------------------------------------------------");
    for f in &detailed.fields {
        println!(
            "{:<22} | supported={:<5} | source={:<12}",
            f.field, f.source_supported, f.provenance
        );
    }
    println!("============================================================");
    Ok(())
}

async fn run_capture(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let recording_id = format!("rec_{}", utc_compact_stamp_now());
    let default_out = format!("artifacts/captures/{}", recording_id);
    let out_dir = args
        .iter()
        .position(|a| a == "--out" || a == "-o")
        .and_then(|idx| args.get(idx + 1))
        .cloned()
        .unwrap_or(default_out);

    let max_events: usize = args
        .iter()
        .position(|a| a == "--events" || a == "-n")
        .and_then(|idx| args.get(idx + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    fs::create_dir_all(&out_dir)?;
    let t_start = Instant::now();
    let mut captured_events: Vec<CoreStreamEvent> = Vec::new();

    let uds_path = env::var("VALIDATOR_UDS_PATH")
        .unwrap_or_else(|_| "/tmp/solana_validator_stream.sock".to_string());
    let is_uds_available = Path::new(&uds_path).exists();

    if is_uds_available {
        let bus = Arc::new(EventBus::new(1024));
        let mut rx = bus.subscribe();
        let mut adapter = ValidatorUdsAdapter::new(&uds_path, Some(bus));
        if adapter.connect().await.is_ok() {
            let deadline = Instant::now() + std::time::Duration::from_secs(5);
            while captured_events.len() < max_events && Instant::now() < deadline {
                if let Ok(Ok(ev)) =
                    tokio::time::timeout(std::time::Duration::from_millis(300), rx.recv()).await
                {
                    captured_events.push((*ev).clone());
                }
            }
            let _ = adapter.disconnect().await;
        }
    }

    if captured_events.is_empty() {
        let adapter = SimulatorFixtureAdapter::new();
        while captured_events.len() < max_events {
            for ev in adapter.emit_slot_batch() {
                captured_events.push(ev);
                if captured_events.len() >= max_events {
                    break;
                }
            }
        }
    }

    let events_file_path = format!("{}/events.jsonl", out_dir);
    let mut events_file = fs::File::create(&events_file_path)?;
    for ev in &captured_events {
        writeln!(events_file, "{}", serde_json::to_string(ev)?)?;
    }

    let metadata = serde_json::json!({
        "recording_id": recording_id,
        "captured_at": utc_iso8601_now(),
        "duration_ms": t_start.elapsed().as_millis() as u64,
        "total_events": captured_events.len(),
        "source": if is_uds_available { "uds" } else { "simulator" },
    });
    fs::write(
        format!("{}/metadata.json", out_dir),
        serde_json::to_string_pretty(&metadata)?,
    )?;

    println!(
        "Captured {} events into {}",
        captured_events.len(),
        events_file_path
    );
    Ok(())
}

async fn run_replay(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let target = args.get(2).map(|s| s.as_str()).unwrap_or("");
    let file_path = if target.ends_with(".jsonl") {
        PathBuf::from(target)
    } else if !target.is_empty() {
        PathBuf::from(target).join("events.jsonl")
    } else {
        eprintln!("Usage: solana-live-websocket replay <path/to/events.jsonl>");
        return Ok(());
    };

    if !file_path.exists() {
        eprintln!("Capture file not found: {}", file_path.display());
        return Ok(());
    }

    let file = fs::File::open(&file_path)?;
    let reader = BufReader::new(file);
    let mut engine = LiveStateEngine::new(ServerConfig::simulator());
    let t_start = Instant::now();
    let mut replayed_count = 0usize;

    for line_res in reader.lines() {
        let line = line_res?;
        if line.trim().is_empty() {
            continue;
        }
        let event: CoreStreamEvent = serde_json::from_str(&line)?;
        let _ = engine.process_event(event);
        replayed_count += 1;
    }

    let snapshot = engine.build_snapshot(0);
    let digest_input = serde_json::json!({
        "events_replayed": replayed_count,
        "final_slot": snapshot.slot.current_slot,
        "total_banks": snapshot.banks.total_banks_tracked,
        "total_update_parents": snapshot.parent.total_update_parents,
    });
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&digest_input)?);
    let digest = format!("{:x}", hasher.finalize());

    let mut slot_events = 0usize;
    for rev in &snapshot.recent_events {
        if rev.event_type == "slot_update" {
            slot_events += 1;
        }
    }
    let _ = CoreEventKind::SlotObserved { parent_slot: None };

    println!("============================================================");
    println!("solana-live-websocket - deterministic replay summary");
    println!("============================================================");
    println!("Events Replayed:      {}", replayed_count);
    println!("Elapsed:              {:.2?}", t_start.elapsed());
    println!("Final Slot:           {}", snapshot.slot.current_slot);
    println!("Banks Tracked:        {}", snapshot.banks.total_banks_tracked);
    println!("Recent Slot Frames:   {}", slot_events);
    println!("UpdateParent Count:   {}", snapshot.parent.total_update_parents);
    println!("State SHA-256 Digest: {}", digest);
    println!("============================================================");

    Ok(())
}
