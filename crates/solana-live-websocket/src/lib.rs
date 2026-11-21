//! Real-time Axum WebSocket server and CLI streaming Solana slots, forks,
//! candidate banks, and leader handoff windows.

pub mod api;
pub mod config;
pub mod envelope;
pub mod service;
pub mod simulator;
pub mod snapshot;
pub mod source;
pub mod state;
pub mod ws;

pub use config::ServerConfig;
pub use envelope::{ClientControlMessage, EventProvenance, LiveStreamEvent, StreamEnvelope};
pub use live_ws_core::CORE_SCHEMA_VERSION;
pub use service::LiveWsServer;
pub use simulator::ForkSimulator;
pub use snapshot::LiveClusterSnapshot;
pub use source::StreamSourceManager;
pub use state::LiveStateEngine;
