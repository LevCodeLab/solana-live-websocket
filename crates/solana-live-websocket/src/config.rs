use live_ws_core::adapters::ClusterConfig;
use serde::{Deserialize, Serialize};
use std::env;

/// Centralized configuration for `solana-live-websocket`.
///
/// Default bind address is strictly `127.0.0.1:8080` (local loopback).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub cluster: String,
    pub rpc_url: String,
    pub ws_url: String,
    pub validator_uds_path: String,
    pub source: String, // "rpc", "uds", "simulator"
    pub ring_buffer_capacity: usize,
    pub max_ws_clients: usize,
    pub heartbeat_interval_ms: u64,
    pub log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let cluster_conf = ClusterConfig::testnet();
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            cluster: "testnet".to_string(),
            rpc_url: cluster_conf.rpc_url,
            ws_url: cluster_conf.ws_url,
            validator_uds_path: "/tmp/solana_validator_stream.sock".to_string(),
            source: "rpc".to_string(),
            ring_buffer_capacity: 10_000,
            max_ws_clients: 100,
            heartbeat_interval_ms: 15_000,
            log_level: "info".to_string(),
        }
    }
}

impl ServerConfig {
    /// Loads configuration from environment variables with safe loopback defaults.
    pub fn from_env() -> Self {
        let cluster = env::var("LIVE_WS_CLUSTER").unwrap_or_else(|_| "testnet".to_string());
        let default_cluster_conf = match cluster.to_lowercase().as_str() {
            "devnet" => ClusterConfig::devnet(),
            "mainnet" | "mainnet-beta" => ClusterConfig::mainnet_beta(),
            "local" => ClusterConfig::local(),
            _ => ClusterConfig::testnet(),
        };

        let host = env::var("LIVE_WS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = env::var("LIVE_WS_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8080);

        let rpc_url = env::var("SOLANA_RPC_URL").unwrap_or(default_cluster_conf.rpc_url);
        let ws_url = env::var("SOLANA_WS_URL").unwrap_or(default_cluster_conf.ws_url);

        let validator_uds_path = env::var("VALIDATOR_UDS_PATH")
            .unwrap_or_else(|_| "/tmp/solana_validator_stream.sock".to_string());

        let source = env::var("LIVE_WS_SOURCE").unwrap_or_else(|_| "rpc".to_string());

        let ring_buffer_capacity = env::var("LIVE_WS_RING_CAPACITY")
            .ok()
            .and_then(|b| b.parse().ok())
            .unwrap_or(10_000);

        let max_ws_clients = env::var("LIVE_WS_MAX_CLIENTS")
            .ok()
            .and_then(|m| m.parse().ok())
            .unwrap_or(100);

        let heartbeat_interval_ms = env::var("LIVE_WS_HEARTBEAT_MS")
            .ok()
            .and_then(|h| h.parse().ok())
            .unwrap_or(15_000);

        let log_level = env::var("LIVE_WS_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());

        Self {
            host,
            port,
            cluster,
            rpc_url,
            ws_url,
            validator_uds_path,
            source,
            ring_buffer_capacity,
            max_ws_clients,
            heartbeat_interval_ms,
            log_level,
        }
    }

    pub fn simulator() -> Self {
        Self {
            source: "simulator".to_string(),
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_server_config_loopback_and_port() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 8080);
        assert_eq!(cfg.validator_uds_path, "/tmp/solana_validator_stream.sock");
        assert_eq!(cfg.ring_buffer_capacity, 10_000);
    }
}
