use crate::types::{ConsensusMode, ProtocolProfile, SlotDuration};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fmt;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, info, warn};

/// Alpenglow Genesis Certificate block reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgGenesisBlock {
    #[serde(rename = "blockId")]
    pub block_id: Vec<u8>,
    pub slot: u64,
}

/// Alpenglow aggregate signature and participation bitmap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgGenesisSignature {
    pub bitmap: Vec<u8>,
    pub signature: Vec<u8>,
}

/// Verified Alpenglow Genesis Certificate returned by `getAgGenesisCert`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgGenesisCert {
    pub block: AgGenesisBlock,
    pub signature: AgGenesisSignature,
}

/// Result of querying `getAgGenesisCert` on a Solana RPC endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GenesisCertResponse {
    Certificate(AgGenesisCert),
    Null,
    MethodUnavailable,
    Error(String),
}

impl GenesisCertResponse {
    pub fn is_alpenglow_active(&self) -> bool {
        matches!(self, Self::Certificate(_))
    }
}

/// Explicit capability state for protocol and streaming features.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityState {
    Available,
    Unavailable,
    Unknown,
}

impl fmt::Display for CapabilityState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Available => write!(f, "AVAILABLE"),
            Self::Unavailable => write!(f, "UNAVAILABLE"),
            Self::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// Consensus and streaming capability set detected on the target cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilitySet {
    pub alpenglow_votor: CapabilityState,
    pub genesis_certificate: CapabilityState,
    pub bank_aware_stream: CapabilityState,
    pub update_parent: CapabilityState,
    pub block_footer: CapabilityState,
    pub finality_evidence: CapabilityState,
}

impl Default for CapabilitySet {
    fn default() -> Self {
        Self {
            alpenglow_votor: CapabilityState::Unknown,
            genesis_certificate: CapabilityState::Unknown,
            bank_aware_stream: CapabilityState::Unknown,
            update_parent: CapabilityState::Unknown,
            block_footer: CapabilityState::Unknown,
            finality_evidence: CapabilityState::Unknown,
        }
    }
}

#[derive(Debug, Error)]
pub enum DetectorError {
    #[error("HTTP transport error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON RPC error: {0}")]
    JsonRpc(String),
    #[error("Invalid cluster response: {0}")]
    InvalidResponse(String),
}

/// Protocol and capability detector querying official Solana JSON-RPC methods.
pub struct ProtocolDetector {
    client: Client,
}

impl ProtocolDetector {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_default(),
        }
    }

    pub async fn detect(
        &self,
        rpc_url: &str,
    ) -> Result<(ProtocolProfile, CapabilitySet), DetectorError> {
        let (version_str, _feature_set) = self.fetch_version(rpc_url).await?;
        let genesis_cert_resp = self.fetch_ag_genesis_cert(rpc_url).await;

        let mut capabilities = CapabilitySet::default();
        let mut profile = ProtocolProfile::default();

        match genesis_cert_resp {
            GenesisCertResponse::Certificate(cert) => {
                info!(
                    "Alpenglow Genesis Certificate detected: slot={}, block_id_len={}, sig_len={}",
                    cert.block.slot,
                    cert.block.block_id.len(),
                    cert.signature.signature.len()
                );
                capabilities.genesis_certificate = CapabilityState::Available;
                capabilities.alpenglow_votor = CapabilityState::Available;
                capabilities.bank_aware_stream = CapabilityState::Available;
                capabilities.update_parent = CapabilityState::Available;
                capabilities.block_footer = CapabilityState::Available;
                capabilities.finality_evidence = CapabilityState::Available;

                profile.consensus_mode = ConsensusMode::AlpenglowVotor;
                profile.alpenglow_genesis_cert_present = true;
                profile.multi_bank_enabled = true;
                profile.update_parent_enabled = true;
            }
            GenesisCertResponse::Null | GenesisCertResponse::MethodUnavailable => {
                debug!("getAgGenesisCert unavailable or null on node");
                capabilities.genesis_certificate = CapabilityState::Unavailable;
                capabilities.alpenglow_votor = CapabilityState::Unavailable;
                profile.consensus_mode = ConsensusMode::LegacyTowerBFT;
                profile.alpenglow_genesis_cert_present = false;
            }
            GenesisCertResponse::Error(err) => {
                warn!("getAgGenesisCert query error: {}", err);
                capabilities.genesis_certificate = CapabilityState::Unknown;
                capabilities.alpenglow_votor = CapabilityState::Unknown;
                profile.consensus_mode = ConsensusMode::Unknown;
            }
        }

        if version_str.contains("legacy") || version_str.contains("pre-simd0525") {
            profile.target_slot_duration = SlotDuration::MS_400;
        } else {
            profile.target_slot_duration = SlotDuration::MS_200;
        }

        Ok((profile, capabilities))
    }

    async fn fetch_version(&self, rpc_url: &str) -> Result<(String, Option<u64>), DetectorError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getVersion"
        });

        let resp = self
            .client
            .post(rpc_url)
            .json(&payload)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        if let Some(err) = resp.get("error") {
            return Err(DetectorError::JsonRpc(err.to_string()));
        }

        let result = resp
            .get("result")
            .ok_or_else(|| DetectorError::InvalidResponse("Missing result".into()))?;

        let version = result
            .get("solana-core")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let feature_set = result.get("feature-set").and_then(|f| f.as_u64());
        Ok((version, feature_set))
    }

    pub async fn fetch_ag_genesis_cert(&self, rpc_url: &str) -> GenesisCertResponse {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAgGenesisCert"
        });

        let resp = match self.client.post(rpc_url).json(&payload).send().await {
            Ok(r) => match r.json::<serde_json::Value>().await {
                Ok(v) => v,
                Err(e) => return GenesisCertResponse::Error(e.to_string()),
            },
            Err(e) => return GenesisCertResponse::Error(e.to_string()),
        };

        if let Some(err) = resp.get("error") {
            let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
            if code == -32601 {
                return GenesisCertResponse::MethodUnavailable;
            }
            return GenesisCertResponse::Error(err.to_string());
        }

        match resp.get("result") {
            Some(res) if res.is_null() => GenesisCertResponse::Null,
            Some(res) => match serde_json::from_value::<AgGenesisCert>(res.clone()) {
                Ok(cert) => GenesisCertResponse::Certificate(cert),
                Err(e) => GenesisCertResponse::Error(format!("Certificate parse error: {}", e)),
            },
            None => GenesisCertResponse::Null,
        }
    }
}

impl Default for ProtocolDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method_unavailable_never_claims_alpenglow() {
        assert!(!GenesisCertResponse::MethodUnavailable.is_alpenglow_active());
        assert!(!GenesisCertResponse::Null.is_alpenglow_active());
    }
}
