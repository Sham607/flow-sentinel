use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;


// Well known Solana Programs
pub const RAYDIUM_V4_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

#[derive(Clone)]
pub struct SolanaScanner {
    rpc_url: String,
    client: Client,
}

impl SolanaScanner {
    pub fn new(rpc_url: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self { rpc_url, client }
    }

    pub async fn get_latest_slot(&self) -> Result<u64> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": [{ "commitment": "confirmed" }]
        });

        let res = self.client.post(&self.rpc_url).json(&payload).send().await?;
        let val: Value = res.json().await?;
        let slot = val["result"].as_u64().context("missing getSlot result")?;
        Ok(slot)
    }

    pub async fn fetch_recent_program_signatures(&self, program_id: &str, limit: usize) -> Result<Vec<String>> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "getSignaturesForAddress",
            "params": [
                program_id,
                { "limit": limit, "commitment": "confirmed" }
            ]
        });

        let res = self.client.post(&self.rpc_url).json(&payload).send().await?;
        let val: Value = res.json().await?;
        let mut sigs = Vec::new();

        if let Some(arr) = val["result"].as_array() {
            for item in arr {
                if let Some(sig) = item["signature"].as_str() {
                    if item["err"].is_null() {
                        sigs.push(sig.to_string());
                    }
                }
            }
        }

        Ok(sigs)
    }
}
