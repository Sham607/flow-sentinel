use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::models::{Chain, SwapRecord, SwapSide};

// Contract addresses on Robinhood Chain (ID: 4663)
pub const POOL_MANAGER_V4: &str = "0x8366a39CC670B4001A1121B8F6A443A643e40951";
pub const V4_SWAP_TOPIC: &str = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f";
pub const V3_SWAP_TOPIC: &str = "0xc42079f94a6350d7e6235f29174924f9d5fb2ce002158f77798b43cfa359d722";

#[derive(Clone)]
pub struct RobinhoodScanner {
    rpc_urls: Vec<String>,
    client: Client,
    current_idx: Arc<AtomicUsize>,
}

impl RobinhoodScanner {
    pub fn new(rpc_urls: Vec<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self {
            rpc_urls,
            client,
            current_idx: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn next_rpc(&self) -> String {
        let idx = self.current_idx.fetch_add(1, Ordering::Relaxed) % self.rpc_urls.len();
        self.rpc_urls[idx].clone()
    }

    pub async fn get_latest_block_number(&self) -> Result<u64> {
        let rpc = self.next_rpc();
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_blockNumber",
            "params": []
        });

        let res = self.client.post(&rpc).json(&payload).send().await?;
        let val: Value = res.json().await?;
        let hex_str = val["result"].as_str().context("missing blockNumber result")?;
        let block = u64::from_str_radix(hex_str.trim_start_matches("0x"), 16)?;
        Ok(block)
    }

    pub async fn fetch_swaps_in_range(&self, from_block: u64, to_block: u64) -> Result<Vec<SwapRecord>> {
        let rpc = self.next_rpc();
        let from_hex = format!("0x{:x}", from_block);
        let to_hex = format!("0x{:x}", to_block);

        // Scan both V4 and V3 swaps
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "eth_getLogs",
            "params": [{
                "fromBlock": from_hex,
                "toBlock": to_hex,
                "topics": [
                    [V4_SWAP_TOPIC, V3_SWAP_TOPIC]
                ]
            }]
        });

        let res = self.client.post(&rpc).json(&payload).send().await?;
        let val: Value = res.json().await?;
        let logs = val["result"].as_array();

        let mut out = Vec::new();
        let now = Utc::now();

        if let Some(arr) = logs {
            for item in arr {
                let pool_addr = item["address"].as_str().unwrap_or("").to_string();
                let tx_hash = item["transactionHash"].as_str().unwrap_or("").to_string();
                let block_hex = item["blockNumber"].as_str().unwrap_or("0x0");
                let block_num = u64::from_str_radix(block_hex.trim_start_matches("0x"), 16).unwrap_or(0);
                
                let topics = item["topics"].as_array();
                if topics.is_none() {
                    continue;
                }
                let topics = topics.unwrap();
                if topics.is_empty() {
                    continue;
                }

                let topic0 = topics[0].as_str().unwrap_or("");
                let is_v4 = topic0.eq_ignore_ascii_case(V4_SWAP_TOPIC);

                // Simple parser for demonstrative order flow
                // Swapper is typically topics[1] or topics[2]
                let swapper = if topics.len() > 1 {
                    let raw = topics[1].as_str().unwrap_or("");
                    if raw.len() >= 42 {
                        format!("0x{}", &raw[raw.len() - 40..])
                    } else {
                        "0x0000000000000000000000000000000000000000".to_string()
                    }
                } else {
                    "0x0000000000000000000000000000000000000000".to_string()
                };

                // Fallback default amount and side extraction
                let record = SwapRecord {
                    chain: Chain::Robinhood,
                    token_address: pool_addr.clone(),
                    token_symbol: if is_v4 { "RH-V4".to_string() } else { "RH-V3".to_string() },
                    swapper_address: swapper,
                    side: SwapSide::Buy,
                    amount_usd: 100.0, // Default estimated fill baseline
                    token_amount: 1.0,
                    pool_address: pool_addr,
                    tx_hash,
                    block_number_or_slot: block_num,
                    timestamp: now,
                };
                out.push(record);
            }
        }

        Ok(out)
    }
}
