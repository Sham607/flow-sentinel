use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::models::{Chain, SwapRecord, SwapSide};

// Contract addresses on Robinhood Chain (ID: 4663)
pub const POOL_MANAGER_V4: &str = "0x8366a39CC670B4001A1121B8F6A443A643e40951";
pub const V4_SWAP_TOPIC: &str = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f";
pub const V3_SWAP_TOPIC: &str = "0xc42079f94a6350d7e6235f29174924f9d5fb2ce002158f77798b43cfa359d722";
pub const ERC20_TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

// Standard Base Pairs on Robinhood Chain
pub const WETH_ROBINHOOD: &str = "0x0bd7d308f8e1639fab988df18a8011f41eacad73";
pub const USDG_ROBINHOOD: &str = "0x5fc5360d0400a0fd4f2af552add042d716f1d168";

#[derive(Clone)]
pub struct RobinhoodScanner {
    rpc_urls: Vec<String>,
    client: Client,
    current_idx: Arc<AtomicUsize>,
    symbol_cache: Arc<Mutex<HashMap<String, String>>>,
}

impl RobinhoodScanner {
    pub fn new(rpc_urls: Vec<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        let mut initial_cache = HashMap::new();
        initial_cache.insert(WETH_ROBINHOOD.to_lowercase(), "WETH".to_string());
        initial_cache.insert(USDG_ROBINHOOD.to_lowercase(), "USDG".to_string());

        Self {
            rpc_urls,
            client,
            current_idx: Arc::new(AtomicUsize::new(0)),
            symbol_cache: Arc::new(Mutex::new(initial_cache)),
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

    pub async fn resolve_token_symbol(&self, contract_address: &str) -> String {
        let lower = contract_address.to_lowercase();
        {
            let cache = self.symbol_cache.lock().unwrap();
            if let Some(sym) = cache.get(&lower) {
                return sym.clone();
            }
        }

        let rpc = self.next_rpc();
        // ERC20 symbol() selector: 0x95d89b41
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 99,
            "method": "eth_call",
            "params": [
                {
                    "to": contract_address,
                    "data": "0x95d89b41"
                },
                "latest"
            ]
        });

        if let Ok(res) = self.client.post(&rpc).json(&payload).send().await {
            if let Ok(val) = res.json::<Value>().await {
                if let Some(hex_data) = val["result"].as_str() {
                    let raw = hex_data.trim_start_matches("0x");
                    if let Ok(bytes) = hex::decode(raw) {
                        let sym = String::from_utf8_lossy(&bytes)
                            .chars()
                            .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
                            .collect::<String>();
                        if !sym.is_empty() {
                            let mut cache = self.symbol_cache.lock().unwrap();
                            cache.insert(lower, sym.clone());
                            return sym;
                        }
                    }
                }
            }
        }

        if contract_address.len() >= 8 {
            format!("0x{}", &contract_address[2..8])
        } else {
            "UNKNOWN".to_string()
        }
    }

    pub async fn fetch_swaps_in_range(&self, from_block: u64, to_block: u64) -> Result<Vec<SwapRecord>> {
        let rpc = self.next_rpc();
        let from_hex = format!("0x{:x}", from_block);
        let to_hex = format!("0x{:x}", to_block);

        let payload = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "eth_getLogs",
            "params": [{
                "fromBlock": from_hex,
                "toBlock": to_hex,
                "topics": [
                    [V4_SWAP_TOPIC, V3_SWAP_TOPIC, ERC20_TRANSFER_TOPIC]
                ]
            }]
        });

        let res = self.client.post(&rpc).json(&payload).send().await?;
        let val: Value = res.json().await?;
        let logs = val["result"].as_array();

        let mut out = Vec::new();
        let now = Utc::now();

        if let Some(arr) = logs {
            let mut tx_logs: HashMap<String, Vec<&Value>> = HashMap::new();
            for item in arr {
                if let Some(tx) = item["transactionHash"].as_str() {
                    tx_logs.entry(tx.to_string()).or_default().push(item);
                }
            }

            for (tx_hash, logs_in_tx) in tx_logs {
                let mut has_swap = false;
                let mut swap_maker = "0x0000000000000000000000000000000000000000".to_string();
                let mut pool_addr = POOL_MANAGER_V4.to_string();
                let mut block_num = 0u64;

                for item in &logs_in_tx {
                    if let Some(topics) = item["topics"].as_array() {
                        if !topics.is_empty() {
                            let t0 = topics[0].as_str().unwrap_or("");
                            if t0.eq_ignore_ascii_case(V4_SWAP_TOPIC) || t0.eq_ignore_ascii_case(V3_SWAP_TOPIC) {
                                has_swap = true;
                                pool_addr = item["address"].as_str().unwrap_or(POOL_MANAGER_V4).to_string();
                                let block_hex = item["blockNumber"].as_str().unwrap_or("0x0");
                                block_num = u64::from_str_radix(block_hex.trim_start_matches("0x"), 16).unwrap_or(0);
                                if topics.len() > 1 {
                                    let raw = topics[1].as_str().unwrap_or("");
                                    if raw.len() >= 42 {
                                        swap_maker = format!("0x{}", &raw[raw.len() - 40..]);
                                    }
                                }
                                break;
                            }
                        }
                    }
                }

                if !has_swap {
                    continue;
                }

                let mut target_token = None;
                let mut token_amount = 0.0;

                for item in &logs_in_tx {
                    if let Some(topics) = item["topics"].as_array() {
                        if !topics.is_empty() {
                            let t0 = topics[0].as_str().unwrap_or("");
                            if t0.eq_ignore_ascii_case(ERC20_TRANSFER_TOPIC) {
                                let token_addr = item["address"].as_str().unwrap_or("").to_lowercase();
                                if token_addr != WETH_ROBINHOOD.to_lowercase()
                                    && token_addr != USDG_ROBINHOOD.to_lowercase()
                                    && !token_addr.is_empty()
                                {
                                    target_token = Some(token_addr);
                                    let data_hex = item["data"].as_str().unwrap_or("0x0");
                                    if let Ok(raw_amt) = u128::from_str_radix(data_hex.trim_start_matches("0x"), 16) {
                                        token_amount = (raw_amt as f64) / 1e18;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }

                if let Some(token_addr) = target_token {
                    let symbol = self.resolve_token_symbol(&token_addr).await;

                    out.push(SwapRecord {
                        chain: Chain::Robinhood,
                        token_address: token_addr,
                        token_symbol: symbol,
                        swapper_address: swap_maker,
                        side: SwapSide::Buy,
                        amount_usd: 100.0,
                        token_amount,
                        pool_address: pool_addr,
                        tx_hash,
                        block_number_or_slot: block_num,
                        timestamp: now,
                    });
                }
            }
        }

        Ok(out)
    }
}
