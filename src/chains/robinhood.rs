use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::models::{Chain, SwapRecord, SwapSide};
use crate::radar::market_cap::MarketCapResolver;

// Contract addresses on Robinhood Chain (ID: 4663)
pub const POOL_MANAGER_V4: &str = "0x8366a39CC670B4001A1121B8F6A443A643e40951";
pub const V4_SWAP_TOPIC: &str = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f";
pub const V3_SWAP_TOPIC: &str = "0xc42079f94a6350d7e6235f29174924f9d5fb2ce002158f77798b43cfa359d722";
pub const ERC20_TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

// Standard Base Pairs on Robinhood Chain
pub const WETH_ROBINHOOD: &str = "0x0bd7d308f8e1639fab988df18a8011f41eacad73";
pub const USDG_ROBINHOOD: &str = "0x5fc5360d0400a0fd4f2af552add042d716f1d168";

/// Intermediate swap reconstructed from a single transaction's token flows.
#[derive(Debug, Clone)]
pub struct RhSwap {
    pub swapper: String,
    pub token: String,
    pub side: SwapSide,
    pub amount_usd: f64,
    pub token_amount: f64,
    pub pool_addr: String,
    pub block_num: u64,
}

fn topic_to_address(topic: &str) -> String {
    let t = topic.trim_start_matches("0x");
    if t.len() >= 40 {
        format!("0x{}", t[t.len() - 40..].to_lowercase())
    } else {
        String::new()
    }
}

fn hex_to_i128(hex_str: &str) -> i128 {
    let raw = hex_str.trim_start_matches("0x");
    if raw.is_empty() {
        return 0;
    }
    match u128::from_str_radix(raw, 16) {
        Ok(v) => i128::try_from(v).unwrap_or(i128::MAX),
        Err(_) => 0,
    }
}

/// Compute context: prices, decimals, EOA flags, and native tx value.
pub struct RhComputeCtx<'a> {
    pub weth_price: Option<f64>,
    pub decimals: &'a HashMap<String, u8>,
    /// Address -> is EOA (getCode == "0x"). Missing addresses default to EOA.
    pub is_eoa: &'a HashMap<String, bool>,
    /// Native ETH attached to the transaction (tx.value), in wei.
    pub tx_value_eth_wei: u128,
}

fn matched_wrap_flow(mints: &[i128], burns: &[i128]) -> i128 {
    // Native ETH entering/leaving via WETH wrap+burn pairs counts once.
    let mut m = mints.to_vec();
    let mut total = 0i128;
    let mut b = burns.to_vec();
    for mv in m.iter_mut() {
        if *mv == 0 {
            continue;
        }
        if let Some(pos) = b.iter().position(|bv| *bv == *mv) {
            total += *mv;
            *mv = 0;
            b[pos] = 0;
        }
    }
    // Unmatched mints/burns are also real wrap/unwrap flows.
    total += m.iter().sum::<i128>().abs() + b.iter().sum::<i128>().abs();
    total
}

/// PURE core (unit-testable): reconstruct user swaps from one transaction's logs.
///
/// Layer 1 (exact): non-pool EOA whose net ERC20 quote flow != 0
///   -> amount = |net quote| (USDG $1 / WETH live price).
/// Layer 2 (fallback for hook/native settlement): EOA with a net token position
///   but zero ERC20 quote flow gets a share of the tx's native value
///   (WETH wrap/burn pairs + tx.value), attributed to the largest token position.
/// Contract intermediaries (hooks/routers, getCode != 0x) are never recorded.
pub fn compute_swaps_from_logs(logs: &[Value], ctx: &RhComputeCtx) -> Vec<RhSwap> {
    let usdg_l = USDG_ROBINHOOD.to_lowercase();
    let weth_l = WETH_ROBINHOOD.to_lowercase();
    let poolmgr_l = POOL_MANAGER_V4.to_lowercase();
    let weth_price = ctx.weth_price.unwrap_or(0.0);

    let mut has_swap = false;
    let mut v3_pools: HashSet<String> = HashSet::new();
    let mut v4_present = false;
    let mut transfers: Vec<(String, String, String, i128)> = Vec::new();
    let mut weth_mints: Vec<i128> = Vec::new();
    let mut weth_burns: Vec<i128> = Vec::new();
    let mut block_num = 0u64;

    for item in logs {
        let topics = match item["topics"].as_array() {
            Some(t) if !t.is_empty() => t,
            _ => continue,
        };
        let t0 = topics[0].as_str().unwrap_or("").to_lowercase();
        let addr = item["address"].as_str().unwrap_or("").to_lowercase();
        let bn = u64::from_str_radix(
            item["blockNumber"].as_str().unwrap_or("0x0").trim_start_matches("0x"),
            16,
        )
        .unwrap_or(0);
        if bn > block_num {
            block_num = bn;
        }

        if t0 == V4_SWAP_TOPIC.to_lowercase() {
            has_swap = true;
            v4_present = true;
        } else if t0 == V3_SWAP_TOPIC.to_lowercase() {
            has_swap = true;
            v3_pools.insert(addr.clone());
        } else if t0 == ERC20_TRANSFER_TOPIC.to_lowercase() && topics.len() >= 3 {
            let from = topic_to_address(topics[1].as_str().unwrap_or(""));
            let to = topic_to_address(topics[2].as_str().unwrap_or(""));
            let val = hex_to_i128(item["data"].as_str().unwrap_or("0x0"));
            if from.is_empty() || to.is_empty() {
                continue;
            }
            if addr == weth_l {
                let zero = "0x0000000000000000000000000000000000000000";
                if from == zero {
                    weth_mints.push(val);
                } else if to == zero {
                    weth_burns.push(val);
                }
            }
            transfers.push((addr, from, to, val));
        }
    }

    if !has_swap {
        return Vec::new();
    }

    let mut pool_side: HashSet<String> = v3_pools.clone();
    if v4_present {
        pool_side.insert(poolmgr_l);
    }

    let mut token_net: HashMap<String, HashMap<String, i128>> = HashMap::new();
    let mut quote_usd_net: HashMap<String, f64> = HashMap::new();

    for (tok, from, to, val) in &transfers {
        if tok == &usdg_l {
            let usd = *val as f64 / 1e6;
            *quote_usd_net.entry(from.clone()).or_insert(0.0) -= usd;
            *quote_usd_net.entry(to.clone()).or_insert(0.0) += usd;
        } else if tok == &weth_l {
            let usd = *val as f64 / 1e18 * weth_price;
            *quote_usd_net.entry(from.clone()).or_insert(0.0) -= usd;
            *quote_usd_net.entry(to.clone()).or_insert(0.0) += usd;
        } else {
            let m = token_net.entry(tok.clone()).or_default();
            *m.entry(from.clone()).or_insert(0) -= val;
            *m.entry(to.clone()).or_insert(0) += val;
        }
    }

    // Group target-token positions per EOA address (pool-side & contracts excluded).
    let is_eoa = |a: &String| -> bool { *ctx.is_eoa.get(a).unwrap_or(&true) };
    let mut per_addr: HashMap<String, Vec<(String, i128)>> = HashMap::new();
    for (tok, holders) in &token_net {
        for (addr, net) in holders {
            if *net != 0 && !pool_side.contains(addr) && is_eoa(addr) {
                per_addr.entry(addr.clone()).or_default().push((tok.clone(), *net));
            }
        }
    }

    let pool_addr = v3_pools
        .iter()
        .next()
        .cloned()
        .unwrap_or_else(|| {
            if v4_present {
                POOL_MANAGER_V4.to_string()
            } else {
                "UNKNOWN".to_string()
            }
        });

    // Fallback value pool: native settlement invisible to ERC20 net flows.
    let wrap_wei = matched_wrap_flow(&weth_mints, &weth_burns);
    let fallback_usd = (wrap_wei as f64 / 1e18) * weth_price
        + (ctx.tx_value_eth_wei as f64 / 1e18) * weth_price;

    let mut out = Vec::new();
    for (addr, positions) in per_addr {
        let q = quote_usd_net.get(&addr).copied().unwrap_or(0.0);
        let exact = q.abs() >= 1e-9;
        if !exact && fallback_usd < 1e-9 {
            continue;
        }
        // Largest token position drives the fallback side & attribution.
        let main = positions
            .iter()
            .max_by_key(|(_, n)| (*n).abs())
            .expect("positions non-empty");
        let side = if exact {
            if q < 0.0 { SwapSide::Buy } else { SwapSide::Sell }
        } else if main.1 < 0 {
            SwapSide::Sell
        } else {
            SwapSide::Buy
        };
        let total_abs: f64 = positions.iter().map(|(_, n)| (*n).abs() as f64).sum();
        for (tok, net) in &positions {
            let dec = ctx.decimals.get(tok).copied().unwrap_or(18) as f64;
            let amount_usd = if exact {
                q.abs() * ((*net).abs() as f64 / total_abs)
            } else if *tok == main.0 {
                fallback_usd
            } else {
                0.0
            };
            if amount_usd <= 0.0 {
                continue;
            }
            out.push(RhSwap {
                swapper: addr.clone(),
                token: tok.clone(),
                side,
                amount_usd,
                token_amount: (*net).abs() as f64 / 10f64.powf(dec),
                pool_addr: pool_addr.clone(),
                block_num,
            });
        }
    }
    out
}

#[derive(Clone)]
pub struct RobinhoodScanner {
    rpc_urls: Vec<String>,
    client: Client,
    current_idx: Arc<AtomicUsize>,
    symbol_cache: Arc<Mutex<HashMap<String, String>>>,
    decimals_cache: Arc<Mutex<HashMap<String, u8>>>,
    eoa_cache: Arc<Mutex<HashMap<String, bool>>>,
    mc: Arc<MarketCapResolver>,
}

impl RobinhoodScanner {
    pub fn new(rpc_urls: Vec<String>, mc: Arc<MarketCapResolver>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();

        let mut initial_cache = HashMap::new();
        initial_cache.insert(WETH_ROBINHOOD.to_lowercase(), "WETH".to_string());
        initial_cache.insert(USDG_ROBINHOOD.to_lowercase(), "USDG".to_string());

        let mut initial_dec = HashMap::new();
        initial_dec.insert(WETH_ROBINHOOD.to_lowercase(), 18u8);
        initial_dec.insert(USDG_ROBINHOOD.to_lowercase(), 6u8);

        Self {
            rpc_urls,
            client,
            current_idx: Arc::new(AtomicUsize::new(0)),
            symbol_cache: Arc::new(Mutex::new(initial_cache)),
            decimals_cache: Arc::new(Mutex::new(initial_dec)),
            eoa_cache: Arc::new(Mutex::new(HashMap::new())),
            mc,
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

    /// ERC20 decimals() with persistent cache (fallback 18 if unresolvable).
    async fn fetch_decimals(&self, contract_address: &str) -> Option<u8> {
        let lower = contract_address.to_lowercase();
        {
            let cache = self.decimals_cache.lock().unwrap();
            if let Some(d) = cache.get(&lower) {
                return Some(*d);
            }
        }
        let rpc = self.next_rpc();
        // ERC20 decimals() selector: 0x313ce567
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 98,
            "method": "eth_call",
            "params": [{"to": contract_address, "data": "0x313ce567"}, "latest"]
        });
        let res = self.client.post(&rpc).json(&payload).send().await.ok()?;
        let val: Value = res.json().await.ok()?;
        let hex_str = val["result"].as_str()?;
        let d = u8::from_str_radix(hex_str.trim_start_matches("0x"), 16).ok()?;
        let mut cache = self.decimals_cache.lock().unwrap();
        cache.insert(lower, d);
        Some(d)
    }

    /// true when the address has no contract code (EOA). Cached. Fails open
    /// (treated as EOA) if the RPC errors, so ingestion never dies on lookup.
    async fn fetch_is_eoa(&self, addr: &str) -> bool {
        let lower = addr.to_lowercase();
        {
            let cache = self.eoa_cache.lock().unwrap();
            if let Some(v) = cache.get(&lower) {
                return *v;
            }
        }
        let rpc = self.next_rpc();
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 97,
            "method": "eth_getCode",
            "params": [addr, "latest"]
        });
        let is_eoa = match self.client.post(&rpc).json(&payload).send().await {
            Ok(res) => match res.json::<Value>().await {
                Ok(val) => val["result"].as_str().map(|c| c == "0x").unwrap_or(true),
                Err(_) => true,
            },
            Err(_) => true,
        };
        let mut cache = self.eoa_cache.lock().unwrap();
        cache.insert(lower, is_eoa);
        is_eoa
    }

    /// Native ETH value attached to a transaction (wei), 0 on failure.
    async fn fetch_tx_value_wei(&self, tx_hash: &str) -> u128 {
        let rpc = self.next_rpc();
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 96,
            "method": "eth_getTransactionByHash",
            "params": [tx_hash]
        });
        match self.client.post(&rpc).json(&payload).send().await {
            Ok(res) => match res.json::<Value>().await {
                Ok(val) => val["result"]["value"]
                    .as_str()
                    .and_then(|v| u128::from_str_radix(v.trim_start_matches("0x"), 16).ok())
                    .unwrap_or(0),
                Err(_) => 0,
            },
            Err(_) => 0,
        }
    }

    pub async fn fetch_swaps_in_range(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<SwapRecord>> {
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
        let logs: Vec<Value> = val["result"].as_array().cloned().unwrap_or_default();

        // Group logs per transaction
        let mut tx_logs: HashMap<String, Vec<Value>> = HashMap::new();
        for item in logs {
            if let Some(tx) = item["transactionHash"].as_str() {
                tx_logs.entry(tx.to_string()).or_default().push(item);
            }
        }

        // Pre-fetch decimals for every target token seen in these txs (await first,
        // so the pure core can run synchronously on a stable snapshot).
        {
            let usdg_l = USDG_ROBINHOOD.to_lowercase();
            let weth_l = WETH_ROBINHOOD.to_lowercase();
            let mut needed: HashSet<String> = HashSet::new();
            for logs_in_tx in tx_logs.values() {
                for item in logs_in_tx {
                    if let Some(t) = item["topics"].as_array() {
                        if t.len() >= 3
                            && t[0].as_str().unwrap_or("").eq_ignore_ascii_case(ERC20_TRANSFER_TOPIC)
                        {
                            let tok = item["address"].as_str().unwrap_or("").to_lowercase();
                            if !tok.is_empty() && tok != usdg_l && tok != weth_l {
                                needed.insert(tok);
                            }
                        }
                    }
                }
            }
            for tok in needed {
                self.fetch_decimals(&tok).await;
            }
        }

        let weth_price = self.mc.get_token_usd_price(WETH_ROBINHOOD).await;
        let decimals_snapshot = self.decimals_cache.lock().unwrap().clone();
        let now = Utc::now();

        // EOA flags for every address appearing in these txs' transfers
        // (hooks/routers are contracts and must never be recorded as swappers).
        let mut candidate_addrs: HashSet<String> = HashSet::new();
        for logs_in_tx in tx_logs.values() {
            for item in logs_in_tx {
                if let Some(t) = item["topics"].as_array() {
                    if t.len() >= 3
                        && t[0].as_str().unwrap_or("").eq_ignore_ascii_case(ERC20_TRANSFER_TOPIC)
                    {
                        candidate_addrs.insert(topic_to_address(t[1].as_str().unwrap_or("")));
                        candidate_addrs.insert(topic_to_address(t[2].as_str().unwrap_or("")));
                    }
                }
            }
        }
        let mut is_eoa: HashMap<String, bool> = HashMap::new();
        for a in candidate_addrs {
            if a.is_empty() {
                continue;
            }
            let eoa = self.fetch_is_eoa(&a).await;
            is_eoa.insert(a, eoa);
        }

        let mut out = Vec::new();
        for (tx_hash, logs_in_tx) in tx_logs {
            let tx_value = self.fetch_tx_value_wei(&tx_hash).await;
            let ctx = RhComputeCtx {
                weth_price,
                decimals: &decimals_snapshot,
                is_eoa: &is_eoa,
                tx_value_eth_wei: tx_value,
            };
            for sw in compute_swaps_from_logs(&logs_in_tx, &ctx) {
                let symbol = self.resolve_token_symbol(&sw.token).await;
                // Unique per real on-chain position: tx + user + token.
                let tx_key = format!("{}:{}:{}", tx_hash, sw.swapper, sw.token);
                out.push(SwapRecord {
                    chain: Chain::Robinhood,
                    token_address: sw.token.clone(),
                    token_symbol: symbol,
                    swapper_address: sw.swapper.clone(),
                    side: sw.side,
                    amount_usd: sw.amount_usd,
                    token_amount: sw.token_amount,
                    pool_address: sw.pool_addr,
                    tx_hash: tx_key,
                    block_number_or_slot: sw.block_num,
                    timestamp: now,
                });
            }
        }
        Ok(out)
    }
}
