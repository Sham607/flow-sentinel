use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::models::{Chain, SwapRecord, SwapSide};

// Verified mainnet program feeds (official docs: Raydium / pump-fun / Jupiter / Orca / Meteora)
pub const JUPITER_V6_PROGRAM: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
pub const PUMPSWAP_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
pub const RAYDIUM_V4_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const RAYDIUM_CPMM_PROGRAM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
pub const RAYDIUM_CLMM_PROGRAM: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
pub const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
pub const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
pub const METEORA_DAMM_PROGRAM: &str = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB";

pub const DEFAULT_TRACKED_PROGRAMS: &[&str] = &[
    JUPITER_V6_PROGRAM,      // Catch-all aggregator routes
    PUMPSWAP_AMM_PROGRAM,    // Pump.fun post-graduation AMM
    PUMPFUN_PROGRAM,         // Pump.fun bonding curve
    RAYDIUM_V4_PROGRAM,      // Raydium legacy AMM
    RAYDIUM_CPMM_PROGRAM,    // Raydium constant product
    RAYDIUM_CLMM_PROGRAM,    // Raydium concentrated liquidity
    ORCA_WHIRLPOOL_PROGRAM,  // Orca CLMM
    METEORA_DLMM_PROGRAM,    // Meteora DLMM
    METEORA_DAMM_PROGRAM,    // Meteora Dynamic AMM
];

pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

const PROCESSED_SIG_CAP: usize = 100_000;

#[derive(Clone)]
pub struct SolanaScanner {
    rpc_url: String,
    client: Client,
    tracked_programs: Vec<String>,
    processed_sigs: Arc<Mutex<HashSet<String>>>,
}

/// Program-agnostic swap extracted from token balance deltas.
pub struct RawSolSwap {
    pub mint: String,
    pub owner: String,
    pub side: SwapSide,
    pub amount_usd: f64,
    pub token_amount: f64,
    pub slot: u64,
    pub timestamp: DateTime<Utc>,
}

struct TokenBalanceEntry {
    owner: String,
    mint: String,
    amount: f64,
    decimals: u8,
}

fn collect_token_balances(v: Option<&Value>) -> HashMap<usize, TokenBalanceEntry> {
    let mut m = HashMap::new();
    if let Some(arr) = v.and_then(|a| a.as_array()) {
        for el in arr {
            let account_index = el.get("accountIndex").and_then(|x| x.as_u64());
            let owner = el.get("owner").and_then(|x| x.as_str());
            let mint = el.get("mint").and_then(|x| x.as_str());
            let amount = el
                .pointer("/uiTokenAmount/amount")
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse::<f64>().ok());
            let decimals = el
                .pointer("/uiTokenAmount/decimals")
                .and_then(|x| x.as_u64());

            if let (Some(ai), Some(owner), Some(mint), Some(amount)) =
                (account_index, owner, mint, amount)
            {
                m.insert(
                    ai as usize,
                    TokenBalanceEntry {
                        owner: owner.to_string(),
                        mint: mint.to_string(),
                        amount,
                        decimals: decimals.unwrap_or(9) as u8,
                    },
                );
            }
        }
    }
    m
}

/// Derive swap records from a getTransaction (jsonParsed) response using
/// preTokenBalances/postTokenBalances deltas + native SOL balance deltas.
/// Works across ALL DEX programs without per-program IDL parsing.
pub fn parse_swaps_from_tx(
    tx: &Value,
    sol_price: Option<f64>,
) -> Vec<RawSolSwap> {
    let mut out = Vec::new();

    // Defensively handle either unwrapped transaction or root JSON-RPC response wrapper
    let tx_body = match tx.get("result") {
        Some(res) if !res.is_null() => res,
        _ => tx,
    };

    let Some(meta) = tx_body.get("meta") else {
        return out;
    };
    if meta.get("err").map(|e| !e.is_null()).unwrap_or(false) {
        return out;
    }
    let slot = tx_body.get("slot").and_then(|s| s.as_u64()).unwrap_or(0);
    let timestamp = tx_body
        .get("blockTime")
        .and_then(|t| t.as_i64())
        .and_then(|t| Utc.timestamp_opt(t, 0).single())
        .unwrap_or_else(Utc::now);

    // --- Account keys (static + loaded addresses, in index order) ---
    let mut keys: Vec<String> = Vec::new();
    if let Some(arr) = tx_body
        .pointer("/transaction/message/accountKeys")
        .and_then(|a| a.as_array())
    {
        for el in arr {
            if let Some(pk) = el.as_str() {
                keys.push(pk.to_string());
            } else if let Some(pk) = el.get("pubkey").and_then(|p| p.as_str()) {
                keys.push(pk.to_string());
            }
        }
    }
    for section in ["/loadedAddresses/writable", "/loadedAddresses/readonly"] {
        if let Some(arr) = meta.pointer(section).and_then(|a| a.as_array()) {
            for el in arr {
                if let Some(s) = el.as_str() {
                    keys.push(s.to_string());
                }
            }
        }
    }
    let idx_of: HashMap<&str, usize> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k.as_str(), i))
        .collect();

    let to_f64_vec = |name: &str| -> Vec<f64> {
        meta.get(name)
            .and_then(|a| a.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_u64())
                    .map(|v| v as f64)
                    .collect()
            })
            .unwrap_or_default()
    };
    let pre_bal = to_f64_vec("preBalances");
    let post_bal = to_f64_vec("postBalances");

    // meta.fee may arrive as a bare integer (lamports) or {lamports: N} object
    let fee_lamports = match meta.get("fee") {
        Some(Value::Number(n)) => n.as_u64().unwrap_or(5000) as f64,
        Some(v) => v.get("lamports").and_then(|f| f.as_u64()).unwrap_or(5000) as f64,
        None => 5000.0,
    };

    // Fee payer = first signer in account keys (fallback: first key)
    let fee_payer: Option<String> = tx_body
        .pointer("/transaction/message/accountKeys")
        .and_then(|a| a.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|el| el.get("signer").and_then(|s| s.as_bool()).unwrap_or(false))
                .and_then(|el| el.get("pubkey").and_then(|p| p.as_str()))
                .map(|s| s.to_string())
        })
        .or_else(|| keys.first().cloned());

    // --- Token balance deltas per (owner, mint) ---
    let pre_tok = collect_token_balances(meta.get("preTokenBalances"));
    let post_tok = collect_token_balances(meta.get("postTokenBalances"));

    let mut deltas: HashMap<(String, String), f64> = HashMap::new();
    let mut decimals: HashMap<String, u8> = HashMap::new();

    for (ai, pb) in &post_tok {
        let pre_amt = pre_tok
            .get(ai)
            .filter(|p| p.mint == pb.mint)
            .map(|p| p.amount)
            .unwrap_or(0.0);
        *deltas
            .entry((pb.owner.clone(), pb.mint.clone()))
            .or_insert(0.0) += pb.amount - pre_amt;
        decimals.entry(pb.mint.clone()).or_insert(pb.decimals);
    }
    // Token accounts closed in this tx: post entry missing -> full pre amount left the wallet
    for (ai, pb) in &pre_tok {
        if !post_tok.contains_key(ai) {
            *deltas
                .entry((pb.owner.clone(), pb.mint.clone()))
                .or_insert(0.0) -= pb.amount;
        }
    }

    let is_quote = |m: &str| m == WSOL_MINT || m == USDC_MINT || m == USDT_MINT;

    let sol_delta_of = |owner: &str| -> Option<f64> {
        let i = *idx_of.get(owner)?;
        Some(post_bal.get(i).copied().unwrap_or(0.0) - pre_bal.get(i).copied().unwrap_or(0.0))
    };

    let owners: Vec<String> = deltas
        .keys()
        .map(|(o, _)| o.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    for owner in owners {
        let owner_mints: Vec<(String, f64)> = deltas
            .iter()
            .filter(|((o, m), _)| o == &owner && !is_quote(m))
            .map(|((_, m), d)| (m.clone(), *d))
            .collect();

        for (mint, d) in owner_mints {
            if d.abs() < 1e-9 {
                continue;
            }

            // Quote leg valuation: WSOL token delta > USDC/USDT delta > native SOL delta
            // NOTE: token deltas are RAW units -> normalize by mint decimals before pricing.
            let mut usd: Option<f64> = None;

            if let Some(qd) = deltas.get(&(owner.clone(), WSOL_MINT.to_string())) {
                if (d > 0.0 && *qd < 0.0) || (d < 0.0 && *qd > 0.0) {
                    if let Some(p) = sol_price {
                        usd = Some((qd.abs() / 1e9) * p); // WSOL = 9 decimals
                    }
                }
            }
            if usd.is_none() {
                for q in [USDC_MINT, USDT_MINT] {
                    if let Some(qd) = deltas.get(&(owner.clone(), q.to_string())) {
                        if (d > 0.0 && *qd < 0.0) || (d < 0.0 && *qd > 0.0) {
                            usd = Some(qd.abs() / 1e6); // USDC/USDT = 6 decimals
                            break;
                        }
                    }
                }
            }
            if usd.is_none() {
                if let Some(mut sd_lamports) = sol_delta_of(&owner) {
                    if Some(&owner) == fee_payer.as_ref() {
                        if sd_lamports < 0.0 {
                            sd_lamports += fee_lamports;
                        } else if sd_lamports > 0.0 {
                            sd_lamports -= fee_lamports;
                        }
                    }
                    if (d > 0.0 && sd_lamports < 0.0) || (d < 0.0 && sd_lamports > 0.0) {
                        if let Some(p) = sol_price {
                            usd = Some((sd_lamports.abs() / 1e9) * p);
                        }
                    }
                }
            }

            let Some(amount_usd) = usd else {
                println!(
                    "[SOL DBG] rejected: mint={} owner={} d={:.2} sol_delta={:?} wsol={:?}",
                    &mint[..8.min(mint.len())],
                    &owner[..8.min(owner.len())],
                    d,
                    sol_delta_of(&owner),
                    deltas.get(&(owner.clone(), WSOL_MINT.to_string()))
                );
                continue;
            };
            if amount_usd < 1.0 {
                continue; // dust filter
            }

            let dec = decimals.get(&mint).copied().unwrap_or(9);
            let token_amount = d.abs() / 10f64.powi(dec as i32);
            let side = if d > 0.0 { SwapSide::Buy } else { SwapSide::Sell };

            out.push(RawSolSwap {
                mint: mint.clone(),
                owner: owner.clone(),
                side,
                amount_usd,
                token_amount,
                slot,
                timestamp,
            });
        }
    }

    out
}

impl SolanaScanner {
    pub fn new(rpc_url: String, tracked_programs: Vec<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_default();

        let programs = if tracked_programs.is_empty() {
            DEFAULT_TRACKED_PROGRAMS
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            tracked_programs
        };

        Self {
            rpc_url,
            client,
            tracked_programs: programs,
            processed_sigs: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    async fn rpc_post(&self, payload: &Value) -> Result<Value> {
        // Public RPC rate limits aggressively (HTTP 429 / JSON-RPC code 429).
        // Retry with backoff before giving up on this call.
        let mut attempt = 0;
        loop {
            attempt += 1;
            let res = self.client.post(&self.rpc_url).json(payload).send().await?;

            if res.status().as_u16() == 429 && attempt < 4 {
                tokio::time::sleep(Duration::from_millis(700 * attempt as u64)).await;
                continue;
            }

            let val: Value = res.json().await?;
            if let Some(err) = val.get("error") {
                let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
                if code == 429 && attempt < 4 {
                    tokio::time::sleep(Duration::from_millis(700 * attempt as u64)).await;
                    continue;
                }
                anyhow::bail!("RPC error: {}", err);
            }
            return Ok(val);
        }
    }

    pub async fn get_latest_slot(&self) -> Result<u64> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": [{ "commitment": "confirmed" }]
        });
        let val = self.rpc_post(&payload).await?;
        let slot = val["result"].as_u64().context("missing getSlot result")?;
        Ok(slot)
    }

    async fn fetch_program_signatures(
        &self,
        program_id: &str,
        limit: usize,
    ) -> Result<Vec<(String, u64)>> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "getSignaturesForAddress",
            "params": [
                program_id,
                { "limit": limit, "commitment": "confirmed" }
            ]
        });
        let val = self.rpc_post(&payload).await?;
        let mut sigs = Vec::new();
        if let Some(arr) = val["result"].as_array() {
            for item in arr {
                if item["err"].is_null() {
                    if let Some(sig) = item["signature"].as_str() {
                        let slot = item["slot"].as_u64().unwrap_or(0);
                        sigs.push((sig.to_string(), slot));
                    }
                }
            }
        }
        Ok(sigs)
    }

    async fn fetch_transaction(&self, sig: &str) -> Result<Value> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "getTransaction",
            "params": [
                sig,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    // Mainnet (2026) requires clients to declare support for tx version 1;
                    // requesting max 0 triggers RPC error -32015 on v1 txs.
                    "maxSupportedTransactionVersion": 2
                }
            ]
        });
        self.rpc_post(&payload).await
    }

    /// Poll tracked program feeds and parse new successful transactions into swaps.
    /// Symbol resolution happens in the caller (needs async DexScreener resolver).
    pub async fn poll_new_swaps(
        &self,
        baseline_slot: u64,
        max_tx: usize,
        sigs_per_feed: usize,
        sol_price: Option<f64>,
    ) -> Vec<SwapRecord> {
        // 1. Round-robin: select 3 feeds per cycle to stay comfortably within
        //    the strict rate-limits of the public Solana RPC endpoint.
        let mut candidates: Vec<(String, String)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut feed_errors: usize = 0;

        // Static cursor to rotate through feeds
        use std::sync::atomic::{AtomicUsize, Ordering};
        static FEED_ROTATION_IDX: AtomicUsize = AtomicUsize::new(0);

        let num_programs = self.tracked_programs.len();
        if num_programs == 0 {
            return Vec::new();
        }

        let start_idx = FEED_ROTATION_IDX.fetch_add(3, Ordering::Relaxed) % num_programs;
        let selected_programs: Vec<String> = (0..3.min(num_programs))
            .map(|i| self.tracked_programs[(start_idx + i) % num_programs].clone())
            .collect();

        for prog in &selected_programs {
            match self.fetch_program_signatures(prog, sigs_per_feed.min(10)).await {
                Ok(sigs) => {
                    let processed = self.processed_sigs.lock().unwrap();
                    for (sig, slot) in sigs {
                        if slot < baseline_slot {
                            continue;
                        }
                        if processed.contains(&sig) || !seen.insert(sig.clone()) {
                            continue;
                        }
                        candidates.push((sig, prog.clone()));
                    }
                }
                Err(e) => {
                    eprintln!("[SOLANA POLL ⚠️] feed {} error: {}", &prog[..8.min(prog.len())], e);
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        candidates.truncate(max_tx.min(8));
        let candidates_len = candidates.len();

        // 2. Fetch + parse each candidate transaction (cycle-time budgeted)
        let deadline = std::time::Instant::now() + Duration::from_secs(8);
        let mut out = Vec::new();
        let mut tx_errors: usize = 0;
        let mut fetched_ok: usize = 0;

        for (sig, venue) in candidates {
            if std::time::Instant::now() >= deadline {
                break;
            }
            match self.fetch_transaction(&sig).await {
                Ok(tx) => {
                    fetched_ok += 1;
                    // Debug snapshot of first fetched tx
                    if fetched_ok == 1 {
                        let _ = std::fs::write("/tmp/soldbg/last_fetched.json", serde_json::to_string_pretty(&tx).unwrap_or_default());
                    }
                    let recs = parse_swaps_from_tx(&tx, sol_price);
                    for raw in recs {
                        let tx_hash = format!("{}:{}:{}", sig, raw.owner, raw.mint);
                        out.push(SwapRecord {
                            chain: Chain::Solana,
                            token_address: raw.mint.clone(),
                            token_symbol: raw.mint.chars().take(6).collect(),
                            swapper_address: raw.owner,
                            side: raw.side,
                            amount_usd: raw.amount_usd,
                            token_amount: raw.token_amount,
                            pool_address: venue.clone(),
                            tx_hash,
                            block_number_or_slot: raw.slot,
                            timestamp: raw.timestamp,
                        });
                    }
                }
                Err(e) => {
                    tx_errors += 1;
                    eprintln!("[SOLANA POLL ⚠️] getTransaction {} failed: {}", &sig[..16.min(sig.len())], e);
                }
            };
            self.processed_sigs.lock().unwrap().insert(sig);
            tokio::time::sleep(Duration::from_millis(400)).await;
        }

        // 3. Cap the in-memory dedupe set
        {
            let mut processed = self.processed_sigs.lock().unwrap();
            if processed.len() > PROCESSED_SIG_CAP {
                processed.clear();
            }
        }

        if candidates_len > 0 || out.len() > 0 || tx_errors > 0 {
            println!(
                "[SOLANA POLL] active_feeds={:?} candidates={} fetched={} swaps={} err={}",
                selected_programs.iter().map(|p| &p[..6.min(p.len())]).collect::<Vec<_>>(),
                candidates_len,
                fetched_ok,
                out.len(),
                tx_errors
            );
        }

        out
    }
}
