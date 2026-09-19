pub mod aggregator;
pub mod chains;
pub mod config;
pub mod models;
pub mod radar;
pub mod storage;

use anyhow::Result;
use chrono::{Duration, Utc};
use std::sync::Arc;

use crate::aggregator::window::RollingAggregator;
use crate::chains::robinhood::RobinhoodScanner;
use crate::chains::solana::SolanaScanner;
use crate::config::Config;
use crate::models::Chain;
use crate::radar::market_cap::MarketCapResolver;
use crate::radar::signal_detector::SignalDetector;
use crate::storage::db::SentinelDb;

#[tokio::main]
async fn main() -> Result<()> {
    println!("========================================================");
    println!("     FLOW-SENTINEL: ON-CHAIN RADAR & FLOW MONITOR      ");
    println!("========================================================");
    println!("[INIT 🛡️] 100% Read-Only Mode. Zero Execution Footprint.");

    let config = Config::load()?;
    let db = Arc::new(SentinelDb::new(&config.database_path)?);

    let rh_scanner = RobinhoodScanner::new(config.robinhood_rpc_urls.clone());
    let sol_scanner = SolanaScanner::new(
        config.solana_rpc_url.clone(),
        config.solana_tracked_programs.clone(),
    );
    let mc_resolver = MarketCapResolver::new();
    let detector = SignalDetector::new(
        config.min_inflow_usd_5m,
        config.min_inflow_usd_15m,
        config.min_inflow_usd_1h,
        config.min_inflow_usd_4h,
        config.min_buy_ratio_pct,
        config.min_unique_buyers,
        config.detect_wash_trading,
        config.min_market_cap_usd,
        config.max_market_cap_usd,
    );

    // Initial check on latest blocks/slots
    match rh_scanner.get_latest_block_number().await {
        Ok(block) => println!("[ROBINHOOD RPC 🌐] Connected! Latest Block: {}", block),
        Err(e) => eprintln!("[ROBINHOOD RPC ⚠️] Connection warning: {}", e),
    }

    match sol_scanner.get_latest_slot().await {
        Ok(slot) => println!("[SOLANA RPC 🌐] Connected! Latest Slot: {}", slot),
        Err(e) => eprintln!("[SOLANA RPC ⚠️] Connection warning: {}", e),
    }

    println!("[RADAR 📡] Sliding Window Aggregator online (1H & 4H evaluation)");
    let sol_baseline_slot = sol_scanner.get_latest_slot().await.unwrap_or(0);
    println!(
        "[SOLANA 📡] {} program feeds armed | baseline slot {} (only newer txs ingested)",
        config.solana_tracked_programs.len(),
        sol_baseline_slot
    );

    // Fast-forward cursor Robinhood jika gap terlalu jauh (> 2000 blok) agar radar selalu live
    if let Ok(latest_rh) = rh_scanner.get_latest_block_number().await {
        let last_rh_cursor = db.get_cursor(Chain::Robinhood)?.unwrap_or(latest_rh.saturating_sub(config.robinhood_scan_chunk));
        if latest_rh.saturating_sub(last_rh_cursor) > 2000 {
            let snapped = latest_rh.saturating_sub(config.robinhood_scan_chunk * 2);
            println!(
                "[ROBINHOOD ⏩] Fast-forwarding cursor from {} to {} (gap: {} blocks)",
                last_rh_cursor,
                snapped,
                latest_rh - last_rh_cursor
            );
            let _ = db.set_cursor(Chain::Robinhood, snapped);
        }
    }

    let db_sol = Arc::clone(&db);
    let sol_programs_count = config.solana_tracked_programs.len();
    let sol_max_tx = config.solana_max_tx_per_cycle;
    let sol_sigs = config.solana_sigs_per_feed;

    // WORKER: Solana Ingestion Loop (Concurrent Task)
    tokio::spawn(async move {
        println!("[WORKER 🧵] Solana Ingestion background task online.");
        let mc = MarketCapResolver::new();
        loop {
            let sol_price = mc.get_sol_price().await.or(Some(110.0));
            let sol_swaps = sol_scanner
                .poll_new_swaps(sol_baseline_slot, sol_max_tx, sol_sigs, sol_price)
                .await;
            let count = sol_swaps.len();
            for mut sw in sol_swaps {
                if let Some(sym) = mc.get_token_symbol(&sw.token_address).await {
                    sw.token_symbol = sym;
                }
                let _ = db_sol.insert_swap(&sw);
            }
            if count > 0 {
                println!(
                    "[SOLANA SCAN] Indexed {} swaps across {} program feeds (SOL=${:.2})",
                    count,
                    sol_programs_count,
                    sol_price.unwrap_or(0.0)
                );
            }
            if let Ok(latest_slot) = sol_scanner.get_latest_slot().await {
                let _ = db_sol.set_cursor(Chain::Solana, latest_slot);
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(6)).await;
        }
    });

    // Main loop: Robinhood scan + Signal evaluation
    loop {
        // 1. Ingest Robinhood L2 Swaps
        if let Ok(latest_rh) = rh_scanner.get_latest_block_number().await {
            let last_cursor = db.get_cursor(Chain::Robinhood)?.unwrap_or(latest_rh.saturating_sub(config.robinhood_scan_chunk));
            let from_block = last_cursor + 1;
            let to_block = latest_rh.min(from_block + config.robinhood_scan_chunk);

            if from_block <= to_block {
                match rh_scanner.fetch_swaps_in_range(from_block, to_block).await {
                    Ok(swaps) => {
                        let count = swaps.len();
                        for sw in swaps {
                            let _ = db.insert_swap(&sw);
                        }
                        let _ = db.set_cursor(Chain::Robinhood, to_block);
                        if count > 0 {
                            println!("[ROBINHOOD SCAN] Indexed {} swaps from blocks {}..{}", count, from_block, to_block);
                        }
                    }
                    Err(e) => eprintln!("[ROBINHOOD SCAN ⚠️] Range {}..{} error: {}", from_block, to_block, e),
                }
            }
        }

        // 2. Evaluate rolling windows (5M / 15M / 1H / 4H) for both chains
        for chain in [Chain::Robinhood, Chain::Solana] {
            let window_start_4h = Utc::now() - Duration::hours(4);
            if let Ok(swaps) = db.get_recent_swaps_in_window(chain, window_start_4h) {
                // Windows in minutes; the 4H swap set is a superset, aggregator filters per window
                for minutes in [5i64, 15, 60, 240] {
                    let metrics = RollingAggregator::compute_metrics(chain, &swaps, minutes);
                    for m in metrics {
                        let mc = mc_resolver.get_market_cap(&m.token_address).await;
                        if let Some(sig) = detector.detect_signal(&m, mc) {
                            println!(
                                "[FLOW SIGNAL 🚨] [{}] [{}] {} ({}) -> {:?} (Conf: {:.1}%): {}",
                                sig.chain, sig.timeframe, sig.token_symbol, sig.token_address,
                                sig.signal_type, sig.confidence_score, sig.summary
                            );
                            let _ = db.insert_signal(&sig);
                        }
                    }
                }
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(config.poll_interval_secs)).await;
    }
}
