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
    let sol_scanner = SolanaScanner::new(config.solana_rpc_url.clone());
    let detector = SignalDetector::new(
        config.min_inflow_usd_1h,
        config.min_inflow_usd_4h,
        config.min_buy_ratio_pct,
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

    // Scan loop
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

        // 2. Ingest Solana Swaps
        if let Ok(latest_slot) = sol_scanner.get_latest_slot().await {
            let _ = db.set_cursor(Chain::Solana, latest_slot);
        }

        // 3. Evaluate rolling windows for both chains
        for chain in [Chain::Robinhood, Chain::Solana] {
            let window_start_4h = Utc::now() - Duration::hours(4);
            if let Ok(swaps) = db.get_recent_swaps_in_window(chain, window_start_4h) {
                // 1H metrics
                let metrics_1h = RollingAggregator::compute_metrics(chain, &swaps, 1);
                for m in metrics_1h {
                    if let Some(sig) = detector.detect_signal(&m) {
                        println!(
                            "[FLOW SIGNAL 🚨] [{}] [{}] {} ({}) -> {:?} (Conf: {:.1}%): {}",
                            sig.chain, sig.timeframe, sig.token_symbol, sig.token_address,
                            sig.signal_type, sig.confidence_score, sig.summary
                        );
                        let _ = db.insert_signal(&sig);
                    }
                }

                // 4H metrics
                let metrics_4h = RollingAggregator::compute_metrics(chain, &swaps, 4);
                for m in metrics_4h {
                    if let Some(sig) = detector.detect_signal(&m) {
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

        tokio::time::sleep(tokio::time::Duration::from_secs(config.poll_interval_secs)).await;
    }
}
