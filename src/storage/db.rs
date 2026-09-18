use anyhow::Result;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::models::{Chain, FlowSignal, SwapRecord, SwapSide};

#[derive(Clone)]
pub struct SentinelDb {
    conn: Arc<Mutex<Connection>>,
}

impl SentinelDb {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS raw_swaps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chain TEXT NOT NULL,
                token_address TEXT NOT NULL,
                token_symbol TEXT NOT NULL,
                swapper_address TEXT NOT NULL,
                side TEXT NOT NULL,
                amount_usd REAL NOT NULL,
                token_amount REAL NOT NULL,
                pool_address TEXT NOT NULL,
                tx_hash TEXT NOT NULL UNIQUE,
                block_number INTEGER NOT NULL,
                timestamp TEXT NOT NULL
            );",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_swaps_token_time 
             ON raw_swaps(chain, token_address, timestamp);",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS scan_cursors (
                chain TEXT PRIMARY KEY,
                last_block_or_slot INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS flow_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chain TEXT NOT NULL,
                token_address TEXT NOT NULL,
                token_symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                net_inflow_usd REAL NOT NULL,
                buy_ratio_pct REAL NOT NULL,
                unique_buyers INTEGER NOT NULL,
                summary TEXT NOT NULL,
                detected_at TEXT NOT NULL
            );",
            [],
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn insert_swap(&self, swap: &SwapRecord) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let chain_str = swap.chain.to_string();
        let side_str = match swap.side {
            SwapSide::Buy => "BUY",
            SwapSide::Sell => "SELL",
        };
        let ts_str = swap.timestamp.to_rfc3339();

        let res = conn.execute(
            "INSERT OR IGNORE INTO raw_swaps (
                chain, token_address, token_symbol, swapper_address, side, 
                amount_usd, token_amount, pool_address, tx_hash, block_number, timestamp
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                chain_str,
                swap.token_address,
                swap.token_symbol,
                swap.swapper_address,
                side_str,
                swap.amount_usd,
                swap.token_amount,
                swap.pool_address,
                swap.tx_hash,
                swap.block_number_or_slot,
                ts_str
            ],
        )?;

        Ok(res > 0)
    }

    pub fn get_cursor(&self, chain: Chain) -> Result<Option<u64>> {
        let conn = self.conn.lock().unwrap();
        let chain_str = chain.to_string();
        let mut stmt = conn.prepare("SELECT last_block_or_slot FROM scan_cursors WHERE chain = ?")?;
        let mut rows = stmt.query([chain_str])?;
        if let Some(r) = rows.next()? {
            let val: i64 = r.get(0)?;
            Ok(Some(val as u64))
        } else {
            Ok(None)
        }
    }

    pub fn set_cursor(&self, chain: Chain, block_or_slot: u64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let chain_str = chain.to_string();
        let now_str = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO scan_cursors (chain, last_block_or_slot, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(chain) DO UPDATE SET 
                last_block_or_slot = excluded.last_block_or_slot,
                updated_at = excluded.updated_at",
            params![chain_str, block_or_slot as i64, now_str],
        )?;
        Ok(())
    }

    pub fn insert_signal(&self, sig: &FlowSignal) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let chain_str = sig.chain.to_string();
        let sig_type_str = format!("{:?}", sig.signal_type);
        let ts_str = sig.detected_at.to_rfc3339();

        conn.execute(
            "INSERT INTO flow_signals (
                chain, token_address, token_symbol, timeframe, signal_type,
                confidence_score, net_inflow_usd, buy_ratio_pct, unique_buyers,
                summary, detected_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                chain_str,
                sig.token_address,
                sig.token_symbol,
                sig.timeframe,
                sig_type_str,
                sig.confidence_score,
                sig.metrics.net_inflow_usd,
                sig.metrics.buy_ratio_pct,
                sig.metrics.unique_buyers as i64,
                sig.summary,
                ts_str
            ],
        )?;
        Ok(())
    }

    pub fn get_recent_swaps_in_window(
        &self,
        chain: Chain,
        since: DateTime<Utc>,
    ) -> Result<Vec<SwapRecord>> {
        let conn = self.conn.lock().unwrap();
        let chain_str = chain.to_string();
        let since_str = since.to_rfc3339();

        let mut stmt = conn.prepare(
            "SELECT chain, token_address, token_symbol, swapper_address, side,
                    amount_usd, token_amount, pool_address, tx_hash, block_number, timestamp
             FROM raw_swaps
             WHERE chain = ?1 AND timestamp >= ?2
             ORDER BY timestamp ASC",
        )?;

        let rows = stmt.query_map(params![chain_str, since_str], |row| {
            let ch_str: String = row.get(0)?;
            let side_str: String = row.get(4)?;
            let ts_str: String = row.get(10)?;

            let chain = if ch_str == "Robinhood" {
                Chain::Robinhood
            } else {
                Chain::Solana
            };
            let side = if side_str == "BUY" {
                SwapSide::Buy
            } else {
                SwapSide::Sell
            };
            let timestamp = DateTime::parse_from_rfc3339(&ts_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());

            Ok(SwapRecord {
                chain,
                token_address: row.get(1)?,
                token_symbol: row.get(2)?,
                swapper_address: row.get(3)?,
                side,
                amount_usd: row.get(5)?,
                token_amount: row.get(6)?,
                pool_address: row.get(7)?,
                tx_hash: row.get(8)?,
                block_number_or_slot: row.get::<_, i64>(9)? as u64,
                timestamp,
            })
        })?;

        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }
}
