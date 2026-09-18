use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Chain {
    Robinhood,
    Solana,
}

impl std::fmt::Display for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Chain::Robinhood => write!(f, "Robinhood"),
            Chain::Solana => write!(f, "Solana"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SwapSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapRecord {
    pub chain: Chain,
    pub token_address: String,
    pub token_symbol: String,
    pub swapper_address: String,
    pub side: SwapSide,
    pub amount_usd: f64,
    pub token_amount: f64,
    pub pool_address: String,
    pub tx_hash: String,
    pub block_number_or_slot: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeframeMetrics {
    pub chain: Chain,
    pub token_address: String,
    pub token_symbol: String,
    pub timeframe: String, // "1H" or "4H"
    pub total_volume_usd: f64,
    pub buy_volume_usd: f64,
    pub sell_volume_usd: f64,
    pub net_inflow_usd: f64,
    pub buy_ratio_pct: f64,
    pub unique_buyers: usize,
    pub unique_sellers: usize,
    pub total_swaps: usize,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignalType {
    StrongAccumulation,   // Net inflow > threshold, Buy ratio >= 70%, Organic buyers >= 3
    OrganicMomentum,      // Steady inflow across unique buyers
    WashTradingSuspect,   // High volume but very low unique buyers (1-2 wallets)
    HeavyDistribution,    // Heavy sell ratio > 70%
    Neutral,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowSignal {
    pub chain: Chain,
    pub token_address: String,
    pub token_symbol: String,
    pub timeframe: String, // "1H" or "4H"
    pub signal_type: SignalType,
    pub confidence_score: f64, // 0.0 - 100.0
    pub summary: String,
    pub metrics: TimeframeMetrics,
    pub detected_at: DateTime<Utc>,
}
