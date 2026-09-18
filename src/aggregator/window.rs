use chrono::{Duration, Utc};
use std::collections::{HashMap, HashSet};

use crate::models::{Chain, SwapRecord, SwapSide, TimeframeMetrics};

pub struct RollingAggregator;

impl RollingAggregator {
    pub fn compute_metrics(
        chain: Chain,
        swaps: &[SwapRecord],
        timeframe_hours: i64,
    ) -> Vec<TimeframeMetrics> {
        let now = Utc::now();
        let window_start = now - Duration::hours(timeframe_hours);
        let timeframe_label = format!("{}H", timeframe_hours);

        // Filter swaps within window
        let recent_swaps: Vec<&SwapRecord> = swaps
            .iter()
            .filter(|s| s.chain == chain && s.timestamp >= window_start)
            .collect();

        // Group by token
        let mut grouped: HashMap<String, Vec<&SwapRecord>> = HashMap::new();
        for s in recent_swaps {
            grouped.entry(s.token_address.clone()).or_default().push(s);
        }

        let mut metrics_list = Vec::new();

        for (token_addr, token_swaps) in grouped {
            let symbol = token_swaps[0].token_symbol.clone();
            let mut buy_vol = 0.0;
            let mut sell_vol = 0.0;
            let mut buyers = HashSet::new();
            let mut sellers = HashSet::new();

            for s in &token_swaps {
                match s.side {
                    SwapSide::Buy => {
                        buy_vol += s.amount_usd;
                        buyers.insert(s.swapper_address.clone());
                    }
                    SwapSide::Sell => {
                        sell_vol += s.amount_usd;
                        sellers.insert(s.swapper_address.clone());
                    }
                }
            }

            let total_vol = buy_vol + sell_vol;
            let net_inflow = buy_vol - sell_vol;
            let buy_ratio = if total_vol > 0.0 {
                (buy_vol / total_vol) * 100.0
            } else {
                0.0
            };

            metrics_list.push(TimeframeMetrics {
                chain,
                token_address: token_addr,
                token_symbol: symbol,
                timeframe: timeframe_label.clone(),
                total_volume_usd: total_vol,
                buy_volume_usd: buy_vol,
                sell_volume_usd: sell_vol,
                net_inflow_usd: net_inflow,
                buy_ratio_pct: buy_ratio,
                unique_buyers: buyers.len(),
                unique_sellers: sellers.len(),
                total_swaps: token_swaps.len(),
                window_start,
                window_end: now,
            });
        }

        metrics_list
    }
}
