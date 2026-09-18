use chrono::Utc;

use crate::models::{FlowSignal, SignalType, TimeframeMetrics};

pub struct SignalDetector {
    min_inflow_1h: f64,
    min_inflow_4h: f64,
    min_buy_ratio: f64,
    min_market_cap: f64,
    max_market_cap: f64,
}

impl SignalDetector {
    pub fn new(
        min_inflow_1h: f64,
        min_inflow_4h: f64,
        min_buy_ratio: f64,
        min_market_cap: f64,
        max_market_cap: f64,
    ) -> Self {
        Self {
            min_inflow_1h,
            min_inflow_4h,
            min_buy_ratio,
            min_market_cap,
            max_market_cap,
        }
    }

    pub fn detect_signal(&self, metrics: &TimeframeMetrics, market_cap: Option<f64>) -> Option<FlowSignal> {
        let min_inflow = if metrics.timeframe == "1H" {
            self.min_inflow_1h
        } else {
            self.min_inflow_4h
        };

        // Rule 1: Wash trading detection (High volume, but only 1 unique buyer)
        if metrics.total_volume_usd >= min_inflow && metrics.unique_buyers <= 1 && metrics.total_swaps >= 3 {
            return Some(FlowSignal {
                chain: metrics.chain,
                token_address: metrics.token_address.clone(),
                token_symbol: metrics.token_symbol.clone(),
                timeframe: metrics.timeframe.clone(),
                signal_type: SignalType::WashTradingSuspect,
                confidence_score: 85.0,
                summary: format!(
                    "Volume ${:.0} across {} swaps driven by only 1 unique buyer. Suspected wash trading.",
                    metrics.total_volume_usd, metrics.total_swaps
                ),
                metrics: metrics.clone(),
                detected_at: Utc::now(),
            });
        }

        // Rule 2: Strong Accumulation (Gated by Market Cap range if available)
        if metrics.net_inflow_usd >= min_inflow
            && metrics.buy_ratio_pct >= self.min_buy_ratio
            && metrics.unique_buyers >= 3
        {
            if let Some(mc) = market_cap {
                if mc < self.min_market_cap || mc > self.max_market_cap {
                    return None; // Exceeds or below market cap gates
                }
            }

            let conf = (metrics.buy_ratio_pct * 0.7 + (metrics.unique_buyers as f64 * 6.0).min(30.0)).min(98.0);
            let mc_str = market_cap
                .map(|mc| {
                    if mc >= 1_000_000.0 {
                        format!(" | MC: ${:.2}M", mc / 1_000_000.0)
                    } else {
                        format!(" | MC: ${:.0}k", mc / 1_000.0)
                    }
                })
                .unwrap_or_default();

            return Some(FlowSignal {
                chain: metrics.chain,
                token_address: metrics.token_address.clone(),
                token_symbol: metrics.token_symbol.clone(),
                timeframe: metrics.timeframe.clone(),
                signal_type: SignalType::StrongAccumulation,
                confidence_score: conf,
                summary: format!(
                    "Net Inflow +${:.0} ({:.1}% buys) driven by {} unique buyers{}.",
                    metrics.net_inflow_usd, metrics.buy_ratio_pct, metrics.unique_buyers, mc_str
                ),
                metrics: metrics.clone(),
                detected_at: Utc::now(),
            });
        }

        // Rule 3: Heavy Distribution
        if metrics.sell_volume_usd >= min_inflow && metrics.buy_ratio_pct <= 30.0 {
            return Some(FlowSignal {
                chain: metrics.chain,
                token_address: metrics.token_address.clone(),
                token_symbol: metrics.token_symbol.clone(),
                timeframe: metrics.timeframe.clone(),
                signal_type: SignalType::HeavyDistribution,
                confidence_score: 80.0,
                summary: format!(
                    "Net Outflow -${:.0} (Heavy sell-off: {:.1}% sells).",
                    metrics.sell_volume_usd - metrics.buy_volume_usd,
                    100.0 - metrics.buy_ratio_pct
                ),
                metrics: metrics.clone(),
                detected_at: Utc::now(),
            });
        }

        None
    }
}
