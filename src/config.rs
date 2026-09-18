use anyhow::Result;
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub robinhood_rpc_urls: Vec<String>,
    pub robinhood_scan_chunk: u64,
    pub solana_rpc_url: String,
    pub database_path: String,
    pub poll_interval_secs: u64,
    pub min_inflow_usd_1h: f64,
    pub min_inflow_usd_4h: f64,
    pub min_buy_ratio_pct: f64,
    pub min_market_cap_usd: f64,
    pub max_market_cap_usd: f64,
}

impl Config {
    pub fn load() -> Result<Self> {
        let _ = dotenvy::dotenv();

        let rh_rpc_str = env::var("ROBINHOOD_RPC_URL")
            .unwrap_or_else(|_| "https://robinhood.api.pocket.network,https://rpc.ordofi.network".to_string());
        let robinhood_rpc_urls: Vec<String> = rh_rpc_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let robinhood_scan_chunk = env::var("ROBINHOOD_SCAN_CHUNK")
            .unwrap_or_else(|_| "1000".to_string())
            .parse::<u64>()
            .unwrap_or(1000);

        let solana_rpc_url = env::var("SOLANA_RPC_URL")
            .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());

        let database_path = env::var("DATABASE_PATH")
            .unwrap_or_else(|_| "data/flow_sentinel.db".to_string());

        let poll_interval_secs = env::var("POLL_INTERVAL_SECS")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<u64>()
            .unwrap_or(10);

        let min_inflow_usd_1h = env::var("MIN_INFLOW_USD_1H")
            .unwrap_or_else(|_| "500.0".to_string())
            .parse::<f64>()
            .unwrap_or(500.0);

        let min_inflow_usd_4h = env::var("MIN_INFLOW_USD_4H")
            .unwrap_or_else(|_| "2000.0".to_string())
            .parse::<f64>()
            .unwrap_or(2000.0);

        let min_buy_ratio_pct = env::var("MIN_BUY_RATIO_PCT")
            .unwrap_or_else(|_| "60.0".to_string())
            .parse::<f64>()
            .unwrap_or(60.0);

        let min_market_cap_usd = env::var("MIN_MARKET_CAP_USD")
            .unwrap_or_else(|_| "50000.0".to_string())
            .parse::<f64>()
            .unwrap_or(50000.0);

        let max_market_cap_usd = env::var("MAX_MARKET_CAP_USD")
            .unwrap_or_else(|_| "5000000.0".to_string())
            .parse::<f64>()
            .unwrap_or(5000000.0);

        Ok(Self {
            robinhood_rpc_urls,
            robinhood_scan_chunk,
            solana_rpc_url,
            database_path,
            poll_interval_secs,
            min_inflow_usd_1h,
            min_inflow_usd_4h,
            min_buy_ratio_pct,
            min_market_cap_usd,
            max_market_cap_usd,
        })
    }
}
