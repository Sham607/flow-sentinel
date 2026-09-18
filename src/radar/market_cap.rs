use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde_json::Value;

#[derive(Clone)]
pub struct MarketCapResolver {
    client: reqwest::Client,
    cache: Arc<RwLock<HashMap<String, (Option<f64>, std::time::Instant)>>>,
    symbol_cache: Arc<RwLock<HashMap<String, (Option<String>, std::time::Instant)>>>,
    sol_price_cache: Arc<RwLock<Option<(f64, std::time::Instant)>>>,
}

impl MarketCapResolver {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap_or_default(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            symbol_cache: Arc::new(RwLock::new(HashMap::new())),
            sol_price_cache: Arc::new(RwLock::new(None)),
        }
    }

    async fn fetch_pair_json(&self, token_address: &str) -> Option<Value> {
        let url = format!(
            "https://api.dexscreener.com/latest/dex/tokens/{}",
            token_address
        );
        let resp = self.client.get(&url).send().await.ok()?;
        resp.json::<Value>().await.ok()
    }

    /// Live SOL/USD price via DexScreener (cached 60 seconds).
    pub async fn get_sol_price(&self) -> Option<f64> {
        {
            let c = self.sol_price_cache.read().await;
            if let Some((price, at)) = *c {
                if at.elapsed() < std::time::Duration::from_secs(60) {
                    return Some(price);
                }
            }
        }

        let price = self
            .fetch_pair_json("So11111111111111111111111111111111111111112")
            .await
            .and_then(|json| json.get("pairs").and_then(|p| p.as_array()).cloned())
            .and_then(|pairs| pairs.first().cloned())
            .and_then(|p| {
                p.get("priceUsd")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
            });

        if let Some(p) = price {
            let mut c = self.sol_price_cache.write().await;
            *c = Some((p, std::time::Instant::now()));
        }
        price
    }

    /// Token symbol via DexScreener base token (cached 6 hours).
    pub async fn get_token_symbol(&self, token_address: &str) -> Option<String> {
        let key = token_address.to_lowercase();
        {
            let c = self.symbol_cache.read().await;
            if let Some((sym, at)) = c.get(&key) {
                if at.elapsed() < std::time::Duration::from_secs(21600) {
                    return sym.clone();
                }
            }
        }

        let symbol = self
            .fetch_pair_json(token_address)
            .await
            .and_then(|json| json.get("pairs").and_then(|p| p.as_array()).cloned())
            .and_then(|pairs| pairs.first().cloned())
            .and_then(|p| {
                p.pointer("/baseToken/symbol")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            });

        {
            let mut c = self.symbol_cache.write().await;
            c.insert(key, (symbol.clone(), std::time::Instant::now()));
        }
        symbol
    }

    pub async fn get_market_cap(&self, token_address: &str) -> Option<f64> {
        let key = token_address.to_lowercase();

        // 1. Check cache (Valid for 15 minutes)
        {
            let cache_read = self.cache.read().await;
            if let Some((mc, instant)) = cache_read.get(&key) {
                if instant.elapsed() < std::time::Duration::from_secs(900) {
                    return *mc;
                }
            }
        }

        // 2. Query DexScreener
        let url = format!("https://api.dexscreener.com/latest/dex/tokens/{}", key);
        let mc = match self.client.get(&url).send().await {
            Ok(resp) => {
                if let Ok(json) = resp.json::<Value>().await {
                    if let Some(pairs) = json.get("pairs").and_then(|p| p.as_array()) {
                        if let Some(first) = pairs.first() {
                            first.get("marketCap")
                                .and_then(|v| v.as_f64())
                                .or_else(|| first.get("fdv").and_then(|v| v.as_f64()))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            Err(_) => None,
        };

        // 3. Update cache
        {
            let mut cache_write = self.cache.write().await;
            cache_write.insert(key, (mc, std::time::Instant::now()));
        }

        mc
    }
}
