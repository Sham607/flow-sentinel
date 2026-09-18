use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde_json::Value;

#[derive(Clone)]
pub struct MarketCapResolver {
    client: reqwest::Client,
    cache: Arc<RwLock<HashMap<String, (Option<f64>, std::time::Instant)>>>,
}

impl MarketCapResolver {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap_or_default(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
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
