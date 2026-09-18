use chrono::{Duration, Utc};
use flow_sentinel::aggregator::window::RollingAggregator;
use flow_sentinel::models::{Chain, SignalType, SwapRecord, SwapSide};
use flow_sentinel::radar::signal_detector::SignalDetector;
use flow_sentinel::storage::db::SentinelDb;

#[test]
fn test_window_aggregator_and_strong_accumulation() {
    let now = Utc::now();
    let token = "0xTOKEN1111111111111111111111111111111111".to_string();

    // 4 distinct buyers in 1H window
    let mut swaps = Vec::new();
    for i in 1..=4 {
        swaps.push(SwapRecord {
            chain: Chain::Robinhood,
            token_address: token.clone(),
            token_symbol: "ALPHA".to_string(),
            swapper_address: format!("0xBUYER_{}", i),
            side: SwapSide::Buy,
            amount_usd: 250.0,
            token_amount: 1000.0,
            pool_address: "0xPOOL".to_string(),
            tx_hash: format!("0xTX_BUY_{}", i),
            block_number_or_slot: 1000 + i,
            timestamp: now - Duration::minutes(10 * i as i64),
        });
    }

    // 1 seller
    swaps.push(SwapRecord {
        chain: Chain::Robinhood,
        token_address: token.clone(),
        token_symbol: "ALPHA".to_string(),
        swapper_address: "0xSELLER_1".to_string(),
        side: SwapSide::Sell,
        amount_usd: 100.0,
        token_amount: 400.0,
        pool_address: "0xPOOL".to_string(),
        tx_hash: "0xTX_SELL_1".to_string(),
        block_number_or_slot: 1005,
        timestamp: now - Duration::minutes(5),
    });

    let metrics_1h = RollingAggregator::compute_metrics(Chain::Robinhood, &swaps, 1);
    assert_eq!(metrics_1h.len(), 1);
    let m = &metrics_1h[0];

    assert_eq!(m.buy_volume_usd, 1000.0);
    assert_eq!(m.sell_volume_usd, 100.0);
    assert_eq!(m.net_inflow_usd, 900.0);
    assert_eq!(m.unique_buyers, 4);
    assert_eq!(m.unique_sellers, 1);
    assert!(m.buy_ratio_pct > 90.0);

    let detector = SignalDetector::new(500.0, 2000.0, 65.0, 50000.0, 5000000.0);
    let sig = detector.detect_signal(m, Some(500000.0));
    assert!(sig.is_some());
    let sig = sig.unwrap();
    assert!(matches!(sig.signal_type, SignalType::StrongAccumulation));
    assert!(sig.confidence_score >= 85.0);

    // Test rejection when market cap exceeds $5M
    let sig_rejected = detector.detect_signal(m, Some(10000000.0));
    assert!(sig_rejected.is_none());
}

#[test]
fn test_wash_trading_detector_gating() {
    let now = Utc::now();
    let token = "0xWASH_TOKEN".to_string();

    // 1 single buyer churning 5 swaps
    let mut swaps = Vec::new();
    for i in 1..=5 {
        swaps.push(SwapRecord {
            chain: Chain::Solana,
            token_address: token.clone(),
            token_symbol: "WASH".to_string(),
            swapper_address: "SOL_CHURNER_WALLET_1111".to_string(),
            side: SwapSide::Buy,
            amount_usd: 500.0,
            token_amount: 10000.0,
            pool_address: "SOL_POOL".to_string(),
            tx_hash: format!("SOL_TX_{}", i),
            block_number_or_slot: 2000 + i,
            timestamp: now - Duration::minutes(5 * i as i64),
        });
    }

    let metrics_1h = RollingAggregator::compute_metrics(Chain::Solana, &swaps, 1);
    let m = &metrics_1h[0];
    assert_eq!(m.unique_buyers, 1);
    assert_eq!(m.total_volume_usd, 2500.0);

    let detector = SignalDetector::new(500.0, 2000.0, 65.0, 50000.0, 5000000.0);
    let sig = detector.detect_signal(m, None).unwrap();
    assert!(matches!(sig.signal_type, SignalType::WashTradingSuspect));
}

#[test]
fn test_database_roundtrip_zero_execution() {
    let temp_db = "/tmp/test_flow_sentinel.db";
    let _ = std::fs::remove_file(temp_db);
    let db = SentinelDb::new(temp_db).unwrap();

    let now = Utc::now();
    let swap = SwapRecord {
        chain: Chain::Robinhood,
        token_address: "0xTEST".to_string(),
        token_symbol: "TST".to_string(),
        swapper_address: "0xUSER".to_string(),
        side: SwapSide::Buy,
        amount_usd: 150.0,
        token_amount: 10.0,
        pool_address: "0xPOOL".to_string(),
        tx_hash: "0xHASH_123".to_string(),
        block_number_or_slot: 12345,
        timestamp: now,
    };

    let inserted = db.insert_swap(&swap).unwrap();
    assert!(inserted);

    // Duplicate tx hash should be ignored idempotently
    let dup = db.insert_swap(&swap).unwrap();
    assert!(!dup);

    let recent = db
        .get_recent_swaps_in_window(Chain::Robinhood, now - Duration::hours(1))
        .unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].token_symbol, "TST");
}
