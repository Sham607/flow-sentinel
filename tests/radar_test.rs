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

    let metrics_1h = RollingAggregator::compute_metrics(Chain::Robinhood, &swaps, 60);
    assert_eq!(metrics_1h.len(), 1);
    let m = &metrics_1h[0];

    assert_eq!(m.buy_volume_usd, 1000.0);
    assert_eq!(m.sell_volume_usd, 100.0);
    assert_eq!(m.net_inflow_usd, 900.0);
    assert_eq!(m.unique_buyers, 4);
    assert_eq!(m.unique_sellers, 1);
    assert!(m.buy_ratio_pct > 90.0);
    assert_eq!(m.timeframe, "1H");

    let detector = SignalDetector::new(100.0, 250.0, 500.0, 1000.0, 55.0, 3, 50000.0, 5000000.0);
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

    let metrics_1h = RollingAggregator::compute_metrics(Chain::Solana, &swaps, 60);
    let m = &metrics_1h[0];
    assert_eq!(m.unique_buyers, 1);
    assert_eq!(m.total_volume_usd, 2500.0);

    let detector = SignalDetector::new(100.0, 250.0, 500.0, 1000.0, 55.0, 3, 50000.0, 5000000.0);
    let sig = detector.detect_signal(m, None).unwrap();
    assert!(matches!(sig.signal_type, SignalType::WashTradingSuspect));
}

#[test]
fn test_five_minute_window_and_threshold() {
    let now = Utc::now();
    let token = "0xFIVE_MIN_TOKEN".to_string();

    // 3 distinct buyers within the last 3 minutes (inside 5M window)
    let mut swaps = Vec::new();
    for i in 1..=3 {
        swaps.push(SwapRecord {
            chain: Chain::Robinhood,
            token_address: token.clone(),
            token_symbol: "FAST".to_string(),
            swapper_address: format!("0xSCALPER_{}", i),
            side: SwapSide::Buy,
            amount_usd: 40.0,
            token_amount: 100.0,
            pool_address: "0xPOOL".to_string(),
            tx_hash: format!("0xTX_FAST_{}", i),
            block_number_or_slot: 3000 + i,
            timestamp: now - Duration::minutes(i as i64),
        });
    }
    // One old buyer 10 minutes ago: inside 15M window, OUTSIDE 5M window
    swaps.push(SwapRecord {
        chain: Chain::Robinhood,
        token_address: token.clone(),
        token_symbol: "FAST".to_string(),
        swapper_address: "0xOLD_BUYER".to_string(),
        side: SwapSide::Buy,
        amount_usd: 40.0,
        token_amount: 100.0,
        pool_address: "0xPOOL".to_string(),
        tx_hash: "0xTX_OLD".to_string(),
        block_number_or_slot: 3010,
        timestamp: now - Duration::minutes(10),
    });

    // 5M window: only the 3 fresh buyers count, label must be "5M"
    let metrics_5m = RollingAggregator::compute_metrics(Chain::Robinhood, &swaps, 5);
    assert_eq!(metrics_5m.len(), 1);
    let m5 = &metrics_5m[0];
    assert_eq!(m5.timeframe, "5M");
    assert_eq!(m5.unique_buyers, 3);
    assert_eq!(m5.buy_volume_usd, 120.0);

    // 15M window: all 4 buyers count, label must be "15M"
    let metrics_15m = RollingAggregator::compute_metrics(Chain::Robinhood, &swaps, 15);
    let m15 = &metrics_15m[0];
    assert_eq!(m15.timeframe, "15M");
    assert_eq!(m15.unique_buyers, 4);
    assert_eq!(m15.buy_volume_usd, 160.0);

    // Detector: 5M label uses the 5M threshold ($100), so $120 net inflow passes
    let detector = SignalDetector::new(100.0, 250.0, 500.0, 1000.0, 55.0, 3, 50000.0, 5000000.0);
    let sig = detector.detect_signal(m5, Some(500000.0)).unwrap();
    assert!(matches!(sig.signal_type, SignalType::StrongAccumulation));
    assert_eq!(sig.timeframe, "5M");

    // Same metrics labelled 1H would need the $500 1H threshold -> rejected
    let mut m1h = m5.clone();
    m1h.timeframe = "1H".to_string();
    assert!(detector.detect_signal(&m1h, Some(500000.0)).is_none());

    // min_unique_buyers gate: only 2 unique buyers must be rejected
    let mut m2buyers = m5.clone();
    m2buyers.unique_buyers = 2;
    assert!(detector.detect_signal(&m2buyers, Some(500000.0)).is_none());
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
