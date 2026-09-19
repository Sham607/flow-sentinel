/// Regression tests for the rewritten Robinhood parser.
/// Fixtures are REAL mainnet transaction logs (Robinhood Chain, ID 4663),
/// dumped via eth_getLogs from live swap transactions.
use flow_sentinel::chains::robinhood::{compute_swaps_from_logs, RhComputeCtx};
use flow_sentinel::models::SwapSide;
use std::collections::HashMap;

fn load_fixture(name: &str) -> Vec<serde_json::Value> {
    let path = format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), name);
    let raw = std::fs::read_to_string(path).expect("fixture file missing");
    serde_json::from_str(&raw).expect("fixture must be a JSON log array")
}

fn filter_tx<'a>(logs: &'a [serde_json::Value], tx: &str) -> Vec<serde_json::Value> {
    logs.iter()
        .filter(|l| l["transactionHash"].as_str().map(|h| h.eq_ignore_ascii_case(tx)).unwrap_or(false))
        .cloned()
        .collect()
}

fn dec_map() -> HashMap<String, u8> {
    let mut m = HashMap::new();
    m.insert("0x156e175dd063a8ce274c50654ef40e0032b3fbcf".to_string(), 18u8);
    m.insert("0x117cc2133c37b721f49de2a7a74833232b3b4c0c".to_string(), 18u8);
    m.insert("0x385307be7df9b45f55f8be72e0cf8b36466813ee".to_string(), 18u8);
    m
}

fn ctx<'a>(
    weth: Option<f64>,
    dec: &'a HashMap<String, u8>,
    eoa: &'a HashMap<String, bool>,
    tx_value: u128,
) -> RhComputeCtx<'a> {
    RhComputeCtx {
        weth_price: weth,
        decimals: dec,
        is_eoa: eoa,
        tx_value_eth_wei: tx_value,
    }
}

/// TX 0x285c33a2... : user 0x5b7c...028c sent 20.901781 USDG through a router
/// and received 0.058671258333857699 of token 0x156e...fbcf => BUY $20.901781.
/// The affiliate payout (0x28c3...: +0.320263 USDG, no token position) and the
/// net-zero router must NOT be recorded.
#[test]
fn test_robinhood_tx0_real_mainnet_buy() {
    let logs = load_fixture("robinhood_tx0.json");
    let dec = dec_map();
    let eoa = HashMap::new();
    let swaps = compute_swaps_from_logs(&logs, &ctx(Some(3000.0), &dec, &eoa, 0));

    assert_eq!(swaps.len(), 1, "only the real buyer should be recorded");
    let s = &swaps[0];
    assert_eq!(s.swapper, "0x5b7c404f1d7d77f9f3885ab13d7764f8a173028c");
    assert_eq!(s.token, "0x156e175dd063a8ce274c50654ef40e0032b3fbcf");
    assert!(matches!(s.side, SwapSide::Buy));
    assert!(
        (s.amount_usd - 20.901781).abs() < 0.0001,
        "USD value must equal the real USDG cash flow, got {}",
        s.amount_usd
    );
    assert!((s.token_amount - 0.058671258333857699).abs() < 1e-12);
    assert!(s.block_num > 0);
}

/// TX 0x32c8959f... : hook-mediated V4+V3 swap. Address 0x05e1...8e0d received
/// net +102.267154 USDG and net-sent 0.133878648295504282 token 0x117c...0c0c
/// => SELL $102.267154. The $1.19 USDG receiver (no token position) is ignored.
#[test]
fn test_robinhood_tx2_real_mainnet_sell() {
    let logs = load_fixture("robinhood_tx2.json");
    let dec = dec_map();
    let eoa = HashMap::new();
    let swaps = compute_swaps_from_logs(&logs, &ctx(Some(3000.0), &dec, &eoa, 0));

    assert_eq!(swaps.len(), 1);
    let s = &swaps[0];
    assert_eq!(s.swapper, "0x05e146f995aa70d0bf8481ee3d27368a537a8e0d");
    assert_eq!(s.token, "0x117cc2133c37b721f49de2a7a74833232b3b4c0c");
    assert!(matches!(s.side, SwapSide::Sell));
    assert!(
        (s.amount_usd - 102.267154).abs() < 0.0001,
        "USD value must equal the real USDG cash flow, got {}",
        s.amount_usd
    );
    assert!((s.token_amount - 0.133878648295504282).abs() < 1e-12);
}

/// TX 0x717ea39a... (native settlement): EOA 0xafcf...2674 sold 634,803.7 tokens
/// of 0x3853...13ee to the V4 PoolManager and was paid via a hook that wrapped
/// and burned exactly 0.025061724826672142 WETH (native ETH, invisible to ERC20
/// net flows). Fallback layer must attribute the wrap flow value => SELL.
#[test]
fn test_robinhood_native_settlement_fallback() {
    let all = load_fixture("robinhood_tx_native.json");
    let logs = filter_tx(
        &all,
        "0x717ea39a6e7c3b443b85971d00bbaedcb502b3265f4a4d8e9d23663af694bc56",
    );
    let dec = dec_map();
    let eoa = HashMap::new();
    let weth_price = 2628.47f64;
    let swaps = compute_swaps_from_logs(&logs, &ctx(Some(weth_price), &dec, &eoa, 0));

    assert_eq!(swaps.len(), 1, "only the EOA seller should be recorded");
    let s = &swaps[0];
    assert_eq!(s.swapper, "0xafcf61f9f69ffa4deeb6deb79f0b7ee4ac3d2674");
    assert_eq!(s.token, "0x385307be7df9b45f55f8be72e0cf8b36466813ee");
    assert!(matches!(s.side, SwapSide::Sell));
    let expected = 0.025061724826672142f64 * weth_price;
    assert!(
        (s.amount_usd - expected).abs() < 0.01,
        "fallback value must equal wrapped WETH x price: got {} want {}",
        s.amount_usd,
        expected
    );
    assert!(
        (s.token_amount / 634803.7395756009 - 1.0).abs() < 1e-9,
        "token_amount = {:.12}",
        s.token_amount
    );
}

/// Empty / no-swap log sets must yield nothing.
#[test]
fn test_robinhood_no_swap_logs_yield_nothing() {
    let dec = dec_map();
    let eoa = HashMap::new();
    let swaps = compute_swaps_from_logs(&[], &ctx(Some(3000.0), &dec, &eoa, 0));
    assert!(swaps.is_empty());
}
