use flow_sentinel::chains::solana::parse_swaps_from_tx;

const FIXTURES: &[&str] = &[
    "tests/fixtures/jupiter.json",
    "tests/fixtures/meteora_damm.json",
    "tests/fixtures/meteora_dlmm.json",
    "tests/fixtures/pumpfun.json",
    "tests/fixtures/raydium_cpmm.json",
];

#[test]
fn test_real_mainnet_txs_produce_swaps() {
    let mut total = 0usize;
    for path in FIXTURES {
        let raw = std::fs::read_to_string(path).expect(path);
        let tx: serde_json::Value = serde_json::from_str(&raw).expect(path);
        let swaps = parse_swaps_from_tx(&tx, Some(110.0)); // approx SOL price
        println!("{} -> {} swaps", path, swaps.len());
        for s in &swaps {
            println!(
                "   {:?} mint={}.. owner={}.. usd={:.2} amt={:.4}",
                s.side,
                &s.mint[..s.mint.len().min(8)],
                &s.owner[..s.owner.len().min(8)],
                s.amount_usd,
                s.token_amount
            );
        }
        total += swaps.len();
    }
    println!("TOTAL: {}", total);
    assert!(total > 0, "real mainnet txs must yield at least one swap");
}
