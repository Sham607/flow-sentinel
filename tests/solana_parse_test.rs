use flow_sentinel::chains::solana::{parse_swaps_from_tx, WSOL_MINT};
use flow_sentinel::models::SwapSide;
use serde_json::{json, Value};

const USER: &str = "USERWALLET11111111111111111111111111111111";
const TOKEN_MINT: &str = "TOKENMINT11111111111111111111111111111111";

fn base_tx() -> Value {
    json!({
        "slot": 12345u64,
        "blockTime": 1789740000i64,
        "transaction": {
            "message": {
                "accountKeys": [
                    { "pubkey": USER, "signer": true, "writable": true, "source": "transaction" },
                    { "pubkey": "WSOLATAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX1", "signer": false, "writable": true, "source": "transaction" },
                    { "pubkey": "TOKENATAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX1", "signer": false, "writable": true, "source": "transaction" }
                ]
            }
        },
        "meta": {
            "err": null,
            "fee": { "lamports": 5000 },
            "preBalances": [],
            "postBalances": [],
            "preTokenBalances": [],
            "postTokenBalances": [],
            "loadedAddresses": { "writable": [], "readonly": [] }
        }
    })
}

#[test]
fn test_parse_raydium_style_buy_with_wsol_leg() {
    let mut tx = base_tx();
    // User pays 5 WSOL (WSOL ATA closed in same tx -> absent from post)
    tx["meta"]["preBalances"] = json!([10000000u64, 25000000u64, 2039280u64]);
    tx["meta"]["postBalances"] = json!([10000000u64, 0u64, 2039280u64]);
    tx["meta"]["preTokenBalances"] = json!([
        { "accountIndex": 1, "mint": WSOL_MINT, "owner": USER,
          "uiTokenAmount": { "amount": "5000000000", "decimals": 9 } }
    ]);
    // User receives 100 TOKEN (6 decimals, ATA created in this tx -> absent from pre)
    tx["meta"]["postTokenBalances"] = json!([
        { "accountIndex": 2, "mint": TOKEN_MINT, "owner": USER,
          "uiTokenAmount": { "amount": "100000000", "decimals": 6 } }
    ]);

    let swaps = parse_swaps_from_tx(&tx, Some(200.0));
    assert_eq!(swaps.len(), 1);
    let s = &swaps[0];
    assert_eq!(s.mint, TOKEN_MINT);
    assert_eq!(s.owner, USER);
    assert!(matches!(s.side, SwapSide::Buy));
    assert!((s.amount_usd - 1000.0).abs() < 1e-6, "got {}", s.amount_usd);
    assert!((s.token_amount - 100.0).abs() < 1e-6);
    assert_eq!(s.slot, 12345);
}

#[test]
fn test_parse_pumpfun_style_sell_with_native_sol_leg() {
    let mut tx = base_tx();
    // User sends 50 TOKEN (9 decimals, ATA closed -> absent from post)
    tx["meta"]["preBalances"] = json!([10000000u64, 2039280u64]);
    tx["meta"]["postBalances"] = json!([1009995000u64, 2039280u64]); // +1 SOL proceeds, -5000 fee handled in parser
    tx["meta"]["preTokenBalances"] = json!([
        { "accountIndex": 1, "mint": TOKEN_MINT, "owner": USER,
          "uiTokenAmount": { "amount": "50000000000", "decimals": 9 } }
    ]);
    tx["meta"]["postTokenBalances"] = json!([]);

    let swaps = parse_swaps_from_tx(&tx, Some(200.0));
    assert_eq!(swaps.len(), 1);
    let s = &swaps[0];
    assert_eq!(s.mint, TOKEN_MINT);
    assert!(matches!(s.side, SwapSide::Sell));
    // proceeds = (0.999995 - 0.000005) SOL * $200
    assert!((s.amount_usd - 199.998).abs() < 1e-6, "got {}", s.amount_usd);
    assert!((s.token_amount - 50.0).abs() < 1e-6);
}

#[test]
fn test_failed_tx_yields_no_swaps() {
    let mut tx = base_tx();
    tx["meta"]["err"] = json!({"InstructionError": [0, {"Custom": 6001}]});
    tx["meta"]["preTokenBalances"] = json!([
        { "accountIndex": 1, "mint": TOKEN_MINT, "owner": USER,
          "uiTokenAmount": { "amount": "1000000", "decimals": 6 } }
    ]);
    let swaps = parse_swaps_from_tx(&tx, Some(200.0));
    assert!(swaps.is_empty());
}
