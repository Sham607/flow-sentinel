# Flow-Sentinel 📡

Flow-Sentinel adalah engine intelijen on-chain **100% Read-Only** untuk memantau aliran likuiditas dan akumulasi modal (*Order Flow & Volume Dynamics*) di **Robinhood Chain (ID: 4663)** dan **Solana**, dengan fokus evaluasi pada timeframe **1H** dan **4H**.

---

## Prinsip Keamanan: Zero Execution Risk 🛡️

1. **Tanpa Private Key:** Tidak ada konfigurasi atau penyimpanan private key apa pun (Solana Keypair maupun EVM Private Key).
2. **Tanpa Transaksi Router:** Tidak ada modul swap builder, aggregator signer, atau broadcast intent.
3. **Murni Analitik & Surveillance:** Bertindak sebagai radar pengintai untuk mendeteksi akumulasi organik, rotasi whale, dan anomali *wash trading*.

---

## Target Chain & Protokol

### 1. Robinhood Chain (Arbitrum Nitro L2, Chain ID: 4663)
- **Uniswap V4:** Memantau swap event via `PoolManager` (`0x8366a39CC670B4001A1121B8F6A443A643e40951`).
- **Uniswap V3:** Memantau `Swap` event topic pada router dan factory resmi.
- **Pons Protocol:** Memantau token yang bertransisi dari *bonding curve* ke pool likuiditas V4.

### 2. Solana Network
- **Raydium AMM V4:** Memantau swap event dan volume pool.
- **Pump.fun:** Memantau bonding curve & graduation alerts.

---

## Logika Deteksi Sinyal (Rolling 1H / 4H Window)

Sinyal dihitung murni dari data on-chain, bukan sekadar candlestick teknikal biasa:

1. **`StrongAccumulation`**:
   - $\text{Net Inflow} \ge \text{Threshold}$ (misal: > $500 dalam 1H, > $2.000 dalam 4H).
   - $\text{Buy Ratio} \ge 60\% - 70\%$.
   - Didorong oleh $\ge 3$ pembeli unik (*Organic Unique Buyers*).
2. **`WashTradingSuspect`**:
   - Volume besar namun hanya digerakkan oleh 1 atau 2 wallet yang melakukan *churning* swap berulang.
3. **`HeavyDistribution`**:
   - $\text{Sell Ratio} \ge 70\%$ dengan arus keluar (*outflow*) signifikan.

---

## Struktur Direktori

```text
/home/node/Developments/flow-sentinel/
├── Cargo.toml
├── .env.example
├── README.md
├── src/
│   ├── lib.rs
│   ├── main.rs                 # CLI daemon radar loop
│   ├── config.rs               # Fail-closed environment parser
│   ├── models.rs               # Data structures (FlowSignal, TimeframeMetrics, SwapRecord)
│   ├── storage/
│   │   ├── mod.rs
│   │   └── db.rs               # SQLite WAL database (raw_swaps, flow_signals, scan_cursors)
│   ├── chains/
│   │   ├── mod.rs
│   │   ├── robinhood.rs        # EVM RPC log reader (Uniswap V4/V3, Pons)
│   │   └── solana.rs           # Solana RPC confirmed signatures reader
│   ├── aggregator/
│   │   ├── mod.rs
│   │   └── window.rs           # Rolling 1H & 4H metrics aggregator
│   └── radar/
│       ├── mod.rs
│       └── signal_detector.rs  # Detection heuristics & confidence scoring
└── tests/
    └── radar_test.rs           # Unit & integration test suite
```

---

## Menjalankan Radar

1. **Build binary:**
   ```bash
   cargo build --release
   ```
2. **Jalankan test suite:**
   ```bash
   cargo test
   ```
3. **Jalankan monitor CLI:**
   ```bash
   cargo run --release
   ```
