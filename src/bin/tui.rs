use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, Paragraph, Tabs},
    Frame, Terminal,
};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use flow_sentinel::config::Config;
use flow_sentinel::models::{Chain, FlowSignal, SignalType, SwapRecord, SwapSide};
use flow_sentinel::storage::db::SentinelDb;

#[derive(PartialEq, Eq)]
enum ActiveTab {
    RadarSignals = 0,
    FlowFeed = 1,
    Metrics1H = 2,
    Metrics4H = 3,
    Metrics15M = 4,
    Metrics5M = 5,
    ConfigChain = 6,
}

struct AppState {
    active_tab: ActiveTab,
    signals: Vec<FlowSignal>,
    swaps: Vec<SwapRecord>,
    leaderboard_1h: Vec<FlowSignal>,
    leaderboard_4h: Vec<FlowSignal>,
    leaderboard_15m: Vec<FlowSignal>,
    leaderboard_5m: Vec<FlowSignal>,
    min_unique_buyers: usize,
    db: Arc<SentinelDb>,
    selected_index: usize,
    status_msg: String,
}

impl AppState {
    fn new(db: Arc<SentinelDb>, min_unique_buyers: usize) -> Self {
        Self {
            active_tab: ActiveTab::RadarSignals,
            signals: Vec::new(),
            swaps: Vec::new(),
            leaderboard_1h: Vec::new(),
            leaderboard_4h: Vec::new(),
            leaderboard_15m: Vec::new(),
            leaderboard_5m: Vec::new(),
            min_unique_buyers,
            db,
            selected_index: 0,
            status_msg: "Radar active. Monitoring 5M/15M/1H/4H flow...".to_string(),
        }
    }

    fn refresh_data(&mut self) {
        // Tab 1 hanya menampilkan sinyal akumulasi; wash/distribution tetap
        // tersimpan di DB & journal tapi tidak memenuhi layar.
        if let Ok(all) = self.db.get_recent_signals(300) {
            self.signals = all
                .into_iter()
                .filter(|s| {
                    !matches!(
                        s.signal_type,
                        SignalType::WashTradingSuspect | SignalType::HeavyDistribution
                    )
                })
                .take(50)
                .collect();
        }
        if let Ok(swaps) = self.db.get_recent_swaps(100) {
            self.swaps = swaps;
        }
        if let Ok(lb1) = self.db.get_timeframe_leaderboard("1H", self.min_unique_buyers, 30) {
            self.leaderboard_1h = lb1;
        }
        if let Ok(lb4) = self.db.get_timeframe_leaderboard("4H", self.min_unique_buyers, 30) {
            self.leaderboard_4h = lb4;
        }
        if let Ok(lb15) = self.db.get_timeframe_leaderboard("15M", self.min_unique_buyers, 30) {
            self.leaderboard_15m = lb15;
        }
        if let Ok(lb5) = self.db.get_timeframe_leaderboard("5M", self.min_unique_buyers, 30) {
            self.leaderboard_5m = lb5;
        }
    }

    fn next_tab(&mut self) {
        self.active_tab = match self.active_tab {
            ActiveTab::RadarSignals => ActiveTab::FlowFeed,
            ActiveTab::FlowFeed => ActiveTab::Metrics1H,
            ActiveTab::Metrics1H => ActiveTab::Metrics4H,
            ActiveTab::Metrics4H => ActiveTab::Metrics15M,
            ActiveTab::Metrics15M => ActiveTab::Metrics5M,
            ActiveTab::Metrics5M => ActiveTab::ConfigChain,
            ActiveTab::ConfigChain => ActiveTab::RadarSignals,
        };
        self.selected_index = 0;
    }

    fn prev_tab(&mut self) {
        self.active_tab = match self.active_tab {
            ActiveTab::RadarSignals => ActiveTab::ConfigChain,
            ActiveTab::FlowFeed => ActiveTab::RadarSignals,
            ActiveTab::Metrics1H => ActiveTab::FlowFeed,
            ActiveTab::Metrics4H => ActiveTab::Metrics1H,
            ActiveTab::Metrics15M => ActiveTab::Metrics4H,
            ActiveTab::Metrics5M => ActiveTab::Metrics15M,
            ActiveTab::ConfigChain => ActiveTab::Metrics5M,
        };
        self.selected_index = 0;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let config = Config::load().unwrap_or_else(|_| Config {
        robinhood_rpc_urls: vec!["https://robinhood.api.pocket.network".to_string()],
        robinhood_scan_chunk: 1000,
        solana_rpc_url: "https://api.mainnet-beta.solana.com".to_string(),
        database_path: "data/flow_sentinel.db".to_string(),
        poll_interval_secs: 10,
        min_inflow_usd_1h: 500.0,
        min_inflow_usd_4h: 1000.0,
        min_inflow_usd_5m: 100.0,
        min_inflow_usd_15m: 250.0,
        min_buy_ratio_pct: 55.0,
        min_unique_buyers: 4,
        detect_wash_trading: true,
        robinhood_ingest_enabled: true,
        min_market_cap_usd: 50000.0,
        max_market_cap_usd: 5000000.0,
        solana_tracked_programs: flow_sentinel::chains::solana::DEFAULT_TRACKED_PROGRAMS
            .iter()
            .map(|s| s.to_string())
            .collect(),
        solana_max_tx_per_cycle: 15,
        solana_sigs_per_feed: 100,
    });

    let db = Arc::new(SentinelDb::new(&config.database_path)?);

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = AppState::new(db, config.min_unique_buyers);
    app.refresh_data();

    let mut last_refresh = std::time::Instant::now();

    loop {
        if last_refresh.elapsed() >= Duration::from_secs(2) {
            app.refresh_data();
            last_refresh = std::time::Instant::now();
        }

        terminal.draw(|f| ui(f, &app))?;

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                    KeyCode::Tab | KeyCode::Right => app.next_tab(),
                    KeyCode::BackTab | KeyCode::Left => app.prev_tab(),
                    KeyCode::Char('1') => app.active_tab = ActiveTab::RadarSignals,
                    KeyCode::Char('2') => app.active_tab = ActiveTab::FlowFeed,
                    KeyCode::Char('3') => app.active_tab = ActiveTab::Metrics1H,
                    KeyCode::Char('4') => app.active_tab = ActiveTab::Metrics4H,
                    KeyCode::Char('5') => app.active_tab = ActiveTab::Metrics15M,
                    KeyCode::Char('6') => app.active_tab = ActiveTab::Metrics5M,
                    KeyCode::Char('7') => app.active_tab = ActiveTab::ConfigChain,
                    KeyCode::Char('r') => {
                        app.refresh_data();
                        app.status_msg = "Data manually refreshed.".to_string();
                    }
                    KeyCode::Up => {
                        if app.selected_index > 0 {
                            app.selected_index -= 1;
                        }
                    }
                    KeyCode::Down => {
                        app.selected_index += 1;
                    }
                    _ => {}
                }
            }
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}

fn ui(f: &mut Frame, app: &AppState) {
    let size = f.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header & Tabs
            Constraint::Min(10),   // Content
            Constraint::Length(3), // Footer / Status Bar
        ])
        .split(size);

    // Header & Tabs
    let titles = vec![
        "[1] RADAR SIGNALS",
        "[2] LIVE FLOW FEED",
        "[3] 1H ACCUMULATION",
        "[4] 4H ACCUMULATION",
        "[5] 15M ACCUMULATION",
        "[6] 5M ACCUMULATION",
        "[7] CHAIN & RPC STATS",
    ];

    let selected_tab_idx = match app.active_tab {
        ActiveTab::RadarSignals => 0,
        ActiveTab::FlowFeed => 1,
        ActiveTab::Metrics1H => 2,
        ActiveTab::Metrics4H => 3,
        ActiveTab::Metrics15M => 4,
        ActiveTab::Metrics5M => 5,
        ActiveTab::ConfigChain => 6,
    };

    let tabs = Tabs::new(titles)
        .select(selected_tab_idx)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" 📡 FLOW-SENTINEL: ON-CHAIN FLOW RADAR (ZERO EXECUTION) ")
                .title_alignment(Alignment::Left)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .style(Style::default().fg(Color::DarkGray))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(tabs, chunks[0]);

    // Body content
    match app.active_tab {
        ActiveTab::RadarSignals => render_radar_tab(f, chunks[1], app),
        ActiveTab::FlowFeed => render_feed_tab(f, chunks[1], app),
        ActiveTab::Metrics1H => render_timeframe_tab(f, chunks[1], &app.leaderboard_1h, "1-HOUR"),
        ActiveTab::Metrics4H => render_timeframe_tab(f, chunks[1], &app.leaderboard_4h, "4-HOUR"),
        ActiveTab::Metrics15M => render_timeframe_tab(f, chunks[1], &app.leaderboard_15m, "15-MIN"),
        ActiveTab::Metrics5M => render_timeframe_tab(f, chunks[1], &app.leaderboard_5m, "5-MIN"),
        ActiveTab::ConfigChain => render_config_tab(f, chunks[1]),
    }

    // Footer
    let footer_text = vec![
        Span::raw("Keys: "),
        Span::styled("[1-7 / Tab]", Style::default().fg(Color::Yellow)),
        Span::raw(" Switch Tab | "),
        Span::styled("[r]", Style::default().fg(Color::Green)),
        Span::raw(" Refresh | "),
        Span::styled("[q]", Style::default().fg(Color::Red)),
        Span::raw(" Quit  ──  "),
        Span::styled(
            &app.status_msg,
            Style::default().fg(Color::White).add_modifier(Modifier::DIM),
        ),
    ];
    let footer = Paragraph::new(Line::from(footer_text)).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(footer, chunks[2]);
}

fn render_radar_tab(f: &mut Frame, area: Rect, app: &AppState) {
    let items: Vec<ListItem> = if app.signals.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            " No flow signals detected yet. Radar is actively scanning swap logs...",
            Style::default().fg(Color::DarkGray),
        )))]
    } else {
        app.signals
            .iter()
            .map(|sig| {
                let (badge_color, badge_text) = match sig.signal_type {
                    SignalType::StrongAccumulation => (Color::Green, "[STRONG ACCUMULATION]"),
                    SignalType::OrganicMomentum => (Color::LightGreen, "[ORGANIC MOMENTUM]"),
                    SignalType::WashTradingSuspect => (Color::Red, "[WASH TRADING SUSPECT]"),
                    SignalType::HeavyDistribution => (Color::LightRed, "[HEAVY DISTRIBUTION]"),
                    SignalType::Neutral => (Color::DarkGray, "[NEUTRAL]"),
                };

                let chain_badge = match sig.chain {
                    Chain::Robinhood => Span::styled(" [ROBINHOOD-L2] ", Style::default().fg(Color::Magenta)),
                    Chain::Solana => Span::styled(" [SOLANA] ", Style::default().fg(Color::Cyan)),
                };

                let line1 = Line::from(vec![
                    Span::styled(sig.detected_at.format("%H:%M:%S ").to_string(), Style::default().fg(Color::DarkGray)),
                    chain_badge,
                    Span::styled(format!("[{}] ", sig.timeframe), Style::default().fg(Color::Yellow)),
                    Span::styled(badge_text, Style::default().fg(badge_color).add_modifier(Modifier::BOLD)),
                    Span::raw(" Token: "),
                    Span::styled(&sig.token_symbol, Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                    Span::raw(format!(" ({})", &sig.token_address[0..sig.token_address.len().min(10)])),
                    Span::styled(format!("  Conf: {:.1}%", sig.confidence_score), Style::default().fg(Color::White)),
                ]);

                let line2 = Line::from(vec![
                    Span::raw("     ↳ Metrics: "),
                    Span::styled(
                        format!(
                            "Net Inflow: ${:.2} | Buy Ratio: {:.1}% | Unique Buyers: {} | Vol: ${:.2}",
                            sig.metrics.net_inflow_usd,
                            sig.metrics.buy_ratio_pct,
                            sig.metrics.unique_buyers,
                            sig.metrics.total_volume_usd
                        ),
                        Style::default().fg(Color::Gray),
                    ),
                ]);

                ListItem::new(vec![line1, line2, Line::from("")])
            })
            .collect()
    };

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" 🎯 Detected Flow & Accumulation Signals (1H/4H Rolling Windows) ")
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::Green)),
    );

    f.render_widget(list, area);
}

fn render_feed_tab(f: &mut Frame, area: Rect, app: &AppState) {
    let items: Vec<ListItem> = if app.swaps.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            " Waiting for live DEX swap logs from Robinhood L2 / Solana...",
            Style::default().fg(Color::DarkGray),
        )))]
    } else {
        app.swaps
            .iter()
            .take(30)
            .map(|sw| {
                let (side_color, side_text) = match sw.side {
                    SwapSide::Buy => (Color::Green, "BUY "),
                    SwapSide::Sell => (Color::Red, "SELL"),
                };

                let chain_str = match sw.chain {
                    Chain::Robinhood => "RH-L2",
                    Chain::Solana => "SOL  ",
                };

                Line::from(vec![
                    Span::styled(sw.timestamp.format("%H:%M:%S ").to_string(), Style::default().fg(Color::DarkGray)),
                    Span::styled(format!("[{}] ", chain_str), Style::default().fg(Color::Cyan)),
                    Span::styled(side_text, Style::default().fg(side_color).add_modifier(Modifier::BOLD)),
                    Span::raw(" "),
                    Span::styled(format!("{:>8}", sw.token_symbol), Style::default().fg(Color::Yellow)),
                    Span::raw(format!(" | ${:>9.2} | Swapper: ", sw.amount_usd)),
                    Span::styled(
                        if sw.swapper_address.len() > 14 {
                            format!("{}...{}", &sw.swapper_address[0..6], &sw.swapper_address[sw.swapper_address.len()-4..])
                        } else {
                            sw.swapper_address.clone()
                        },
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::raw(format!(" | Pool: {}", &sw.pool_address[0..sw.pool_address.len().min(8)])),
                ])
                .into()
            })
            .collect()
    };

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" 🌊 Real-Time Raw DEX Swap Feed ")
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::Blue)),
    );

    f.render_widget(list, area);
}

fn render_timeframe_tab(f: &mut Frame, area: Rect, items_list: &[FlowSignal], tf: &str) {
    if items_list.is_empty() {
        let content = vec![
            Line::from(Span::styled(
                format!("=== {} ACCUMULATION RADAR MATRIX ===", tf),
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(vec![
                Span::styled("Timeframe Window: ", Style::default().fg(Color::Cyan)),
                Span::raw(format!("Rolling {}", tf)),
            ]),
            Line::from(vec![
                Span::styled("Status: ", Style::default().fg(Color::DarkGray)),
                Span::raw("Accumulating on-chain window metrics..."),
            ]),
        ];
        let p = Paragraph::new(content).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" 📊 Top Accumulated Tokens ({}) ", tf))
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(Color::Yellow)),
        );
        f.render_widget(p, area);
        return;
    }

    let rows: Vec<ListItem> = items_list
        .iter()
        .enumerate()
        .map(|(rank, sig)| {
            let (tag_color, tag_str) = match sig.signal_type {
                SignalType::StrongAccumulation => (Color::Green, "STRONG ACCUMULATION"),
                SignalType::OrganicMomentum => (Color::LightGreen, "ORGANIC MOMENTUM"),
                SignalType::WashTradingSuspect => (Color::Red, "WASH TRADING SUSPECT"),
                SignalType::HeavyDistribution => (Color::LightRed, "HEAVY DISTRIBUTION"),
                SignalType::Neutral => (Color::DarkGray, "NEUTRAL"),
            };

            let line1 = Line::from(vec![
                Span::styled(format!("#{:02} ", rank + 1), Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                Span::styled(format!("{:>10} ", sig.token_symbol), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
                Span::styled(format!("[{}] ", tag_str), Style::default().fg(tag_color)),
                Span::styled(format!("Net Inflow: +${:<9.2}", sig.metrics.net_inflow_usd), Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                Span::raw(format!(" | Buy Ratio: {:.1}%", sig.metrics.buy_ratio_pct)),
                Span::raw(format!(" | Unique Buyers: {}", sig.metrics.unique_buyers)),
                Span::styled(format!(" | Conf: {:.1}%", sig.confidence_score), Style::default().fg(Color::Cyan)),
            ]);

            let line2 = Line::from(vec![
                Span::raw("     ↳ Contract: "),
                Span::styled(&sig.token_address, Style::default().fg(Color::DarkGray)),
            ]);

            ListItem::new(vec![line1, line2, Line::from("")])
        })
        .collect();

    let list = List::new(rows).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" 📊 Top Accumulated Tokens (Ranked by Inflow - {}) ", tf))
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::Yellow)),
    );

    f.render_widget(list, area);
}

fn render_config_tab(f: &mut Frame, area: Rect) {
    let content = vec![
        Line::from(Span::styled(
            "=== CHAIN & RADAR CONFIGURATION (READ-ONLY) ===",
            Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled("• Target Chains: ", Style::default().fg(Color::Cyan)),
            Span::raw("Robinhood Chain (Arbitrum Nitro L2, Chain ID 4663) & Solana Mainnet"),
        ]),
        Line::from(vec![
            Span::styled("• Monitored Protocols: ", Style::default().fg(Color::Cyan)),
            Span::raw("Uniswap V4 (PoolManager), Uniswap V3, Pons Protocol, Raydium AMM, Pump.fun"),
        ]),
        Line::from(vec![
            Span::styled("• Execution Footprint: ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw("ZERO. No private keys, no swap routing, no wallet signing."),
        ]),
        Line::from(vec![
            Span::styled("• Database: ", Style::default().fg(Color::Cyan)),
            Span::raw("data/flow_sentinel.db (SQLite WAL Mode)"),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "To adjust scanner intervals or inflow thresholds, edit .env and restart via 'make restart'.",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let p = Paragraph::new(content).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" ⚙️ System & Chain Topology ")
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::Magenta)),
    );

    f.render_widget(p, area);
}
