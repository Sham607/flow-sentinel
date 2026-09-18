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
    ConfigChain = 4,
}

struct AppState {
    active_tab: ActiveTab,
    signals: Vec<FlowSignal>,
    swaps: Vec<SwapRecord>,
    db: Arc<SentinelDb>,
    selected_index: usize,
    status_msg: String,
}

impl AppState {
    fn new(db: Arc<SentinelDb>) -> Self {
        Self {
            active_tab: ActiveTab::RadarSignals,
            signals: Vec::new(),
            swaps: Vec::new(),
            db,
            selected_index: 0,
            status_msg: "Radar active. Monitoring 1H/4H flow...".to_string(),
        }
    }

    fn refresh_data(&mut self) {
        if let Ok(signals) = self.db.get_recent_signals(50) {
            self.signals = signals;
        }
        if let Ok(swaps) = self.db.get_recent_swaps(100) {
            self.swaps = swaps;
        }
    }

    fn next_tab(&mut self) {
        self.active_tab = match self.active_tab {
            ActiveTab::RadarSignals => ActiveTab::FlowFeed,
            ActiveTab::FlowFeed => ActiveTab::Metrics1H,
            ActiveTab::Metrics1H => ActiveTab::Metrics4H,
            ActiveTab::Metrics4H => ActiveTab::ConfigChain,
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
            ActiveTab::ConfigChain => ActiveTab::Metrics4H,
        };
        self.selected_index = 0;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let config = Config::load().unwrap_or_else(|_| Config {
        robinhood_rpc_urls: vec!["https://rpc.robinhood.com".to_string()],
        robinhood_scan_chunk: 50,
        solana_rpc_url: "https://api.mainnet-beta.solana.com".to_string(),
        database_path: "data/flow_sentinel.db".to_string(),
        min_inflow_usd_1h: 500.0,
        min_inflow_usd_4h: 2000.0,
        min_buy_ratio_pct: 65.0,
        poll_interval_secs: 10,
    });

    let db = Arc::new(SentinelDb::new(&config.database_path)?);

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = AppState::new(db);
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
                    KeyCode::Char('5') => app.active_tab = ActiveTab::ConfigChain,
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
        "[5] CHAIN & RPC STATS",
    ];

    let selected_tab_idx = match app.active_tab {
        ActiveTab::RadarSignals => 0,
        ActiveTab::FlowFeed => 1,
        ActiveTab::Metrics1H => 2,
        ActiveTab::Metrics4H => 3,
        ActiveTab::ConfigChain => 4,
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
        ActiveTab::Metrics1H => render_timeframe_tab(f, chunks[1], app, "1-HOUR"),
        ActiveTab::Metrics4H => render_timeframe_tab(f, chunks[1], app, "4-HOUR"),
        ActiveTab::ConfigChain => render_config_tab(f, chunks[1]),
    }

    // Footer
    let footer_text = vec![
        Span::raw("Keys: "),
        Span::styled("[1-5 / Tab]", Style::default().fg(Color::Yellow)),
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

fn render_timeframe_tab(f: &mut Frame, area: Rect, _app: &AppState, tf: &str) {
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
            Span::styled("Evaluation Criteria: ", Style::default().fg(Color::Cyan)),
            Span::raw("Net Inflow Velocity + Unique Buyer Density + Anti-Wash Churn Gate"),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "Historical tokens with active 1H/4H accumulation signals are aggregated and ranked by confidence score.",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let p = Paragraph::new(content).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" 📊 {} Window Aggregates ", tf))
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::Yellow)),
    );

    f.render_widget(p, area);
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
