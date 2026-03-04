# Stock Analyser Signal

Automated NASDAQ market analysis tool powered by Claude AI. Runs on a schedule via [ClaudeCodeIde](https://github.com/anthropics/claude-code) `automate` infrastructure — fetches market data, scrapes financial news, produces scored buy/sell recommendations, and delivers bilingual (EN/PL) reports to Discord.

## Pipeline

```
[1] Load config.json
[2] Load previous analyses (CSV + conclusions.json)
[3] Fetch OHLCV data via yfinance (hourly/daily/weekly, 6 symbols)
[4] Save price snapshot to CSV
[5] Scrape news (Reuters, CNBC, Yahoo Finance, MarketWatch)
[6] Send all data to Claude — get structured JSON analysis
[7] Evaluate significance — decide whether to notify or skip
[8] Translate to Polish, combine EN+PL, send to Discord
```

### Smart notification logic

Not every run triggers a full report. The system evaluates whether changes are significant:

| Condition | Action |
|-----------|--------|
| First run ever | Always notify |
| 7+ days since last saved analysis | Always notify (weekly summary) |
| Recommendation changed (e.g. HOLD -> BUY) | Notify |
| Short/medium-term score shifted by 10+ points | Notify |
| No significant change | Skip — send short diagnostic only |

When skipped, no data is saved to history files (saves storage), no translation is performed (saves cost), and Discord receives only a brief diagnostic message.

## Tracked symbols

| Category | Symbols |
|----------|---------|
| Primary | QQQ, ^IXIC (NASDAQ Composite) |
| Secondary | SPY (S&P 500), ^VIX (Volatility), ^TNX (10Y Treasury) |
| Crypto reference | BTC-USD |

## Output

Each analysis produces a scored recommendation:

| Score | Label | Meaning |
|-------|-------|---------|
| 1-20 | STRONG SELL | Clear downtrend, negative catalysts, high risk |
| 21-40 | SELL | Bearish signals outweigh bullish |
| 41-60 | HOLD | Mixed signals, no clear direction |
| 61-80 | BUY | Bullish signals outweigh bearish |
| 81-100 | STRONG BUY | Clear uptrend, positive catalysts, low risk |

Three time horizons are scored independently: short-term (~1 week), medium-term (1-3 months), long-term (6-12 months).

### Self-improving loop

Each run reads its own previous conclusions from `data/conclusions.json`. Claude sees what it previously predicted, at what prices, and is asked to compare — did the market move as expected? This creates a feedback loop where the analysis references and learns from its own track record.

## Data files

| File | Description | Retention |
|------|-------------|-----------|
| `data/analysis_history.csv` | Scores, recommendations, outlooks, key factors, Discord message sent | Last 100 entries |
| `data/conclusions.json` | Full structured conclusions from each run (fed back to Claude) | Last 50 entries |
| `data/market_snapshots.csv` | Price snapshots with SMAs for all symbols at each run | Last 600 rows |

## Setup

### Prerequisites

- [Claude Code CLI](https://github.com/anthropics/claude-code) installed and authenticated
- [ClaudeCodeIde](https://github.com/anthropics/claude-code) `automate` infrastructure at `~/.local/share/claude-code-ide/`
- Python 3.12+ with the automate venv

### Install

```bash
# Install yfinance into the automate venv
~/.local/share/claude-code-ide/.venv/bin/pip install yfinance

# Set your Discord webhook URL in config.json
# Edit the "webhook_url" field under "discord"
```

## Usage

### GUI (recommended)

```bash
automate-gui
```

Then **Open** (Ctrl+O) and select `config.json` from this directory. The GUI will:
- Load all settings (scheduler, Discord) into the GUI tabs
- Load `analyse.py` into the code editor
- Set the working directory to the project folder

Press **F5** to run manually, or **Save** (Ctrl+S) to save both script and config.

### CLI

```bash
cd /path/to/stock_analyser_signal/

# Manual one-time run
automate --run analyse.py

# Start scheduler daemon (daily at 18:00 by default)
automate
```

## Configuration

All settings are in `config.json`:

### Symbols

```json
"symbols": {
  "primary": ["QQQ", "^IXIC"],
  "secondary": ["SPY", "^VIX", "^TNX"],
  "crypto_ref": ["BTC-USD"]
}
```

### Schedule

Default: daily at 18:00. For volatile markets, switch to interval mode:

```json
{
  "mode": "interval",
  "interval_min": 240,
  "time_str": ""
}
```

### Data periods

```json
"data_periods": {
  "hourly_days": 59,
  "daily_months": 6,
  "weekly_months": 24
}
```

### Claude settings

```json
"claude": {
  "model": null,
  "timeout": 300,
  "max_budget_usd": 0.50,
  "system_prompt": null
}
```

- `model` — Claude model to use (`null` = default)
- `max_budget_usd` — cost cap per analysis run (translation has a separate $0.10 cap)
- `system_prompt` — override for the analyst persona (falls back to `context_keeper.prompt`)

### News sources

```json
"news_urls": [
  "https://www.reuters.com/markets/",
  "https://www.cnbc.com/world-markets/",
  "https://finance.yahoo.com/topic/stock-market-news/",
  "https://www.marketwatch.com/latest-news"
]
```

## Discord message format

Full reports follow a fixed template (always identical structure):

```
[emoji] NASDAQ Signal │ Run #N │ YYYY-MM-DD HH:MM
════════════════════════════════════════
[target] RECOMMENDATION
[score bars with emoji indicators for 3 time horizons]
[market snapshot: QQQ, SPY, VIX, BTC with % change]
[key factors with specific numbers]
[outlook: short / medium / long]
[risks]
[opportunities]
[comparison to previous run]
────────────────────────────────────────
[disclaimer]

[PL flag] WERSJA POLSKA:
════════════════════════════════════════
[same content translated to Polish]
```

Diagnostic messages (when no significant change):

```
[refresh] Signal Check │ Run #N │ YYYY-MM-DD HH:MM
no significant change vs previous analysis — no update sent
```

## Disclaimer

This tool generates automated AI analysis for informational purposes only. It is not investment advice. Always do your own research before making any investment decisions.
