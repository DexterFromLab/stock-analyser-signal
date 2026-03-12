"""
Stock Analyser Signal — NASDAQ Market Analysis Tool
Fetches market data, scrapes news, runs AI analysis, logs to CSV, notifies Discord.
"""

import json
import csv
import logging
import os
import re
import sys
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Infrastructure imports (provided by ClaudeCodeIde / automate)
# ---------------------------------------------------------------------------
from claude_code import ClaudeCode
from discord_notifier import DiscordNotifier
import yfinance as yf

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent if "__file__" in dir() else Path.cwd()
CONFIG_PATH = SCRIPT_DIR / "config.json"
DATA_DIR = SCRIPT_DIR / "data"
HISTORY_CSV = DATA_DIR / "analysis_history.csv"
SNAPSHOT_CSV = DATA_DIR / "market_snapshots.csv"
CONCLUSIONS_FILE = DATA_DIR / "conclusions.json"

LOG_FILE = DATA_DIR / "analyse.log"
LOG_MAX_LINES = 30_000

log = logging.getLogger("analyse")

HISTORY_COLUMNS = [
    "timestamp", "run_number", "run_id",
    "score_short_term", "score_medium_term", "score_long_term",
    "recommendation",
    "qqq_price", "ixic_price", "spy_price", "vix_value", "btc_price",
    "key_factors", "short_term_outlook", "medium_term_outlook", "long_term_outlook",
    "risks", "opportunities",
    "detailed_analysis", "comparison_to_previous",
    "discord_message",
    "model", "cost_usd",
]

SNAPSHOT_COLUMNS = [
    "timestamp", "run_id", "symbol",
    "price", "previous_close", "change_pct",
    "volume", "avg_volume",
    "pe_ratio", "week52_high", "week52_low",
    "sma5", "sma20", "sma50",
]

ANALYSIS_SCHEMA = {
    "type": "object",
    "properties": {
        "score_short_term":       {"type": "integer", "minimum": 1, "maximum": 100, "description": "Buy/sell score for ~1 week horizon (1=strong sell, 100=strong buy)"},
        "score_medium_term":      {"type": "integer", "minimum": 1, "maximum": 100, "description": "Buy/sell score for 1-3 month horizon"},
        "score_long_term":        {"type": "integer", "minimum": 1, "maximum": 100, "description": "Buy/sell score for 6-12 month horizon"},
        "recommendation":        {"type": "string", "enum": ["STRONG_BUY", "BUY", "HOLD", "SELL", "STRONG_SELL"]},
        "short_term_outlook":    {"type": "string", "description": "One-line outlook for ~1 week"},
        "medium_term_outlook":   {"type": "string", "description": "One-line outlook for 1-3 months"},
        "long_term_outlook":     {"type": "string", "description": "One-line outlook for 6-12 months"},
        "key_factors":           {"type": "array", "items": {"type": "string"}, "minItems": 3, "maxItems": 5, "description": "Top factors with specific numbers"},
        "risks":                 {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 3},
        "opportunities":         {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 3},
        "detailed_analysis":     {"type": "string", "description": "200-400 word analysis paragraph"},
        "comparison_to_previous": {"type": "string", "description": "What changed vs the last analysis (or 'First analysis' if none)"},
    },
    "required": [
        "score_short_term", "score_medium_term", "score_long_term",
        "recommendation",
        "short_term_outlook", "medium_term_outlook", "long_term_outlook",
        "key_factors", "risks", "opportunities",
        "detailed_analysis", "comparison_to_previous",
    ],
}


# ═══════════════════════════════════════════════════════════════════════════
# Logging setup
# ═══════════════════════════════════════════════════════════════════════════
def trim_log_file():
    """Keep log file to LOG_MAX_LINES lines by removing oldest entries."""
    if not LOG_FILE.exists():
        return
    try:
        lines = LOG_FILE.read_text(encoding="utf-8").splitlines()
        if len(lines) > LOG_MAX_LINES:
            LOG_FILE.write_text("\n".join(lines[-LOG_MAX_LINES:]) + "\n", encoding="utf-8")
    except OSError:
        pass


def setup_logging():
    """Configure logging to console + file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    trim_log_file()

    # Prevent duplicate handlers if called multiple times
    if log.handlers:
        return

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)

    log.setLevel(logging.DEBUG)
    log.addHandler(file_handler)
    log.addHandler(console_handler)


# ═══════════════════════════════════════════════════════════════════════════
# Step 1 — Load config
# ═══════════════════════════════════════════════════════════════════════════
def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ═══════════════════════════════════════════════════════════════════════════
# Step 2 — Load previous analyses + conclusions
# ═══════════════════════════════════════════════════════════════════════════
def _ensure_csv_header(path, columns):
    """Ensure CSV file has the correct header row. Fix if missing."""
    if not path.exists() or path.stat().st_size == 0:
        return
    with open(path, "r", encoding="utf-8") as f:
        first_line = f.readline()
    if first_line.strip().startswith(columns[0] + ","):
        return  # header present
    # Header missing — prepend it
    log.warning("CSV %s missing header — repairing.", path.name)
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    header = ",".join(columns) + "\n"
    with open(path, "w", newline="", encoding="utf-8") as f:
        f.write(header + content)


def load_history(lookback: int) -> list[dict]:
    if not HISTORY_CSV.exists():
        return []
    _ensure_csv_header(HISTORY_CSV, HISTORY_COLUMNS)
    rows = []
    with open(HISTORY_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows[-lookback:] if len(rows) > lookback else rows


def load_conclusions(lookback: int) -> list[dict]:
    if not CONCLUSIONS_FILE.exists():
        return []
    try:
        with open(CONCLUSIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data[-lookback:] if len(data) > lookback else data
    except (json.JSONDecodeError, IOError):
        return []


def _first_sentence(text: str, max_len: int = 200) -> str:
    """Extract first sentence, truncate if needed."""
    if not text:
        return ""
    # Find first sentence end
    for end in [". ", ".\n", "."]:
        idx = text.find(end)
        if 0 < idx < max_len:
            return text[:idx + 1]
    return text[:max_len] + "..." if len(text) > max_len else text


def save_conclusion(result: dict, run_number: int, timestamp: str, market_data: dict):
    """Append condensed conclusion to conclusions.json — key data only."""
    conclusions = []
    if CONCLUSIONS_FILE.exists():
        try:
            with open(CONCLUSIONS_FILE, "r", encoding="utf-8") as f:
                conclusions = json.load(f)
        except (json.JSONDecodeError, IOError):
            conclusions = []

    entry = {
        "timestamp": timestamp,
        "run_number": run_number,
        "recommendation": result.get("recommendation"),
        "score_short_term": result.get("score_short_term"),
        "score_medium_term": result.get("score_medium_term"),
        "score_long_term": result.get("score_long_term"),
        "short_term_outlook": _first_sentence(result.get("short_term_outlook", "")),
        "medium_term_outlook": _first_sentence(result.get("medium_term_outlook", "")),
        "long_term_outlook": _first_sentence(result.get("long_term_outlook", "")),
        "key_factors": [f[:120] for f in result.get("key_factors", [])[:4]],
        "risks": [r[:100] for r in result.get("risks", [])[:2]],
        "opportunities": [o[:100] for o in result.get("opportunities", [])[:2]],
        "what_changed": _first_sentence(result.get("comparison_to_previous", "")),
        "prices": {
            "QQQ": market_data.get("QQQ", {}).get("info", {}).get("price"),
            "SPY": market_data.get("SPY", {}).get("info", {}).get("price"),
            "VIX": market_data.get("^VIX", {}).get("info", {}).get("price"),
            "BTC": market_data.get("BTC-USD", {}).get("info", {}).get("price"),
        },
    }

    conclusions.append(entry)
    # Keep last 10 entries max
    if len(conclusions) > 10:
        conclusions = conclusions[-10:]
    with open(CONCLUSIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(conclusions, f, indent=2, ensure_ascii=False)


def format_history_for_prompt(history: list[dict], conclusions: list[dict]) -> str:
    """Format previous analyses for prompt — condensed, ~8 lines per run."""
    if not conclusions:
        return "No previous analyses available. This is the first run."

    lines = ["## Your Previous Analyses (most recent last)\n"]
    for c in conclusions:
        prices = c.get("prices", {})
        lines.append(
            f"**Run #{c.get('run_number', '?')}** — {c.get('timestamp', '?')} | "
            f"**{c.get('recommendation', '?')}** | "
            f"ST={c.get('score_short_term', '?')} MT={c.get('score_medium_term', '?')} LT={c.get('score_long_term', '?')}"
        )
        lines.append(
            f"QQQ=${prices.get('QQQ', '?')} | SPY=${prices.get('SPY', '?')} | "
            f"VIX={prices.get('VIX', '?')} | BTC=${prices.get('BTC', '?')}"
        )
        lines.append(f"ST: {c.get('short_term_outlook', '?')}")
        lines.append(f"MT: {c.get('medium_term_outlook', '?')}")
        lines.append(f"LT: {c.get('long_term_outlook', '?')}")
        kf = c.get("key_factors", [])
        if kf:
            lines.append("Factors: " + " | ".join(kf))
        changed = c.get("what_changed") or c.get("comparison_to_previous", "")
        if changed:
            lines.append(f"Changed: {_first_sentence(changed)}")
        lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Step 3 — Fetch OHLCV market data via yfinance
# ═══════════════════════════════════════════════════════════════════════════
def fetch_market_data(symbols_cfg: dict, periods_cfg: dict) -> dict:
    """Fetch hourly/daily/weekly OHLCV for all symbols. Returns dict keyed by symbol."""
    all_symbols = symbols_cfg["primary"] + symbols_cfg["secondary"] + symbols_cfg["crypto_ref"]
    data = {}

    for sym in all_symbols:
        log.info("Fetching %s...", sym)
        ticker = yf.Ticker(sym)
        entry = {"symbol": sym, "info": {}, "hourly": None, "daily": None, "weekly": None}

        try:
            info = ticker.info or {}
            entry["info"] = {
                "price": info.get("regularMarketPrice") or info.get("previousClose"),
                "previous_close": info.get("previousClose") or info.get("regularMarketPreviousClose"),
                "volume": info.get("regularMarketVolume") or info.get("volume"),
                "avg_volume": info.get("averageDailyVolume10Day") or info.get("averageVolume"),
                "pe_ratio": info.get("trailingPE"),
                "week52_high": info.get("fiftyTwoWeekHigh"),
                "week52_low": info.get("fiftyTwoWeekLow"),
            }
        except Exception as e:
            log.warning("Info fetch failed for %s: %s", sym, e)

        try:
            h_days = periods_cfg.get("hourly_days", 59)
            entry["hourly"] = ticker.history(period=f"{h_days}d", interval="1h")
        except Exception as e:
            log.warning("Hourly fetch failed for %s: %s", sym, e)

        try:
            d_months = periods_cfg.get("daily_months", 6)
            entry["daily"] = ticker.history(period=f"{d_months}mo", interval="1d")
        except Exception as e:
            log.warning("Daily fetch failed for %s: %s", sym, e)

        try:
            w_months = periods_cfg.get("weekly_months", 24)
            entry["weekly"] = ticker.history(period=f"{w_months}mo", interval="1wk")
        except Exception as e:
            log.warning("Weekly fetch failed for %s: %s", sym, e)

        data[sym] = entry

    return data


def compute_sma(df, period: int):
    if df is None or df.empty or len(df) < period:
        return None
    return round(df["Close"].rolling(window=period).mean().iloc[-1], 4)


def compute_rsi(df, period: int = 14):
    if df is None or df.empty or len(df) < period + 1:
        return None
    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(window=period).mean()
    loss = (-delta.clip(upper=0)).rolling(window=period).mean()
    rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] != 0 else 100
    return round(100 - (100 / (1 + rs)), 1)


def compute_macd(df):
    if df is None or df.empty or len(df) < 26:
        return None, None
    ema12 = df["Close"].ewm(span=12).mean()
    ema26 = df["Close"].ewm(span=26).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9).mean()
    return round(macd_line.iloc[-1], 4), round(signal_line.iloc[-1], 4)


def compute_bollinger(df, period: int = 20, std_dev: float = 2.0):
    if df is None or df.empty or len(df) < period:
        return None, None
    sma = df["Close"].rolling(window=period).mean().iloc[-1]
    std = df["Close"].rolling(window=period).std().iloc[-1]
    return round(sma + std_dev * std, 2), round(sma - std_dev * std, 2)


def compute_atr(df, period: int = 14):
    if df is None or df.empty or len(df) < period + 1:
        return None
    import numpy as np
    high = df["High"].values
    low = df["Low"].values
    close = np.roll(df["Close"].values, 1)
    close[0] = np.nan
    tr = np.maximum(high - low, np.maximum(np.abs(high - close), np.abs(low - close)))
    atr_series = float(np.nanmean(tr[-period:]))
    return round(atr_series, 4)


def format_ohlcv_table(df, label: str, last_n: int) -> str:
    if df is None or df.empty:
        return f"*{label}: No data available*\n"

    subset = df.tail(last_n).copy()
    lines = [f"**{label}** (last {len(subset)} points)\n"]
    lines.append("| Date | Open | High | Low | Close | Volume |")
    lines.append("|------|------|------|-----|-------|--------|")

    for idx, row in subset.iterrows():
        dt = idx.strftime("%Y-%m-%d %H:%M") if hasattr(idx, "strftime") else str(idx)
        lines.append(
            f"| {dt} | {row['Open']:.2f} | {row['High']:.2f} | "
            f"{row['Low']:.2f} | {row['Close']:.2f} | {int(row.get('Volume', 0)):,} |"
        )

    sma5 = compute_sma(df, 5)
    sma20 = compute_sma(df, 20)
    sma50 = compute_sma(df, 50)
    period_high = round(df["High"].max(), 2)
    period_low = round(df["Low"].min(), 2)
    current = round(df["Close"].iloc[-1], 2)
    vol_recent = int(df["Volume"].tail(5).mean()) if "Volume" in df.columns else 0
    vol_prior = int(df["Volume"].tail(20).mean()) if "Volume" in df.columns and len(df) >= 20 else vol_recent

    stats = [f"Current: {current}"]
    if sma5:  stats.append(f"SMA5: {sma5}")
    if sma20: stats.append(f"SMA20: {sma20}")
    if sma50: stats.append(f"SMA50: {sma50}")
    stats.append(f"Range: {period_low}-{period_high}")
    if vol_prior > 0:
        vol_trend = round(vol_recent / vol_prior * 100 - 100, 1)
        stats.append(f"Vol trend: {vol_trend:+.1f}%")

    # Technical indicators (only for daily/weekly with enough data)
    rsi = compute_rsi(df)
    macd, macd_signal = compute_macd(df)
    bb_upper, bb_lower = compute_bollinger(df)
    atr = compute_atr(df)

    tech = []
    if rsi is not None:
        tech.append(f"RSI14: {rsi}")
    if macd is not None:
        tech.append(f"MACD: {macd}/{macd_signal}")
    if bb_upper is not None:
        tech.append(f"BB: {bb_lower}-{bb_upper}")
    if atr is not None:
        tech.append(f"ATR14: {atr}")

    lines.append(f"\n*Stats: {' | '.join(stats)}*")
    if tech:
        lines.append(f"*Technicals: {' | '.join(tech)}*")
    lines.append("")
    return "\n".join(lines)


def format_market_data_for_prompt(market_data: dict) -> str:
    sections = ["## Market Data\n"]

    for sym, entry in market_data.items():
        sections.append(f"### {sym}\n")

        info = entry.get("info", {})
        if info.get("price"):
            info_parts = [f"Price: ${info['price']}"]
            if info.get("previous_close"):
                chg = round((info["price"] - info["previous_close"]) / info["previous_close"] * 100, 2)
                info_parts.append(f"Prev Close: ${info['previous_close']} ({chg:+.2f}%)")
            if info.get("volume"):
                info_parts.append(f"Volume: {info['volume']:,}")
            if info.get("avg_volume"):
                info_parts.append(f"Avg Vol: {info['avg_volume']:,}")
            if info.get("pe_ratio"):
                info_parts.append(f"P/E: {info['pe_ratio']:.1f}")
            if info.get("week52_high"):
                info_parts.append(f"52w H/L: ${info['week52_high']:.2f}/${info.get('week52_low', 0):.2f}")
            sections.append("**Info:** " + " | ".join(info_parts) + "\n")

        sections.append(format_ohlcv_table(entry.get("hourly"), "Hourly", 12))
        sections.append(format_ohlcv_table(entry.get("daily"), "Daily", 15))
        sections.append(format_ohlcv_table(entry.get("weekly"), "Weekly", 8))

    return "\n".join(sections)


# ═══════════════════════════════════════════════════════════════════════════
# Step 4 — Save price snapshot to CSV
# ═══════════════════════════════════════════════════════════════════════════
def save_snapshot(market_data: dict, run_id: str, timestamp: str):
    write_header = not SNAPSHOT_CSV.exists()
    with open(SNAPSHOT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SNAPSHOT_COLUMNS)
        if write_header:
            writer.writeheader()
        for sym, entry in market_data.items():
            info = entry.get("info", {})
            daily = entry.get("daily")
            price = info.get("price")
            prev = info.get("previous_close")
            change_pct = round((price - prev) / prev * 100, 2) if price and prev else None
            writer.writerow({
                "timestamp": timestamp,
                "run_id": run_id,
                "symbol": sym,
                "price": price,
                "previous_close": prev,
                "change_pct": change_pct,
                "volume": info.get("volume"),
                "avg_volume": info.get("avg_volume"),
                "pe_ratio": info.get("pe_ratio"),
                "week52_high": info.get("week52_high"),
                "week52_low": info.get("week52_low"),
                "sma5": compute_sma(daily, 5) if daily is not None else None,
                "sma20": compute_sma(daily, 20) if daily is not None else None,
                "sma50": compute_sma(daily, 50) if daily is not None else None,
            })


# ═══════════════════════════════════════════════════════════════════════════
# Step 5 — Scrape news
# ═══════════════════════════════════════════════════════════════════════════
def fetch_news(rss_feeds: list[dict]) -> str:
    """Fetch news from RSS feeds. Each feed: {"url": "...", "name": "..."}."""
    sections = ["## Latest Market News\n"]

    for feed in rss_feeds:
        url = feed["url"]
        name = feed.get("name", url)
        max_items = feed.get("max_items", 15)
        log.info("Fetching RSS: %s", name)

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                xml_data = resp.read()

            root = ET.fromstring(xml_data)
            items = root.findall(".//item")

            sections.append(f"### {name}\n")
            count = 0
            for item in items[:max_items]:
                title = (item.findtext("title") or "").strip()
                desc = item.findtext("description") or ""
                pub_date = (item.findtext("pubDate") or "").strip()

                desc_clean = re.sub(r"<[^>]+>", "", desc).strip()
                if len(desc_clean) > 500:
                    desc_clean = desc_clean[:500] + "..."

                sections.append(f"**{title}** ({pub_date})")
                if desc_clean:
                    sections.append(f"{desc_clean}\n")
                else:
                    sections.append("")
                count += 1

            log.info("Got %d items from %s", count, name)

        except Exception as e:
            log.warning("RSS fetch failed for %s: %s", name, e)
            sections.append(f"**{name}**: *Fetch failed — {e}*\n")

    return "\n".join(sections)


# ═══════════════════════════════════════════════════════════════════════════
# Step 6 — Claude analysis
# ═══════════════════════════════════════════════════════════════════════════
def run_analysis(
    market_prompt: str,
    news_prompt: str,
    history_prompt: str,
    claude_cfg: dict,
    config: dict = None,
    history: list[dict] = None,
) -> tuple[dict, str, float | None]:
    """Send data to Claude and get structured analysis."""

    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    run_number = (int(history[-1]["run_number"]) + 1) if history else 1

    time_since = "N/A (first run)"
    if history:
        try:
            last_ts = datetime.strptime(history[-1]["timestamp"], "%Y-%m-%d %H:%M:%S")
            delta = now - last_ts
            hours, remainder = divmod(int(delta.total_seconds()), 3600)
            minutes = remainder // 60
            if hours >= 24:
                days = hours // 24
                hours = hours % 24
                time_since = f"{days}d {hours}h {minutes}m"
            else:
                time_since = f"{hours}h {minutes}m"
        except (ValueError, KeyError):
            time_since = "unknown"

    json_example = json.dumps({
        "score_short_term": 65,
        "score_medium_term": 58,
        "score_long_term": 72,
        "recommendation": "BUY",
        "short_term_outlook": "...",
        "medium_term_outlook": "...",
        "long_term_outlook": "...",
        "key_factors": ["factor 1 with numbers", "factor 2", "factor 3"],
        "risks": ["risk 1", "risk 2"],
        "opportunities": ["opportunity 1", "opportunity 2"],
        "detailed_analysis": "200-400 word paragraph...",
        "comparison_to_previous": "What changed vs last run..."
    }, indent=2)

    prompt = f"""# NASDAQ Signal Analysis — {now_str}

**Run #{run_number}** | Date: {now.strftime("%A, %B %d, %Y")} | Time: {now.strftime("%H:%M:%S")} | Time since last analysis: {time_since}

{history_prompt}

{market_prompt}

{news_prompt}

---

## Instructions

Analyse ALL the data above — price action, technical indicators (SMAs, volume trends), news sentiment, and macro signals (VIX, TNX, BTC).

Scoring guide:
- 1-20: STRONG_SELL — clear downtrend, negative catalysts, high risk
- 21-40: SELL — bearish signals outweigh bullish
- 41-60: HOLD — mixed signals, no clear direction
- 61-80: BUY — bullish signals outweigh bearish
- 81-100: STRONG_BUY — clear uptrend, positive catalysts, low risk

Compare with your previous analyses — reference your own past conclusions, what you predicted, and whether the market moved as you expected.

Respond with ONLY this JSON structure (no other text):

```json
{json_example}
```

Fill every field with real analysis. key_factors must include specific numbers. recommendation must be one of: STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL.
"""

    system_prompt = claude_cfg.get("system_prompt")
    if not system_prompt:
        system_prompt = config.get("context_keeper", {}).get("prompt") or None

    # Use dedicated system prompt that enforces JSON output
    json_system = "You are a quantitative market analyst. You ALWAYS respond with valid JSON only. Never use markdown formatting, never add text outside the JSON object."
    if system_prompt:
        json_system = f"{system_prompt}\n\nIMPORTANT: You MUST respond with valid JSON only. No markdown, no text outside the JSON."

    log.debug("=" * 40 + " SYSTEM PROMPT " + "=" * 40)
    log.debug(json_system)
    log.debug("=" * 40 + " USER PROMPT " + "=" * 40)
    log.debug(prompt)
    log.debug("=" * 40 + " END PROMPT " + "=" * 40)

    claude = ClaudeCode(
        model=claude_cfg.get("model"),
        system_prompt=json_system,
        timeout=claude_cfg.get("timeout", 300),
        max_budget_usd=claude_cfg.get("max_budget_usd", 0.50),
    )

    resp = claude.ask(prompt)
    model = claude_cfg.get("model") or "default"
    cost = resp.cost_usd
    if resp.model:
        model = resp.model

    log.debug("=" * 40 + " RAW RESPONSE " + "=" * 40)
    log.debug("resp.text: %s", resp.text)
    log.debug("resp.raw_json: %s", resp.raw_json)
    log.debug("resp.model: %s | resp.cost_usd: %s", resp.model, resp.cost_usd)
    log.debug("=" * 40 + " END RESPONSE " + "=" * 40)

    result = _parse_json_response(resp)
    log.debug("Parsed result: %s", json.dumps(result, ensure_ascii=False, indent=2))
    return result, model, cost


def _parse_json_response(resp) -> dict:
    """Extract JSON from Claude response, trying multiple strategies."""
    # Strategy 1: raw_json envelope from CLI
    if resp.raw_json and "result" in resp.raw_json:
        raw = resp.raw_json["result"]
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                pass
            parsed = _extract_json_from_text(raw)
            if parsed:
                return parsed

    # Strategy 2: parse resp.text directly
    text = resp.text or ""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Strategy 3: extract from mixed text
    parsed = _extract_json_from_text(text)
    if parsed:
        return parsed

    return {"raw_text": text}


def _extract_json_from_text(text: str) -> dict | None:
    """Try to find and parse a JSON object from text that may contain markdown."""
    import re as _re

    m = _re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, _re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if first_brace != -1 and last_brace > first_brace:
        candidate = text[first_brace:last_brace + 1]
        try:
            parsed = json.loads(candidate)
            if "score_short_term" in parsed:
                return parsed
        except json.JSONDecodeError:
            pass

    return None


# ═══════════════════════════════════════════════════════════════════════════
# Step 7 — Save result + Discord notification
# ═══════════════════════════════════════════════════════════════════════════
def save_result(result: dict, run_number: int, run_id: str, timestamp: str,
                market_data: dict, model: str, cost: float | None, discord_msg: str):
    write_header = not HISTORY_CSV.exists() or HISTORY_CSV.stat().st_size == 0

    qqq_price = market_data.get("QQQ", {}).get("info", {}).get("price")
    ixic_price = market_data.get("^IXIC", {}).get("info", {}).get("price")
    spy_price = market_data.get("SPY", {}).get("info", {}).get("price")
    vix_value = market_data.get("^VIX", {}).get("info", {}).get("price")
    btc_price = market_data.get("BTC-USD", {}).get("info", {}).get("price")

    row = {
        "timestamp": timestamp,
        "run_number": run_number,
        "run_id": run_id,
        "score_short_term": result.get("score_short_term"),
        "score_medium_term": result.get("score_medium_term"),
        "score_long_term": result.get("score_long_term"),
        "recommendation": result.get("recommendation"),
        "qqq_price": qqq_price,
        "ixic_price": ixic_price,
        "spy_price": spy_price,
        "vix_value": vix_value,
        "btc_price": btc_price,
        "key_factors": "|".join(result.get("key_factors", [])),
        "short_term_outlook": result.get("short_term_outlook"),
        "medium_term_outlook": result.get("medium_term_outlook"),
        "long_term_outlook": result.get("long_term_outlook"),
        "risks": "|".join(result.get("risks", [])),
        "opportunities": "|".join(result.get("opportunities", [])),
        "detailed_analysis": result.get("detailed_analysis"),
        "comparison_to_previous": result.get("comparison_to_previous"),
        "discord_message": discord_msg,
        "model": model,
        "cost_usd": cost,
    }

    with open(HISTORY_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HISTORY_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def score_to_label(score) -> str:
    if not isinstance(score, int):
        return "?"
    if score >= 81: return "STRONG BUY"
    if score >= 61: return "BUY"
    if score >= 41: return "HOLD"
    if score >= 21: return "SELL"
    return "STRONG SELL"


def score_bar(score) -> str:
    """Visual bar for score: filled/empty blocks."""
    if not isinstance(score, int):
        return "?" * 10
    filled = round(score / 10)
    return "\u2588" * filled + "\u2591" * (10 - filled)


def score_emoji(score) -> str:
    if not isinstance(score, int):
        return "\u2753"
    if score >= 81: return "\U0001f7e2"  # green circle
    if score >= 61: return "\U0001f7e1"  # yellow circle
    if score >= 41: return "\U0001f7e0"  # orange circle
    if score >= 21: return "\U0001f534"  # red circle
    return "\u26d4"  # no entry


def rec_emoji(rec: str) -> str:
    return {
        "STRONG_BUY": "\U0001f680",
        "BUY": "\U0001f4c8",
        "HOLD": "\u23f8\ufe0f",
        "SELL": "\U0001f4c9",
        "STRONG_SELL": "\U0001f6a8",
    }.get(rec, "\u2753")


def build_discord_message(result: dict, timestamp: str, run_number: int,
                          market_data: dict, history: list[dict]) -> str:
    """Build fixed-format Discord message. Always identical structure."""

    rec = result.get("recommendation", "?")
    s_short = result.get("score_short_term", "?")
    s_med = result.get("score_medium_term", "?")
    s_long = result.get("score_long_term", "?")

    # Prices (safe defaults)
    qqq = market_data.get("QQQ", {}).get("info", {}).get("price") or 0
    spy = market_data.get("SPY", {}).get("info", {}).get("price") or 0
    vix = market_data.get("^VIX", {}).get("info", {}).get("price") or 0
    btc = market_data.get("BTC-USD", {}).get("info", {}).get("price") or 0
    qqq_prev = market_data.get("QQQ", {}).get("info", {}).get("previous_close") or 0
    spy_prev = market_data.get("SPY", {}).get("info", {}).get("previous_close") or 0

    qqq_chg = f"{(qqq - qqq_prev) / qqq_prev * 100:+.2f}%" if qqq and qqq_prev else "N/A"
    spy_chg = f"{(spy - spy_prev) / spy_prev * 100:+.2f}%" if spy and spy_prev else "N/A"

    # Previous comparison
    prev_section = ""
    if history:
        prev = history[-1]
        prev_rec = prev.get("recommendation", "?")
        prev_short = prev.get("score_short_term", "?")
        prev_section = (
            f"\n\U0001f504 **vs Previous (Run #{prev.get('run_number', '?')}):**\n"
            f"Was: {prev_rec} ({prev_short}/100) \u2192 Now: {rec} ({s_short}/100)\n"
            f"{result.get('comparison_to_previous', '')}"
        )

    msg = f"""{rec_emoji(rec)} **NASDAQ Signal** \u2502 Run #{run_number} \u2502 {timestamp}
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

\U0001f3af **{rec.replace('_', ' ')}**

{score_emoji(s_short)} Short  (~1w):  `{score_bar(s_short)}` **{s_short}/100** {score_to_label(s_short)}
{score_emoji(s_med)} Medium (1-3m): `{score_bar(s_med)}` **{s_med}/100** {score_to_label(s_med)}
{score_emoji(s_long)} Long   (6-12m):`{score_bar(s_long)}` **{s_long}/100** {score_to_label(s_long)}

\U0001f4b9 **Market Snapshot:**
\u2502 QQQ: **${qqq:.2f}** ({qqq_chg})
\u2502 SPY: **${spy:.2f}** ({spy_chg})
\u2502 VIX: **{vix:.2f}**
\u2502 BTC: **${btc:,.0f}**

\U0001f50d **Key Factors:**
{chr(10).join(f'\u2502 \u2022 {f}' for f in result.get("key_factors", []))}

\U0001f3af **Outlook:**
\u2502 Short:  {result.get("short_term_outlook", "?")}
\u2502 Medium: {result.get("medium_term_outlook", "?")}
\u2502 Long:   {result.get("long_term_outlook", "?")}

\u26a0\ufe0f **Risks:**
{chr(10).join(f'\u2502 \u2022 {r}' for r in result.get("risks", []))}

\U0001f4a1 **Opportunities:**
{chr(10).join(f'\u2502 \u2022 {o}' for o in result.get("opportunities", []))}
{prev_section}
\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
_This is not investment advice. This is an automated AI-generated analysis of current market conditions for informational purposes only. Always do your own research before making any investment decisions._"""

    return msg.strip()


def build_fallback_discord(raw_text: str, timestamp: str, run_number: int, market_data: dict) -> str:
    """Fallback message when structured parse fails."""
    qqq = market_data.get("QQQ", {}).get("info", {}).get("price") or 0
    spy = market_data.get("SPY", {}).get("info", {}).get("price") or 0
    vix = market_data.get("^VIX", {}).get("info", {}).get("price") or 0
    btc = market_data.get("BTC-USD", {}).get("info", {}).get("price") or 0
    if not raw_text or not raw_text.strip():
        raw_text = "Analysis unavailable — Claude returned an empty response."

    return f"""\u2753 **NASDAQ Signal** \u2502 Run #{run_number} \u2502 {timestamp}
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

\U0001f4b9 **Market Snapshot:**
\u2502 QQQ: **${qqq:.2f}** | SPY: **${spy:.2f}** | VIX: **{vix:.2f}** | BTC: **${btc:,.0f}**

\U0001f4dd **Analysis (unstructured):**
{raw_text[:1500]}
\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
_This is not investment advice. This is an automated AI-generated analysis of current market conditions for informational purposes only. Always do your own research before making any investment decisions._""".strip()


def should_notify(history: list[dict], result: dict) -> tuple[bool, str]:
    """Determine if Discord notification should be sent. Returns (should_send, reason)."""
    # Always on first run
    if not history:
        return True, "first run"

    # Always if a week passed since last saved analysis
    try:
        last_ts = datetime.strptime(history[-1]["timestamp"], "%Y-%m-%d %H:%M:%S")
        elapsed = datetime.now() - last_ts
        if elapsed.total_seconds() >= 7 * 24 * 3600:
            return True, f"weekly summary ({elapsed.days}d since last)"
    except (ValueError, KeyError):
        return True, "unable to determine last run time"

    prev = history[-1]

    # Recommendation changed
    prev_rec = prev.get("recommendation", "")
    new_rec = result.get("recommendation", "")
    if new_rec and prev_rec != new_rec:
        return True, f"recommendation changed: {prev_rec} -> {new_rec}"

    # Short-term score changed by 10+ points
    try:
        prev_score = int(prev.get("score_short_term", 0))
        new_score = int(result.get("score_short_term", 0))
        diff = abs(new_score - prev_score)
        if diff >= 10:
            return True, f"short-term score shifted {diff} pts ({prev_score} -> {new_score})"
    except (ValueError, TypeError):
        pass

    # Medium-term score changed by 10+ points
    try:
        prev_med = int(prev.get("score_medium_term", 0))
        new_med = int(result.get("score_medium_term", 0))
        diff_med = abs(new_med - prev_med)
        if diff_med >= 10:
            return True, f"medium-term score shifted {diff_med} pts ({prev_med} -> {new_med})"
    except (ValueError, TypeError):
        pass

    # Also compare against the last *notified* run (non-empty discord_message)
    # to catch gradual drift that individually never triggers the threshold
    last_notified = None
    for h in reversed(history):
        if h.get("discord_message"):
            last_notified = h
            break

    if last_notified and last_notified is not prev:
        # Recommendation changed since last notification
        notified_rec = last_notified.get("recommendation", "")
        if new_rec and notified_rec != new_rec:
            return True, f"recommendation changed since last notification: {notified_rec} -> {new_rec}"

        # Score drift since last notification
        try:
            notified_st = int(last_notified.get("score_short_term", 0))
            new_st = int(result.get("score_short_term", 0))
            if abs(new_st - notified_st) >= 10:
                return True, f"short-term score drifted {abs(new_st - notified_st)} pts since last notification ({notified_st} -> {new_st})"
        except (ValueError, TypeError):
            pass
        try:
            notified_mt = int(last_notified.get("score_medium_term", 0))
            new_mt = int(result.get("score_medium_term", 0))
            if abs(new_mt - notified_mt) >= 10:
                return True, f"medium-term score drifted {abs(new_mt - notified_mt)} pts since last notification ({notified_mt} -> {new_mt})"
        except (ValueError, TypeError):
            pass

    # No significant change
    return False, "no significant change vs previous analysis"


def translate_to_polish(english_msg: str, claude_cfg: dict) -> str:
    """Translate Discord message to Polish using Claude."""
    translate_system = "You are a translator. Translate the text to Polish. Keep ALL formatting intact: emoji, bold (**), lines (│ ═ ─), bars (█ ░), structure. Translate ONLY the text content — never change numbers, ticker symbols, dates, or formatting characters. Return ONLY the translated text, nothing else."
    translate_prompt = f"Translate to Polish. Keep exact formatting:\n\n{english_msg}"
    log.debug("=" * 40 + " TRANSLATION REQUEST " + "=" * 40)
    log.debug("System: %s", translate_system)
    log.debug("Prompt: %s", translate_prompt)
    claude = ClaudeCode(
        system_prompt=translate_system,
        timeout=claude_cfg.get("timeout", 300),
        max_budget_usd=0.30,
    )
    resp = claude.ask(translate_prompt)
    log.debug("Translation resp.text: %s", resp.text)
    log.debug("Translation resp.model: %s | cost: %s", resp.model, resp.cost_usd)
    text = resp.text.strip() if resp.text else ""
    # If translation failed or returned JSON error, return empty
    if not text or text.startswith("{") or "error" in text[:50].lower():
        log.warning("Translation failed, sending English only.")
        return ""
    return text


def send_discord_diagnostic(config: dict, timestamp: str, run_number: int,
                            reason: str, result: dict, market_data: dict):
    """Send a short diagnostic message when analysis is skipped."""
    rec = result.get("recommendation", "?")
    s_short = result.get("score_short_term", "?")
    s_med = result.get("score_medium_term", "?")
    qqq = market_data.get("QQQ", {}).get("info", {}).get("price") or 0

    msg = (
        f"\U0001f504 **Signal Check** \u2502 Run #{run_number} \u2502 {timestamp}\n"
        f"_{reason} \u2014 no update sent_"
    )
    send_discord(config, msg)


def trim_history_files():
    """Keep last 10 entries in CSV, conclusions.json, and snapshots."""
    # Trim CSV
    if HISTORY_CSV.exists():
        rows = []
        with open(HISTORY_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                rows.append(row)
        if len(rows) > 10:
            rows = rows[-10:]
            with open(HISTORY_CSV, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

    # Trim conclusions (already handled in save_conclusion with 10 limit)

    # Trim snapshots
    if SNAPSHOT_CSV.exists():
        rows = []
        with open(SNAPSHOT_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                rows.append(row)
        # ~6 symbols per run * 10 runs = 60 rows max
        if len(rows) > 60:
            rows = rows[-60:]
            with open(SNAPSHOT_CSV, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)


def send_discord(config: dict, message: str):
    discord_cfg = config.get("discord", {})
    if not discord_cfg.get("active") or not discord_cfg.get("webhook_url"):
        log.info("Discord not configured — skipping notification.")
        return
    notifier = DiscordNotifier(
        webhook_url=discord_cfg["webhook_url"],
        active=True,
    )
    log.debug("Discord message content:\n%s", message)
    notifier.send_sync(message)
    log.info("Discord notification sent.")


# ═══════════════════════════════════════════════════════════════════════════
# Main pipeline
# ═══════════════════════════════════════════════════════════════════════════
def main():
    setup_logging()
    log.info("=" * 60)
    log.info("Stock Analyser Signal — Starting analysis pipeline")
    log.info("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Load config
    log.info("[1/7] Loading config...")
    config = load_config()
    analysis_cfg = config.get("analysis", {})
    symbols_cfg = analysis_cfg.get("symbols", {})
    periods_cfg = analysis_cfg.get("data_periods", {})
    claude_cfg = analysis_cfg.get("claude", {})
    rss_feeds = analysis_cfg.get("rss_feeds", [])
    lookback = analysis_cfg.get("history_lookback", 10)

    run_id = str(uuid.uuid4())[:8]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info("Run ID: %s | Timestamp: %s", run_id, timestamp)

    # Step 2: Load history + conclusions
    log.info("[2/7] Loading previous analyses...")
    history = load_history(lookback)
    conclusions = load_conclusions(lookback)
    run_number = int(history[-1]["run_number"]) + 1 if history else 1
    history_prompt = format_history_for_prompt(history, conclusions)
    log.info("Loaded %d previous analyses, %d conclusions.", len(history), len(conclusions))

    # Step 3: Fetch market data
    log.info("[3/7] Fetching market data...")
    market_data = fetch_market_data(symbols_cfg, periods_cfg)
    market_prompt = format_market_data_for_prompt(market_data)
    log.info("Fetched data for %d symbols.", len(market_data))

    # Step 4: Save snapshot
    log.info("[4/7] Saving market snapshot...")
    save_snapshot(market_data, run_id, timestamp)
    log.info("Snapshot saved to %s", SNAPSHOT_CSV)

    # Step 5: Scrape news
    log.info("[5/7] Scraping news...")
    news_prompt = fetch_news(rss_feeds)
    log.info("News scraping complete.")

    # Step 6: Run Claude analysis
    log.info("[6/7] Running Claude analysis...")
    result, model, cost = run_analysis(market_prompt, news_prompt, history_prompt, claude_cfg, config, history)

    is_structured = not ("raw_text" in result and len(result) == 1)

    if is_structured:
        log.info("Analysis complete. Model: %s | Cost: $%s", model, cost or "?")
        log.info("Recommendation: %s | Scores: %s/%s/%s",
                 result.get("recommendation"),
                 result.get("score_short_term"), result.get("score_medium_term"), result.get("score_long_term"))
    else:
        log.warning("Claude returned raw text instead of structured JSON.")
        log.warning("Will use raw text for notification.")

    # Step 7: Decide whether to notify
    log.info("[7/8] Evaluating significance...")
    notify, notify_reason = should_notify(history, result if is_structured else {})

    if not notify:
        # Diagnostic message — save result (for continuity) but no translation
        rec = result.get("recommendation", "?") if is_structured else "?"
        s_short = result.get("score_short_term", "?") if is_structured else "?"
        log.info("SKIP — %s", notify_reason)
        log.info("Current: %s (%s/100) | Saving result, skipping translation.", rec, s_short)
        # Save so run_number stays continuous and next run sees this result
        if is_structured:
            save_result(result, run_number, run_id, timestamp, market_data, model, cost, "")
            save_conclusion(result, run_number, timestamp, market_data)
        trim_history_files()
        send_discord_diagnostic(config, timestamp, run_number, notify_reason,
                                result if is_structured else {}, market_data)
        log.info("Pipeline complete (no significant change).")
        return

    log.info("NOTIFY — %s", notify_reason)

    # Step 8: Build message, save, translate, send
    log.info("[8/8] Building message, saving, translating...")

    if is_structured:
        discord_msg_en = build_discord_message(result, timestamp, run_number, market_data, history)
    else:
        raw_text = result.get("raw_text", "Analysis unavailable")
        discord_msg_en = build_fallback_discord(raw_text, timestamp, run_number, market_data)

    # Save to CSV + conclusions (only when notifying)
    if is_structured:
        save_result(result, run_number, run_id, timestamp, market_data, model, cost, discord_msg_en)
        save_conclusion(result, run_number, timestamp, market_data)
        log.info("Result saved to %s", HISTORY_CSV)
        log.info("Conclusions saved to %s", CONCLUSIONS_FILE)
    trim_history_files()

    # Translate
    log.info("Translating to Polish...")
    discord_msg_pl = translate_to_polish(discord_msg_en, claude_cfg)

    # Combine EN + PL (skip Polish section if translation failed)
    if discord_msg_pl:
        log.info("Translation complete.")
        separator = "\n\n\U0001f1f5\U0001f1f1 **WERSJA POLSKA:**\n" + "\u2550" * 40 + "\n\n"
        discord_full = discord_msg_en + separator + discord_msg_pl
    else:
        discord_full = discord_msg_en

    log.info("─" * 60)
    log.info("Discord message:\n%s", discord_full)
    log.info("─" * 60)

    send_discord(config, discord_full)

    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
else:
    # When executed via exec() from scheduler
    main()

