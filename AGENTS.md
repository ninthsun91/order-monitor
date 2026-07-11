# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## What this is

A single-process, asyncio-based headless service that watches Binance Spot BTC/USDT orderbook depth and trades over WebSocket, detects when a large displayed order ("intent") actually gets filled (vs. cancelled/spoofed), and sends Telegram alerts on confirmed fills. No trading, no UI — monitoring only.

The requirements source of truth is [PRD_orderbook_intent_monitor.md](PRD_orderbook_intent_monitor.md) (Korean). [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) tracks milestone progress, per-session notes, and decisions that diverge from the PRD — **read it first** to know what's actually implemented vs. planned, since the codebase is still early (M0 scaffolding only as of this writing; `ingestion/`, `state/`, `detectors/`, `alerting/`, `watchdog/`, `persistence/` are currently empty packages).

**Commit frequently**: per DEVELOPMENT_PLAN.md's stated rule, commit after each meaningful unit of work (one detector, one loader, one test batch) once tests pass — don't batch until a milestone is done. Commit messages should reference which PRD/DEVELOPMENT_PLAN checkbox they correspond to. Only push on explicit user request or milestone completion.

## Commands

```bash
# Setup (Python 3.12 required — see DEVELOPMENT_PLAN decision log for the version rationale)
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run all tests
pytest

# Run a single test file / test
pytest tests/test_config.py
pytest tests/test_config.py::test_missing_file_raises

# Run the service (needs a local config.yaml — see below)
cp config.example.yaml config.yaml   # config.yaml is gitignored
order-monitor --config config.yaml --log-file order_monitor.log
```

There is no configured lint/format/typecheck tool yet.

## Architecture

Pipeline (per PRD §6), one asyncio process, components split as modules — not separate processes:

```
Binance WS (@depth20@100ms, @aggTrade, @depth@100ms diff)
  → Ingestion (reconnect, keepalive, normalize, timestamp; ccxt was ruled out by the M1 spike —
    binance-connector-python vs raw aiohttp still to be decided, see DEVELOPMENT_PLAN decision log)
  → In-memory State (order_book, level_tracker, trade_window, wall_registry) → Persistence (SQLite)
  → Detector Layer (D1 large-order, D2 volume burst, D3 absorption, D4 iceberg)
  → D5 Intent→Execution state machine (the core signal)
  → Alerting (severity gate, dedup, cooldown → Telegram)
Watchdog/Supervisor observes the pipeline from outside (process liveness, feed staleness).
```

Directory layout mirrors this 1:1:

```
src/order_monitor/
  ingestion/      # WS client, reconnect/keepalive
  state/          # order_book, level_tracker, trade_window
  detectors/      # D1–D5
  alerting/       # Telegram sending, dedup/cooldown
  watchdog/       # in-process watchdog, heartbeat
  persistence/    # SQLite (PRD §12)
  config.py       # YAML config loader + schema validation
  logging_setup.py # JSON-lines logging setup
  main.py         # entrypoint (registered as `order-monitor` script)
```

### Key design decisions to preserve

- **Three streams with strictly separated roles (PRD §5.1, v1.1)**: `@depth20@100ms` partial snapshots are the near-book (best price, D3/D4 contact-time measurement) — chosen over maintaining a full book from diffs to avoid the local-orderbook-drift bug class. The diff `@depth@100ms` stream IS consumed, but only as a **wall-registry event tap**: diff events carry *absolute* quantities at unlimited price distance, so large levels (walls) are tracked by overwriting per-price values with no sequencing/resync — self-healing, no drift. Never turn this tap into a full-book reconstruction, and never try to detect wall appearance from the top-20 window (it spans only ~$0.2–5 around price; D1 sources exclusively from the wall registry). Two-tier thresholds: `wall_tracker.record_min_qty_btc` (100, observation floor) vs `thresholds.size_threshold_btc` (1000, D1/D5 intent gate).
- **Reliability over precision/speed everywhere**: auto-reconnect with exponential backoff, judgments withheld until the first post-reconnect depth snapshot arrives, Telegram sends go through an async queue so alerting failures never block the pipeline, and a watchdog must independently detect both feed staleness and process hangs (silent failure is treated as the primary risk to design against).
- **All detection thresholds live in `config.yaml`**, never hardcoded — see `config.example.yaml` and `ThresholdsConfig`/`AlertsConfig`/etc. in [config.py](src/order_monitor/config.py). Config changes only need a restart (no hot reload requirement).
- **`config.py` schema validation is strict and manual**: uses `typing.get_type_hints` per dataclass to require exact key sets (rejects both missing *and* unknown keys) and coerces int→float but never bool→int/str etc. Follow this same pattern (explicit `_build_section`-style validation) if new config sections are added, rather than switching to a schema library mid-project.
- **Telegram bot token is env-var only** (`TELEGRAM_BOT_TOKEN`), never in config file or code. `telegram.chat_id` in config is typed as `str` — note that Binance-style negative group chat IDs must be quoted in YAML or they'll fail validation (a known gotcha, see DEVELOPMENT_PLAN M0 notes).
- **Detectors D1–D5** (see PRD §8 for full conditions/formulas): D1 flags large resting orders appearing/disappearing (with a persistence-time spoof filter and FILLED-vs-PULLED attribution); D2 flags time-windowed volume bursts; D3/D4 (absorption, iceberg/refill) feed D5 but never alert directly; D5 is the intent-registered → execution-confirmed/withdrawn/expired state machine and is the only detector whose confirmation events actually get sent to Telegram (alongside D2, and watchdog alerts).
- **No in-memory state survives restart — except the wall registry**: `trade_window` and `intents` are bounded by time/count and reset on restart by design (PRD §12) — persistence is for offline tuning/analysis, not state recovery. Don't add restart-recovery logic for these without checking DEVELOPMENT_PLAN's decision log first. The one deliberate exception (PRD §12.1): `wall_registry` syncs to SQLite and is restored on restart, because far-wall visibility only accumulates from listening to diff events. After any listening gap (restart or reconnect), restored walls are flagged `unconfirmed` (D1 APPEARED suppressed, `first_seen_*` preserved) until the next diff event at that price; levels with no event for `wall_tracker.ttl_days` (14) get pruned from the registry only — the `events` history log is kept.
- **JSON-lines structured logging**: `logging_setup.setup_logging()` replaces root logger handlers with a `JsonFormatter` (stdlib only, no external logging lib) over a `RotatingFileHandler`, plus stdout by default. Extra fields passed via `extra={...}` get merged into the JSON payload automatically; reserved `LogRecord` attributes (including Python 3.12's `taskName`) are filtered out already — don't re-filter them.
