# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-process, asyncio-based headless service that watches Binance Spot BTC/USDT orderbook depth and trades over WebSocket, detects when a large displayed order ("intent") actually gets filled (vs. cancelled/spoofed), and sends Telegram alerts on confirmed fills. No trading, no UI — monitoring only.

The requirements source of truth is [PRD_orderbook_intent_monitor.md](PRD_orderbook_intent_monitor.md) (Korean). [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) tracks milestone progress, per-session notes, and decisions that diverge from the PRD — **read it first** to know what's actually implemented vs. planned (M0–M4 done as of 2026-07-14 — ingestion, state, D1–D5, Telegram alerting incl. crash-safe outbox, replay tests; live case-1/2 alert confirmation and deployment/M5 pending).

**Commit frequently**: per DEVELOPMENT_PLAN.md's stated rule, commit after each meaningful unit of work (one detector, one loader, one test batch) once tests pass — don't batch until a milestone is done. Commit messages should reference which PRD/DEVELOPMENT_PLAN checkbox they correspond to. Only push on explicit user request or milestone completion.

**Confirm before changing**: when the user raises a PRD/spec critique or requests a change (to PRD, DEVELOPMENT_PLAN, config schema, or code), first state the planned change — what will be edited, where, and why — and wait for explicit confirmation before editing or committing. Don't implement-then-report. This applies for the rest of the project, not just the current session.

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

- **Three streams with strictly separated roles (PRD §5.1, v1.1)**: `@depth20@100ms` partial snapshots are the near-book (best price, D3/D4 contact-time measurement) — chosen over maintaining a full book from diffs to avoid the local-orderbook-drift bug class. The diff `@depth@100ms` stream IS consumed, but only as a **wall-registry event tap**: diff events carry *absolute* quantities at unlimited price distance, so large levels (walls) are tracked by overwriting per-price values with no sequencing/resync — self-healing, no drift. Never turn this tap into a full-book reconstruction, and never try to detect wall appearance from the top-20 window (it spans only ~$0.2–5 around price; D1 sources exclusively from the wall registry). Two-tier thresholds: `wall_tracker.record_min_qty_btc` (100, observation floor) vs `thresholds.size_threshold_btc` (1000, D1/D5 intent gate). The observation floor gates **new registrations only** — once a price is tracked, every diff event for it is applied regardless of quantity (qty 0 = tombstone; drop below the floor = run removal judgment, then deactivate), otherwise ghost walls persist (PRD §8 D1 v1.2).
- **Reliability over precision/speed everywhere**: auto-reconnect with exponential backoff, Telegram sends go through an async queue so alerting failures never block the pipeline, and a watchdog must independently detect both feed staleness and process hangs (silent failure is treated as the primary risk to design against). Detector judgments run only inside a **session epoch** (PRD §5.4 v1.2): per-stream health is tracked individually, any stream drop/staleness/diff U/u sequence gap ends the epoch (all judgments suspended, active intents marked `INTERRUPTED`, state ingestion continues), and a new epoch starts only after all three subscriptions are confirmed plus the first depth snapshot — a single depth snapshot alone is NOT enough to resume. diff `U`/`u` is used for gap detection only, never for book reconstruction.
- **All detection thresholds live in `config.yaml`**, never hardcoded — see `config.example.yaml` and `ThresholdsConfig`/`AlertsConfig`/etc. in [config.py](src/order_monitor/config.py). Config changes only need a restart (no hot reload requirement).
- **`config.py` schema validation is strict and manual**: uses `typing.get_type_hints` per dataclass to require exact key sets (rejects both missing *and* unknown keys) and coerces int→float but never bool→int/str etc. Follow this same pattern (explicit `_build_section`-style validation) if new config sections are added, rather than switching to a schema library mid-project.
- **Telegram bot token is env-var only** (`TELEGRAM_BOT_TOKEN`), never in config file or code. `telegram.chat_id` in config is typed as `str` — note that Binance-style negative group chat IDs must be quoted in YAML or they'll fail validation (a known gotcha, see DEVELOPMENT_PLAN M0 notes).
- **Detectors D1–D5** (see PRD §8 for full conditions/formulas): D1 flags large resting orders appearing/disappearing (with a persistence-time spoof filter and FILLED-vs-PULLED attribution); D2 flags time-windowed volume bursts; D3/D4 (absorption, iceberg/refill) feed D5 but never alert directly; D5 is the intent-registered → execution-confirmed/withdrawn state machine and is the only detector whose confirmation events actually get sent to Telegram (alongside D2, and watchdog alerts). **Intents never expire** (PRD v1.5 abolished the 30-min TTL): intent lifetime = wall lifetime, terminal only on wall removal / case-1/2 threshold / epoch end — the TTL created a dead window for standing far walls (the system's primary target) where a fully-consumed wall would never alert. Don't reintroduce a time-based expiry without re-reading that decision record.
- **D3/D4 judge per contact episode** (PRD §8 D3/D4 v1.2, v1.4): a shared tracker (`detectors/contact.py`) owns episode lifecycle (best price reaches level → rebound / pierce / removal) and the pierce judgment (same-side trade-price primary signal + best-persistence secondary with flicker reset). Episodes open for **every** contacted level, not just D1 walls — D5 case 2 (M4) sums D4 refills at non-wall levels; D3 filters to D1-active walls and fires only when an episode ends **unpierced** (v1.4 — never mid-episode, so a later pierce can't falsify an already-emitted event). Wiring order on wall removal matters: episode REMOVED end → D3 judgment → D1 REMOVED → D3 deregistration (see service.py docstring). `level_tracker` retains wall-registry prices across top-20 window exit (PRD §7 v1.4 exception) so lifetime `cum_traded_at_level` survives between contact episodes — don't "simplify" that back to pure top-20 scope.
- **Deterministic replay tests are the M3/M4 correctness gate** (PRD §13): `tests/replay/` drives the **real** MonitorService (clock/monotonic injectable via constructor) with JSONL fixtures of raw Binance frames; required scenarios are reconnect, stream order inversion, and diff U/u gap. D3/D4 do all time math on event `local_monotonic_receive_time` (never a clock() call) precisely so replay is deterministic. Re-capture live fixtures with `scripts/capture_stream.py`, then regenerate the goldens in `tests/test_replay.py`.
- **D5 (`detectors/d5.py`, M4) is independent of D3/contact episodes** — case 1 (`EXECUTION_CONFIRMED`) only reads the same `cum_traded_lookup` D1/D3 use, with **no pierce exclusion** (unlike D3: D5 asks "was S×REALIZE_PCT actually traded here", D3 asks "did it hold without breaking" — piercing after heavy trading doesn't invalidate case 1). Case 2 sums D4's *lifetime* (not episode-scoped) `refill_added` between the intent price and the same-side best price via `D4Detector.sum_lifetime_refill_above` — don't route case 2 through D4's episode-scoped `_acc`. `D5Detector.reset()` uniquely **returns events** (unlike D1–D4's void `reset()`) since epoch-end INTERRUPTED is itself a loggable record; in `service.py`'s `EpochEnded` handler, `d5.reset()` must run **before** `d4.reset()` or `above_realized_rate` reads zeroed-out lifetime data. D5's clock is `monotonic` (like D2) — intents don't survive restart, so no wall-clock persistence need.
- **`alerts_outbox` (M4, PRD §9.4)** persists only D5's two alertable terminal states (`EXECUTION_CONFIRMED`/`EXECUTION_INFERRED_ABOVE`) — pre-record before `telegram.enqueue(text, on_sent=...)`, mark sent only on delivery confirmation (the `on_sent` callback added to `TelegramSender.enqueue` fires on success only, never after retry exhaustion). Its idempotency key is `(side, price, terminal_state, recorded_at)` with `recorded_at` stamped by the service's wall-clock at record time — **not** D5Detector's `intent_id` (that's a per-process counter that resets to 0 on restart, so using it as the outbox's unique key risks silently colliding with an unrelated intent after a crash).
- **No in-memory state survives restart — except the wall registry**: `trade_window` is time-bounded, `intents` are bounded by wall lifetime + an active-count cap, and both reset on restart by design (PRD §12) — persistence is for offline tuning/analysis, not state recovery. Don't add restart-recovery logic for these without checking DEVELOPMENT_PLAN's decision log first. The one deliberate exception (PRD §12.1): `wall_registry` syncs to SQLite and is restored on restart, because far-wall visibility only accumulates from listening to diff events. After any listening gap (restart or reconnect), restored walls are flagged `unconfirmed` (D1 APPEARED suppressed, `first_seen_*` preserved) until the next diff event at that price. TTL pruning applies to **unconfirmed entries only** — not reconfirmed within `wall_tracker.ttl_days` (7) of `unconfirmed_since` → deleted; confirmed walls are never age-pruned (no event while connected = unchanged and valid, PRD §12.1 v1.2) — and prunes the registry only, the `events` history log is kept.
- **JSON-lines structured logging**: `logging_setup.setup_logging()` replaces root logger handlers with a `JsonFormatter` (stdlib only, no external logging lib) over a `RotatingFileHandler`, plus stdout by default. Extra fields passed via `extra={...}` get merged into the JSON payload automatically; reserved `LogRecord` attributes (including Python 3.12's `taskName`) are filtered out already — don't re-filter them.
