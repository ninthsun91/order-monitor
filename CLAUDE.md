# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-process, asyncio-based headless service that watches Binance Spot BTC/USDT orderbook depth and trades over WebSocket, detects when a large displayed order ("intent") actually gets filled (vs. cancelled/spoofed), and sends Telegram alerts on confirmed fills. No trading, no UI — monitoring only. Since M8 (PRD §5.5 v1.16) it can additionally run fully independent per-exchange pipelines (Coinbase BTC-USD first, opt-in via the `exchanges:` config section) — D1+D3+D5 only on new exchanges, no consolidated book, no cross-exchange logic in real-time judgment.

## Documentation map (read on demand — don't load everything)

All project docs live in `docs/`. Load only what the task needs:

- [docs/NEXT_STEPS.md](docs/NEXT_STEPS.md) — snapshot of remaining work. **Start here in a new session** to know what's next.
- [docs/DEVELOPMENT_PLAN.md](docs/DEVELOPMENT_PLAN.md) — status table + in-progress milestones only (checkboxes, verification records, open questions). Read the relevant milestone section before starting work on it.
- [docs/PRD_orderbook_intent_monitor.md](docs/PRD_orderbook_intent_monitor.md) — requirements source of truth (Korean), ~670 lines. Cited everywhere as "PRD §N" — read only the § relevant to the task, not the whole file.
- [docs/DECISIONS.md](docs/DECISIONS.md) — dated decision log; "결정 기록 YYYY-MM-DD" citations in code/commits/docs point here. Grep by date or keyword, read only matching rows.
- [docs/MILESTONE_ARCHIVE.md](docs/MILESTONE_ARCHIVE.md) — frozen details of completed milestones (M0–M5). Only for researching past implementation history.

Rule of thumb: spec question → PRD §; "why is it this way" → DECISIONS.md; "what's done / what's next" → DEVELOPMENT_PLAN / NEXT_STEPS; historical archaeology → MILESTONE_ARCHIVE.

**Commit frequently**: per docs/DEVELOPMENT_PLAN.md's stated rule, commit after each meaningful unit of work (one detector, one loader, one test batch) once tests pass — don't batch until a milestone is done. Commit messages should reference which PRD/DEVELOPMENT_PLAN checkbox they correspond to. Only push on explicit user request or milestone completion.

**Confirm before changing**: when the user raises a PRD/spec critique or requests a change (to PRD, DEVELOPMENT_PLAN, config schema, or code), first state the planned change — what will be edited, where, and why — and wait for explicit confirmation before editing or committing. Don't implement-then-report. This applies for the rest of the project, not just the current session.

## Commands

```bash
# Setup (Python 3.12 required — see docs/DECISIONS.md for the version rationale)
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
  → Ingestion (reconnect, keepalive, normalize, timestamp; raw aiohttp — ccxt and
    binance-connector-python were ruled out by the M1 spike, see docs/DECISIONS.md 2026-07-11)
  → In-memory State (order_book, level_tracker, trade_window, wall_registry, candles) → Persistence (SQLite)
  → Detector Layer (D1 large-order, D2 volume burst, D3 absorption, D4 level-absorption-defense
    (redesigned PRD v1.11–v1.12, rewired 2026-07-23), W watch-level observer (PRD v1.13))
  → D5 Intent→Execution state machine (the core signal; case 1 only — case 2 abolished in v1.11)
  → Alerting (severity gate, dedup, cooldown → Telegram; inbound Telegram commands for W (§9.5))
Watchdog/Supervisor observes the pipeline from outside (process liveness, feed staleness).
```

Directory layout mirrors this 1:1:

```
src/order_monitor/
  ingestion/      # WS clients (Binance ws_client.py, Coinbase coinbase.py), normalize, reconnect/keepalive, health/epoch
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

- **Multi-exchange = independent pipelines, never a consolidated book (M8, PRD §5.5 v1.16)**: one `MonitorService` instance per exchange (`exchange` ctor param; default `"binance"` = full feature set, unchanged behavior — the existing replay goldens are the regression gate). New exchanges run **D1+D3+D5 only** (D2/D4/W/candles are `None` — their logic is coupled to the 100ms near-book cadence); the near-book input is a ticker-synthesized top-1 `DepthSnapshot`, and **local full-book maintenance stays forbidden**. Adapters (e.g. `ingestion/coinbase.py`) only produce the three normalized event types — everything below reuses unchanged. Per-exchange specifics live in config `exchanges:` (**required section, must contain `binance`** — since v1.17 it is the single home for exchange-scoped keys: `symbol`/`size_threshold_btc`/`record_min_qty_btc`/`band_pct`; judgment params stay global in `thresholds:`) and in ctor branches: Coinbase uses a registration **band filter** (`band_pct`, with event-derived mid fallback — full-book snapshots contain garbage far orders like 65,000 BTC at $0.01), match `side` is the **maker** side (side=="sell" → buy aggressor, live-capture verified), epoch gap detection is match trade_id continuity (`trade_gap` ends the epoch but does **NOT** mark the registry unconfirmed — trade loss is not a diff listening gap; reconnect's full snapshot re-confirms instantly), and `LevelTracker(create_on_trade_for_retained=True)` covers trades arriving before the ticker. All SQLite tables carry an `exchange` column (stores are constructor-bound to their exchange); dedup/alerts stay per-pipeline via separate dispatcher instances + `venue_label`. Shared process resources (TelegramSender, heartbeat, command loop) belong to the primary (sender-owning) instance only.
- **Three streams with strictly separated roles (PRD §5.1, v1.1)**: `@depth20@100ms` partial snapshots are the near-book (best price, D3/D4 contact-time measurement) — chosen over maintaining a full book from diffs to avoid the local-orderbook-drift bug class. The diff `@depth@100ms` stream IS consumed, but only as a **wall-registry event tap**: diff events carry *absolute* quantities at unlimited price distance, so large levels (walls) are tracked by overwriting per-price values with no sequencing/resync — self-healing, no drift. Never turn this tap into a full-book reconstruction, and never try to detect wall appearance from the top-20 window (it spans only ~$0.2–5 around price; D1 sources exclusively from the wall registry). Two-tier thresholds, per exchange since v1.17: `exchanges.<name>.record_min_qty_btc` (binance 100, observation floor) vs `exchanges.<name>.size_threshold_btc` (binance 1000, D1/D5 intent gate). The observation floor gates **new registrations only** — once a price is tracked, every diff event for it is applied regardless of quantity (qty 0 = tombstone; drop below the floor = run removal judgment, then deactivate), otherwise ghost walls persist (PRD §8 D1 v1.2).
- **Reliability over precision/speed everywhere**: auto-reconnect with exponential backoff, Telegram sends go through an async queue so alerting failures never block the pipeline, and a watchdog must independently detect both feed staleness and process hangs (silent failure is treated as the primary risk to design against). Detector judgments run only inside a **session epoch** (PRD §5.4 v1.2): per-stream health is tracked individually, any stream drop/staleness/diff U/u sequence gap ends the epoch (all judgments suspended, active intents marked `INTERRUPTED`, state ingestion continues), and a new epoch starts only after all three subscriptions are confirmed plus the first depth snapshot — a single depth snapshot alone is NOT enough to resume. diff `U`/`u` is used for gap detection only, never for book reconstruction.
- **All detection thresholds live in `config.yaml`**, never hardcoded — see `config.example.yaml` and `ThresholdsConfig`/`AlertsConfig`/etc. in [config.py](src/order_monitor/config.py). Config changes only need a restart (no hot reload requirement).
- **`config.py` schema validation is strict and manual**: uses `typing.get_type_hints` per dataclass to require exact key sets (rejects both missing *and* unknown keys) and coerces int→float but never bool→int/str etc. Follow this same pattern (explicit `_build_section`-style validation) if new config sections are added, rather than switching to a schema library mid-project.
- **Telegram bot token is env-var only** (`TELEGRAM_BOT_TOKEN`), never in config file or code. `telegram.chat_id` in config is typed as `str` — note that Binance-style negative group chat IDs must be quoted in YAML or they'll fail validation (a known gotcha, see docs/MILESTONE_ARCHIVE.md M0 notes). Inbound commands are authorized against the separate `telegram.command_chat_ids` list (v1.14) — outbound alerts still go to the single `chat_id`, command replies route back to the sender's chat.
- **Detectors D1–D5 + W** (see PRD §8 for full conditions/formulas): D1 flags large resting orders appearing/disappearing (persistence-time spoof filter, FILLED-vs-PULLED attribution; APPEARED alerts once per threshold streak — restart/reconnect re-detections are suppressed at the dispatcher via `walls.appeared_alerted_since`, PRD v1.8). D2 flags episode-based relative-threshold volume bursts (onset + summary with price-reaction verdict). D3 (absorption) is log-only. **D4 is the level-absorption-defense detector (redesigned PRD v1.11–v1.12, rewired 2026-07-23** — the old iceberg/case-2 D4 is gone): it targets ALL registry-tracked levels (100+ floor, not just D1 walls), counts only refill-proven absorption (visible 500ms pairing + hidden per-100ms-tick `max(0, traded − displayed-decrease − carry)` with a 1-tick carry correction), and fires `DEFENSE_DETECTED` at `absorbed ≥ absorb_multiple × R` with latch/progress/closure lifecycle; streaks restart on epoch start with R re-fixed. **D5 is case 1 only** (case 2 / `EXECUTION_INFERRED_ABOVE` abolished in v1.11 — attribution of above-level activity is structurally unknowable from public data; don't reintroduce it). Case-1 confirmation is a **latch, not a terminal** (v1.9): unbounded 20%-step progress alerts continue past 100% until wall removal (`CONFIRMED_CLOSED`, log-only) or epoch end. **Intents never expire** (v1.5 abolished the TTL): intent lifetime = wall lifetime — don't reintroduce time-based expiry without reading that decision record. W (v1.13) is a judgment-free watch-level observer driven by inbound Telegram commands (`/watch`, `/unwatch`, `/watching`; accepts `message` + `channel_post`, v1.14) with 15m-close-based invalidation.
- **D3/D4 judge per contact episode** (PRD §8 D3/D4 v1.2, v1.4): a shared tracker (`detectors/contact.py`) owns episode lifecycle (best price reaches level → rebound / pierce / removal) and the pierce judgment (same-side trade-price primary signal + best-persistence secondary with flicker reset). Episodes open for **every** contacted level, not just D1 walls (the sub-threshold consumer is D4, whose non-pierce gate reuses the shared pierce judgment); D3 filters to D1-active walls and fires only when an episode ends **unpierced** (v1.4 — never mid-episode, so a later pierce can't falsify an already-emitted event). Wiring order on wall removal matters: episode REMOVED end → D3 judgment → D1 REMOVED → D3 deregistration (see service.py docstring). `level_tracker` retains wall-registry prices across top-20 window exit (PRD §7 v1.4 exception) so lifetime `cum_traded_at_level` survives between contact episodes — don't "simplify" that back to pure top-20 scope.
- **Deterministic replay tests are the M3/M4 correctness gate** (PRD §13): `tests/replay/` drives the **real** MonitorService (clock/monotonic injectable via constructor) with JSONL fixtures of raw Binance frames; required scenarios are reconnect, stream order inversion, and diff U/u gap. D3/D4 do all time math on event `local_monotonic_receive_time` (never a clock() call) precisely so replay is deterministic. Re-capture live fixtures with `scripts/capture_stream.py`, then regenerate the goldens in `tests/test_replay.py`.
- **D5 (`detectors/d5.py`, M4) is independent of D3/contact episodes** — case 1 (`EXECUTION_CONFIRMED`) only reads the same `cum_traded_lookup` D1/D3 use, with **no pierce exclusion** (unlike D3: D5 asks "was S×REALIZE_PCT actually traded here", D3 asks "did it hold without breaking" — piercing after heavy trading doesn't invalidate case 1). `D5Detector.reset()` uniquely **returns events** (unlike the other detectors' void `reset()`) since epoch-end INTERRUPTED is itself a loggable record. The old `d5.reset()`-before-`d4.reset()` ordering constraint died with case 2 (v1.12 — no cross-detector read remains). D5's clock is `monotonic` (like D2) — intents don't survive restart, so no wall-clock persistence need.
- **`alerts_outbox` (M4, PRD §9.4)** persists only D5's alertable confirmation (`EXECUTION_CONFIRMED`; `EXECUTION_INFERRED_ABOVE` was removed with case 2) — pre-record before `telegram.enqueue(text, on_sent=...)`, mark sent only on delivery confirmation (the `on_sent` callback added to `TelegramSender.enqueue` fires on success only, never after retry exhaustion). Its idempotency key is `(side, price, terminal_state, recorded_at)` with `recorded_at` stamped by the service's wall-clock at record time — **not** D5Detector's `intent_id` (that's a per-process counter that resets to 0 on restart, so using it as the outbox's unique key risks silently colliding with an unrelated intent after a crash).
- **No in-memory state survives restart — except the wall registry (§12.1) and W watch levels (§12.2, v1.13)**: `trade_window` is time-bounded, `intents` are bounded by wall lifetime + an active-count cap, and both reset on restart by design (PRD §12) — persistence is for offline tuning/analysis, not state recovery. Don't add restart-recovery logic for anything else without checking docs/DECISIONS.md first. The wall-registry exception (PRD §12.1): `wall_registry` syncs to SQLite and is restored on restart, because far-wall visibility only accumulates from listening to diff events. After any listening gap (restart or reconnect), restored walls are flagged `unconfirmed` (D1 APPEARED suppressed, `first_seen_*` preserved) until the next diff event at that price. TTL pruning applies to **unconfirmed entries only** — not reconfirmed within `wall_tracker.ttl_days` (7) of `unconfirmed_since` → deleted; confirmed walls are never age-pruned (no event while connected = unchanged and valid, PRD §12.1 v1.2) — and prunes the registry only, the `events` history log is kept.
- **JSON-lines structured logging**: `logging_setup.setup_logging()` replaces root logger handlers with a `JsonFormatter` (stdlib only, no external logging lib) over a `RotatingFileHandler`, plus stdout by default. Extra fields passed via `extra={...}` get merged into the JSON payload automatically; reserved `LogRecord` attributes (including Python 3.12's `taskName`) are filtered out already — don't re-filter them.
