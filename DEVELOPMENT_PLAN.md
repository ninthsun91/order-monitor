# 개발 계획 및 진행 트래킹 — BTC 오더북 인텐트→실체결 모니터

> 이 문서는 [PRD](PRD_orderbook_intent_monitor.md)의 구현 계획(§13)을 실제 작업 단위로 분해하고 진행 상태를 기록한다.
> **규칙**:
> - 작업 완료 시 체크박스를 채우고, 단계 완료 시 상태 표를 갱신한다. PRD와 어긋나는 결정이 생기면 §"결정 기록"에 남긴다. 요구사항의 원천은 항상 PRD이며, 이 문서는 "무엇이 언제 어디까지 되었는가"만 다룬다.
> - **자주 커밋한다**: 의미 있는 단위 작업(디텍터 하나, 로더 하나, 테스트 묶음 등)이 끝나고 테스트가 통과하면 그 즉시 커밋한다. 마일스톤 끝까지 몰아서 하지 않는다. 커밋 메시지에는 어떤 PRD 항목/체크박스에 대응하는지 드러낸다. 푸시는 사용자 요청 시 또는 마일스톤 완료 시 수행한다.

## 전체 현황

| 단계 | 내용 | 상태 | 완료일 |
|---|---|---|---|
| M0 | 프로젝트 스캐폴딩 | ✅ 완료 | 2026-07-11 |
| M0.5 | PRD v1.1(벽 레지스트리) 반영 — 기존 스캐폴딩 정합화 | ✅ 완료 | 2026-07-11 |
| M1 | Ingestion + 상태 모델 + 로그 | ⬜ 미착수 | |
| M2 | D1 + D2 + Telegram 알림 | ⬜ 미착수 | |
| M3 | 레벨별 체결 집계 + D3 + D4 | ⬜ 미착수 | |
| M4 | D5 상태기계 + 확정 알림 | ⬜ 미착수 | |
| M5 | Watchdog + 배포 | ⬜ 미착수 | |
| M6 | SQLite 영속화 + 임계치 튜닝 | ⬜ 미착수 | |

상태 표기: ⬜ 미착수 · 🟡 진행 중 · 🟠 검증 대기 · ✅ 완료 · ⏸ 보류

---

## M0 — 프로젝트 스캐폴딩

PRD에는 없지만 M1 착수 전 필요한 기반 작업.

- [x] Python 3.12 설치 및 가상환경 생성 (Homebrew `python@3.12`, `.venv`)
- [x] `pyproject.toml` 작성 (기본 메타데이터, `requires-python = ">=3.12"`, `pyyaml`, dev: `pytest`)
- [x] 의존성 선택: `ccxt`를 1차로 채택, 한계 발견 시 `binance-connector-python`으로 보완/전환 (PRD §5.2) → 결정 기록 참고
- [x] 디렉터리 구조 확정 (src 레이아웃, PRD §6 컴포넌트에 1:1 대응 — 아래 참고)
- [x] `config.yaml` 로더 + 스키마 검증 (PRD §10의 모든 키) — `src/order_monitor/config.py`, 예시 `config.example.yaml`, 테스트 `tests/test_config.py` (6 passed)
- [x] 구조화 로그(JSON lines) 기반 셋업 (PRD §11.4) — `src/order_monitor/logging_setup.py` (stdlib `RotatingFileHandler`로 로테이션까지 포함, 외부 로그 라이브러리 불필요), 테스트 `tests/test_logging_setup.py` (8 passed 누적)
- [x] git 저장소 초기화, `.gitignore` (`.venv/`, `*.log`, `*.db`/`*.sqlite3`, `config.yaml`, `.env`, `.claude/settings.local.json` 등 제외). 초기 커밋 `3a8d8e2` 후 원격 `origin`(github.com/ninthsun91/order-monitor)에 푸시 완료

**완료 기준**: `config.yaml`을 읽어 로그 한 줄 남기고 종료하는 엔트리포인트가 동작한다.
→ **달성**. `src/order_monitor/main.py` + `[project.scripts] order-monitor` 등록. `config.example.yaml`을 로컬 `config.yaml`로 복사해 `order-monitor` CLI 실행 시 JSON 로그 한 줄(`config loaded`) 출력 및 로그 파일 기록 확인 (파일 1줄 + stderr 콘솔 출력은 별개 핸들러, 중복 아님). 테스트 9개 전체 통과.

**M0 완료.**

**디렉터리 구조** (PRD §6 아키텍처 다이어그램의 컴포넌트에 대응):

```
src/order_monitor/
  ingestion/      # WS 클라이언트, 재연결·keepalive
  state/          # order_book, level_tracker, trade_window
  detectors/      # D1~D5
  alerting/       # Telegram 발송, dedup·쿨다운
  watchdog/       # 인프로세스 워치독, 하트비트
  persistence/    # SQLite (§12)
tests/
```

`src/order_monitor/` 최상위에 `config.py`(로더), `logging_setup.py`(JSON 로그), `main.py`(엔트리포인트)가 추가되어 있다.

### M0 후속 세션 참고 노트

DEVELOPMENT_PLAN 본문에는 드러나지 않지만 이후 작업 시 알아두면 좋은 사실들:

- **의존성 실측 상태**: `ccxt 4.5.64` 설치됨, `import ccxt.pro` 정상(asyncio WebSocket), `aiohttp 3.14.1` 동반 설치됨. → M1 ccxt 스파이크 바로 착수 가능.
- **`config.chat_id`는 문자열만 허용 (M2 주의)**: 현재 스키마는 `telegram.chat_id`를 `str`로 강제. 그런데 Telegram 그룹/채널 chat_id는 흔히 음의 정수(예: `-1001234567890`)라, YAML에 정수로 쓰면 `ConfigError`로 거부됨. `config.example.yaml`이 `"..."`(문자열)이라 이 함정이 가려져 있음. M2에서 Telegram 배선할 때 int→str 허용/강제 변환을 결정할 것.
- **로컬 `config.yaml`은 gitignore됨**: 레포에는 `config.example.yaml`만 있음. 새 환경/세션에서는 이를 복사해 `config.yaml`을 만들어야 CLI가 동작함.
- **로깅 이중 출력**: `setup_logging(also_stdout=True)`가 기본. 파일(`RotatingFileHandler`) + 콘솔(stderr) 양쪽에 찍힘. systemd 배포(M5) 시 저널 중복을 피하려면 `also_stdout` 조정 고려.
- **`_RESERVED_ATTRS` 방식 검증됨**: `JsonFormatter`가 LogRecord 인스턴스에서 예약 속성 집합을 동적으로 구성하므로, Python 3.12가 추가한 `taskName` 속성도 자동으로 걸러짐(로그에 누출 안 됨). `extra=`로 넘긴 커스텀 필드만 JSON에 병합됨.

## M0.5 — PRD v1.1(벽 레지스트리) 반영: 기존 스캐폴딩 정합화

PRD v1.1 개정(diff 탭 벽 레지스트리 도입, 2026-07-11 결정 기록 참고)으로 M0 산출물 중 설정 스키마가 구스펙 상태가 됨. M1 착수 전 정합화.

- [x] `config.py`: `WallTrackerConfig` 섹션 추가 (`record_min_qty_btc`, `ttl_days`) — 기존 `_build_section` 수동 검증 패턴 유지 (이 로더는 기본값 없이 전 키 필수 — 기본값 100/14는 `config.example.yaml`에 반영)
- [x] `config.example.yaml`: `thresholds.size_threshold_btc` 300 → **1000** (PRD v1.1 확정값, 오픈 퀘스천 #1 결론)
- [x] `config.example.yaml`: `wall_tracker` 섹션 추가 (PRD §10 개정본과 일치)
- [x] `tests/test_config.py`: 새 섹션 로드/누락 키/미지 키/타입 검증 테스트 4건 추가 (누적 12 passed)
- [x] CLAUDE.md 갱신: 3-스트림 파이프라인, ccxt 탈락 반영, "diff 탭 ≠ full book" 가드레일, 벽 레지스트리 영속화 예외 서술

**완료 기준**: 새 스키마로 `config.example.yaml` 로드 성공 + 전체 테스트 통과. → **달성** (12 passed, `WallTrackerConfig(record_min_qty_btc=100.0, ttl_days=14.0)` 로드 확인).

**M0.5 완료.**

## M1 — Ingestion + 상태 모델 + 로그

- [x] **스파이크(우선 작업)**: ccxt `watch_order_book`이 `btcusdt@depth20@100ms`(partial snapshot)를 diff 스트림이 아니라 정확히 그 스트림으로 구독하는지 검증. 실패 시 `binance-connector-python`으로 전환(콜백→asyncio 브릿지 직접 구현) 후 이어서 진행 → **실패 확정 (2026-07-11)**: ccxt는 `btcusdt@depth@100ms`(diff)로 SUBSCRIBE 프레임을 고정 생성하고 REST 스냅샷+델타 병합으로 로컬 full book을 유지함(실측: 전송 프레임 캡처 + 수신 `depthUpdate` 이벤트 확인, 소스에도 `todo add support for <levels>-snapshots` 주석 존재. `limit=20`은 반환 시 잘라주는 용도일 뿐). fallback인 `binance-connector-python 3.13.0`의 `partial_book_depth(symbol, level=20, speed=100)`은 정확히 목표 스트림 구독 실측 확인(3초간 29msg, 20레벨 스냅샷, 간격 중앙값 100ms) → 결정 기록 참고
- [ ] WS 클라이언트: `btcusdt@depth20@100ms` 구독 → `order_book` 상태 갱신 (통째 교체)
- [ ] WS 클라이언트: `btcusdt@aggTrade` 구독 → `trade_window` deque 적재 + 만료 pop
- [ ] **(v1.1)** WS 클라이언트: `btcusdt@depth@100ms`(diff) 구독 → **벽 레지스트리 이벤트 탭** (PRD §5.1 설계 결정 2). full book 미유지 — 신규 가격은 `wall_tracker.record_min_qty_btc`(100) 이상일 때만 등록, **추적 중인 가격은 잔량 값 불문 모든 이벤트 처리 (v1.2 유령 벽 방지 — PRD §8 D1 소멸 규칙)**: 잔량 0 = tombstone 소멸, 하한 미만 하락 시 D1 소멸 판정 후 활성 제거. D1 발화 게이트는 `size_threshold_btc`(1000) — 2단 임계치 (PRD §8 D1)
- [ ] **(v1.1)** `wall_registry` SQLite 영속화 선행 구현 (PRD §12.1 — `walls` 테이블 한정, 전체 영속화는 여전히 M6): 재시작 복원, 청취 공백 시 전체 `unconfirmed` 마킹 + 가격별 첫 이벤트로 해제, `first_seen_at` 보존, `ttl_days` 청소
- [ ] `level_tracker` 구현: 레벨 생애주기 필드 (PRD §7)
- [ ] 재연결: 지수 백오프(1s→60s), 24h 강제 단절·ping/pong은 라이브러리 위임 확인 (PRD §5.3)
- [ ] 재연결 직후 첫 depth 스냅샷 수신 전 판정 보류 플래그
- [ ] 이벤트 타임스탬프는 거래소 시각 사용 (PRD §11.1) — **주의(스파이크 발견)**: spot partial depth(`@depth20@100ms`) 메시지에는 거래소 타임스탬프(`E`)가 없음(`lastUpdateId`/`bids`/`asks`만). 거래소 시각은 aggTrade(`E`/`T`)에만 존재 → depth 이벤트는 로컬 수신 시각 사용 등 구현 시 결정 필요
- [ ] 연결 이벤트(연결/단절/재연결) 구조화 로그
- [ ] 메모리 바운드 확인: `trade_window` 시간 상한 동작 테스트

**완료 기준 (PRD)**: 24h 무중단 수집, 재연결 자동 복구 확인.
**검증 방법**: 24h 실행 로그에서 (1) depth 이벤트 공백 구간 없음 (2) 강제 단절 시점에 재연결 로그 존재 (3) 메모리 사용량 평탄 — 를 확인하고 결과를 아래 검증 기록에 남긴다.

검증 기록:
- (예: 2026-07-15 24h 런 — 재연결 2회, 공백 최대 4s, RSS 120MB 안정)

## M2 — D1 + D2 + Telegram 발송

- [ ] D1 APPEARED: `SIZE_THRESHOLD` + `PERSIST_SECONDS` 지속 필터 + 레벨당 1회 발화 (PRD §8 D1)
- [ ] D1 REMOVED: `EXIT_RATIO` 하락 시 `FILLED` / `PULLED` 판정 (`FILL_ATTRIBUTION`)
- [ ] D2 볼륨 버스트: 방향 분리 집계 + 합산 모드, `BURST_COOLDOWN` (PRD §8 D2)
- [ ] Telegram 발송기: 비동기 큐, 초당 상한, 실패 시 백오프 재시도 (PRD §9.2, §11.1)
- [ ] 토큰은 `TELEGRAM_BOT_TOKEN` 환경변수로만 주입
- [ ] dedup: `(detector, side, price_bucket)` 키 + `ALERT_COOLDOWN`
- [ ] 알림 on/off 설정 반영 (`send_d1` 기본 off, `send_d2` 기본 on)
- [ ] 단위 테스트: 지속시간 필터(스푸핑 배제), FILLED/PULLED 분기, dedup/쿨다운

**완료 기준 (PRD)**: 실제 알림 수신, 스푸핑 필터 동작 확인.
**검증 방법**: (1) 실계정으로 D2 알림 1건 이상 수신 (2) 로그에서 PERSIST 미달로 발화 억제된 레벨 사례 확인.

검증 기록:
-

## M3 — 레벨별 체결 집계 + D3 + D4

- [ ] `cum_traded_at_level` 집계: aggTrade를 가격·aggressor 방향으로 레벨에 귀속
- [ ] D3 흡수: 가격 도달 + `ABSORPTION_MIN` + 비관통 조건, 실현률 산출 (PRD §8 D3)
- [ ] D4 아이스버그: 초과 체결 마진 + `ICEBERG_MIN_TRADES` (PRD §8 D4)
- [ ] D3/D4는 알림 발송 없이 로그(및 DB)만 기록 (PRD §9.1)
- [ ] 단위 테스트: 합성 이벤트 시퀀스로 D3/D4 발화 조건 검증
- [ ] Bookmap 육안 대조 절차 확정 (PRD 오픈 퀘스천 #5: 스크린샷 vs 녹화) → 결정 기록

**완료 기준 (PRD)**: FILLED/PULLED 구분 정확성 육안 검증 (Bookmap 대조).
**검증 방법**: 대형 레벨 이벤트 N건(최소 5건)을 Bookmap 화면과 대조하여 판정 일치 여부 기록.

검증 기록:
-

## M4 — D5 상태기계 + 확정 알림

- [ ] `INTENT_REGISTERED` 등록 (D1 APPEARED 입력, 크기 S 기록)
- [ ] 전이 1: 가격 도달 + 체결 누적 ≥ S×`REALIZE_PCT` → `EXECUTION_CONFIRMED` (케이스 1 — "해당 레벨 체결" 확인이지 원 표시 주문의 체결 확인 아님, PRD §8 D5 판정 의미)
- [ ] 전이 2: PULLED → 케이스 2 누적을 **먼저** 평가(충족 시 `EXECUTION_INFERRED_ABOVE` 우선), 미충족 시 `INTENT_WITHDRAWN` (로그만 — 레벨 실현률·상위 추정 실현률 필드 포함, PRD §8 D5 v1.2)
- [ ] 전이 3: 상위 구간 D4 누적 ≥ S×`REALIZE_PCT_ABOVE` → `EXECUTION_INFERRED_ABOVE` (케이스 2, 리필 확인분만 합산 — PRD §8 D5 집계 방식. v1.2에서 `_CONFIRMED_ABOVE`에서 개명: 귀속 불가로 "추정" 등급)
- [ ] 레벨 소멸(REMOVED/tombstone/하한 미달) 시 D5 즉시 종국 평가 — TTL 대기 없음, 소멸 원인 3분류 (PRD §8 D5 v1.2 "소멸 시 종국 평가")
- [ ] `INTENT_TTL` 만료 → `INTENT_EXPIRED` (로그만)
- [ ] `intents` 개수/시간 상한 (메모리 바운드)
- [ ] 확정 알림 포맷: 실현률 %, 등록→확정 소요시간 포함 (PRD §9.3), 케이스 2는 "추정" 명시
- [ ] 단위 테스트: 상태 전이 전 경로 (확정 1/2, 철회, 만료)
- [ ] **(코어 전이 테스트 통과 후)** D5 진행률 알림: `progress_step_pct`(0.2) 경계당 1회, 계열(케이스 1/2)별 독립 커서, dedup 키에 경계값 포함해 쿨다운 우회 (PRD §8 D5 진행률 알림, §9.2 예외)

**완료 기준 (PRD)**: 케이스 1/2 알림 각 1건 이상 실전 확인.
**검증 방법**: 실전 수신한 알림의 근거 이벤트를 로그에서 역추적해 타당성 확인.

검증 기록:
-

## M5 — Watchdog + systemd + 배포

- [ ] 인프로세스 워치독: `stale_seconds` 동안 depth 없으면 `FEED_STALE` 알림 (PRD §11.1)
- [ ] 하트비트 파일 기록 + 외부 경량 워치독(cron/systemd timer)이 행(hang) 상태 감지 → `PROCESS_DOWN` 알림
- [ ] systemd 유닛: `Restart=always, RestartSec=5` (또는 Docker `restart: unless-stopped`)
- [ ] 로그 로테이션 설정
- [ ] VPS 준비: 리전 선정(도쿄/싱가포르), **배포 전 해당 IP에서 Binance WS/REST 접속 검증** (PRD §11.3)
- [ ] 배포 절차 문서화 (README 또는 runbook)

**완료 기준 (PRD)**: VPS에서 7일 무인 운영, 조용한 실패 0건.
**검증 방법**: 7일 후 로그 감사 — 모든 단절/재시작 이벤트에 대응하는 Telegram 통지가 존재하는지 대조.

검증 기록:
-

## M6 — SQLite 영속화 + 임계치 튜닝 루프

- [ ] SQLite 스키마: `events`, `intents`, `trades_sample`(선택) (PRD §12)
- [ ] 모든 디텍터 이벤트/상태 전이 기록
- [ ] 재시작 시 진행 중 intent를 DB에 `INTERRUPTED` 마킹
- [ ] 1주 데이터로 top-20 잔량 분포 분석 → `SIZE_THRESHOLD` 확정 (오픈 퀘스천 #1)
- [ ] 나머지 임계치 1차 튜닝 (오탐/침묵 리뷰)
- [ ] 확정 임계치를 `config.yaml`에 반영하고 결정 기록에 근거 기재
- [ ] (선택) 일일 요약 메시지 (감지 건수, 재연결 횟수, 업타임)

**완료 기준 (PRD)**: 1주 데이터 기반 임계치 1차 확정.

검증 기록:
-

---

## 결정 기록

PRD와 다르게 결정했거나 PRD가 열어둔 것을 확정한 사항. 날짜·결정·근거를 남긴다.

| 날짜 | 결정 | 근거 |
|---|---|---|
| 2026-07-11 | Python 버전 = 3.12 (Homebrew `python@3.12`) | 3.9는 EOL. 3.14는 생태계 미성숙 + 저사양 VPS에서 소스 빌드 리스크. `binance-connector-python`이 3.13.1+에서 알려진 호환성 버그(binance/binance-connector-python#394) 있어 3.13 계열도 회피. 3.12는 성숙도·wheel 지원·asyncio 성능의 균형점 |
| 2026-07-11 | WS 라이브러리 = **ccxt 1차 채택**, 한계 시 `binance-connector-python`으로 보완 | ccxt.pro가 무료로 ccxt 본체에 병합되어 asyncio 네이티브(`watch_order_book`/`watch_trades`)로 PRD §6 아키텍처에 바로 부합. 단, 범용 추상화라 `depth20@100ms` partial snapshot을 정확히 구독하는지 미검증 → M1 착수 전 스파이크로 확인 후 실패 시 binance-connector-python(콜백→asyncio 브릿지 직접 구현)으로 전환 |
| 2026-07-11 | **ccxt 탈락 확정** — M1 스파이크 결과 ccxt `watch_order_book`은 diff 스트림(`@depth@100ms`) 전용, partial snapshot 구독 불가 (M1 스파이크 체크박스 참고) | fallback 후보 실측: (a) `binance-connector-python 3.13.0` `partial_book_depth` — 목표 스트림 정확 구독 확인, 단 websocket-client 기반 콜백/스레드 모델이라 asyncio 브릿지 필요. (b) raw WS(`aiohttp`, ccxt 동반 설치로 이미 의존성에 존재) 직접 구독 — 역시 정상 동작 확인, asyncio 네이티브지만 재연결·keepalive 직접 구현 필요. **WS 클라이언트 구현 착수 시 (a)/(b) 중 택일** (PRD가 지수 백오프·staleness 제어를 명시 요구하므로 어느 쪽이든 재연결 정책 코드는 우리 소유). PRD §5.2의 "raw 직접 구현 금지" 조항은 v1.1에서 해제됨 |
| 2026-07-11 | **PRD v1.1 개정: diff 스트림을 "대형 레벨 이벤트 탭"으로 도입, 벽 레지스트리 신설** (§5.1 한계 조항 번복) | 사용자 확인: 프로젝트의 실제 목적은 원거리 고래 벽(예: 61k의 1.36k BTC bid, Coinglass/Bookmap에서 관측) 감시. 실측: top-20 창은 현재가 ±$0.2~5, REST depth(limit 상한 5000)도 ±$970 한계 → partial/REST로는 원천 관측 불가. Binance diff 이벤트는 가격 거리 무제한 + **절대 잔량** 운반이므로 full book 없이 레벨 단위 추적 가능(자가치유, 드리프트 버그 클래스 비해당). 신규 접속 4분 청취로 61000 레벨 1,364.86 BTC 실포착(Coinglass 표시값과 일치), 부하 초당 ~10 이벤트. Coinglass/Bookmap도 동일 원천의 상시 청취 누적임을 확인 → 외부 API 구독 대안 기각 |
| 2026-07-11 | **D1 소스 = 벽 레지스트리 전용 + 2단 임계치**: 기록 하한 `record_min_qty_btc` = 100 BTC, D1/D5 인텐트 기준 `size_threshold_btc` = 1000 BTC (기존 300에서 변경, 둘 다 설정값) | top-20 창은 ±$0.2~5(실측)라 벽이 창에 들어온 시점 = 가격 접촉 시점 → top-20 기반 D1 출현 감지는 성립 불가(사용자 지적, 65k의 102 BTC ask도 top-20 밖). top-20은 D3/D4 정밀 계측 + best price 전용으로 축소. 1000 근거(사용자 시장 판단): Binance 현물에서 가격 전달력 있는 유동성은 1k BTC급부터 — 실측상 현재 해당 벽이 61k 하나뿐인 것은 의도된 결과. 기록 하한 100 근거: 65k 102 BTC급 근접 물량 포착(150이면 누락) + M6 임계치 튜닝용 분포 데이터 확보. 세션 중 검토했던 "근접용/원거리용 별도 임계치" 안은 D1 근접 소스 자체가 폐기되어 철회 |
| 2026-07-11 | 벽 레지스트리 SQLite 영속화 + unconfirmed 플래그 + TTL 14일 — "인메모리 상태 미복원" 원칙(PRD §12)의 유일한 예외 | 원거리 벽 시야는 청취 누적으로만 형성되므로 재시작 초기화 비용이 큼. 신뢰도 하락은 **청취 공백**(재시작·재연결 갭)에서만 발생(연결 중 무이벤트 = 무변화 = 값 유효) → 공백 시 전체 unconfirmed 마킹, 가격별 새 이벤트(절대 잔량)로 자동 해제, 능동 재검증 API는 부재. unconfirmed 중 APPEARED 발화 억제 + `first_seen_at` 보존(스푸핑 필터 타이머 유지). TTL 14일(설정값, 사용자 경험상 2주 초과 지속 벽 드묾)은 신뢰도가 아닌 저장 위생 규칙 — `events` 이력은 보존해 M6 튜닝 데이터 유지 |

## 오픈 퀘스천 트래킹 (PRD §15)

| # | 질문 | 결정 시점 | 상태 | 결론 |
|---|---|---|---|---|
| 1 | `SIZE_THRESHOLD` 초기값 (BTC 수량 vs USDT 노셔널) | M1 데이터 수집 후 / 최종 M6 | ✅ 확정 | BTC 수량 기준, **1000 BTC** (2026-07-11, 사용자 시장 판단 — 전달력 있는 유동성 기준). 기록 하한은 별도 100 BTC(`record_min_qty_btc`). M6에서 실데이터 재검토 여지만 유지 |
| 2 | 케이스 2 "상위 구간" 범위 제한 필요 여부 | M4 설계 시 | ⬜ 미결 | |
| 3 | 매도 의도(ask 벽) 대칭 지원 v1 포함 여부 (PRD는 포함 권장) | M0~M1 설계 시 | ✅ 확정 | v1 포함 — 벽 레지스트리 설계 논의(2026-07-11)에서 사용자가 ask/bid 양측 추적을 전제로 함. 구현 비용도 낮음 |
| 4 | 알림 언어/포맷: 한국어 고정 vs 템플릿화 | M2 착수 시 | ⬜ 미결 | |
| 5 | Bookmap 대조 기록 방식 (스크린샷 vs 녹화) | M3 착수 시 | ⬜ 미결 | |
