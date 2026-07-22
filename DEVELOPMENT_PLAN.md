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
| M1 | Ingestion + 상태 모델 + 로그 | ✅ 완료 | 2026-07-12 |
| M2 | D1 + D2 + Telegram 알림 | ✅ 완료 | 2026-07-12 |
| M3 | 레벨별 체결 집계 + D3 + D4 | ✅ 완료 | 2026-07-13 |
| M4 | D5 상태기계 + 확정 알림 | ✅ 완료 | 2026-07-22 (케이스 1 실전 확인 2026-07-20 — 완료 기준 전부 충족) |
| M5 | Watchdog + 배포 | 🟠 검증 대기 | (7일 무인 운영 감사 통과 2026-07-22, 행 훈련만 잔여) |
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

- [x] `config.py`: `WallTrackerConfig` 섹션 추가 (`record_min_qty_btc`, `ttl_days`) — 기존 `_build_section` 수동 검증 패턴 유지 (이 로더는 기본값 없이 전 키 필수 — 기본값 100/14는 `config.example.yaml`에 반영. ttl은 이후 PRD v1.2에서 7일로 단축됨)
- [x] `config.example.yaml`: `thresholds.size_threshold_btc` 300 → **1000** (PRD v1.1 확정값, 오픈 퀘스천 #1 결론)
- [x] `config.example.yaml`: `wall_tracker` 섹션 추가 (PRD §10 개정본과 일치)
- [x] `tests/test_config.py`: 새 섹션 로드/누락 키/미지 키/타입 검증 테스트 4건 추가 (누적 12 passed)
- [x] CLAUDE.md 갱신: 3-스트림 파이프라인, ccxt 탈락 반영, "diff 탭 ≠ full book" 가드레일, 벽 레지스트리 영속화 예외 서술
- [x] **(v1.2 추가)** config 불변조건 검증: 양수·비율 (0,1]·`record_min < size_threshold`·`heartbeat < stale_seconds`, 위반 테스트 4건 (PRD §10 v1.2 — 16 passed)
- [x] **(v1.2 검수 4회차)** config 최상위 키 `exchange`·`depth_stream` 삭제 — 설계 고정 사항(§5.1)의 추측성 파라미터화 제거, `symbol`만 유지 (PRD §10 개정 (15), config.py·config.example.yaml·테스트 동반 수정)

**완료 기준**: 새 스키마로 `config.example.yaml` 로드 성공 + 전체 테스트 통과. → **달성** (12 passed, `WallTrackerConfig(record_min_qty_btc=100.0, ttl_days=14.0)` 로드 확인).

**M0.5 완료.**

## M1 — Ingestion + 상태 모델 + 로그

- [x] **스파이크(우선 작업)**: ccxt `watch_order_book`이 `btcusdt@depth20@100ms`(partial snapshot)를 diff 스트림이 아니라 정확히 그 스트림으로 구독하는지 검증. 실패 시 `binance-connector-python`으로 전환(콜백→asyncio 브릿지 직접 구현) 후 이어서 진행 → **실패 확정 (2026-07-11)**: ccxt는 `btcusdt@depth@100ms`(diff)로 SUBSCRIBE 프레임을 고정 생성하고 REST 스냅샷+델타 병합으로 로컬 full book을 유지함(실측: 전송 프레임 캡처 + 수신 `depthUpdate` 이벤트 확인, 소스에도 `todo add support for <levels>-snapshots` 주석 존재. `limit=20`은 반환 시 잘라주는 용도일 뿐). fallback인 `binance-connector-python 3.13.0`의 `partial_book_depth(symbol, level=20, speed=100)`은 정확히 목표 스트림 구독 실측 확인(3초간 29msg, 20레벨 스냅샷, 간격 중앙값 100ms) → 결정 기록 참고
- [x] 정규화 **(v1.2)**: 가격·수량은 수신 문자열 → `Decimal` 파싱, 레벨 키는 정규화 Decimal — 판정 경로 float 금지 (PRD §7 수치 표현형) — `ingestion/events.py` (파싱 실패는 `NormalizationError` 명시 감지, Decimal 키 조인은 값 기반 해시로 성립. config 수치의 Decimal 변환은 service 경계에서 수행 — 로더 스키마는 float 유지)
- [x] WS 클라이언트: `btcusdt@depth20@100ms` 구독 → `order_book` 상태 갱신 (통째 교체) — `state/order_book.py` + `service.py` 배선
- [x] WS 클라이언트: `btcusdt@aggTrade` 구독 → `trade_window` deque 적재 + 만료 pop — `state/trade_window.py` (만료는 exchange_time 기준, §11.1 단일 스트림 시간창)
- [x] **(v1.1)** WS 클라이언트: `btcusdt@depth@100ms`(diff) 구독 → **벽 레지스트리 이벤트 탭** (PRD §5.1 설계 결정 2). full book 미유지 — 신규 가격은 `wall_tracker.record_min_qty_btc`(100) 이상일 때만 등록, **추적 중인 가격은 잔량 값 불문 모든 이벤트 처리 (v1.2 유령 벽 방지 — PRD §8 D1 소멸 규칙)**: 잔량 0 = tombstone 소멸, 하한 미만 하락 시 D1 소멸 판정 후 활성 제거. D1 발화 게이트는 `size_threshold_btc`(1000) — 2단 임계치 (PRD §8 D1) — `state/wall_registry.py` (소멸은 `WallRemoval` 레코드 반환, FILLED/PULLED 판정 소비는 M2)
- [x] **(v1.1)** `wall_registry` SQLite 영속화 선행 구현 (PRD §12.1 — `walls` 테이블 한정, 전체 영속화는 여전히 M6): 재시작 복원, 청취 공백 시 전체 `unconfirmed` 마킹(`unconfirmed_since` 기록) + 가격별 첫 이벤트로 해제, `first_seen_above_threshold`(스푸핑 타이머 기준)·`first_seen_at`(관측용) 보존 (PRD §12.1 v1.2 정정 반영), `ttl_days` 청소는 **unconfirmed 전용** (`unconfirmed_since` 기준 7일, 확인된 벽은 무기한 — PRD §12.1 v1.2) — `persistence/walls.py` (Decimal TEXT 저장, 가격 키 정규화 문자열)
- [x] `level_tracker` 구현: top-20 크기 + 체결 귀속 `{price, side, current_size, cum_traded_at_level}` (PRD §7 v1.2 축소 — 생애주기 필드는 wall_registry로 일원화) — `state/level_tracker.py` (귀속 집계 규칙의 본격 검증은 M3)
- [x] 재연결: 지수 백오프(1s→60s), 24h 강제 단절·ping/pong은 라이브러리 위임 또는 자체 구현으로 흡수 확인 (PRD §5.3 — §5.2 클라이언트 택일에 따름) — `ingestion/ws_client.py` (raw aiohttp 확정 — 결정 기록 참고. pong은 aiohttp autoping, 안정 연결 60s+ 후 백오프 리셋)
- [x] **(v1.2)** 스트림별 헬스 추적(최종 수신 시각) + 세션 epoch: 하나라도 단절·스테일·diff U/u 갭이면 전 디텍터 판정 보류(상태 적재는 계속), 세 스트림 구독 확인 + 첫 depth 스냅샷 후 새 epoch (PRD §5.4) — `ingestion/health.py` (M1은 상태 산출·통지까지, 판정 보류 소비는 M2+. depth는 매 메시지가 완전 스냅샷이므로 "depth 수신 재개 = 첫 스냅샷" 충족)
- [x] **(v1.2)** diff `U`/`u` 연속성 검사 — 누락 탐지 전용(재구성 금지 유지), 갭 시 청취 공백 취급(레지스트리 전체 unconfirmed) (PRD §5.1) — `DiffListeningGap` 통지를 epoch 종료와 분리 (diff 외 스트림만의 공백은 레지스트리 마킹 없음, §12.1)
- [x] 이벤트 타임스탬프 **(v1.2 결정 — PRD §11.1)**: 전 이벤트에 `local_monotonic_receive_time` 스탬프, 있으면 `exchange_time`(aggTrade `T`, diff `E` — depth20에는 부재, 스파이크 실측) 병기 저장. 크로스 스트림 비교(D4 refill 근접성 등)·지속시간 타이머·staleness = monotonic, 단일 스트림 시간창(D2)·표기 = exchange
- [x] 연결 이벤트(연결/단절/재연결) 구조화 로그 — ws_client(connected/disconnected/reconnect wait) + service(epoch/stale/gap/wall removed)
- [x] 메모리 바운드 확인: `trade_window` 시간 상한 동작 테스트 — `tests/test_state.py` (1000초분 적재 후 창 크기 상한 확인)

**완료 기준 (PRD)**: 24h 무중단 수집, 재연결 자동 복구 확인.
**검증 방법**: 24h 실행 로그에서 (1) depth 이벤트 공백 구간 없음 (2) 강제 단절 시점에 재연결 로그 존재 (3) 메모리 사용량 평탄 — 를 확인하고 결과를 아래 검증 기록에 남긴다.

검증 기록:
- 2026-07-11 25s 스모크 런 (구현 완료 직후): 연결 → epoch 1 시작 → 벽 1건 포착(40000 bid 259 BTC, walls DB 기록) → SIGINT 종료 시 disconnect 마킹(unconfirmed=1) DB 미러 확인. 92 tests passed
- **2026-07-12 검증 런 통과** (프로토콜: 8h+수동 단절 — 결정 기록 참고. 실제 11h 15m, 07-11 23:33 ~ 07-12 10:49):
  - **공백 없음**: 수동 단절 구간 외 staleness/epoch 경고 0건 (depth·diff 30s+, aggTrade 60s+ 공백 전무)
  - **재연결 복구**: 09:45 네트워크 ~6분 차단 → 09:46:03 depth·diff stale(31s)로 epoch 종료 + 벽 22개 unconfirmed 마킹 → 09:46:33 aggTrade stale(61s, 별도 임계 실증) → 09:52:09 복귀 이벤트로 epoch 2 → 09:52:10 서버 CLOSE 도착, disconnect 재마킹, 백오프 1.0s → 09:52:11 재연결·epoch 3, 이후 무결점. 종료 SIGINT 시 disconnect 마킹 정상
  - **메모리 평탄**: RSS 45.9MB → 2h 워밍업 후 48.7~49.8MB 밴드 9시간 플랫 (5분 간격 135샘플)
  - 부수: 벽 레지스트리 22개 수렴(61k bid 1,364 BTC — 스파이크 관측값과 일치). 65000 ask가 record_min(100) 경계에서 3회 등록/소멸 반복 — 하한 플래핑 사례, M6 튜닝 참고
  - **발견 사항**: 단절 6분간 재연결 시도 0회 — 클라이언트가 half-open TCP의 `receive()`에 블록, 복구는 서버 CLOSE 도착 덕분. CLOSE가 영영 안 오는 시나리오(NAT 타임아웃 등)면 무한 대기 = 조용한 실패. 후속 조치는 아래 M1 구현 노트 참고

### M1 구현 노트 (2026-07-11)

- **모듈 배치**: 파이프라인 코디네이터는 `src/order_monitor/service.py`(신규, 최상위) — 디렉터리 구조의 컴포넌트 패키지들은 순수 로직만 갖고, 배선·주기 작업(staleness 1s 점검, unconfirmed TTL 청소 1h)은 service가 소유. `main.py`는 argparse+기동만
- **DB 경로는 CLI 인자** `--db-file`(기본 `order_monitor.db`): PRD §10 config에 영속화 키가 없음(스키마 엄격 검증 유지) → `--log-file`과 같은 관례
- **pyproject 의존성 정리**: ccxt 제거(탈락 확정), `aiohttp>=3.9` 직접 의존성 승격, dev에 `pytest-asyncio` 추가
- **epoch 재시작 타이밍**: 라이브 연결 중 diff U/u 갭은 같은 이벤트 처리 내에서 즉시 새 epoch 시작 가능(세 스트림 모두 healthy + 직전 depth 스냅샷이 유효하므로). M2 디텍터는 EpochEnded에서 누적 리셋만 정확히 하면 됨
- **wall_registry 시각 = wall-clock** (결정 기록 참고): D1(M2)의 `PERSIST_SECONDS` 판정은 이 필드(`first_seen_above_threshold`)와 비교하므로 wall-clock 기준으로 구현할 것
- **(검증 런 발견 → 해결됨 2026-07-12)** half-open TCP 무한 대기: staleness는 epoch만 종료하고 연결을 끊지 않으며, 클라이언트 자체 ping이 없어(`aiohttp` autoping은 서버 ping 응답 전용) 서버 CLOSE가 안 오면 `receive()` 무한 블록이었음 → `ws_connect(heartbeat=20)` 적용(사용자 승인): pong 미수신 시 receive()가 실패해 기존 재연결 루프 작동. M5 워치독의 staleness 감시와 상호 보완(이중 방어)

## M2 — D1 + D2 + Telegram 발송

- [x] D1 APPEARED: `SIZE_THRESHOLD` + `PERSIST_SECONDS` 지속 필터 + 레벨당 1회 발화 + `unconfirmed` 레벨 발화 억제 (PRD §8 D1, §12.1 규칙 2) — `detectors/d1.py` (지속 타이머는 `first_seen_above_threshold` 기준 wall-clock — M1 결정 기록에 예고된 대로. 발화 게이트는 diff 이벤트 + 1s 주기 틱 양쪽)
- [x] D1 REMOVED: `EXIT_RATIO` 하락 시 `FILLED` / `PULLED` 판정 (`FILL_ATTRIBUTION`) — 레지스트리 소멸(tombstone/하한 미만) 경로 포함. 지속 필터 미달 후보는 `D1Suppressed` 로그 전용 이벤트로 기록 (검증 방법 (2)의 근거 데이터)
- [x] D2 볼륨 버스트 **(v1.3 전면 개편 — PRD §8 D2)**: 에피소드형 상대 임계 — `THR = max(vol_floor 30, vol_multiplier 10 × 24h 분당 기준선)`, 총 볼륨 트리거 + 델타비 성격 라벨(방향성/혼합/양방향), 온셋→히스테리시스 잠정 종료→10분 병합→요약의 2단 이벤트 — `detectors/d2.py`, `state/volume_baseline.py`, `ingestion/baseline_bootstrap.py`(REST 워밍업, 실패 비치명). 구판(고정 100 BTC 방향 분리 + BURST_COOLDOWN)은 폐기
- [x] Telegram 발송기: 비동기 큐, 초당 상한, 실패 시 백오프 재시도 (PRD §9.2, §11.1) — `alerting/telegram.py` (초당 1건·재시도 5회 후 드롭은 코드 상수 — §10 config 키 전수 고정. 로그에서 토큰 redact)
- [x] 토큰은 `TELEGRAM_BOT_TOKEN` 환경변수로만 주입 — main에서 필수 검증, 없으면 기동 거부 (알림 없는 조용한 실행 방지)
- [x] dedup **(v1.3 — D1 전용으로 재분리)**: `(detector, side, price_bucket)` 키 + `ALERT_COOLDOWN`. D2는 시간 쿨다운 미적용 — 에피소드+병합이 억제 (PRD §9.2 v1.3. D5는 intent 기반 별도, M4)
- [x] 알림 on/off 설정 반영 (`send_d1` 기본 off, `send_d2`·`send_d2_summary` 기본 on — 온셋/요약 독립)
- [x] 단위 테스트: 지속시간 필터(스푸핑 배제), FILLED/PULLED 분기, dedup/쿨다운, D2 에피소드(온셋/종료/병합/라벨/워밍업 보류/epoch 폐기), 기준선/부트스트랩 — `tests/test_d1.py` `test_d2.py` `test_alerting.py` `test_baseline_bootstrap.py` 등 (누적 150 passed)

**완료 기준 (PRD)**: 실제 알림 수신, 스푸핑 필터 동작 확인.
**검증 방법**: (1) 실계정으로 D2 알림 1건 이상 수신 (2) 로그에서 PERSIST 미달로 발화 억제된 레벨 사례 확인.

검증 기록:
- 2026-07-12 25s 스모크 런 (구현 완료 직후, 더미 토큰): 연결 → epoch 1 시작 → 벽 1건 기록, 에러/경고 0건. 실토큰 D2 알림 수신 + `D1Suppressed` 사례 확인은 **미실시** — 완료 기준 검증 런 필요 (실계정 `TELEGRAM_BOT_TOKEN` + `telegram.chat_id` 설정 후 실행)
- **2026-07-12 D2 v1.3 백테스트 검증**: 실 `D2Detector`에 6/29~7/12 1분봉 18,926개 재생(`scripts/backtest_d2.py`) — 사용자 지정 스파이크/흡수 9개 구간 **전부 포착**, 7.3 에피소드/일 (구판 고정 임계는 ~17회/일). 대표: 7/6 22:30 KST 2,349 BTC `directional_buy`(사용자 "강한 델타" 평가와 라벨 일치), 7/12 09:42 지속형 1,187 BTC 단일 에피소드로 병합
- 2026-07-12 D2 v1.3 30s 라이브 스모크 런 (더미 토큰): REST 부트스트랩 1440봉 → `per_minute_mean` 8.43 BTC (임계 ≈ 84 BTC, 분석치와 일치) → epoch 1 시작, 에러/경고 0건
- **2026-07-12 실토큰 검증 런 (13:23~21:16 KST, 재시작 1회 포함 약 8h — `order_monitor_m2_verify.log`)**:
  - **완료 기준 (1) 충족 — D2 알림 실수신**: 에피소드 3건 전부 온셋+요약이 Telegram 실수신 확인 (스크린샷 + 로그 `telegram alert sent` 7건, 전부 attempt 1 성공, 실패/재시도 0건)
  - **상대 임계 추종 실증**: 기준선이 저녁 유동성 증가를 따라 8.55 → 9.80 → 10.63 BTC/분으로 이동, 온셋 임계도 85.5 → 98.0 → 106.3 BTC로 자동 상향
  - **성격 라벨 3종 실발화**: balanced(0.04) / directional_sell(0.60) / 온셋 directional_buy(0.55)→요약 mixed(0.30) — 에피소드 진행에 따른 라벨 정제 확인
  - **요약 타이밍 정합**: 세 건 모두 잠정 종료 +10분(병합 창) 정각에 발송. 발화 빈도 3건/8h ≈ 9/일로 백테스트(7.3/일)와 정합
  - **부수 확인**: D1 APPEARED 실수신 1건 (61k 벽, `persisted_seconds` 49,885→50,203s로 재시작·unconfirmed를 넘는 스푸핑 타이머 보존 §12.1 규칙 2 실증. 세션 1은 send_d1 off라 로그만 — 정책 게이트 동작 확인). 13:27 단절 시 epoch 종료 + diff gap 마킹 + 정상 종료 경로 확인
  - **완료 기준 (2) — `D1Suppressed` 실전 사례 0건, 합성 검증으로 갈음 (사용자 결정)**: 8h 동안 1000 BTC를 일시 돌파했다 3s 내 이탈한 벽이 없었음 (현재 1k+ 벽은 61k 하나뿐이고 안정적 — 자연 발생은 희소 사건). 억제 로직은 단위 테스트 3건(지속 미달 하회/소멸/스트릭 교체)이 커버하며, 실전 사례는 M5 VPS 상시 운영 중 로그 관찰로 이월. 발생 시 이 기록에 추기

**M2 완료** (2026-07-12).

### M2 구현 노트 (2026-07-12)

- **D1 데드밴드 재발화 안 함 (PRD §8 D1 조건 3 해석)**: 활성(APPEARED 발화) latch는 REMOVED까지 유지 — exit(500)~임계(1000) 데드밴드로 내려갔다 회복해도 재발화하지 않는다. 조건 3의 "임계 밑으로 내려갔다 다시 올라오면 리셋"은 REMOVED 이후의 재출현 재계측으로 해석: 데드밴드 내 재발화는 같은 벽에 중복 인텐트(M4 D5)를 만들고 `EXIT_RATIO` 히스테리시스 설계 의도와 충돌. **사용자 리뷰 대상** — 다르게 읽는다면 detectors/d1.py의 활성 분기만 수정하면 됨
- **epoch 종료 시 D1 리셋**: 활성·후보 전부 폐기, 보류 중 소멸은 REMOVED 무판정 (aggTrade 공백 중 체결 귀속이 얼어 FILLED/PULLED 오판 — §5.4). 새 epoch에서 임계 이상 벽은 다시 APPEARED부터 (타이머는 레지스트리 필드가 보존하므로 재개 직후 빠르게 재발화 — M4에서 INTERRUPTED→재등록 의미와 정합). D2 쿨다운·dedup 쿨다운은 판정 누적이 아닌 스팸 억제기라 epoch를 넘겨 유지
- **`telegram.chat_id`는 str 유지** (M0 노트의 int→str 허용 여부 종결): 스키마 완화 없이 현행 유지 — 그룹 chat_id는 YAML에서 `"-100..."`처럼 따옴표 필수. 로더의 "조용한 강변환 없음" 원칙 유지가 함정 하나보다 우선
- **알림 실패 시 5회 재시도 후 드롭**: D1/D2는 시효성 신호라 무한 재시도 무가치. D5 종국 알림의 유실 방지는 M4 `alerts_outbox` 소관 (PRD §9.4 적용 범위 한정과 일치)

### M2 구현 노트 — D2 v1.3 개편 (2026-07-12 추가)

- **로컬 `config.yaml` 재동기화 필요**: v1.3에서 `thresholds` D2 키가 교체되어 구 config는 `ConfigError`로 기동 거부됨(의도된 동작). 이 세션에서 로컬 config.yaml을 example로 재복사함 — 커스터마이즈가 없었음을 diff로 확인
- **에피소드 요약의 체크포인트 규칙**: 병합 대기(잠정 종료~확정 종료) 중의 저볼륨 체결은 재점화 시에만 에피소드에 편입되고 확정 종료 요약에서는 제외 — "구간"과 누적치의 정합 유지
- **틱의 체결 두절 처리**: trade_window 만료는 새 체결 도착에 의존하므로, 체결이 창 길이(60s) 이상 두절되면 틱이 이를 "창이 식음"과 동치로 보고 잠정 종료를 시작
- **요약 구간 시각은 KST 표기** (dispatcher 상수) — 단일 사용자 전제, 필요 시 설정화는 M6 이후
- **백테스트 재생 주의**: `scripts/backtest_d2.py`는 1분봉 근사 재생이라 창 경계 정렬 아티팩트를 1ms 축소로 보정함 — 실서비스(체결 단위 롤링)와 감도가 약간 다를 수 있음. M2 검증 런에서 실제 발화 빈도 확인 필요

## M3 — 레벨별 체결 집계 + D3 + D4

- [x] `cum_traded_at_level` 집계: aggTrade를 가격·aggressor 방향으로 레벨에 귀속 — M1 구현을 유지하되 **벽 레벨 보존 예외 추가 (PRD §7 v1.4, 사용자 확정)**: 벽 레지스트리 등록 가격은 top-20 창 이탈에도 엔트리(생애 누적) 보존, 벽 소멸 시 자연 제거 — `state/level_tracker.py` `retain` predicate 주입 (결정 기록 참고)
- [x] 접촉 episode 트래커 (D3/D4 공용 판정 단위, PRD §8 D3 v1.2) — `detectors/contact.py`: best 도달~반등/관통/소멸, 관통은 체결가 주 신호(즉시) + best 지속 보조 신호(플리커 복귀 시 카운터 리셋). D5 케이스 2가 비-벽 레벨 리필을 합산하므로(M4) episode는 접촉된 모든 레벨에 열고 D3가 D1 활성 벽만 걸러 소비
- [x] D3 흡수: 접촉 episode 단위 판정 — 가격 도달 + `ABSORPTION_MIN` + 비관통, 실현률 산출 — `detectors/d3.py`. **발화는 episode 종료 시 확정 (PRD §8 D3 v1.4, 사용자 확정 — 결정 기록)**, 등록 크기 S는 `D1Appeared.qty`를 service가 중개
- [x] D4 아이스버그 (v1.2 경로 누적): episode 내 체결 근접(`REFILL_WINDOW_MS`) 양의 델타만 `refill_added`로 인정, `ICEBERG_MARGIN` + 체결→회복 쌍 ≥ `ICEBERG_MIN_TRADES`(aggTrade 메시지 수 기준) — `detectors/d4.py` (체결 버퍼는 episode와 독립 — 순서 역전 대응, 전역 시간 프루닝으로 메모리 바운드. episode당 1회 래치)
- [x] D3/D4는 알림 발송 없이 로그만 기록 (PRD §9.1 — dispatcher 미대상으로 자연 성립. **DB events 테이블은 M6 소관 유지, 사용자 확정** — 구조화 로그가 튜닝 데이터로 이미 조회 가능)
- [x] 단위 테스트: 관통 3분기(체결가/best 지속/플리커 비관통) + D4 경로 케이스(체결 직후 회복 = 인정 / 500ms 밖 무관 추가 = 배제) + 생애 누적 다회 episode — `tests/test_contact.py` `test_d3.py` `test_d4.py` `test_state.py` (누적 210 passed)
- [x] **(v1.2)** 결정적 replay 테스트 픽스처: 합성 + 실캡처 시퀀스 재생 — 재연결·스트림 순서 역전·diff U/u 갭 3종 필수 커버 (PRD §13) — `tests/replay/`(러너 + JSONL 픽스처), `tests/test_replay.py`. **실물 MonitorService 구동** (service에 clock/monotonic 주입 추가 — 하니스가 배선을 미러링하면 배선이 검증 밖에 남기 때문). 실캡처는 `scripts/capture_stream.py`로 60s 기록(1,476 프레임), 골든 고정 + 이중 재생 결정성 단언
- [x] ~~Bookmap 육안 대조 절차 확정 (PRD 오픈 퀘스천 #5)~~ → **기각 (사용자 확정 2026-07-13)**: Bookmap 대조 자체를 M3에서 제외 — 결정 기록·PRD v1.4 참고

**완료 기준 (PRD v1.4)**: 결정적 replay 테스트 통과 (Bookmap 육안 대조는 기각).

검증 기록:
- **2026-07-13 replay 테스트 통과 (완료 기준 충족)**: 필수 3 시나리오 — ① 재연결: 단절 시 진행 episode 무판정 폐기 + 재개 후 생애 누적(300+200=500, 33%)으로 D3 확정, D1 재발화(타이머 보존 §12.1 규칙 2) ② 순서 역전: 접촉 스냅샷보다 선착한 체결도 리필 쌍 성립(쌍 5/5 충족으로 입증) ③ diff U/u 갭: 전 벽 unconfirmed + 진행 판정 폐기 + 같은 이벤트 내 epoch 재개. 각각 이중 재생 결정성 단언 포함, 실캡처 60s 재생 골든(벽 7개 수렴, 실존 1k+ 벽 D1 APPEARED 1건) 일치. 전체 219 tests passed
- 2026-07-13 30s 스모크 런 (더미 토큰): config → 레지스트리 복원 → 기준선 부트스트랩(1440봉, 10.25 BTC/분) → 연결 → epoch 1 시작, 에러/경고 0건
- 실전 D3/D4 발화 관찰은 상시 운영 로그로 이월 (대형 벽 접촉·아이스버그는 희소 사건 — M2의 `D1Suppressed` 이월과 동일 패턴). 발생 시 이 기록에 추기

### M3 구현 노트 (2026-07-13)

- **배선 순서가 판정 정합성을 가름 (service.py docstring에 명시)**: 벽 소멸 diff 처리 시 episode REMOVED 종료 → D3 확정 판정 → D1 REMOVED 판정 → D1 이벤트의 D3 등록 해제 라우팅 순. D3 판정 시점에 D1 등록이 살아있어야 소멸(FILLED성 소진)로 끝난 흡수가 잡힌다
- **D4 체결 버퍼는 epoch 게이트 밖** (상태 적재 성격, 500ms 바운드) — epoch 종료 시 D4 누적(`_acc`)만 리셋. LevelTracker의 생애 누적도 상태 계층이라 epoch를 넘겨 유지 (D1 귀속과 동일 — PRD §5.4 "상태 적재는 계속")
- **판정 시간축은 이벤트 타임스탬프**: D3/D4의 크로스 스트림 근접·episode 지속시간은 `clock()` 호출이 아닌 `local_monotonic_receive_time` 연산 → replay 결정성이 픽스처 타임스탬프만으로 성립. 클록 주입이 필요한 것은 D1(wall-clock)·epoch 추적기뿐이고 MonitorService 생성자 파라미터로 노출 (운영 기본값 실물 시계)
- **실캡처 픽스처 재캡처 시**: `scripts/capture_stream.py --duration 60 --out tests/replay/fixtures/<name>.jsonl` 후 `tests/test_replay.py::TestLiveCapture` 골든(벽 수·이벤트 시퀀스)을 재생성할 것

## M4 — D5 상태기계 + 확정 알림

- [x] `INTENT_REGISTERED` 등록 (D1 APPEARED 입력, S = 발화 시점 `last_qty` 고정 — 이후 크기 변화 미반영, PRD §8 D5 v1.2) — `detectors/d5.py` `on_d1_appeared`
- [x] 전이 1: 가격 도달 + 체결 누적 ≥ S×`REALIZE_PCT` → `EXECUTION_CONFIRMED` (케이스 1 — "해당 레벨 체결" 확인이지 원 표시 주문의 체결 확인 아님, PRD §8 D5 판정 의미). **D3와 달리 관통 배제 없음** — 관통 여부와 무관하게 그만큼 체결됐다는 사실 자체가 판정 대상(결정 기록 참고)
- [x] 전이 2: PULLED → 케이스 2 누적을 **먼저** 평가(충족 시 `EXECUTION_INFERRED_ABOVE` 우선), 미충족 시 `INTENT_WITHDRAWN`; FILLED 귀속 + 실현률 미달은 `PARTIALLY_EXECUTED` (로그만 — 전 종국 레코드에 실현률 필드, PRD §8 D5 v1.2) — `on_d1_removed`
- [x] 전이 3: 상위 구간 D4 누적 ≥ S×`REALIZE_PCT_ABOVE` → `EXECUTION_INFERRED_ABOVE` (케이스 2, 리필 확인분만 합산 — PRD §8 D5 집계 방식). 상위 구간 = 의도 레벨과 같은 side best(mid 아님, 결정 기록 참고) 사이 — D4에 lifetime(episode 비종속) 리필 누적 신설(`sum_lifetime_refill_above`) → **(주) PRD v1.6에서 D4와 함께 임시 비활성 (2026-07-15 결정 기록)** — 코드·테스트는 보존, 배선만 제외
- [x] 레벨 소멸(REMOVED/tombstone/하한 미달) 시 D5 즉시 종국 평가 — TTL 대기 없음, 소멸 원인 4분류 + 동시 발생 우선순위(CONFIRMED > INFERRED_ABOVE > PARTIALLY_EXECUTED > WITHDRAWN, epoch 종료는 무조건 INTERRUPTED) (PRD §8 D5 v1.2)
- [x] ~~`INTENT_TTL` 만료 → `INTENT_EXPIRED` (로그만)~~ → **폐지 (PRD v1.5, 2026-07-14)**: 인텐트 수명 = 벽 수명 — 상시 벽 사각지대 누락 시나리오로 배포 직후 사용자 지적, 결정 기록 참고
- [x] **(v1.2)** epoch 종료 시 활성 intent `INTERRUPTED` 마킹 + D3/D4 누적 리셋 (PRD §5.4, §12) — `d5.reset()`이 이벤트를 반환(D1~D4의 void reset()과 다름). **배선 순서 위험**: `d5.reset()`은 반드시 `d4.reset()`보다 먼저 — INTERRUPTED의 `above_realized_rate`가 D4 lifetime 리필에 의존(service.py 모듈 docstring에 명시)
- [x] `intents` 개수/시간 상한 (메모리 바운드) — 활성 인텐트 수 상한(`MAX_ACTIVE_INTENTS=1000`, 코드 상수) + 벽 소멸 시 종국 (v1.5 — 구 TTL 시간 상한은 폐지, 벽 희소성으로 충분)
- [x] 확정 알림 포맷: 실현률 %, 등록→확정 소요시간 포함 (PRD §9.3), 케이스 2는 "추정" 명시 — PRD §9.3 템플릿 그대로 + 발생 시각(KST) 한 줄 추가(§9.4 "지연 발송임을 수신자가 알 수 있게")
- [x] **(v1.2)** D5 알림 dedup: 종국 `(intent_id, terminal_state)` / 진행률 `(intent_id, 계열, 경계)` — 시간 쿨다운 미적용 (PRD §9.2) — 순수 멱등 셋(`AlertDispatcher._d5_sent`), 기존 쿨다운 기반 `AlertDeduper`와 별개
- [x] **(v1.2)** `alerts_outbox`: D5 종국 알림 선기록→발송→sent 마킹, 재시작 시 미발송 재전송 (멱등 키 중복 차단, 원 이벤트 시각 표기 — PRD §9.4) — `persistence/alerts_outbox.py`, `TelegramSender.enqueue(on_sent=...)` 콜백으로 발송 확인. 멱등 키는 intent_id가 아니라 `(side,price,terminal_state,recorded_at)`(서비스 wall-clock) — 근거는 결정 기록
- [x] 단위 테스트: 상태 전이 전 경로 (확정 1/2, 부분 체결, 철회, 만료, INTERRUPTED) + 동시 발생 우선순위 케이스 — `tests/test_d5.py` (21 tests)
- [x] **(v1.2)** D5 replay 테스트: M3 픽스처 확장 — 재연결·순서 역전·누락 시나리오에서 상태기계 결정성 검증 (PRD §13 — M4 완료 기준) — 재연결·diff갭 픽스처에 INTERRUPTED 결정성 단언 추가(`tests/test_replay.py`). 순서 역전 픽스처는 D1 비적격 벽(500<1000)이라 D5 관여 없음 — 케이스1/2/TTL은 `test_service.py` 파이프라인 테스트로 커버(신규 replay 픽스처 불필요 판단, 사용자 확인 없이 개발자 판단)
- [x] **(코어 전이 테스트 통과 후)** D5 진행률 알림: `progress_step_pct`(0.2) 경계당 1회, 계열(케이스 1/2)별 독립 커서, dedup 키에 경계값 포함해 쿨다운 우회 (PRD §8 D5 진행률 알림, §9.2 예외) — `evaluate()`의 `_progress_events`, 종국 임계 미만 경계만(0.6 기본값 기준 실효 0.2/0.4)

**완료 기준 (PRD)**: 케이스 1/2 알림 각 1건 이상 실전 확인.
**검증 방법**: 실전 수신한 알림의 근거 이벤트를 로그에서 역추적해 타당성 확인.

검증 기록:
- **2026-07-14 자동화 완료** (이번 세션 완료 기준 — 사용자 확정, 실전 알림 확인은 아래 참고): 단위 테스트(`test_d5.py` 21, `test_d4.py` lifetime 추가 6, `test_alerts_outbox.py` 7, `test_alerting.py` D5 배선 12) + service 파이프라인 테스트(케이스1 확정+outbox 기록, 케이스2 추정, TTL 만료, epoch 종료 INTERRUPTED, 재시작 재전송 — `test_service.py`) + replay 결정성(재연결·diff갭 시나리오 INTERRUPTED, 실캡처 60s 인텐트 등록) 전부 통과. 전체 271 tests passed. 30s 스모크 런(더미 토큰): epoch 시작 → 실제 D1Appeared(1361.66 BTC 벽) 발화 → Telegram 404(더미 토큰) 5회 재시도 후 정상 드롭 로그, 크래시 없음
- **실전 케이스1/2 알림 확인 — 미실시, 후속 필요**: PRD 완료 기준의 "실전 확인"은 1000+ BTC 벽이 실제로 60%+ 체결/상위 리필되는 예측 불가능한 시장 이벤트를 요구(M3 60s 실캡처에서도 벽 1개만 관측되는 수준의 희소 사건). M2도 동일 사유로 구현 완료 후 별도 세션(8h 검증 런)에서 진행한 전례를 따라 이번 세션 범위에서 제외(사용자 확정 2026-07-14). 실토큰 상시 운영 중 로그 관찰로 확인되는 대로 이 기록에 추기
- **2026-07-14 케이스 2 실전 알림 1건 확인** (03:43:27 KST, 61k bid — VPS 로그·DB 역추적으로 타당성 검증): 01:20:10 KST TTL 폐지 배포 재시작(직전 01:20:01의 구 코드 마지막 `intent_expired`가 TTL 사각지대의 실물 증거) → 01:21:06 D1 재APPEARED·인텐트 등록(S=1366.01013) → 01:45/03:24 진행률 20%/40%(경계값 정확 일치) → 03:43:27 `execution_inferred_above`(추정 실현률 62.6%, 등록→추정 8,541s = 142m 21s). outbox 왕복(선기록 → sent=1) 확인, Telegram 메시지 필드 전수 로그와 일치. 정황 근거: 같은 시간대 D2가 매도 우위 버스트 반복 판정("매도 흡수(정체)"/"양방향 충돌"), 기준선 대비 리필 확인분 855 BTC는 노이즈 수준 초과. **반사실: 구 TTL 코드였으면 01:51 만료로 이 알림은 발화 불가** — TTL 폐지(PRD v1.5)의 실전 검증을 겸함. 단, 검토 과정에서 케이스 2 구조 한계가 확인되어 07-15 D4·케이스 2 임시 비활성(결정 기록 참고). **케이스 1 실전 확인은 계속 미실시** — 완료 기준 잔여분 (→ 2026-07-20 확인, 아래)
- **2026-07-20 케이스 1 실전 알림 1건 확인 → M4 완료 기준 전부 충족** (17:34:48 KST, 64,242.01 bid — 2026-07-22 전량 추출본(`scripts/fetch_vps_full_extract.sh`) 로그·outbox 역추적으로 타당성 검증): 16:53:49 KST D1 APPEARED·인텐트 등록(S=1,118.29591) → 진행률 20%(17:23:40)/40%(17:34:28, `progress_step_pct` 경계값 정확 일치) → 17:34:48 D1 REMOVED `attribution=filled`(peak 1,215.17 → 잔량 368.34) 동시 `execution_confirmed` — 레벨 실측 체결 678.1 BTC, 실현률 60.6%(임계 60%), 등록→확정 2,458.8s = 40m 58s. outbox 왕복(id 3, 선기록 → sent=1) 확인, Telegram 메시지 필드 전수 로그와 일치. 케이스 2(07-14)에 이어 케이스 1까지 실전 확인 — **M4 완료**. 부기: 이 벽은 07-12부터 추적된 1,000+ BTC 이동 벽(61k→60.5k→61.5k→61k→64080→64242→63k→61k→62k)의 유일한 FILLED 종국 — 나머지 8회 이동은 전부 PULLED로, 스푸핑성 이동과 실체결이 attribution으로 구분됨을 실증

### M4 구현 노트 (2026-07-14)

- **D5는 D3/접촉 episode에 의존하지 않음**: 케이스1은 D1/D3가 이미 쓰는 `cum_traded_lookup` 콜백만 사용 — 그 가격에 체결이 있었다는 사실 자체가 "가격 도달"의 증거이므로 별도 판정 불필요. D3의 "관통 없이 버텼는가"와 D5 케이스1의 "실제로 그만큼 체결됐는가"는 별개 질문 — 관통 후에도 케이스1은 발화할 수 있다(의도적).
- **D4 lifetime 리필과 기존 episode-scoped `_acc`는 같은 인정 리필 델타를 이중 기록**하지만 리셋 시점이 다르다(전자는 epoch 종료만, 후자는 episode 종료도) — 데이터 중복이 아니라 서로 다른 소비자(D4 자신의 발화 판정 vs D5 케이스2 lifetime 합산)를 위한 병행 관측.
- **"상위 구간" 상한은 같은 side의 best 가격**(BUY 인텐트=best_bid) — mid 아님. bid/ask 레벨은 정의상 각자의 best를 넘어 존재할 수 없어 mid를 쓰든 same-side best를 쓰든 그 사이엔 데이터가 없어 결과가 같다(위험 없는 단순화).
- **`D5Detector.reset()`은 이벤트를 반환** — D1~D4의 `reset() -> None`과 다른 유일한 시그니처. epoch 종료 시 INTERRUPTED 자체가 유효한 종국 레코드이기 때문. 호출자(service)는 `for event in d5.reset(): self._emit(event)` 형태로 소비해야 한다.
- **service의 `EpochEnded` 처리 순서가 정합성을 가름**: `d5.reset()`을 `d4.reset()`보다 반드시 먼저 호출 — INTERRUPTED의 `above_realized_rate`가 D4 lifetime 리필 조회에 의존하는데 `d4.reset()`이 먼저 돌면 그 데이터가 이미 지워진다(M3의 D3 vs D1 REMOVED 순서 교훈과 같은 종류).
- **outbox 멱등 키는 `(side, price, terminal_state, recorded_at)`** — `intent_id`(D5Detector 내부 프로세스 카운터)를 쓰지 않은 이유: 재시작마다 0부터 다시 시작해 다른 인텐트가 우연히 같은 값을 얻으면 `INSERT OR IGNORE`가 진짜 알림을 조용히 삼킬 수 있음. `recorded_at`은 서비스가 outbox 기록 시점에 wall-clock으로 찍는다(D5Detector 자체는 monotonic만 다룸 — TTL/소요시간 계산용).
- **`AlertDispatcher`에 `set_outbox()` setter가 있는 이유**: outbox는 `db_path` 확정 후 `startup()`에서 열리는데(`WallStore`와 같은 지연 오픈 패턴), dispatcher는 생성자에서 이미 만들어지므로 사후 주입이 필요하다.
- **`_cum_traded_at_level`/`_refill_above_lookup` 파이프라인 테스트 함정**: 실제 top-20 depth 스냅샷으로 해당 가격이 `LevelTracker`에 먼저 진입해야 그 뒤의 체결이 `cum_traded_at_level`에 반영된다(레벨 미존재 시 조용히 0) — D3 파이프라인 테스트(M3)와 동일한 패턴, D5 케이스1 파이프라인 테스트에서도 재확인.

## M5 — Watchdog + systemd + 배포

- [x] 인프로세스 워치독 **(v1.2 스트림별)**: depth·diff는 `stale_seconds`, aggTrade는 `trade_stale_seconds` 초과 시 스트림명 명시한 `FEED_STALE` 알림 + epoch 종료 (PRD §11.1, §5.4) — **2026-07-12 D1&D2 우선 배포 준비로 선행 구현** (staleness 감지+epoch 종료는 M1 기존, Telegram 배선 추가). on/off 없이 상시 발송, 재연결 플랩 억제는 스트림별 쿨다운(`cooldown_seconds`) 재사용
- [x] 하트비트 파일 기록 + 외부 경량 워치독(systemd timer)이 행(hang) 상태 감지 → `PROCESS_DOWN` 알림 **+ 자동 재시작** (PRD §11.1 v1.7 — 2026-07-15): service가 `heartbeat_interval`(10s) 주기로 파일 touch(이벤트 루프 생존 신호, 경로는 `--heartbeat-file` CLI 인자 — `--db-file` 관례), `deploy/watchdog_check.py`(시스템 python3 stdlib 전용)를 timer 60s 주기 구동 — mtime 나이 > 60s(상수)면 정지 알림 1회 + `systemctl restart` + 해소 통지 1회, `is-active` inactive(의도적 정지) 스킵 — `watchdog/heartbeat.py`, `deploy/order-monitor-watchdog.{service,timer}`, 테스트 18건 (누적 289 passed)
- [x] systemd 유닛: `Restart=always, RestartSec=5` — `deploy/order-monitor.service` 작성 (2026-07-12, Docker 기각 — 결정 기록). VPS 실적용·검증은 배포 시
- [x] 로그 로테이션 설정 — `RotatingFileHandler`(M0 기존) + journald 용량 제한 절차를 RUNBOOK §5에 문서화 (2026-07-12)
- [x] VPS 준비: 리전 선정 + Binance WS/REST 접속 검증 (PRD §11.3, 절차는 RUNBOOK §0) — **Hostinger KVM 말레이시아(쿠알라룸푸르) 리전**, 2026-07-12 D1&D2 우선 배포 시 검증·적용 완료 (이후 M3/M4 실운영이 접속 유효성을 지속 실증 — 07-14 케이스 2 실전 알림 등. 사용자 확인 2026-07-15로 기록 정리)
- [x] 배포 절차 문서화 (README 또는 runbook) — `deploy/RUNBOOK.md` (2026-07-12: 지오블록 검증, 설치, env 시크릿, systemd, 운영 절차, 7일 검증 기준)

**완료 기준 (PRD)**: VPS에서 7일 무인 운영, 조용한 실패 0건.
**검증 방법**: 7일 후 로그 감사 — 모든 단절/재시작 이벤트에 대응하는 Telegram 통지가 존재하는지 대조 (절차·행 훈련 포함 RUNBOOK §7).

검증 기록:
- 2026-07-15 구현분 자동화 검증: 하트비트(기록·mtime 전진·배선·기록 실패 생존) 4건 + watchdog_check(stale 3분기·전이 1회성·발송 실패 격리·의도적 정지 스킵·파싱) 14건, 전체 289 passed. `watchdog_check.py`는 시스템 python3 파싱 확인
- VPS 배포 + 행 훈련(`kill -STOP` → PROCESS_DOWN → 자동 재시작 → 해소 통지) + 7일 무인 운영 검증 — ~~미실시, 배포 세션에서 진행~~ → 워치독 배포 2026-07-15, 7일 감사 2026-07-22 (아래). **행 훈련만 계속 잔여**
- **2026-07-22 7일 무인 운영 감사 통과 — 조용한 실패 0건** (감사 대상: 2026-07-12 14:26 UTC 최초 기동 ~ 07-22 03:30 UTC 전량 추출, 약 9.5일. v1.7 코드+워치독 타이머 완비 기준으로는 07-15부터 약 6.9일. RUNBOOK §7 기준 대조):
  - **단절/재시작 전수 대조**: WS 단절 3회(07-17 05:19, 07-19 05:25, 07-21 05:38 UTC) 전부 disconnect → epoch 종료(INTERRUPTED) → 2s 내 재연결·epoch 재시작. staleness 1회(07-12 18:11, depth·diff 동시)도 FEED_STALE 경로로 자동 복구. 크래시성 재시작 0건 — `config loaded` 8회는 전부 의도적 갱신(systemd 로그 clean stop 확인). 대응 통지 누락 없음
  - **하트비트/워치독 오탐 0건**: 타이머 가동(07-15) 후 점검 8,036회 전원 `stale=False` — PROCESS_DOWN 발화 0, 오탐 0 (단, 발화 경로 자체는 행 훈련 미실시로 실전 미검증)
  - **알림 전달 유실 0건**: 기간 총 433건 발송. 07-19 00:36 UTC Telegram TimeoutError 3연속 후 attempt=4 성공 — 재시도 경로 실전 실증(재시도 소진 드롭 0건)
  - **관찰 (전제 보정)**: RUNBOOK §7의 "Binance 24h 강제 단절 매일 발생" 전제와 달리 실측 단절 주기는 약 48h — 검증 항목으로서의 재연결 실증에는 영향 없음
  - **발견 이슈 (후속)**: 재시작·재연결마다 서 있는 벽에 D1 APPEARED 재발화 — 기간 중 APPEARED 19건 vs REMOVED 8건, 차이 11건이 재시작 8회+재연결 3회와 일치 (send_d1 활성화 후 중복 알림으로 표면화. 해소 방향은 결정 기록 참고)
  - D1Suppressed 실전 사례(M2 이월 관찰 항목): 기간 중 0건 — 계속 이월

### M5 구현 노트 (2026-07-15)

- **하트비트는 파이프라인 헬스로 게이트하지 않는다** — 이벤트 루프 생존 신호일 뿐, 피드 정지는 인프로세스 FEED_STALE 소관 (관심사 분리). 기록 실패(OSError)도 파이프라인을 죽이지 않음 — stale해진 하트비트를 외부 워치독이 PROCESS_DOWN으로 승격하는 것이 설계된 에스컬레이션 경로
- **워치독 설치 순서가 오탐을 가름**: 하트비트를 쓰는 서비스 유닛을 먼저 갱신·재시작한 뒤 타이머를 켠다 — 역순이면 하트비트 파일 부재로 즉시 PROCESS_DOWN 오탐 (RUNBOOK §4에 명시)
- **전이 처리 중 스크립트 사망 시 재시도 의미론**: 상태 파일 기록이 알림·재시작 뒤라, 도중 죽으면 다음 주기에 전체 재시도 — 알림 유실보다 중복을 택함
- **chat_id는 config.yaml에서 정규식 추출** (env 이중화 기각): 단일 진실원 유지 — chat 변경 시 한 곳만 수정. 토큰은 기존 `/etc/order-monitor/env` 파싱

## M6 — SQLite 영속화 + 임계치 튜닝 루프

- [ ] SQLite 스키마: `events`, `intents`, `trades_sample`(선택) (PRD §12 — `walls`는 M1, `alerts_outbox`는 M4 선행)
- [ ] 모든 디텍터 이벤트/상태 전이 기록
- [ ] 재시작 시 진행 중 intent를 DB에 `INTERRUPTED` 마킹
- [ ] 1주 벽 레지스트리 기록(`record_min_qty_btc` 이상) 분포 분석 → `SIZE_THRESHOLD`·`record_min_qty_btc` 재검토 (OQ #1 확정값 1000의 실데이터 검증 — v1.1 이후 D1 소스는 top-20이 아닌 벽 레지스트리)
- [ ] 나머지 임계치 1차 튜닝 (오탐/침묵 리뷰)
- [ ] **(2026-07-22 백로그, PRD v1.11)** D4 재구현 — 레벨 흡수 방어 디텍터 (§8 D4 v1.11이 기준): `detectors/d4.py` 전면 교체(스트릭 생애 누적, 가시 리필 페어링 + 틱 단위 은닉 리필 대조, 배수 판정 + 래치/진행/종결), service 배선 복원, config 키 교체(`absorb_multiple`/`absorb_progress_step`/`absorb_min_events` 신설, `iceberg_margin_btc`·`iceberg_min_trades`·`realize_pct_above` 삭제 — **배포 시 VPS config.yaml도 동시 수정**, 엄격 스키마), `send_d4` 추가, d5의 `refill_above_lookup` 파라미터·`EXECUTION_INFERRED_ABOVE` 잔재 제거, 구 D4 단위 테스트 교체 + replay 골든 재생성. **관측 기록 포함**(§8 D4 v1.11): 흡수 인정 이벤트 단위 로그 + 스트릭 종료 요약 로그(발화 무관, absorbed > 0 전건) — 임계 미달 near-miss 분포가 `absorb_multiple` 튜닝의 직접 근거
- [ ] **(2026-07-22 백로그)** 추적 벽(관측 하한 100 이상) 수량 궤적 로깅 — 레지스트리 등록·소멸 시 구조화 로그 1줄씩. 현재는 소멸만 로그라 준임계 벽의 생애가 무기록 (`events` 테이블의 선행 단계, 근거: 결정 기록 2026-07-22 — 07-17 62.8k 사례). **범위 조정(v1.11)**: 리필 델타 기록은 D4 재구현의 관측 기록(흡수 인정 이벤트 단위 로그, 위 항목)이 포섭 — 이 항목은 등록/소멸 궤적만 담당
- [ ] **(2026-07-22 백로그)** D2 요약(verdict)에 근접 벽 컨텍스트 첨부 — 종가 부근 추적 벽의 peak/last(궤적 로깅 도입 시 리필량 포함)를 요약 메시지에 동봉해 `sell_absorbed_stall` 류 흡수 판정의 해석 근거 제공 (근거: 동일 사례 — 판정 자체는 적중했으나 알림만으로 배경 파악 불가)
- [ ] **(v1.2)** 오탐 지표 달성 확인: D5 오탐 주 1건 이하(목표 0), D1/D2 시간당 평균 3건 이하 — 초기 제안값, 실데이터로 재조정 후 확정 (PRD §13)
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
| 2026-07-11 | **시계 정책 확정 (PRD §11.1 v1.2)**: 크로스 스트림 시간 비교·지속시간 타이머·staleness는 `local_monotonic_receive_time`, `exchange_time`(aggTrade `T`/diff `E`)은 단일 스트림 시간창과 표기 전용. "판정에 거래소 시각만" 원칙은 폐기 | spot partial depth에 거래소 시각 부재(M1 스파이크 실측)로 원칙 자체가 성립 불가 + D4 refill 근접성 등 크로스 스트림 비교는 aggTrade↔depth라 단일 시계 필수. monotonic은 세 스트림 공통 존재 + 시스템 시계 점프 면역 |
| 2026-07-11 | **WS 클라이언트 = raw aiohttp 확정** (결정 기록 2행 위의 (a)/(b) 택일 종결, 사용자 승인) | asyncio 네이티브 + 이미 의존성에 존재. 구독 스트림 3개 고정(combined URL 1연결 = 구독 완료, SUBSCRIBE 프레임 불필요)이고 지수 백오프·staleness는 어차피 자체 구현 요구사항이라 binance-connector의 콜백→asyncio 브릿지 비용을 정당화할 이점이 없음. pyproject에서 ccxt 제거, aiohttp 직접 의존성으로 승격 |
| 2026-07-11 | **wall_registry 시각 필드는 wall-clock(epoch 초)** — §11.1 "지속시간 타이머 = monotonic" 원칙의 의도된 예외 | `first_seen_above_threshold`(D1 스푸핑 타이머 기준)가 §12.1 규칙 2에 의해 SQLite로 재시작을 넘어 보존되어야 하는데 monotonic은 부팅 기준이라 재시작 간 비교 불가 → 양립 불가능한 두 요구 중 §12.1이 우선. 시스템 시계 점프 시 3s 지속 필터가 왜곡될 수 있는 트레이드오프 수용 (NTP 운영 전제) |
| 2026-07-11 | SQLite 경로는 config가 아닌 CLI `--db-file` 인자 | PRD §10(v1.2 검수 4회차)이 config 키 전수를 명세 역추적으로 고정했고 영속화 경로 키는 없음. 로더의 엄격 스키마(미지 키 거부)를 건드리지 않고 `--log-file`과 동일 관례로 처리 |
| 2026-07-11 | **M1 검증 런 = 8h + 수동 단절 1회** (PRD §13 완료 기준 "24h"에서 단축, 사용자 승인) | 24h라는 숫자의 고유 목적은 Binance 24h 강제 단절→자동 재연결 실증 하나뿐 — 메모리 평탄·수집 안정성은 8h로 충분(trade_window는 60s 창, 레지스트리 증가 완만). 재연결 경로는 런 중간 네트워크 2~3분 차단으로 대체 실증(30s 미만 차단은 TCP가 살아남아 단절 경로를 안 밟을 수 있음), 24h 강제 단절 자체는 M5 7일 무인 운영에서 매일 자연 발생하므로 그때 커버 |
| 2026-07-12 | **알림 메시지 한국어 고정** (오픈 퀘스천 #4 종결) — 템플릿화 없이 `alerting/dispatcher.py`에 포맷 함수로 직접 구현 | 단일 사용자·한국어 수신 전제에서 템플릿 계층은 요구 없는 유연성(추측성 추상화). PRD §9.3 예시도 한국어. 다국어 필요가 실제로 생기면 그때 포맷 함수만 교체하면 됨 |
| 2026-07-12 | **D1 활성 latch는 REMOVED까지 유지** — exit~임계 데드밴드 회복 시 재발화 안 함 (PRD §8 D1 조건 3의 "리셋"을 REMOVED 이후로 해석) | 데드밴드 내 재발화는 같은 벽에 중복 인텐트를 만들고 EXIT_RATIO 히스테리시스 의도와 충돌. M2 구현 노트 참고 — 사용자 리뷰 대상 |
| 2026-07-12 | **D2 v1.3 전면 개편 — 에피소드형 상대 임계** (PRD v1.3, 사용자 승인 플랜): 24h 이동 기준선 × k=10 + 하한 30 BTC, 총 볼륨 트리거 + 델타비 라벨(0.5/0.2), 온셋/요약 2단, 병합 10분, D2 시간 쿨다운 폐지, REST 워밍업 부트스트랩 | 사용자 요구 3건(고정 임계의 유동성 비적응 / 15m 기준의 최대 15분 지연 / 델타·흡수 스파이크 미구분). 파라미터는 백테스트로 확정: 6/29~7/12 1분봉에서 사용자 지정 9개 구간(7/12 09:45 지속형, 7/9 21:15 스파이크, 7/6 델타 2건, 7/1~7/2 흡수+델타 연쇄, 7/1 10:00·6/30 22:30 흡수) 전부 포착 + 7.3 에피소드/일 (k=8: 11/일, k=12: 6/일 — 일부 에피소드 축소). 업계 방식(RVOL 상대 볼륨, z-score/EWMA 적응 임계, CVD 델타 분류) 조사 반영. 시간대별 기준선(RVOL-TOD, 일중 계절성 실측 2.8x)은 스파이크 배수(9~28x) 대비 작아 v1에서 보류 — M6 재검토. 백테스트 도구는 `scripts/backtest_d2.py`로 보존 |
| 2026-07-13 | **호가벽 리포트 개정** (실운영 D+1 피드백, 사용자 요청 4건): ① 발송을 기동 기준 간격 → 벽시계 정시 경계(epoch 초의 interval 배수, 60분 = 매시 정각)로 변경 ② 용어 "물량벽" → 트레이더 통용어 "호가벽"(섹션은 매도벽(저항)/매수벽(지지)) ③ 범례 푸터(`🧱 ≥1,000 BTC · ? = 미확인`) 제거 ④ 헤더의 "추적 N개" 제거. 푸터 제거에 따라 unconfirmed `?` 접미도 제거(범례 없는 기호는 소음) — 반대편 unconfirmed 잔재 표시 제외 필터는 유지 | 2026-07-12 결정의 표시 규칙 일부를 대체. 정시 정렬은 차트 정각 캔들과 대조하기 위함 (KST가 정수 시간 오프셋이라 UTC epoch 배수 = KST 정각) |
| 2026-07-14 | **D5 케이스1은 D3와 달리 관통 배제 조건 없음** (PRD §8 D5, M4 착수 시 확정) — `cum_traded_at_level`만으로 판정, ContactEpisodeTracker 미사용 | 케이스1의 질문은 "실제로 그만큼 체결됐는가"이지 "관통 없이 버텼는가"(D3)가 아님. 그 가격에 체결이 있었다는 사실 자체가 가격 도달의 증거이므로 접촉 판정이 불필요. PRD 원문에도 케이스1엔 관통 배제 조건이 없음 |
| 2026-07-14 | **D4에 lifetime(episode 비종속) 리필 누적 병행 추가** — `sum_lifetime_refill_above(side, intent_price, current_price)` | D5 케이스2는 인텐트 등록부터 최대 30분(TTL)에 걸쳐 여러 접촉 episode·여러 레벨을 넘나드는 누적이 필요한데, 기존 `_acc`는 episode 종료마다 리셋돼 범위가 안 맞음. episode 종료 신호는 그대로 유지(D4 자신의 발화 판정용), lifetime은 epoch 종료에만 리셋 |
| 2026-07-14 | **"상위 구간"의 현재가 = 같은 side의 best 가격**(BUY 인텐트=best_bid, SELL 인텐트=best_ask) — mid 아님 | bid 레벨은 정의상 best_bid 위에, ask 레벨은 best_ask 아래에 존재할 수 없어, mid를 쓰든 same-side best를 쓰든 그 사이 구간엔 애초에 데이터가 없어 결과가 동일 — 위험 없는 단순화 |
| 2026-07-14 | **6개 종국 상태를 단일 `D5Terminal{state}`로 통합** (D1처럼 필드가 다른 별도 클래스가 아님) | PRD가 "모든 종국 레코드에는 레벨 실현률과 상위 구간 추정 실현률을 함께 남긴다"고 명시 — 6개 상태의 필드셋이 동일해 별도 클래스는 중복 |
| 2026-07-14 | **`D5Detector.reset()`은 이벤트를 반환** (D1~D4의 `reset() -> None`과 다름) | epoch 종료 시 INTERRUPTED 자체가 로그에 남아야 하는 유효 레코드이기 때문 — void reset()으로는 이 정보가 유실됨 |
| 2026-07-14 | **EpochEnded 처리에서 `d5.reset()`을 `d4.reset()`보다 먼저 호출** (service.py 모듈 docstring 명시) | INTERRUPTED 레코드의 `above_realized_rate`가 D4의 lifetime 리필 조회에 의존 — `d4.reset()`이 먼저 돌면 그 데이터가 이미 지워져 항상 0으로 남는다. M3의 "D3 확정 판정은 D1 REMOVED 라우팅 전에" 교훈과 같은 종류 |
| 2026-07-14 | **D5의 시간축은 `monotonic`** (등록 시각·TTL·진행률/종국 소요시간) | 인텐트는 재시작을 넘어 살아남지 않음(wall_registry만 예외, PRD §12) — wall-clock 영속성이 불필요하고 D2와 같은 주입 패턴으로 replay 결정성도 정합 |
| 2026-07-14 | **Telegram `enqueue()`에 `on_sent` 발송 확인 콜백 추가** — 성공(status 200) 시에만 호출, 재시도 소진 실패 시 미호출 | outbox가 sent 마킹 여부를 이 신호로만 판단하므로, 실패 시 미발송 상태 유지가 재시작 재전송의 전제. D1/D2 등 콜백 생략 호출부는 기존과 동일 동작 |
| 2026-07-14 | **`alerts_outbox` 멱등 키는 `intent_id`가 아니라 `(side, price, terminal_state, recorded_at)`**, `recorded_at`은 서비스가 outbox 기록 시점에 wall-clock으로 찍음 | `intent_id`는 D5Detector 내부 프로세스 카운터라 재시작마다 0부터 다시 시작 — 이를 유일키에 쓰면 재시작 직후 다른 인텐트가 우연히 같은 (side,price,state,intent_id)를 얻어 `INSERT OR IGNORE`가 진짜 알림을 조용히 삼킬 위험 |
| 2026-07-14 | **D3/D4는 M3 결정대로 여전히 구조화 로그만, D5 종국(확정/추정)만 `alerts_outbox` 대상** — PRD §9.4 범위 그대로 (진행률·D2·watchdog은 outbox 미사용) | 진행률/D2/watchdog은 시효성 신호라 재시작 후 재전송 가치가 없음(다음 경계/버스트/재연결 통지가 대체) — PRD 명시 범위 한정을 그대로 구현 |
| 2026-07-14 | **M4 완료 기준을 자동화 테스트로 한정, 실전 케이스1/2 알림 확인은 후속 세션으로 이월** (사용자 확정) | 1000+ BTC 벽의 60%+ 체결/상위 리필은 예측 불가능한 시장 이벤트(M3 60s 실캡처에서도 벽 1개만 관측) — M2가 실토큰 8h 검증 런을 구현 완료 후 별도 세션에서 진행한 전례와 동일 패턴 |
| 2026-07-15 | **외부 워치독은 행 감지 시 알림 + `systemctl restart` 자동 재시작** (PRD §11.1 v1.7 — 원문 "알림"에서 확장, 사용자 확정) | 행 상태는 systemd Restart=always로도 복구되지 않아(프로세스 생존) 수동 개입까지 감시 공백 지속 — 새벽 발생 시 수 시간. 인메모리 상태는 어차피 리셋 설계(PRD §12, 벽 레지스트리는 SQLite 복원)라 재시작 비용이 낮고, 오탐 재시작도 무해 |
| 2026-07-15 | **PROCESS_DOWN 알림 정책 = 정지 전이 1회 + 해소 전이 1회, 반복 재알림 없음** + `is-active` inactive(의도적 정지)는 스킵 (사용자 확정) | 자동 재시작이 있어 에스컬레이션 재알림의 실익이 낮고, 상태 파일 전이 추적만으로 성립하는 가장 단순한 구조. inactive 스킵은 유지보수 정지 중 오알림/오재시작 방지 — failed(재시작 반복 실패, 디스크 풀 등)는 감지 대상 유지 |
| 2026-07-15 | **외부 워치독 = 시스템 python3 stdlib 스크립트 + systemd timer** (cron 기각), 나이 임계 60s·타이머 주기 60s는 스크립트/유닛 상수, 하트비트 경로는 `--heartbeat-file` CLI 인자 | 배포가 이미 systemd 일원화(2026-07-12 결정)라 timer가 정합 + `journalctl`로 판정 로그 일원 조회. 앱 venv 미사용은 venv 붕괴 시에도 동작해야 하는 최후 방어선이기 때문(stdlib urllib 발송). 상수·CLI 인자는 PRD §10 config 키 전수 고정 유지 (Telegram 재시도 상수·`--db-file`과 동일 취급) |
| 2026-07-15 | **D4 + D5 케이스 2 임시 비활성 — D1/D2/D3 + 케이스 1만 운용** (PRD v1.6, 첫 실전 케이스 2 알림 검토 후 사용자 확정): service 배선에서 D4 제외 + `_refill_above_lookup` 상수 0 고정 — config 키·스키마·`detectors/d4.py` 모듈·단위 테스트는 무변경 보존(재도입 논의 기반). 재도입 시 검토 항목: ① 케이스 2를 종국이 아닌 래치로 격하(발화 후에도 인텐트 유지, 케이스 1 승격 경로) ② 상위 구간 거리 상한(오픈 퀘스천 #2) ③ 리필 페어링 대신/병행 "레벨 체결량 vs 잔량 감소 대조" 방식(네이티브 아이스버그 포착) | 실전 첫 케이스 2 알림(07-14 03:43 KST — 아래 M4 검증 기록)은 역추적상 타당했으나 검토 중 구조 한계 3건 확인: ① 거리 무제한 상위 구간의 귀속 불확실성 — 의도 레벨 +1.5~2.6% 위 무관한 MM·별개 중형 벽(실측 62,498 bid 피크 131 BTC)의 리필도 전부 합산 ② 100ms 순(net) 델타 기반이라 네이티브 아이스버그(엔진이 소진과 원자적으로 재표시 — 양의 델타 자체가 없음)에 사각 → 과소 추정 ③ 케이스 2 종국이 인텐트를 소모해 벽이 살아있는데도 같은 epoch 내 케이스 1(핵심 신호) 감시가 꺼짐(07-14 실사례: 03:43 발화 후 61k 벽 생존 중 확정 감시 부재, 재장전은 다음 epoch — 최악 ~24h). 비활성으로 ③은 자연 해소(인텐트가 벽 소멸/epoch 종료까지 케이스 1 감시). 케이스 1 중심으로 실전 조율 후 표본 기반 재설계 |
| 2026-07-14 | **D5 인텐트 TTL 폐지 — 인텐트 수명 = 벽 수명** (PRD v1.5, M4 배포 직후 검증 준비 중 사용자 지적으로 발견·확정): `INTENT_EXPIRED` 상태·`intent_ttl_seconds` config 키 삭제, 종국은 소멸/케이스1·2/epoch 종료로만. staleness 틱의 D5 evaluate 호출도 제거(시간 경과만으로 바뀌는 판정이 없어짐) | 구체적 누락 시나리오(실측): TTL 기산점이 등록 시점이라 상시 벽(61k bid, 실측 지속 83,114s vs TTL 1,800s)은 등록 30분 후~다음 epoch 재시작까지 인텐트 부재 사각지대가 하루 대부분 — 이 구간에 벽이 전량 소진되면 D1 latch 탓에 재등록 경로가 없어 `on_d1_removed`가 no-op, **가장 극적인 실체결일수록 케이스1 알림이 영영 누락**. 원거리 고래 벽 감시라는 주 목적과 정면 충돌. 메모리 바운드는 벽 희소성+`MAX_ACTIVE_INTENTS`가 대체, "의도 신선도"는 메시지의 소요시간·실현률 표기로 수신자가 판단. 검토된 대안: 접촉 기산 TTL(접촉 후 저속 흡수를 여전히 누락), 만료 시 즉시 재등록(S 재고정·생애 누적 조합으로 실현률 의미 훼손) — 모두 기각. **배포 주의**: 엄격 스키마라 로컬·VPS config.yaml에서 `intent_ttl_seconds` 키를 제거해야 기동됨 |
| 2026-07-13 | **D2 요약 "판정" 신설 — 델타비 × 가격 반응 결합** (실운영 D+1 피드백, 사용자 승인 플랜): 요약에 `verdict` 추가 — 쏠림(델타비 ≥ `summary_absorb_delta_min` 0.35)인데 에피소드 변화 < `summary_move_min_pct` 0.1% → 흡수(정체), 밀렸어도 확정 시점(종료 +병합 창) 최근 체결가가 에피소드 시가 회복 → 흡수(되돌림), 따라가고 유지 → 관철, 델타비 ≤ 0.2 → 양방향 충돌, 사이 → 혼합. 흡수 방향 = 흡수당한 테이커 쪽(매도 흡수 = 지지 후보). `finalize_price` 필드 신설, 온셋 라벨은 현행 유지 | 실운영 첫날 13 에피소드 검증에서 델타비 단독 라벨의 오분류 실측: 14:27 델타비 0.90 "방향성 매도"가 -0.11%밖에 못 밀고 요약 시점 시가 회복(실체는 매도 흡수), 16:38 361 BTC 매도 우위 가격 정체가 "혼합"으로 뭉개짐. 방향성 매도 3건 모두 30분 내 회복 — 가격 반응 없는 델타 라벨은 오독 유발. 요약이 +10분 뒤 발송되는 구조를 근거 데이터로 재활용(추가 대기 없음). 임계 2개는 n=13 잠정값 — M6 튜닝 대상. 실시간 벽 단위 흡수(D3)와 층위 구분 명시 |
| 2026-07-12 | **배포 방식 = git pull + venv + systemd, Docker 기각** (사용자 확정. 대상: Hostinger KVM2 / Ubuntu 24.04). D1&D2 상태 우선 배포를 위해 M5 중 워치독 알림·systemd 유닛·런북을 선행하고 M3/M4는 배포 후 진행 | 의존성 2개(pyyaml·aiohttp)·순수 Python·SQLite 표준 라이브러리라 Docker의 재현성 이점이 없음. Ubuntu 24.04 기본 python3 = 3.12로 요구 버전 일치. 프로세스 감독은 어차피 systemd 몫이고, M6 임계치 튜닝의 config 수정→restart 사이클도 이미지 재빌드 없는 쪽이 단순 |
| 2026-07-12 | **물량벽 정기 리포트 신설** (PRD 외 신규 기능, 사용자 요청·플랜 승인 — `alerting/wall_report.py`): `alerts.send_wall_report` + `wall_report_interval_minutes`(기본 60분) 추가. 벽 레지스트리 스냅샷을 저항(ask)/지지(bid)로 나눠 정기 발송 — 대형(≥`size_threshold_btc`) 전부 + 소형은 현재가 근접순 8개 캡("외 N개" 표기), unconfirmed `?` 접미, 현재가 반대편 unconfirmed 잔재(가격 통과 후 미재확인)는 표시 제외. epoch 활성 + 오더북 존재 시에만 발송(스킵 시 로그), dedup/쿨다운 미적용 | 이벤트형 알림(D1/D2)만으로는 현 시점 벽 분포를 한눈에 볼 수 없다는 사용자 요청. 정기 스냅샷이라 쿨다운 불필요, 현재가는 depth 기반이므로 epoch 게이팅 준수. 캡은 텔레그램 4,096자 한도 + 가독성 |
| 2026-07-13 | **D3 발화 시점 = episode 종료 시 확정** (PRD §8 D3 v1.4, 사용자 확정 — M3 착수 시 계획 질의): "모두 만족 시 발화"(즉시)와 "관통 확정 시 해당 episode 무발화"(episode 전체 필요)의 해석 충돌을 종료 시 확정으로 종결 | 중도 발화 후 같은 episode에서 관통되면 이미 낸 흡수 이벤트가 사후에 거짓이 되어 M6 튜닝 데이터를 오염. D3는 로그 전용이고 D5 케이스 1은 자체 누적 감시(M4)라 지연 비용 없음 |
| 2026-07-13 | **LevelTracker 벽 레벨 보존 예외** (PRD §7 v1.4, 사용자 확정): 벽 레지스트리 등록 가격은 top-20 창 이탈에도 엔트리(생애 누적) 보존, 벽 소멸 시 predicate 거짓화로 다음 스냅샷에서 자연 제거 | 구체적 누락 시나리오: 1,200 벽 1차 접촉 300 흡수(25%, 미발화) → $60 반등으로 창(±$0.2~5) 이탈, 누적 소실 → 2차 접촉 150 → 실제 생애 450(37.5%)인데 150(12.5%)으로 D3 침묵·M4 D5 케이스 1 미확정. 창이 좁아 접촉 간 이탈은 사실상 항상 발생, D1 FILLED/PULLED 귀속도 동일 손실. 별도 lifetime 누적기 안은 같은 데이터의 이중 집계라 기각. 메모리는 레지스트리 크기로 바운드 |
| 2026-07-13 | **D3/D4는 M3에서 구조화 로그만 기록, events 테이블은 M6 유지** (사용자 확정) | PRD §9.1의 "로그/DB 기록" 중 DB는 M6 events 테이블 소관. 모든 디텍터 이벤트가 `_emit()` 경유 JSON-lines에 전 필드로 남아(D1/D2와 동일 경로) 튜닝 데이터로 이미 조회 가능 — M3 범위 축소 + 스키마 설계 일원화(M6) |
| 2026-07-13 | **Bookmap 육안 대조 기각 — M3 완료 기준을 replay 테스트로 일원화** (PRD §13/§14/§15 #5 v1.4, 사용자 확정) | "이벤트 발화 시 어차피 별도로 보고, D1~D4가 정상 동작하면 결국 Bookmap을 참조할 필요가 없다"(사용자). 소표본 육안 대조는 경계 경로를 커버하지 못해 v1.2부터 replay가 주 수단이었음 — 유일 기준으로 승격. D4 휴리스틱(REFILL_WINDOW_MS 등) 튜닝은 M6 실운영 로그 리뷰로 이관 |
| 2026-07-22 | **D1 알림 기본값 = on 으로 확정** (PRD v1.10 개정, 사용자 확정) — `config.example.yaml`의 튜닝 기간 임시 on(52230f2, 07-15)을 정식 기본값으로 승격 | 실운영에서 계속 유지되며 사실상 정상 운영값으로 굳어졌고, 애초에 기본 off였던 이유(재시작·재연결마다 반복 재발화 스팸 우려)가 같은 날 도입한 스트릭당 1회 억제(§8 D1 v1.8)로 해소되어 "튜닝 기간에만 on" 제한을 유지할 근거가 없어짐 |
| 2026-07-22 | **D5 케이스 1 확정을 종국이 아닌 래치로 — 확정 후에도 20% 경계 진행률 알림을 상한 없이 지속** (PRD v1.9 개정, 사용자 확정 — 첫 실전 케이스 1 검토 후속): 확정 알림·outbox 선기록은 기존대로 1회, 인텐트는 벽 소멸/epoch 종료까지 유지. 래치 후 소멸은 신규 `CONFIRMED_CLOSED`(로그 전용)로 마감 — 최종 실현률을 튜닝 데이터로 남기되 텔레그램은 미발송(D1 REMOVED 알림이 체결량·귀속을 동반해 중복). 확정 후 진행 중 재시작 시 인텐트·누적 리셋(§12)은 예외 처리 없이 수용(사용자 확정) | 실측(07-20 17시 KST, 64,242 bid S=1,118): 진행 20%→40% 후 60.6% 확정 종국이 인텐트를 소모해 추적 종료 — 벽은 40분+ 더 생존하며 흡수가 계속됐지만 무통지. v1.6 개정이 케이스 2에 지적한 파생 결함 ③("종국이 인텐트를 소모해 감시가 꺼짐")의 케이스 1 버전으로, 그때 재검토 항목 "케이스 2를 래치로 격하"와 같은 원리를 케이스 1에 선적용 — D4 재설계 토의 시 케이스 2도 이 패턴을 따를 근거. 실현률 분모 S 고정 + 분자 생애 누적이라 100% 초과 경계(120%, …)가 자연 성립 — "그 구간에서 얼마나 흡수됐나"의 직접 가시화 |
| 2026-07-22 | **D1 APPEARED 알림은 임계 스트릭당 1회 — 재시작·재연결 재발화 발송 억제** (PRD v1.8 개정, 사용자 확정 — M5 감사의 재발화 11건 후속): 탐지 이벤트는 그대로(D5 인텐트 재등록·로그 유지), 발송만 dispatcher의 스트릭 게이트에서 억제. 멱등 키 = `walls.appeared_alerted_since`(발송된 스트릭의 `first_seen_above_threshold` 값, SQLite 영속 — 기존 DB는 열 때 `ALTER TABLE` 경량 마이그레이션). 게이트는 쿨다운 **뒤에** 평가 — 쿨다운에 눌린 발화가 스트릭을 소모(마킹)하면 그 벽이 영영 미통지되므로, 마킹은 실제 발송 시에만 | 사용자 판단: 정기 호가벽 리포트가 "지금도 있음"을 커버해 중간 재통지는 정보 가치 없음 — **생성 1회 + 해소 1회**. 대안 기각 2건: ① `persisted_seconds` 크면 억제(휴리스틱) — 재시작 공백 중 새로 등장한 벽(통지된 적 없음)이 첫 평가부터 지속시간이 커서 영영 미통지되는 누락 시나리오 ② 디텍터 레벨 억제(latch 영속화) — epoch 재시작 시 D1 재발화가 없으면 INTERRUPTED된 D5 인텐트의 재등록 경로가 끊겨 케이스 1 감시 공백(M2 노트의 의도된 재발화 설계와 충돌). 스트릭 값이 바뀐 재돌파·REMOVED 후 재출현은 새 등장으로 정상 발송 |
| 2026-07-22 | **D4 재설계 확정 — 케이스 2(상위 구간 귀속) 폐지, D4 = "레벨 흡수 방어" 독립 알림 디텍터** (PRD v1.11 개정, 스펙 토의 후 사용자 확정. 구현은 하단 백로그 — 그때까지 D4 비배선 유지): ① `EXECUTION_INFERRED_ABOVE`·케이스 2 집계·`realize_pct_above`·`iceberg_margin_btc` 폐지, D5는 케이스 1 전용 ② D4 대상 = 레지스트리 추적 레벨 전체(하한 100+), 판정 = `absorbed ≥ M(2.0 잠정) × 등록 시점 크기 R` — absorbed는 원시 체결 총량이 아니라 **리필로 입증된 흡수만**: 가시 리필(500ms 페어링, v1.2 유지) + 은닉 리필(**100ms 틱 단위** `max(0, 체결 − 표시감소)` 대조 — v1.6 한계 ② 네이티브 아이스버그 해소) ③ 비관통(D3 판정 재사용)·최소 이벤트 수(5) 병행 ④ 수명 = 스트릭 생애 누적 + 래치 + 진행 무상한 + 종결 통지(v1.8/v1.9 대칭) | 오귀속 실증(사용자 지적): 07-17 62.8k(peak 220, ~200 BTC급 반복 리필)는 v1.6 서술("무관한 MM 노이즈")과 달리 **1000 기준 미달일 뿐인 독립 매수 의도** — 구 케이스 2였다면 61k 인텐트의 실행 증거로 합산(오귀속)됐을 활동. 귀속은 공개 데이터로 성립 불가(PRD §8 판정 의미)이므로 "관측은 기계, 해석은 수신자"로 선을 재획정. 배수 기준 채택 근거: 표시 크기 고정 상수(예: 200 BTC)는 출현 시점 판정이라 자의적(다음엔 180을 놓침) — 행동(자기 크기의 몇 배를 먹으며 버텼나) 기준은 벽 크기에 자동 스케일. 원시 `cum_traded` 기각 근거(사용자 우려): 저유동성 횡보의 일반 회전(소진+무관 재유입)도 총량은 배수를 넘음 — 리필 입증 집계는 횡보에서 틱별 mismatch ≈ 0·페어링 실패로 자연 배제, 저유동성 국면 발화는 의도된 동작(축은 유동성이 아니라 리필 입증). 07-15 재검토 항목 3건 처분: ① 래치 격하 → 반영(케이스 2 자체는 폐지됐지만 D4 수명 모델로) ② 거리 상한 → 소멸(귀속 폐지로 거리 개념 없음, OQ #2 종결) ③ 대조 방식 → 반영(틱 단위로 정제) |
| 2026-07-22 | **07-17 62.8k 준임계 벽 리필·흡수 사례 검수 → 관측 보완 2건 백로그 채택** (M6 체크박스 참고): ① 추적 벽 수량 궤적 로깅(등록+리필 델타) ② D2 요약에 근접 벽 컨텍스트 첨부. **D4 재설계 조기 재개 여부는 별도 스펙 토의 후 결정** (2026-07-15 결정 기록의 재검토 항목 3건이 출발점, 이 사례가 표적 실측 표본) | full 추출(07-22) 실측: 07-17 15:02 KST buy 62,800 벽이 peak 220.65 → 16.13 BTC로 소멸(하한 미달), 동시각 D2 `directional_sell` 온셋(가격 62,800) + 15:13 요약 verdict `sell_absorbed_stall` 텔레그램 발송 — 순간 자체는 D2·레지스트리가 포착·통보했고, 같은 가격이 07-19 06:19 KST peak 248로 재소멸(반복 방어 가격대). 그러나 사용자가 관찰한 "~200 BTC 반복 리필"은 무기록: D4(리필 담당) 07-15부터 비배선(로그상 마지막 D4 이벤트 07-14 15:58 UTC), D3·`cum_traded` 기록은 D1 벽(≥1000) 게이트라 220 BTC 벽 제외, 레지스트리는 상태 스냅샷 미러라 소멸 로그 외 이력 없음(잔존 데이터 = peak 1개). 준임계(100~1000 BTC) 벽의 반복 방어·흡수는 현 계층 구성의 구조적 사각 |
| 2026-07-23 | **D4 구현 착수 전 스펙 검토 → 보강 3건 확정 (PRD v1.12 개정, 사용자 확정)**: ① ② 은닉 리필에 **1틱 이월 보정** — `carry = max(0, 직전 틱 표시잔량 감소분 − 직전 틱 체결량)`을 다음 틱에서 차감(`hidden = max(0, 체결 − 감소 − carry)`), 이월은 1틱 한정 ② **epoch 시작 시 스트릭 재개시** — 새 epoch 시작(재시작 복원 포함) 시 활성 레지스트리 벽 전체에 새 스트릭 개시, R = 그 시점 `last_qty` 재고정 ③ D2 요약 근접 벽 컨텍스트의 선정 기준 = **에피소드 종가 기준 bid/ask 각 최근접 추적 벽 1개**(가격·last·peak 동봉, 거리 제한·config 키 신설 없음). 부수: §10 삭제 목록에 `iceberg_min_trades` 명시 정리, `d5.reset()`→`d4.reset()` 순서 제약(2026-07-14)은 케이스 2 폐지로 **소멸 확정**(근거인 `above_realized_rate`의 D4 lifetime 조회 자체가 삭제됨 — service docstring에서 제거) | ① 스트림 순서 역전(§13 replay 필수 시나리오로 실재 확인된 현상) 시 감소분은 앞 틱에서 `max(0,…)` 클램프로 버려지고 체결은 뒤 틱에서 은닉 리필로 오인 — 오차가 **양의 방향으로만** 누적되어 리필 없이 먹히기만 하는 벽에서도 absorbed가 쌓이는 오발화 경로. v1.11의 수용 오차 문단은 다른 메커니즘(무관 신규 주문 우연 겹침)만 다뤘음. 대안 기각: 수신시각 재버킷팅은 이론상 더 정확하나 구현·결정적 replay 검증 복잡도 과다, 현행 수용은 체계적 편향 방치 ② v1.11은 epoch 종료의 누적 리셋만 정의 — 스트릭 개시가 신규 등록에만 걸리면 복원·생존 벽이 재등록 전까지 D4 사각(장수 준임계 벽 = 주 표적, v1.5가 D5에서 폐지한 "죽은 창"과 같은 부류의 누락 시나리오) ③ 07-17 사례(종가 = 벽 가격 62,800)를 확실히 포섭하면서 엄격 스키마·VPS config 동시 수정 부담 회피 |
| 2026-07-11 | 벽 레지스트리 SQLite 영속화 + unconfirmed 플래그 + TTL 14일 — "인메모리 상태 미복원" 원칙(PRD §12)의 유일한 예외 | 원거리 벽 시야는 청취 누적으로만 형성되므로 재시작 초기화 비용이 큼. 신뢰도 하락은 **청취 공백**(재시작·재연결 갭)에서만 발생(연결 중 무이벤트 = 무변화 = 값 유효) → 공백 시 전체 unconfirmed 마킹, 가격별 새 이벤트(절대 잔량)로 자동 해제, 능동 재검증 API는 부재. unconfirmed 중 APPEARED 발화 억제 + `first_seen_at` 보존(스푸핑 필터 타이머 유지). TTL 14일(설정값, 사용자 경험상 2주 초과 지속 벽 드묾)은 신뢰도가 아닌 저장 위생 규칙 — `events` 이력은 보존해 M6 튜닝 데이터 유지. **(주)** 이후 PRD v1.2 (5)에서 TTL은 unconfirmed 전용·기산점 `unconfirmed_since`·기본 7일로 개정됨 |

## 오픈 퀘스천 트래킹 (PRD §15)

| # | 질문 | 결정 시점 | 상태 | 결론 |
|---|---|---|---|---|
| 1 | `SIZE_THRESHOLD` 초기값 (BTC 수량 vs USDT 노셔널) | M1 데이터 수집 후 / 최종 M6 | ✅ 확정 | BTC 수량 기준, **1000 BTC** (2026-07-11, 사용자 시장 판단 — 전달력 있는 유동성 기준). 기록 하한은 별도 100 BTC(`record_min_qty_btc`). M6에서 실데이터 재검토 여지만 유지 |
| 2 | 케이스 2 "상위 구간" 범위 제한 필요 여부 | M4 설계 시 | ✅ 종결 (소멸) | 케이스 2 자체가 v1.11(2026-07-22)에서 폐지 — 귀속 해석을 하지 않으므로 거리 개념이 성립하지 않음. 상위 구간 활동은 D4 흡수 방어가 자기 레벨에서 독립 판정 (결정 기록 2026-07-22 D4 재설계 참고) |
| 3 | 매도 의도(ask 벽) 대칭 지원 v1 포함 여부 (PRD는 포함 권장) | M0~M1 설계 시 | ✅ 확정 | v1 포함 — 벽 레지스트리 설계 논의(2026-07-11)에서 사용자가 ask/bid 양측 추적을 전제로 함. 구현 비용도 낮음 |
| 4 | 알림 언어/포맷: 한국어 고정 vs 템플릿화 | M2 착수 시 | ✅ 확정 | 한국어 고정 (2026-07-12, 결정 기록 참고 — 템플릿화는 요구 없는 유연성으로 기각) |
| 5 | Bookmap 대조 기록 방식 (스크린샷 vs 녹화) | M3 착수 시 | ✅ 종결 (기각) | Bookmap 대조 자체를 M3에서 제외 (2026-07-13, 사용자 확정 — 결정 기록 참고). M3 완료 기준은 결정적 replay 테스트로 일원화 |
