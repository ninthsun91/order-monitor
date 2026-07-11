# 개발 계획 및 진행 트래킹 — BTC 오더북 인텐트→실체결 모니터

> 이 문서는 [PRD](PRD_orderbook_intent_monitor.md)의 구현 계획(§13)을 실제 작업 단위로 분해하고 진행 상태를 기록한다.
> **규칙**: 작업 완료 시 체크박스를 채우고, 단계 완료 시 상태 표를 갱신한다. PRD와 어긋나는 결정이 생기면 §"결정 기록"에 남긴다. 요구사항의 원천은 항상 PRD이며, 이 문서는 "무엇이 언제 어디까지 되었는가"만 다룬다.

## 전체 현황

| 단계 | 내용 | 상태 | 완료일 |
|---|---|---|---|
| M0 | 프로젝트 스캐폴딩 | ✅ 완료 | 2026-07-11 |
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
- [x] git 저장소 초기화, `.gitignore` (`.venv/`, `*.log`, `*.db`/`*.sqlite3`, `config.yaml`, `.env` 등 제외 — 아직 커밋은 하지 않음, 사용자 요청 시 진행)

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

`config.py`, `logging_setup.py`, `main.py`는 아직 없음 — 다음 M0 작업(`config.yaml` 로더, 로그 셋업)에서 `src/order_monitor/` 최상위에 추가 예정.

## M1 — Ingestion + 상태 모델 + 로그

- [ ] **스파이크(우선 작업)**: ccxt `watch_order_book`이 `btcusdt@depth20@100ms`(partial snapshot)를 diff 스트림이 아니라 정확히 그 스트림으로 구독하는지 검증. 실패 시 `binance-connector-python`으로 전환(콜백→asyncio 브릿지 직접 구현) 후 이어서 진행
- [ ] WS 클라이언트: `btcusdt@depth20@100ms` 구독 → `order_book` 상태 갱신 (통째 교체)
- [ ] WS 클라이언트: `btcusdt@aggTrade` 구독 → `trade_window` deque 적재 + 만료 pop
- [ ] `level_tracker` 구현: 레벨 생애주기 필드 (PRD §7)
- [ ] 재연결: 지수 백오프(1s→60s), 24h 강제 단절·ping/pong은 라이브러리 위임 확인 (PRD §5.3)
- [ ] 재연결 직후 첫 depth 스냅샷 수신 전 판정 보류 플래그
- [ ] 이벤트 타임스탬프는 거래소 시각 사용 (PRD §11.1)
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
- [ ] 전이 1: 가격 도달 + 체결 누적 ≥ S×`REALIZE_PCT` → `EXECUTION_CONFIRMED` (케이스 1)
- [ ] 전이 2: PULLED → `INTENT_WITHDRAWN` (로그만)
- [ ] 전이 3: 상위 구간 D4 누적 ≥ S×`REALIZE_PCT_ABOVE` → `EXECUTION_CONFIRMED_ABOVE` (케이스 2, 리필 확인분만 합산 — PRD §8 D5 집계 방식)
- [ ] `INTENT_TTL` 만료 → `INTENT_EXPIRED` (로그만)
- [ ] `intents` 개수/시간 상한 (메모리 바운드)
- [ ] 확정 알림 포맷: 실현률 %, 등록→확정 소요시간 포함 (PRD §9.3), 케이스 2는 "추정" 명시
- [ ] 단위 테스트: 상태 전이 전 경로 (확정 1/2, 철회, 만료)

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

## 오픈 퀘스천 트래킹 (PRD §15)

| # | 질문 | 결정 시점 | 상태 | 결론 |
|---|---|---|---|---|
| 1 | `SIZE_THRESHOLD` 초기값 (BTC 수량 vs USDT 노셔널) | M1 데이터 수집 후 / 최종 M6 | ⬜ 미결 | |
| 2 | 케이스 2 "상위 구간" 범위 제한 필요 여부 | M4 설계 시 | ⬜ 미결 | |
| 3 | 매도 의도(ask 벽) 대칭 지원 v1 포함 여부 (PRD는 포함 권장) | M0~M1 설계 시 | ⬜ 미결 | |
| 4 | 알림 언어/포맷: 한국어 고정 vs 템플릿화 | M2 착수 시 | ⬜ 미결 | |
| 5 | Bookmap 대조 기록 방식 (스크린샷 vs 녹화) | M3 착수 시 | ⬜ 미결 | |
