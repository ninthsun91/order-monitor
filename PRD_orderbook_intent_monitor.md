# PRD — BTC 오더북 인텐트→실체결 모니터 (경로 B)

| 항목 | 내용 |
|---|---|
| 문서 버전 | v1.0 (초안) |
| 작성일 | 2026-07-11 |
| 상태 | Draft |
| 대상 | 단일 개발자(운영자 겸), Claude Code 기반 개발 |

---

## 1. 배경 및 문제 정의

Bookmap 히트맵에서 관찰되는 대형 리밋 오더(예: 특정 가격의 1k+ BTC bid)는 그 자체로는 "체결 의도의 가설"일 뿐이다. 실제로 확인하고 싶은 것은 다음 질문이다:

> **"확인된 매수 의도에 준하는 물량 체결이 실제로 이루어졌는가?"**

구체적으로 두 가지 케이스를 판별해야 한다:

- **케이스 1**: 가격이 해당 레벨까지 도달하여 그 자리에서 실제 체결이 이루어짐 (흡수, absorption)
- **케이스 2**: 가격이 해당 레벨까지 도달하지 못했지만, 그 위쪽 가격대에서 아이스버그 등으로 충분한 물량을 확보함

Bookmap으로 육안 확인은 가능하나 24/7 자동 모니터링이 필요하다. Bookmap API는 원격 호출형이 아닌 **실행 중인 GUI 앱에 로드되는 인프로세스 애드온** 모델이므로, 24/7 운영 시 GUI 앱 상주라는 운영 부담이 생긴다. 반면 필요한 원천 데이터(오더북 depth, 체결)는 Binance 등 거래소 WebSocket으로 무료·무인증 제공된다.

**결정: 경로 B — Bookmap 없이 거래소 WebSocket을 직접 소비하는 헤드리스 Python 서비스로 구현한다.** Bookmap은 데스크톱에서 리서치/임계치 튜닝/육안 검증용으로만 사용한다.

## 2. 목표

1. Binance 현물 BTC/USDT 오더북과 체결을 실시간 소비하여, "매수(또는 매도) 의도가 실체결로 이어진 순간"을 자동 감지한다.
2. 확정 신호만 Telegram으로 발송한다 (노이즈 최소화).
3. **신뢰성 최우선**: 정밀도·속도보다 무중단·자가복구·오탐지 억제를 우선한다.
4. 임계치·심볼 등 모든 판정 기준은 코드 수정 없이 설정 파일로 조정 가능해야 한다.

### 2.1 성공 기준

- 서비스가 재시작·네트워크 단절·거래소 유지보수를 사람 개입 없이 스스로 복구한다.
- 모니터 자체가 죽거나 피드가 끊기면 그 사실이 역으로 Telegram에 통지된다 (조용한 실패 없음).
- `EXECUTION_CONFIRMED` 계열 알림이 "표시 의도 대비 실현률(%)"과 함께 도착한다.

## 3. 비목표 (Out of Scope, v1)

- 자동 매매/주문 실행 (모니터링·알림 전용)
- 다중 거래소 오더북 합산 (Multibook 스타일) — v2 후보
- 전체 depth(20레벨 초과) 추적 — v2 후보 (diff 스트림 필요)
- 웹 대시보드/시각화 UI
- Bookmap 독자 지표(Absorption Indicator 등)의 정밀 재현 — 휴리스틱 근사로 대체
- 선물(USDⓈ-M) 시장 — v1은 현물만. 단, 확장 가능하게 설계

## 4. 용어

| 용어 | 정의 |
|---|---|
| 레벨(level) | 오더북의 특정 가격과 그 가격의 표시 잔량 |
| aggressor | 시장가로 체결을 일으킨 쪽. aggTrade의 `m=true`→매도 aggressor(비드 히트), `m=false`→매수 aggressor(애스크 리프트) |
| 의도(intent) | 임계 크기 이상이고 최소 지속시간을 통과한 대형 레벨. "체결을 희망하는 물량"의 후보 |
| 실현률 | 의도로 등록된 표시 크기 S 대비, 해당 레벨(또는 상위 구간)에서 실제 체결된 누적량의 비율 |
| pull | 체결 없이 레벨이 축소/소멸됨 (취소, 스푸핑 추정) |
| 리필(refill) | 체결이 일어나는데도 표시 잔량이 유지/회복됨 (아이스버그 추정) |

## 5. 데이터 소스

### 5.1 스트림 (Binance Spot WebSocket)

| 스트림 | 용도 | 주기/특성 |
|---|---|---|
| `btcusdt@depth20@100ms` | 오더북 상태 | 상위 20레벨 bid/ask **완전 스냅샷**을 100ms마다 push. 시퀀스 재조정 불필요 |
| `btcusdt@aggTrade` | 체결 | 체결 발생 시 push. 가격·수량·시각·`m` 플래그(aggressor 방향) 포함 |

**설계 결정 — diff depth(`@depth`) 대신 partial snapshot(`@depth20@100ms`) 채택.**
diff 스트림은 REST 스냅샷 + U/u 시퀀스 재조정 + 갭 재동기화가 필요해 로컬 북 드리프트라는 버그 클래스가 존재한다. 본 시스템의 관심 대상(현재가 근처의 대형 잔량과 그 레벨로의 체결)은 상위 20레벨로 충분하므로, 매 메시지가 완전 스냅샷인 partial 스트림을 채택해 해당 버그 클래스를 원천 제거한다. 신뢰성 우선 원칙에 부합.

**한계 (수용)**: 현재가에서 20레벨보다 멀리 떨어진 대형 잔량은 가격이 접근하기 전까지 보이지 않는다. v1에서는 수용하고, 필요 시 v2에서 diff 스트림 병행으로 확장한다.

### 5.2 클라이언트 라이브러리

`ccxt.pro`(`watch_order_book`, `watch_trades`) 또는 `binance-connector-python`(공식) 사용. 재연결, ping/pong keepalive, 파싱을 라이브러리에 위임하여 유지보수 표면적 축소. raw websocket 직접 구현은 금지(정당한 사유가 생기기 전까지).

### 5.3 연결 수명 제약 (라이브러리/재연결 로직이 흡수해야 함)

- 단일 연결은 24시간 후 강제 단절됨 → 자동 재연결 필수
- 서버가 20초마다 ping frame 전송, 1분 내 pong 없으면 단절 → keepalive 필수
- 거래소 인프라 업그레이드(사전 공지형, `serverShutdown` 이벤트 포함)로 단절 가능 → 지수 백오프 재연결로 투명 처리
- 재연결 직후에는 depth 스냅샷 1개를 수신하기 전까지 판정을 보류한다

## 6. 시스템 아키텍처

```
┌────────────────────────────────────────────────┐
│ Binance Spot WebSocket                          │
│   @depth20@100ms          @aggTrade             │
└──────────────┬─────────────────┬───────────────┘
               ▼                 ▼
┌────────────────────────────────────────────────┐
│ Ingestion (ccxt.pro)                            │
│   재연결 · keepalive · 정규화 · 타임스탬프       │
└──────────────┬─────────────────────────────────┘
               ▼
┌────────────────────────────────────────────────┐     ┌──────────────┐
│ In-memory State                                 │────▶│ Persistence  │
│   order_book · level_tracker · trade_window     │     │ (SQLite, 선택)│
└──────────────┬─────────────────────────────────┘     └──────────────┘
               ▼
┌────────────────────────────────────────────────┐
│ Detector Layer                                  │
│   D1 대형잔량  D2 볼륨버스트  D3 흡수  D4 아이스버그 │
└──────────────┬─────────────────────────────────┘
               ▼
┌────────────────────────────────────────────────┐
│ D5 Intent → Execution Monitor (핵심 상태기계)     │
└──────────────┬─────────────────────────────────┘
               ▼
┌────────────────────────────────────────────────┐     ┌──────────────┐
│ Alerting                                        │     │ Supervisor / │
│   심각도 게이트 · dedup · 쿨다운 → Telegram Bot   │     │ Watchdog     │
└────────────────────────────────────────────────┘     └──────────────┘
```

- 단일 프로세스, asyncio 기반. 컴포넌트는 모듈로 분리하되 프로세스 분리는 하지 않는다 (운영 단순성).
- Supervisor/Watchdog은 파이프라인 외부에서 프로세스 생존과 피드 신선도를 감시한다.

## 7. 상태 모델 (In-memory State)

| 구조 | 내용 | 갱신 트리거 |
|---|---|---|
| `order_book` | 최신 top-20 스냅샷 `{price: qty}` (bid/ask 각각) | depth 이벤트마다 통째 교체 |
| `level_tracker` | 레벨별 생애주기: `{price, side, current_size, peak_size, first_seen_above_threshold, active, cum_traded_at_level}` | depth 이벤트(크기), aggTrade(체결 누적) |
| `trade_window` | 최근 `WINDOW_SECONDS` 체결 deque: `(ts, price, qty, aggressor_side)` | aggTrade push, 만료분 pop |
| `intents` | D5 상태기계 인스턴스 목록 (아래 §9) | 디텍터 이벤트 |

메모리 상한: `trade_window`와 `intents`는 시간/개수 상한으로 바운드. 재시작 시 상태는 초기화되며(§12 참고), 이는 수용 가능한 트레이드오프다.

## 8. 디텍터 명세

모든 임계치는 설정값이며, 괄호 안은 초기 기본값 제안이다.

### D1 — 대형 잔량 발생/제거

**목적**: "1k 이상 bid/ask 발생/제거 시 알림" 요구의 구현 + D5의 입력.

**발생(APPEARED) 조건** — 다음을 모두 만족하는 순간 발화:
1. 어떤 레벨의 `current_size ≥ SIZE_THRESHOLD` (예: 300 BTC — 심볼·시장상황에 맞게 튜닝)
2. 그 상태가 `PERSIST_SECONDS`(3s) 이상 연속 유지 ← **스푸핑 1차 필터**
3. 해당 레벨에 대해 미발화 상태 (레벨당 1회, 레벨이 임계 밑으로 내려갔다 다시 올라오면 리셋)

**제거(REMOVED) 조건** — 활성 레벨의 `current_size < SIZE_THRESHOLD × EXIT_RATIO`(0.5)로 하락 시, 감소 원인을 판정하여 발화:
- `cum_traded_at_level ≥ (peak_size − current_size) × FILL_ATTRIBUTION`(0.7) → **`FILLED`** (실체결로 소진)
- 그 외 → **`PULLED`** (취소/스푸핑 추정)

**평가 시점**: depth 이벤트(100ms)마다 갱신, 발화는 지속시간 타이머로 게이트.

### D2 — 시간창 볼륨 버스트

**목적**: "특정 시간 내 일정 볼륨 발생 시 알림" 요구의 구현.

**조건**: `trade_window` 내 합계 ≥ `VOL_THRESHOLD`(60초에 100 BTC). 매수/매도 aggressor 분리 집계를 기본으로 하고, 합산 모드도 설정으로 제공.

**평가 시점**: aggTrade 수신마다. `(방향)` 단위 쿨다운 `BURST_COOLDOWN`(120s)으로 연속 발화 억제.

### D3 — 흡수 (케이스 1)

**전제**: 가격 P에 D1 활성(APPEARED) 레벨 존재.

**조건** — 모두 만족 시 발화:
1. 베스트 프라이스가 P에 도달 (bid 레벨이면 best bid가 P까지 하락 접촉)
2. P에서의 공격적 체결 누적 `cum_traded_at_level ≥ ABSORPTION_MIN`(표시 크기의 30%)
3. 관측 구간 동안 가격이 P를 관통하지 못함 (bid 레벨이면 best ask 기준가가 P 아래로 이탈하지 않음)

**산출**: 흡수량, 실현률(`cum_traded / 등록 시 표시크기`).

### D4 — 아이스버그/리필 (케이스 2의 재료)

**목적**: 표시 잔량 대비 초과 체결(숨은 물량) 감지. Bookmap Absorption Indicator의 휴리스틱 근사.

**조건**: 어떤 레벨에서 관측 구간 동안
`cum_traded_at_level > (peak_size − current_size) + ICEBERG_MARGIN`
즉, 체결량이 표시 잔량의 순감소분을 유의미하게 초과 (레벨이 계속 리필됨). `ICEBERG_MIN_TRADES`(5회) 이상의 반복 체결을 함께 요구해 단발 노이즈 배제.

**산출**: 초과 체결량(숨은 물량 추정치), 레벨, 방향.

### D5 — 인텐트→실체결 상태기계 (핵심 신호)

D1~D4는 재료이고, 최종 사용자 신호는 D5가 생성한다.

**상태 전이**:

```
                    D1 APPEARED (크기 S 기록)
                          │
                          ▼
                  INTENT_REGISTERED
                          │
      ┌───────────────────┼──────────────────────┐
      ▼                   ▼                      ▼
가격 도달 +          가격 미도달 +           가격 미도달 +
레벨 체결 누적       레벨 PULLED            같은 방향 상위 구간에서
≥ S × REALIZE_PCT                          D4 아이스버그 누적
      │                   │                ≥ S × REALIZE_PCT_ABOVE
      ▼                   ▼                      ▼
EXECUTION_CONFIRMED  INTENT_WITHDRAWN   EXECUTION_CONFIRMED_ABOVE
   (케이스 1)          (스푸핑, 로그만)        (케이스 2)
      │                                        │
      └────────────── 알림 발송 ────────────────┘

만료: INTENT_TTL(30분) 내 어떤 전이도 없으면 INTENT_EXPIRED (로그만)
```

**파라미터**: `REALIZE_PCT`(0.6), `REALIZE_PCT_ABOVE`(0.6), `INTENT_TTL`(1800s), 케이스 2의 "상위 구간" 정의 = 의도 레벨과 현재가 사이의 같은 side 레벨들.

**케이스 2 집계 방식**: `INTENT_REGISTERED` 시점 이후, 의도 레벨 위쪽(같은 방향) 가격대에서 발생한 D4 초과 체결량 + 해당 방향 aggressor 체결 중 리필 패턴이 확인된 것만 합산한다. 단순 시장가 매수 총량을 쓰지 않는 이유: 의도 주체와 무관한 흐름까지 포함되어 오탐이 커지기 때문. (완전한 귀속은 불가능하며, 이 근사가 v1의 한계임을 명시)

## 9. 알림 (Telegram)

### 9.1 발송 정책

| 이벤트 | 기본 발송 | 비고 |
|---|---|---|
| D5 `EXECUTION_CONFIRMED` / `_ABOVE` | **발송** | 핵심 신호 |
| D1 `APPEARED` / `FILLED` / `PULLED` | 설정으로 on/off (기본 off) | 튜닝 기간에만 on 권장 |
| D2 볼륨 버스트 | 설정으로 on/off (기본 on) | |
| D3, D4 단독 | 발송 안 함 (D5 입력 전용) | 로그/DB에는 기록 |
| Watchdog `FEED_STALE` / `PROCESS_DOWN` | **발송** | 운영 알림 |

### 9.2 스팸 억제

- dedup 키: `(detector, side, price_bucket)` — `price_bucket`은 가격을 `BUCKET_SIZE`(50 USDT)로 양자화
- 키당 쿨다운 `ALERT_COOLDOWN`(300s)
- Telegram Bot API 레이트리밋 대응: 발송 큐 + 초당 발송 상한, 실패 시 재시도(백오프)

### 9.3 메시지 포맷 (예)

```
🟢 매수 의도 실체결 확인 (케이스 1)
심볼: BTC/USDT (Binance Spot)
의도 레벨: 106,250 (bid) · 표시 342 BTC
체결: 231 BTC (실현률 68%)
등록→확정: 14m 32s
```

발송 채널: Telegram Bot API (`sendMessage`), 대상 chat_id는 설정값. 봇 토큰은 환경변수로만 주입 (파일/코드에 저장 금지).

## 10. 설정 (config.yaml)

```yaml
exchange: binance
symbol: BTC/USDT
depth_stream: depth20@100ms

thresholds:
  size_threshold_btc: 300        # D1 대형 잔량 기준
  persist_seconds: 3             # D1 스푸핑 필터
  exit_ratio: 0.5
  fill_attribution: 0.7
  vol_threshold_btc: 100         # D2
  window_seconds: 60             # D2
  burst_cooldown_seconds: 120
  absorption_min_pct: 0.3        # D3
  iceberg_margin_btc: 20         # D4
  iceberg_min_trades: 5
  realize_pct: 0.6               # D5
  realize_pct_above: 0.6
  intent_ttl_seconds: 1800

alerts:
  send_d1: false
  send_d2: true
  bucket_size_usdt: 50
  cooldown_seconds: 300

telegram:
  chat_id: "..."                 # 토큰은 TELEGRAM_BOT_TOKEN env로

watchdog:
  stale_seconds: 30              # 이 시간 동안 depth 이벤트 없으면 FEED_STALE
  heartbeat_interval: 10
```

모든 판정 파라미터는 재시작으로 반영되면 충분하다 (핫 리로드는 비목표).

## 11. 비기능 요구사항

### 11.1 신뢰성 (최우선)

- **자가복구**: systemd `Restart=always, RestartSec=5` (또는 Docker `restart: unless-stopped`)
- **워치독**: `stale_seconds` 동안 depth 이벤트가 없으면 `FEED_STALE`을 Telegram으로 발송. 프로세스 하트비트 파일/타임스탬프를 별도 경량 워치독(cron 또는 systemd timer)이 감시하여, 메인 프로세스가 행 상태여도 감지
- **재연결**: 지수 백오프(1s→최대 60s), 재연결 후 첫 depth 스냅샷 수신 전 판정 보류
- **부분 실패 격리**: Telegram 발송 실패가 파이프라인을 막지 않도록 발송은 비동기 큐로 분리
- **시계**: 판정에는 거래소 이벤트 타임스탬프 사용, 로컬 시계는 워치독에만 사용

### 11.2 성능 (충분 조건만)

- 처리량: depth 10 msg/s + aggTrade 피크 수백 msg/s 수준 — 단일 asyncio 프로세스로 충분
- 알림 지연: 이벤트 발생 → Telegram 발송 요청까지 5초 이내면 충분 (속도 비최우선)

### 11.3 운영 환경

- 저사양 Linux VPS (1 vCPU / 1GB RAM 급) — DigitalOcean/Vultr/Lightsail 등, 도쿄·싱가포르 리전 권장
- 배포 전 확인: 해당 리전 IP에서 Binance WS/REST 접속 가능 여부 (클라우드 대역 지오블록 사전 검증)
- GUI/GPU/Windows 불필요 (경로 B 채택의 핵심 이점)

### 11.4 관측성

- 구조화 로그(JSON lines): 모든 디텍터 이벤트, 상태 전이, 연결 이벤트
- 로그 로테이션 (logrotate 또는 라이브러리)
- (선택) 일일 요약 메시지: 감지 건수, 재연결 횟수, 업타임

## 12. 영속화 (선택, 권장)

- SQLite 단일 파일: `events`(디텍터 이벤트), `intents`(상태기계 이력), `trades_sample`(선택)
- 목적: 임계치 튜닝, 사후 분석, 오탐 리뷰. 리플레이 백테스트는 v2
- 재시작 시 인메모리 상태는 복원하지 않는다 — 진행 중이던 intent는 유실됨을 수용 (단순성 우선). DB에는 `INTERRUPTED`로 마킹

## 13. 구현 계획 (마일스톤)

| 단계 | 내용 | 완료 기준 |
|---|---|---|
| M1 | Ingestion + 상태 모델 + 로그 | 24h 무중단 수집, 재연결 자동 복구 확인 |
| M2 | D1 + D2 + Telegram 발송 + 쿨다운 | 실제 알림 수신, 스푸핑 필터 동작 확인 |
| M3 | 레벨별 체결 집계 + D3 + D4 | FILLED/PULLED 구분 정확성 육안 검증 (Bookmap 대조) |
| M4 | D5 상태기계 + 확정 알림 | 케이스 1/2 알림 각 1건 이상 실전 확인 |
| M5 | Watchdog + systemd + 배포 | VPS에서 7일 무인 운영, 조용한 실패 0건 |
| M6 | SQLite 영속화 + 임계치 튜닝 루프 | 1주 데이터 기반 임계치 1차 확정 |

각 단계는 독립 배포 가능. M2 시점부터 실사용 가치 발생.

## 14. 리스크 및 완화

| 리스크 | 영향 | 완화 |
|---|---|---|
| top-20 밖 대형 잔량 미관측 | 먼 레벨의 의도 등록 불가 | v1 수용. 필요 시 v2에서 diff 스트림 병행 |
| 아이스버그 휴리스틱 오탐/미탐 | 케이스 2 신뢰도 저하 | Bookmap 육안 대조로 튜닝(M3), 보수적 임계치, D4 단독 알림 금지 |
| 케이스 2의 체결 귀속 불완전 | 실현률 과대/과소 평가 | 리필 패턴 확인분만 합산, 알림에 "추정" 명시 |
| 100ms 스냅샷 사이의 플래시 이벤트 미관측 | 극단기 스푸핑 일부 미탐 | PERSIST 필터 목적상 오히려 무해 (지속 레벨만 의도로 취급) |
| 거래소 API 스펙 변경 | 파싱 실패 | 라이브러리 위임 + 파싱 실패 시 FEED_STALE 경보 |
| VPS 리전 지오블록 | 접속 불가 | 배포 전 사전 검증 (§11.3) |
| 임계치 초기값 부적합 | 알림 폭주/침묵 | M2~M6 튜닝 루프, D1 알림 off 기본값 |

## 15. 오픈 퀘스천

1. `SIZE_THRESHOLD` 초기값 — "1k"는 예시였음. 실제 BTC 현물 top-20에서 현실적인 대형 기준(BTC 수량 vs USDT 노셔널 중 무엇으로 정의할지 포함) 확정 필요 → M1 수집 데이터로 분포 확인 후 결정
2. 케이스 2 "상위 구간"의 범위 제한(예: 의도 레벨 ± N%) 필요 여부
3. 매도 의도(ask 벽) 대칭 지원을 v1에 포함할지 (구현 비용은 낮음 — 포함 권장)
4. 알림 언어/포맷: 한국어 고정 vs 템플릿화
5. Bookmap 육안 대조(M3) 시 비교 기준 데이터를 어떻게 기록할지 (스크린샷 vs Bookmap 녹화)

## 16. 참고 — 기각된 대안

- **경로 A (Bookmap 인더루프)**: Bookmap L1 Python API 애드온으로 구현. Absorption/Sweeps 독자 지표를 그대로 소비할 수 있으나, GUI 앱 24/7 상주(Windows/GPU VPS, RDP, 크래시 관리)라는 운영 부담과 Global+ 구독 비용 발생. 신뢰성 우선 원칙에서 헤드리스 서비스 대비 열위 → 기각. 단, Bookmap 독자 시그널이 필수로 판명되면 재검토
- **Bookmap 데이터 export 활용**: 메뉴 export는 체결(Time & Sales)만 CSV 지원, 오더북은 불가(.bmf는 암호화 바이너리). 실시간성도 없음 → 기각
- **diff depth 스트림으로 전체 북 유지**: 정밀하나 시퀀스 재조정 복잡도와 드리프트 리스크. v1 요구에 불필요 → 기각(v2 후보)
