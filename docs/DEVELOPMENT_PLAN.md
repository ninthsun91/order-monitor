# 개발 계획 및 진행 트래킹 — BTC 오더북 인텐트→실체결 모니터

> 이 문서는 [PRD](PRD_orderbook_intent_monitor.md)의 구현 계획(§13)을 실제 작업 단위로 분해하고 진행 상태를 기록한다.
> 이 문서에는 **진행 중인 작업만** 남긴다 — 결정 기록은 [DECISIONS.md](DECISIONS.md), 완료 마일스톤(M0–M5) 상세는 [MILESTONE_ARCHIVE.md](MILESTONE_ARCHIVE.md)로 분리 (2026-07-23). 마일스톤이 완료되면 그 절을 아카이브로 옮긴다.
> **규칙**:
> - 작업 완료 시 체크박스를 채우고, 단계 완료 시 상태 표를 갱신한다. PRD와 어긋나는 결정이 생기면 [DECISIONS.md](DECISIONS.md)에 남긴다. 요구사항의 원천은 항상 PRD이며, 이 문서는 "무엇이 언제 어디까지 되었는가"만 다룬다.
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
| M7 | W 주시 레벨 관측기 + 텔레그램 수신 명령 (PRD v1.13) | 🟠 검증 대기 | (구현 완료 2026-07-23, 실전 주시 1건 사이클 확인 잔여) |
| M8 | 멀티 거래소 확장 1단계 — 공통 기반 + Coinbase 어댑터, D1+D3+D5 (PRD v1.16 §5.5) | ⬜ 미착수 | |

상태 표기: ⬜ 미착수 · 🟡 진행 중 · 🟠 검증 대기 · ✅ 완료 · ⏸ 보류

M0–M5 상세(체크박스·구현 노트·검증 기록)는 [MILESTONE_ARCHIVE.md](MILESTONE_ARCHIVE.md). 이월 관찰 항목: M5 행 훈련(`kill -STOP` → PROCESS_DOWN → 자동 재시작 검증), M2 `D1Suppressed` 실전 사례 관찰 — 발생 시 아카이브의 해당 검증 기록에 추기.

---

## M6 — SQLite 영속화 + 임계치 튜닝 루프

- [x] SQLite 스키마: `events`, `intents` (PRD §12 — `walls`는 M1, `alerts_outbox`는 M4 선행. `trades_sample`(선택)은 미도입 — 필요 확인 시 별도) → **완료 2026-07-23** (`persistence/events.py`·`persistence/intents.py` — 스키마 형태는 결정 기록 2026-07-23 참고: events는 payload JSON 단일 테이블 + side/price 조회 승격, intents는 인텐트당 1행 + 상태 갱신·전이 이력은 events 소관, 키 `(run_started_at, intent_id)`)
- [x] 모든 디텍터 이벤트/상태 전이 기록 → **완료 2026-07-23** (`_emit` 관문에서 전 이벤트 적재 — payload는 JSON-lines 로그와 동일 직렬화라 로그/DB 동일 필드 조회. D5 인텐트는 등록 active → 확정 confirmed 래치(v1.9, 열린 상태) → 종국을 호출부 문맥 배선으로 기록 — evaluate() 경유 EXECUTION_CONFIRMED는 래치, on_d1_removed/reset() 경유는 종국)
- [x] 재시작 시 진행 중 intent를 DB에 `INTERRUPTED` 마킹 → **완료 2026-07-23** (startup()에서 열린 행(active·confirmed) 일괄 UPDATE — 정상 epoch 종료는 D5 reset()이 이미 종국을 기록하므로 여기 걸리는 건 크래시 잔여분. 복원은 하지 않음 — §12 원칙)
- [ ] 1주 벽 레지스트리 기록(`record_min_qty_btc` 이상) 분포 분석 → `SIZE_THRESHOLD`·`record_min_qty_btc` 재검토 (OQ #1 확정값 1000의 실데이터 검증 — v1.1 이후 D1 소스는 top-20이 아닌 벽 레지스트리)
- [ ] 나머지 임계치 1차 튜닝 (오탐/침묵 리뷰)
- [x] **(2026-07-22 백로그, PRD v1.11)** D4 재구현 — 레벨 흡수 방어 디텍터 (§8 D4 v1.11이 기준) → **완료 2026-07-23** (스펙 검토 보강 3건 = PRD v1.12 선행 확정 후 구현 — 결정 기록 2026-07-23. 1틱 이월 보정·epoch 시작 스트릭 재개시 포함, 관통·활성 게이트는 구조적 충족 — d4.py docstring. **VPS 배포 시 config.yaml 동시 수정 필수** — config.example.yaml 대조): `detectors/d4.py` 전면 교체(스트릭 생애 누적, 가시 리필 페어링 + 틱 단위 은닉 리필 대조, 배수 판정 + 래치/진행/종결), service 배선 복원, config 키 교체(`absorb_multiple`/`absorb_progress_step`/`absorb_min_events` 신설, `iceberg_margin_btc`·`iceberg_min_trades`·`realize_pct_above` 삭제 — **배포 시 VPS config.yaml도 동시 수정**, 엄격 스키마), `send_d4` 추가, d5의 `refill_above_lookup` 파라미터·`EXECUTION_INFERRED_ABOVE` 잔재 제거, 구 D4 단위 테스트 교체 + replay 골든 재생성. **관측 기록 포함**(§8 D4 v1.11): 흡수 인정 이벤트 단위 로그 + 스트릭 종료 요약 로그(발화 무관, absorbed > 0 전건) — 임계 미달 near-miss 분포가 `absorb_multiple` 튜닝의 직접 근거
- [x] **(2026-07-22 백로그)** → **완료 2026-07-23** (`apply_diff`가 `DiffResult(등록+소멸)` 반환, service가 "wall registered" 구조화 로그 — D4 스트릭 개시 배선의 선행 인프라 겸용) 추적 벽(관측 하한 100 이상) 수량 궤적 로깅 — 레지스트리 등록·소멸 시 구조화 로그 1줄씩. 현재는 소멸만 로그라 준임계 벽의 생애가 무기록 (`events` 테이블의 선행 단계, 근거: 결정 기록 2026-07-22 — 07-17 62.8k 사례). **범위 조정(v1.11)**: 리필 델타 기록은 D4 재구현의 관측 기록(흡수 인정 이벤트 단위 로그, 위 항목)이 포섭 — 이 항목은 등록/소멸 궤적만 담당
- [x] **(2026-07-22 백로그)** → **완료 2026-07-23** (에피소드 종가 기준 bid/ask 각 최근접 추적 벽 1개 — 가격·잔량·피크 동봉, 결정 기록 2026-07-23) D2 요약(verdict)에 근접 벽 컨텍스트 첨부 — 종가 부근 추적 벽의 peak/last(궤적 로깅 도입 시 리필량 포함)를 요약 메시지에 동봉해 `sell_absorbed_stall` 류 흡수 판정의 해석 근거 제공 (근거: 동일 사례 — 판정 자체는 적중했으나 알림만으로 배경 파악 불가)
- [ ] **(v1.2)** 오탐 지표 달성 확인: D5 오탐 주 1건 이하(목표 0), D1/D2 시간당 평균 3건 이하 — 초기 제안값, 실데이터로 재조정 후 확정 (PRD §13)
- [x] **(2026-08-01 실운영 피드백, PRD v1.15)** D1 REMOVED 출현↔소멸 페어링 보장 → **완료 2026-08-01** (07-31 63k 사례: 출현 알림 86초 뒤 소멸이 같은 버킷 쿨다운에 억제 — 발송된 스트릭의 소멸은 쿨다운 우회 무조건 발송, `D1Removed.announced` 필드 + dispatcher 분기. 결정 기록 2026-08-01)
- [ ] 확정 임계치를 `config.yaml`에 반영하고 결정 기록에 근거 기재
- [ ] (선택) 일일 요약 메시지 (감지 건수, 재연결 횟수, 업타임)

**완료 기준 (PRD)**: 1주 데이터 기반 임계치 1차 확정.

검증 기록:
- 2026-07-23 events/intents 영속화: pytest 432건 전건 통과 (신규 16건 — EventStore 단위 6: 직렬화/승격/canonical/이력 보존, IntentStore 단위 6: 전이/런 간 키/기동 마킹 멱등, service 파이프라인 4: 이벤트 기록·인텐트 풀사이클(등록→래치→종국)·epoch INTERRUPTED·크래시 재시작 마킹). 기존 replay 골든 무변경 (이벤트 목록 비교 — DB 기록은 부수 효과)
- 2026-08-01 D1 페어링 보장: pytest 439건 전건 통과 (신규 — d1 단위 4건: announced 전파 양경로·억제 스트릭·구 스트릭 마킹 불일치, dispatcher 2건: 쿨다운 내 우회 발송(63k 재현)·deduper 미기록, service e2e 1건: 출현 발송→쿨다운 내 tombstone 소멸 발송). dispatcher 변경만 되돌린 상태에서 재현 테스트 2건 실패 확인 — 운영 로그의 억제와 동일 동작
- 2026-07-23 D4 재구현 + 관측 보완 2건: pytest 314건 전건 통과 (신규 — D4 단위 34건 교체: 횡보 회전 배제·은닉 틱·1틱 이월 보정·발화 4조건·래치/진행/종결/재개시·관측 로그 2종, service 파이프라인 D4 감지 1건, config 신 스키마 수용/구 키 거부, replay 역전 픽스처 61000 벽 등록 확장 — 스트릭 누적 골든). 실전 검증은 배포 후 ([NEXT_STEPS.md](NEXT_STEPS.md) §3)

---

## M7 — W 주시 레벨 관측기 + 텔레그램 수신 명령 (PRD v1.13)

PRD §8 W·§9.5·§12.2가 기준. 착수 전 v1.13 개정 이력과 결정 기록 2026-07-23(W 3행)을 읽을 것.

- [x] 텔레그램 수신 루프 (`alerting/telegram_commands.py`): getUpdates 롱폴 + 지수 백오프, chat_id 검증, `update_id` offset 영속(`kv`), 명령 파서(`/watch <price>`·`/watch <lo>-<hi>`·`/unwatch`·`/watching`) + 성공/오류 응답 — 발송기와 동일 격리(수신 장애가 파이프라인 불침범, PRD §9.5) → **완료 2026-07-23** (`poll_once` 분리로 결정적 테스트, 그룹 챗 `/watch@BotName` 접미 수용, service 층에서 핸들러 예외 격리 한 겹 추가)
- [x] 봉 조립 (`state/candles.py`): aggTrade `T` 버킷(15m/1h), 마감가 = 버킷 내 마지막 체결가, epoch 공백 낀 봉 마킹(무효화 판정 제외용) — 시계 정책 §11.1 (버킷 = exchange_time, 리포트 주기 = monotonic) → **완료 2026-07-23** (gap 오염은 경계를 걸치면 직전 봉+재개 봉 모두 전파, 초기 봉은 재시작 공백으로 tainted 시작, 무체결 구간 빈 봉 미생성)
- [x] W 코어 (`detectors/watch_level.py` — 모듈 배치는 결정 기록 참고): 구역/접촉 밴드, 역할·회차(구역 내부 등록 시 "대기" 포함), 단방향 무제한 계측(taker 분리 + excursion), 무효화(연속 마감 카운트 + 공백 리셋), epoch 상호작용(계측 = 적재 지속, 판정만 보류 — §5.4) → **완료 2026-07-23** (역할은 회차마다 재판정 — 반대편 재진입 시 전환 + 이탈 카운트 리셋, 대기 판정은 `prev_zone_rel` 이력 기반으로 일원화)
- [x] 리포트 포맷·발송 배선 (dispatcher): 첫 접촉 / 주기(활동 게이팅) / 최종(종료 사유 병기) / `/watching`, 이탈 마감 연속 카운트 표시, 구역 내 벽 라인, 공백 플래그 → **완료 2026-07-23** (구역 내 벽 = 계측 영향권(구역 ∪ excursion) 필터·경계 근접순 2개 캡 — `_nearby_wall_line` 재사용이 아닌 신설, §9.3 예시의 65,500 사례 포섭)
- [x] SQLite 영속 (persistence): `watch_levels`(등록/해소 즉시 + 리포트·회차 종료·무효화 flush) + `kv`, 재시작 복원(회차 단절·카운트 리셋·공백 플래그), 무효화 마킹 미발송 행 재발송 후 삭제 (§9.4 보장 — outbox 미사용, 전용 테이블 방식) → **완료 2026-07-23** (outbox처럼 완성 텍스트를 `final_text`로 선기록 — 재시작 재전송에 상태 재구성 불필요)
- [x] config: `watch` 섹션 5키 + 불변조건(timeframe enum·closes 정수 ≥ 1·interval ≥ 60) — 엄격 스키마 `_build_section` 패턴 유지 → **완료 2026-07-23**. **배포 시 VPS config.yaml 동시 수정** (config.example.yaml 대조 — v1.12 키 교체와 함께. 로컬 config.yaml은 반영 완료)
- [x] 단위 + 파이프라인 테스트: 역할/회차 경계(관통-복귀·양방향 재진입·구역 내부 등록), 계측 범위(깊은 excursion·급락), 무효화(연속·꼬리 무시·공백 리셋·버퍼), 활동 게이팅, 명령 파서(오입력·chat_id 불일치), 복원(누적 보존·미발송 최종 리포트 재발송) → **완료 2026-07-23** (아래 검증 기록)
- [x] replay: 주시 활성 픽스처 시나리오 (재연결 공백 → 봉 판정 제외·카운트 리셋 경로 포함) → **완료 2026-07-23** (`watch_invalidation.jsonl` + runner에 `watch`/`unwatch`/`tick` 컨트롤 레코드 — 실제 명령 핸들러·staleness 틱 경로 그대로 구동, 골든 + 이중 재생 결정성)

**완료 기준 (PRD §13 M7)**: 결정적 replay 테스트 통과 ✅ + 실전 주시 1건 등록→주기 리포트→해소 사이클 확인 (**잔여** — 배포 후. 실토큰 로컬 테스트 시 VPS 정지 필수, getUpdates 단일 소비자).

검증 기록:
- 2026-07-23 M7 구현: pytest 406건 전건 통과 (신규 92건 — config 7, candles 9, watch_level 28, persistence 9, telegram_commands 24, dispatcher W 포맷 8, service 파이프라인 5(등록→접촉→리포트→무효화 풀사이클·epoch 공백 중 계측 지속·복원·미발송 final 재전송·수동 해소), replay 골든+결정성 2). 기존 replay 골든 무변경(기존 경로 무간섭) 확인

---

## M8 — 멀티 거래소 확장 1단계: 공통 기반 + Coinbase 어댑터 (PRD v1.16)

PRD §5.5(어댑터 계약·채널 매핑·한계 수용)·§12(마이그레이션 4건)·§10(`exchanges` 섹션)이 기준. 착수 전 v1.16 개정 이력과 결정 기록 2026-08-02를 읽을 것. 스코프 불변식: **D1+D3+D5만, 로컬 풀북 유지 금지, 거래소 간 실시간 결합 금지** (§5.5).

공통 기반 (거래소 수와 무관하게 1회):

- [ ] DB 마이그레이션 4건 (PRD §12 v1.16): `walls` PK 재생성 `(exchange, side, price)` · `events` ALTER `exchange` 컬럼+인덱스 · `intents` PK 재생성 `(exchange, run_started_at, intent_id)` · `alerts_outbox` UNIQUE 재생성 — 기존 행 전부 `'binance'` 백필. 재생성은 신규 테이블 생성→복사→rename (v1.8 ALTER 선례는 events에만 해당)
- [ ] config `exchanges` 섹션 (PRD §10): 거래소별 `symbol`/`size_threshold_btc`/`record_min_qty_btc`/`band_pct`, 엄격 `_build_section` 패턴 + 불변조건 3건. 섹션 미기재 = 바이낸스 단독(기존 config 무변경 기동 보장)
- [ ] service 다중 파이프라인 재배선: 거래소별 독립 인스턴스 묶음(state·detectors·health/epoch), 한 거래소 장애가 타 거래소 판정에 불침범. 알림 dispatcher venue 표기 파라미터화 (§9.3)
- [ ] persistence 계층 exchange 스코프 배선: WallStore load/sync·IntentStore·EventStore·outbox 전부 자기 거래소 행만

Coinbase 어댑터 (PRD §5.5 표):

- [ ] WS 클라이언트: `level2_batch` + `matches` + `ticker` 구독, 재연결·keepalive (기존 ingestion 골격 재사용)
- [ ] 정규화 파서: 원시 프레임 → `DiffDepthEvent`(절대잔량) / `AggTradeEvent` / top-1 `DepthSnapshot`(ticker 합성)
- [ ] 레지스트리 등록 가격대역 필터 `band_pct` — full-book 원거리 쓰레기 주문 차단 (실측: $0.01에 65,000 BTC 매수 주문 실재)
- [ ] epoch 규칙: 시퀀스 갭·heartbeat trade_id 갭 → epoch 종료 (diff U/u 갭 등가). `matches` 드랍의 REST 갭필 보정은 실측 드랍률 보고 결정 (PRD §14)
- [ ] replay: Coinbase 원시 프레임 픽스처 캡처(`scripts/capture_stream.py` 확장) + 골든 — 재연결·순서 역전·갭 시나리오 포함 (§13 필수 시나리오 준용)
- [ ] 낮은 관측 플로어로 events 분포 수집 개시 → 거래소별 임계 확정 (오픈 퀘스천 #6)

**완료 기준 (PRD §13 M8)**: Coinbase replay 통과 + **기존 바이낸스 replay 골든 무변경** + 실전 Coinbase D1 출현→소멸 사이클 1건 + 분포 수집 개시. Kraken·Bitfinex 어댑터는 M8 완료 후 후속 마일스톤.

---

## 오픈 퀘스천 트래킹 (PRD §15)

| # | 질문 | 결정 시점 | 상태 | 결론 |
|---|---|---|---|---|
| 1 | `SIZE_THRESHOLD` 초기값 (BTC 수량 vs USDT 노셔널) | M1 데이터 수집 후 / 최종 M6 | ✅ 확정 | BTC 수량 기준, **1000 BTC** (2026-07-11, 사용자 시장 판단 — 전달력 있는 유동성 기준). 기록 하한은 별도 100 BTC(`record_min_qty_btc`). M6에서 실데이터 재검토 여지만 유지 |
| 2 | 케이스 2 "상위 구간" 범위 제한 필요 여부 | M4 설계 시 | ✅ 종결 (소멸) | 케이스 2 자체가 v1.11(2026-07-22)에서 폐지 — 귀속 해석을 하지 않으므로 거리 개념이 성립하지 않음. 상위 구간 활동은 D4 흡수 방어가 자기 레벨에서 독립 판정 (결정 기록 2026-07-22 D4 재설계 참고) |
| 3 | 매도 의도(ask 벽) 대칭 지원 v1 포함 여부 (PRD는 포함 권장) | M0~M1 설계 시 | ✅ 확정 | v1 포함 — 벽 레지스트리 설계 논의(2026-07-11)에서 사용자가 ask/bid 양측 추적을 전제로 함. 구현 비용도 낮음 |
| 4 | 알림 언어/포맷: 한국어 고정 vs 템플릿화 | M2 착수 시 | ✅ 확정 | 한국어 고정 (2026-07-12, 결정 기록 참고 — 템플릿화는 요구 없는 유연성으로 기각) |
| 5 | Bookmap 대조 기록 방식 (스크린샷 vs 녹화) | M3 착수 시 | ✅ 종결 (기각) | Bookmap 대조 자체를 M3에서 제외 (2026-07-13, 사용자 확정 — 결정 기록 참고). M3 완료 기준은 결정적 replay 테스트로 일원화 |
| 6 | 신규 거래소별 `size_threshold_btc`·`record_min_qty_btc` 확정값 (PRD §15 #6, v1.16) | M8 분포 수집 후 | ⬜ 미결 | 1000은 바이낸스 스코프 값 — 실측(2026-08-02) 신규 3곳 최대 오더 30~60 BTC. §10의 100/10은 잠정 표기 |
