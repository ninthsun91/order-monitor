# NEXT_STEPS — 남은 작업 가이드

작성: 2026-07-22 (PRD v1.11 확정 세션 직후). **이 문서는 스냅샷이다** — 진행 상태의 진실원은 [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md)(체크박스·결정 기록)이고, 요구사항의 진실원은 [PRD_orderbook_intent_monitor.md](PRD_orderbook_intent_monitor.md)다. 작업 착수 전 반드시 두 문서의 해당 절을 먼저 읽을 것. 항목을 완료하면 DEVELOPMENT_PLAN 체크박스를 갱신하고 이 문서의 해당 절을 삭제/갱신한다.

작업 규칙 (CLAUDE.md): 스펙 변경은 **문서(PRD·결정 기록) 갱신 → 사용자 확인 → 코드** 순서. 커밋은 의미 단위마다, push는 명시 요청 시에만.

---

## 0. 운영 — VPS 배포 대기 (코드는 완성, 배포만 남음)

로컬 main(= origin/main, `49817d0`)에는 VPS 미반영 변경이 쌓여 있다:

- **D1 APPEARED 스트릭당 1회 억제** (PRD §8 D1 v1.8, §12.1) — walls DB에 `appeared_alerted_since` 컬럼 자동 마이그레이션 포함 (`persistence/walls.py`가 기동 시 `ALTER TABLE`)
- **D5 케이스 1 래치화** (PRD §8 D5 v1.9) — 확정 후 무상한 진행 알림 + `CONFIRMED_CLOSED`
- VPS 추출 스크립트 2종 (`scripts/fetch_vps_*.sh`), 테스트 정리

**배포 절차**: VPS에서 `git pull` → `systemctl restart order-monitor`. **config.yaml 수정 불필요** (이번 변경들은 config 키 추가/삭제 없음 — `send_d1`은 VPS에 이미 on). DB 마이그레이션은 자동.

**배포 후 검증 포인트**: ① 재시작 직후 기존 벽들의 D1 APPEARED 재발화가 **텔레그램에 안 오는지** (로그에는 남음 — 정상) ② 다음 케이스 1 확정 후 80%+ 진행 알림이 계속 오는지 ③ 정시 호가벽 리포트 정상 수신.

---

## 1. D4 재구현 — 레벨 흡수 방어 디텍터 (개발 백로그 최우선)

**스펙 기준**: PRD **§8 D4 (v1.12)** 전체 — 산식(가시 리필 페어링 + 틱 단위 은닉 대조 + 1틱 이월 보정), 배수 판정, 스트릭/래치/진행/종결 수명(epoch 시작 시 재개시 포함), **관측 기록** 문단까지 구현 범위다.
**참조**: PRD §9.1(발송 정책 `send_d4`)·§9.2(dedup 키)·§9.4(outbox 미적용)·§10(config 키), v1.11 개정 주석(설계 배경), DEVELOPMENT_PLAN **결정 기록 2026-07-22 "D4 재설계 확정"** 행(대안 기각 근거 — 재논의 방지용), M6 체크박스 "D4 재구현" 항목(구현 체크리스트).

구현 시 주의 (스펙에 명시되어 있으나 놓치기 쉬운 것):

- **은닉 리필은 반드시 100ms 틱 단위 대조** — 스트릭/episode 전체 뭉뚱그림 대조는 v1.2가 폐기한 경로 소실 결함의 재발 (PRD §8 D4 산식 문단에 금지 조항 명시)
- **관측 기록 2종은 발화와 별개** — 인정 이벤트 단위 로그 + 스트릭 종료 요약(absorbed > 0 전건, 발화 무관). near-miss 분포가 `absorb_multiple` 튜닝의 직접 근거라 빠지면 M6 튜닝 불가
- **config 키 교체는 구현 커밋과 동시** (엄격 스키마 — 선행 금지 주석이 §10에 있음): `absorb_multiple`/`absorb_progress_step`/`absorb_min_events`/`send_d4` 신설, `iceberg_margin_btc`·`iceberg_min_trades`·`realize_pct_above` 삭제. **배포 시 VPS config.yaml 동시 수정 필수** (미수정 시 기동 실패 — v1.5 `intent_ttl_seconds` 전례)
- **D5 잔재 제거**: `refill_above_lookup` 파라미터, `EXECUTION_INFERRED_ABOVE` 상태, 케이스 2 관련 코드 경로. `d5.reset()`을 `d4.reset()`보다 먼저 호출하던 순서 제약(결정 기록 2026-07-14)은 케이스 2 폐지로 소멸하는지 확인
- **테스트**: 구 `tests/test_d4.py`(구 스펙 보존용이었음) 전면 교체 + replay 골든 재생성. 횡보 회전 배제(틱별 mismatch ≈ 0)·비관통 조건·near-miss 로그를 커버하는 합성 시나리오 필수
- 비관통 판정은 D3의 관통 판정(§8 D3 조건 3) 재사용 — `contact.py` 공유 트래커 활용, 단 D4는 episode 스코프가 아니라 스트릭 생애 누적임에 유의

---

## 2. 관측 보완 2건 (D4와 독립, 저비용 — 순서 무관)

**참조**: DEVELOPMENT_PLAN M6 체크박스 2건 + 결정 기록 2026-07-22 "07-17 62.8k 사례 검수" 행 (배경 실측).

- **추적 벽 등록/소멸 궤적 로깅** — 레지스트리 등록·소멸 시 구조화 로그 1줄씩. 리필 델타 부분은 D4 관측 기록이 포섭하므로 **등록/소멸만** (범위 조정 v1.11 주석 참고)
- **D2 요약(verdict)에 근접 벽 컨텍스트 첨부** — 에피소드 종가 기준 bid/ask 각 최근접 추적 벽 1개(가격·last·peak 동봉, 거리 제한·config 키 없음 — 결정 기록 2026-07-23). 구현 위치: `alerting/dispatcher.py` D2 요약 포맷 + wall_registry 조회

---

## 3. M6 본류 — SQLite 영속화 + 튜닝 루프

**참조**: DEVELOPMENT_PLAN "M6" 절 전체, PRD §12(스키마)·§13(오탐 지표).

- `events`/`intents` 테이블 (모든 디텍터 이벤트·상태 전이 DB 기록 — 현재는 JSON-lines만)
- 재시작 시 진행 중 intent DB `INTERRUPTED` 마킹
- 1주 레지스트리 분포 분석 → `size_threshold_btc`(1000)·`record_min_qty_btc`(100) 재검토 (오픈 퀘스천 #1의 실데이터 검증)
- 임계치 1차 튜닝 + 오탐 지표 확정 (D5 오탐 주 1건 이하, D1/D2 시간당 3건 이하 — PRD §13)
- (선택) 일일 요약 메시지

**튜닝 대기 잠정값 목록** (구현 완료 후 실데이터로 재검토할 것들):
- D4: `absorb_multiple` 2.0, `absorb_progress_step` 0.5, `absorb_min_events` 5, `refill_window_ms` 500 (전부 v1.11 잠정 — near-miss 요약 로그가 근거 데이터)
- D2 verdict: `summary_absorb_delta_min` 0.35, `summary_move_min_pct` 0.1% (n=13 잠정 — 결정 기록 2026-07-13)
- D2 시간대별 기준선(RVOL-TOD)은 v1 보류 — M6 재검토 (결정 기록 2026-07-12)

---

## 4. 결정 대기 / 비작업 메모

- **오픈 퀘스천 #4 이후 열린 질문 없음** — #1만 M6 재검토 예정, #2는 v1.11로 종결 (DEVELOPMENT_PLAN 오픈 퀘스천 트래킹 표)
- D5 케이스 1 래치·D1 스트릭 억제의 **실전 검증**은 배포(§0) 후 다음 대형 벽 이벤트에서 자연 확인 — 별도 작업 아님
- 재시작 시 인텐트·누적 리셋은 **의도된 수용 한계** (v1.9 사용자 확정) — 복원 로직을 추가하지 말 것 (PRD §12 원칙, 예외는 wall_registry뿐)
