# NEXT_STEPS — 남은 작업 가이드

작성: 2026-07-22, 갱신: 2026-08-02 (PRD v1.16 멀티 거래소 확장 스펙 확정 — M8 신설). **이 문서는 스냅샷이다** — 진행 상태의 진실원은 [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md)(체크박스), 결정의 진실원은 [DECISIONS.md](DECISIONS.md), 요구사항의 진실원은 [PRD_orderbook_intent_monitor.md](PRD_orderbook_intent_monitor.md)다. 작업 착수 전 반드시 두 문서의 해당 절을 먼저 읽을 것. 항목을 완료하면 DEVELOPMENT_PLAN 체크박스를 갱신하고 이 문서의 해당 절을 삭제/갱신한다.

작업 규칙 (CLAUDE.md): 스펙 변경은 **문서(PRD·결정 기록) 갱신 → 사용자 확인 → 코드** 순서. 커밋은 의미 단위마다, push는 명시 요청 시에만.

(2026-07-23 완료되어 삭제된 절: D4 재구현 — PRD §8 D4 v1.12 기준 구현 완료, 관측 보완 2건 — 등록/소멸 궤적 로깅 + D2 요약 근접 벽 컨텍스트. DEVELOPMENT_PLAN M6 체크박스·검증 기록·결정 기록 2026-07-23 참고. §0 "VPS 배포 대기" 절은 배포가 수시 진행되는 일상 운영이 되어 상시 트래킹에서 제외 — 배포 시 config.yaml 대조 기준은 [config.example.yaml](../config.example.yaml), 엄격 스키마라 키 불일치 시 기동 실패.)

---

## 1. M6 본류 — 튜닝 루프 (영속화 완료, 실데이터 대기)

**참조**: DEVELOPMENT_PLAN "M6" 절 전체, PRD §12(스키마)·§13(오탐 지표).

(2026-07-23 완료: `events`/`intents` 테이블 + 전 디텍터 이벤트 DB 기록 + 재시작 시 열린 인텐트 `INTERRUPTED` 마킹 — 스키마 형태는 결정 기록 2026-07-23, pytest 432건. 배포 시 config 변경 없음 — 테이블은 기동 시 자동 생성. **1주 데이터 수집 시계는 이 코드의 배포 시점부터 시작.**)

- 1주 레지스트리 분포 분석 → `size_threshold_btc`(1000)·`record_min_qty_btc`(100) 재검토 (오픈 퀘스천 #1의 실데이터 검증)
- 임계치 1차 튜닝 + 오탐 지표 확정 (D5 오탐 주 1건 이하, D1/D2 시간당 3건 이하 — PRD §13)
- (선택) 일일 요약 메시지 (2026-07-23 사용자 확정 — 영속화 범위에서 제외, 별도 작업)

**튜닝 대기 잠정값 목록** (실데이터로 재검토할 것들):
- D4: `absorb_multiple` 2.0, `absorb_progress_step` 0.5, `absorb_min_events` 5, `refill_window_ms` 500 (전부 v1.11 잠정 — **`d4 streak summary` near-miss 분포가 근거 데이터**, 가시/은닉 분해 타당성은 `d4 absorb event` 로그로 검증)
- D2 verdict: `summary_absorb_delta_min` 0.35, `summary_move_min_pct` 0.1% (n=13 잠정 — 결정 기록 2026-07-13)
- D2 시간대별 기준선(RVOL-TOD)은 v1 보류 — M6 재검토 (결정 기록 2026-07-12)

---

## 2. M7 — W 주시 레벨 관측기 + 텔레그램 수신 명령 (구현 완료, 실전 확인 잔여)

**참조**: PRD v1.13 (§8 W, §9.5, §12.2) + DEVELOPMENT_PLAN "M7" 절(체크박스 전부 완료, 검증 기록 2026-07-23 — pytest 406건). 잔여 = 완료 기준의 실전 사이클 1건(`/watch` 등록 → 주기 리포트 → 해소) 확인 — 배포 후 자연 진행. **배포 시 VPS config.yaml에 `watch` 섹션 5키 + `telegram.command_chat_ids` 추가 필수** (로컬 config.yaml은 반영 완료, 대조 기준 config.example.yaml). 실토큰 로컬 수신 테스트는 getUpdates 단일 소비자 제약으로 VPS 정지 후 진행 (PRD §9.5). DB 테이블(`watch_levels`·`kv`)은 기동 시 자동 생성 — 마이그레이션 불필요.

---

## 3. M8 — 멀티 거래소 확장 1단계 (스펙 확정, 미착수)

**참조**: PRD v1.16 (§5.5 신설·§10 `exchanges`·§12 마이그레이션 4건·§13 M8) + DEVELOPMENT_PLAN "M8" 절 + 결정 기록 2026-08-02. 스코프: 공통 기반(다중 파이프라인 재배선·DB 마이그레이션·config) + Coinbase 어댑터, **D1+D3+D5만** (D2·D4·W 제외). 착수 순서 권장: DB 마이그레이션·config → service 재배선(바이낸스 단독으로 기존 replay 골든 무변경 확인) → Coinbase 어댑터 → 픽스처·골든 → 분포 수집. 신규 거래소 임계는 오픈 퀘스천 #6 (분포 수집 후 확정 — §10 값은 잠정). Kraken·Bitfinex는 M8 완료 후 후속 마일스톤.

---

## 4. 결정 대기 / 비작업 메모

- 오픈 퀘스천 #1(M6 재검토)·**#6(M8 — 신규 거래소 임계 확정값, 분포 수집 후)** 외 열린 질문 없음 (DEVELOPMENT_PLAN 오픈 퀘스천 트래킹 표)
- D5 케이스 1 래치·D1 스트릭 억제·**D4 방어 감지**의 **실전 검증**은 배포 후 다음 대형/준임계 벽 이벤트에서 자연 확인 — 별도 작업 아님
- ② 은닉 리필의 1틱 이월 보정(v1.12)·수용 오차는 배포 후 `d4 absorb event` 로그 리뷰로 실측 검증 (M6 튜닝 루프에 포함)
- 재시작 시 인텐트·누적 리셋은 **의도된 수용 한계** (v1.9 사용자 확정) — 복원 로직을 추가하지 말 것 (PRD §12 원칙, 예외는 wall_registry(§12.1)와 W 주시 레벨(§12.2, v1.13)의 둘뿐). D4 스트릭도 동일 — epoch/재시작 시 재개시가 스펙 (v1.12)
