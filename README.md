# order-monitor

Binance Spot BTC/USDT 오더북·체결을 WebSocket으로 감시하고, 대형 물량벽(D1)과
볼륨 버스트(D2)를 Telegram으로 알리는 단일 프로세스 headless 서비스.
요구사항 원천은 [PRD](PRD_orderbook_intent_monitor.md), 진행 상황은
[DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) 참고.

## 요구 사항

- Python 3.12
- Telegram 봇 토큰 (`TELEGRAM_BOT_TOKEN` 환경변수로만 주입 — 파일/코드에 저장 금지)

## 로컬 실행

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

cp config.example.yaml config.yaml   # telegram.chat_id 수정 (음수 그룹 ID는 따옴표 필수)
export TELEGRAM_BOT_TOKEN=<토큰>

order-monitor --config config.yaml --log-file order_monitor.log
```

테스트: `pytest`

## 배포 환경 실행 (VPS / Ubuntu 24.04)

전체 절차는 **[deploy/RUNBOOK.md](deploy/RUNBOOK.md)** 를 따른다. 요약:

```bash
# 0. (필수 선행) Binance 지오블록 검증 — 미국 리전 불가, RUNBOOK §0
# 1. 설치
git clone git@github.com:<owner>/order-monitor.git /opt/order-monitor
cd /opt/order-monitor
python3 -m venv .venv && .venv/bin/pip install -e .
cp config.example.yaml config.yaml   # chat_id 수정

# 2. 토큰 주입 (systemd EnvironmentFile — 여기에만 존재)
printf 'TELEGRAM_BOT_TOKEN=<토큰>\n' > /etc/order-monitor/env
chmod 600 /etc/order-monitor/env

# 3. systemd 기동 (Restart=always)
cp deploy/order-monitor.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now order-monitor
```

기동 확인: 로그에서 `epoch started` + `volume baseline bootstrapped`.

```bash
systemctl status order-monitor
tail -f /var/lib/order-monitor/order_monitor.log | grep -E 'epoch|bootstrap|error'
```

운영(재시작·config 변경·코드 업데이트·토큰 교체)은 [RUNBOOK §6](deploy/RUNBOOK.md) 표 참고.

## 알림 용어 정의 (D2 볼륨 버스트)

텔레그램 채널 고정 공지로 쓰기 좋은 요약. 수치는 config 기본값 기준
([config.example.yaml](config.example.yaml) — 변경 시 이 정의의 수치도 달라진다).

- **기준선** — 최근 24시간의 분당 평균 체결량(BTC). 알림 임계는
  `max(30 BTC, 기준선 × 10)` — 시장 유동성을 따라 움직이는 상대 임계.
- **델타비** — `|매수 − 매도| ÷ 총 체결량` (테이커 기준, 0~1). 체결 쏠림의 정도.
  - **≥ 0.5 → 방향성 매수/매도**: 한쪽이 시장가로 밀어붙이는 흐름
  - **0.2~0.5 → 혼합**: 쏠림은 있으나 뚜렷하지 않음
  - **≤ 0.2 → 양방향(흡수성 후보)**: 대량 체결인데 방향이 안 남 — 가격까지 안
    움직였다면 지정가 물량이 받아내고 있다는 신호일 수 있음
- **시작 알림** — 60초 창 체결량이 임계에 닿는 순간 발송. 수치·델타비는 그
  60초 창 기준.
- **요약 알림** — 버스트가 식고(임계의 절반 미만) 10분간 재점화가 없으면 확정
  종료로 보고 발송. 구간 시각·누적·델타비는 에피소드 전체 기준. 구간 시작은
  온셋 알림보다 최대 1분 앞선다(온셋 순간의 60초 창 첫 체결부터).
  "평상시 N분치의 X배"는 누적량을 같은 길이의 평상시 체결량
  (분당 기준선 × 구간 분수)과 비교한 배수.
- **판정** — 요약 전용. 델타비(테이커 쏠림)에 실제 가격 반응을 결합한 종합
  판단. 요약이 종료 +10분 뒤에 발송되는 점을 이용해, 그 시점의 가격("요약
  시점" — 에피소드 종가 대비 %)까지 근거로 쓴다.
  - **관철** — 쏠린 방향으로 가격이 움직였고(≥ 0.1%) 요약 시점까지 유지
  - **흡수 (정체)** — 쏠림(델타비 ≥ 0.35)인데 가격 변화가 0.1% 미만. 반대편
    지정가 물량이 받아냄 — 매도 흡수 = 지지 후보, 매수 흡수 = 저항 후보
  - **흡수 (되돌림)** — 가격이 밀렸지만 요약 시점에 에피소드 시가를 회복
  - **양방향 충돌** — 쏠림 자체가 없음(델타비 ≤ 0.2). 대량 체결인데 방향이 안 남
  - **혼합** — 그 사이 구간(델타비 0.2~0.35), 판단 유보
