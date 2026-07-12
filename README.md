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
