# 배포 런북 — Hostinger KVM / Ubuntu 24.04 (D1&D2 우선 배포)

방식: **git pull + venv + systemd** (Docker 미사용 — 결정 기록 2026-07-12).
경로 규약: 코드 `/opt/order-monitor`, 가변 데이터(로그·DB) `/var/lib/order-monitor`, 시크릿 `/etc/order-monitor/env`.

## 0. 사전 검증 — Binance 지오블록 (VPS 발급 직후, 다른 작업 전에)

Binance는 미국 등 일부 리전 IP를 차단한다(HTTP 451). **미국 리전 금지**, 싱가포르/유럽 권장.
발급받은 VPS에서 서비스가 실제 쓰는 세 경로를 전부 확인:

```bash
# REST (D2 기준선 부트스트랩 경로) — {} 와 kline 배열이 나와야 정상, 451이면 리전 변경
curl -sw '\n%{http_code}\n' https://api.binance.com/api/v3/ping
curl -sw '\n%{http_code}\n' "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1"

# WS (설치 후 실행 — 5초 내 depth 메시지 수신이면 정상)
/opt/order-monitor/.venv/bin/python - <<'EOF'
import asyncio, aiohttp
async def main():
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(
            "wss://stream.binance.com:9443/stream?streams=btcusdt@depth20@100ms", timeout=10
        ) as ws:
            msg = await asyncio.wait_for(ws.receive(), 5)
            print("WS OK:", str(msg.data)[:80])
asyncio.run(main())
EOF
```

## 1. 시스템 준비

```bash
apt update && apt install -y git python3-venv   # Ubuntu 24.04의 python3 = 3.12 (요구 버전)
python3 --version                                # 3.12.x 확인
timedatectl                                      # "System clock synchronized: yes" 확인
                                                 # (wall_registry 시각 필드가 wall-clock — NTP 전제, 결정 기록 2026-07-11)
adduser --system --group --home /var/lib/order-monitor monitor
```

## 2. 코드 설치

```bash
# GitHub read-only deploy key: ssh-keygen -t ed25519 -f /root/.ssh/order_monitor_deploy
# → 공개키를 레포 Settings > Deploy keys에 등록
git clone git@github.com:<owner>/order-monitor.git /opt/order-monitor
cd /opt/order-monitor
python3 -m venv .venv
.venv/bin/pip install -e .
```

## 3. 설정 + 시크릿

```bash
cp config.example.yaml config.yaml     # telegram.chat_id 수정 (음수 그룹 ID는 따옴표 필수)

mkdir -p /etc/order-monitor
printf 'TELEGRAM_BOT_TOKEN=<토큰>\n' > /etc/order-monitor/env
chmod 600 /etc/order-monitor/env && chown root:root /etc/order-monitor/env
chown -R monitor:monitor /var/lib/order-monitor
```

토큰은 이 env 파일에만 존재한다 — config·코드·레포·셸 히스토리에 넣지 않는다 (PRD §9.3).

## 4. systemd 기동

```bash
cp deploy/order-monitor.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now order-monitor
systemctl status order-monitor          # active (running) 확인
```

외부 워치독 (M5 — PROCESS_DOWN, PRD §11.1): 하트비트 파일(`/var/lib/order-monitor/heartbeat`)
mtime을 60s 주기로 점검, 나이 > 60s(스크립트 상수, `deploy/watchdog_check.py` 상단)면
정지 알림 1회 + `systemctl restart` + 해소 시 통지 1회. **서비스 유닛(하트비트 기록 포함)을
먼저 갱신·재시작한 뒤** 타이머를 켠다 — 순서가 반대면 하트비트 파일 부재로 오탐:

```bash
cp deploy/order-monitor-watchdog.service deploy/order-monitor-watchdog.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now order-monitor-watchdog.timer
systemctl list-timers order-monitor-watchdog.timer   # NEXT 시각 확인
```

유지보수로 서비스를 세울 때는 `systemctl stop order-monitor`만 하면 된다 —
워치독이 `is-active` == inactive(의도적 정지)를 인식해 조용히 스킵한다.

기동 직후 확인할 것: 텔레그램 수신 대기 없이 로그에서 `epoch started` + `volume baseline bootstrapped`.

```bash
tail -f /var/lib/order-monitor/order_monitor.log | grep -E 'epoch|bootstrap|error'
```

## 5. 로그 이중 기록 조정

앱 로그는 `RotatingFileHandler`(파일)와 stdout(→ journald) 양쪽에 남는다. 파일 쪽이 주 소비처이고
journald는 기동 실패(토큰 미설정 등 로깅 셋업 이전 에러) 확인용으로 유지하되 용량만 제한:

```bash
mkdir -p /etc/systemd/journald.conf.d
printf '[Journal]\nSystemMaxUse=500M\n' > /etc/systemd/journald.conf.d/order-monitor.conf
systemctl restart systemd-journald
```

## 6. 운영 절차

| 작업 | 명령 |
|---|---|
| 상태/로그 | `systemctl status order-monitor` / `journalctl -u order-monitor -e` / 로그 파일 tail |
| 재시작 | `systemctl restart order-monitor` (인메모리 상태는 설계상 리셋, 벽 레지스트리는 SQLite 복원 — PRD §12) |
| config 변경 | `/opt/order-monitor/config.yaml` 수정 → restart (핫 리로드 없음) |
| 코드 업데이트 | `cd /opt/order-monitor && git pull && .venv/bin/pip install -e . && systemctl restart order-monitor` |
| 토큰 교체 | `/etc/order-monitor/env` 수정 → restart |
| 워치독 상태 | `systemctl list-timers order-monitor-watchdog.timer` / `journalctl -u order-monitor-watchdog -e` |

## 7. 배포 후 검증 (M5 완료 기준)

**행 훈련(drill)** — 워치독 배포 직후 1회, PROCESS_DOWN 경로의 엔드투엔드 실증
(희소 이벤트 대기가 아니라 능동 유발 가능):

```bash
kill -STOP $(systemctl show -p MainPID --value order-monitor)   # 이벤트 루프 동결
# ~2분 내(나이 임계 60s + 타이머 주기 60s) 기대 시퀀스:
#   ① Telegram PROCESS_DOWN 수신 → ② 자동 재시작(systemctl restart는 SIGTERM→
#   SIGKILL이라 STOP 상태여도 종료됨) → ③ 다음 주기 해소 통지 수신
journalctl -u order-monitor-watchdog -e                          # 판정 로그 대조
```

7일 무인 운영 검증:

- 조용한 실패 0건 — 로그에서 단절/재시작 이벤트마다 대응 통지(FEED_STALE·재연결·PROCESS_DOWN)가 있는지 대조
- Binance 24h 강제 단절이 매일 발생 → 자동 재연결 + epoch 재시작 로그 확인
- 하트비트 오탐 0건 — 정상 운영 중 PROCESS_DOWN이 발화했다면 임계(60s) 재검토
- `D1Suppressed` 실전 사례 발생 시 docs/MILESTONE_ARCHIVE.md M2 검증 기록에 추기 (이월 항목)
- D5 케이스 1 실전 알림 발생 시 M4 검증 기록에 추기 (이월 항목)
