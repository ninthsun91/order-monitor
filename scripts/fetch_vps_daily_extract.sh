#!/usr/bin/env bash
# VPS(order-monitor)에서 최근 N일치 로그·DB 데이터를 로컬로 추출한다.
# 경로 규약은 deploy/RUNBOOK.md 기준: 코드 /opt/order-monitor, 가변 데이터 /var/lib/order-monitor.
#
# 사용법:
#   scripts/fetch_vps_daily_extract.sh [days] [output_dir]
#
# 예:
#   scripts/fetch_vps_daily_extract.sh
#   scripts/fetch_vps_daily_extract.sh 3 ./extracts
#
# 원격에서 /var/lib/order-monitor 읽기 권한이 없으면 REMOTE_SUDO=1 환경변수를 붙인다:
#   REMOTE_SUDO=1 scripts/fetch_vps_daily_extract.sh

set -euo pipefail

SSH_HOST="root@72.61.125.198"
DAYS="${1:-1}"
OUTPUT_ROOT="${2:-./extracts}"

REMOTE_LOG="/var/lib/order-monitor/order_monitor.log"
REMOTE_DB="/var/lib/order-monitor/order_monitor.db"
REMOTE_TMP="/tmp/order-monitor-extract.$$"
SERVICE_UNIT="order-monitor"

SUDO=""
if [ "${REMOTE_SUDO:-0}" = "1" ]; then
  SUDO="sudo"
fi

STAMP="$(date +%Y%m%d_%H%M%S)"
LOCAL_DIR="${OUTPUT_ROOT}/${STAMP}"
mkdir -p "$LOCAL_DIR"

echo "[1/4] 원격($SSH_HOST)에서 최근 ${DAYS}일치 데이터 준비 중..."

ssh "$SSH_HOST" "DAYS='$DAYS' REMOTE_LOG='$REMOTE_LOG' REMOTE_DB='$REMOTE_DB' REMOTE_TMP='$REMOTE_TMP' SUDO='$SUDO' bash -s" <<'REMOTE_SCRIPT'
set -euo pipefail
mkdir -p "$REMOTE_TMP"

# 로그 파일: 최근 DAYS일 날짜 패턴에 걸리는 라인만 추출 (JSON 라인 선두 timestamp 필드 기준)
PATTERN_FILE="$REMOTE_TMP/date_patterns.txt"
: > "$PATTERN_FILE"
for i in $(seq 0 $((DAYS - 1))); do
  date -d "-${i} day" +%Y-%m-%d >> "$PATTERN_FILE"
done
$SUDO grep -f "$PATTERN_FILE" "$REMOTE_LOG" > "$REMOTE_TMP/order_monitor.filtered.log" || true
gzip -f "$REMOTE_TMP/order_monitor.filtered.log"

# journald: 같은 기간 서비스 유닛 로그 (재연결/워치독 이벤트 대조용)
$SUDO journalctl -u order-monitor -u order-monitor-watchdog --since "${DAYS} days ago" \
  > "$REMOTE_TMP/journalctl.log" 2>/dev/null || true
gzip -f "$REMOTE_TMP/journalctl.log"

# DB: 라이브 중에도 안전한 온라인 백업 (WAL 대응)
$SUDO sqlite3 "$REMOTE_DB" ".backup '$REMOTE_TMP/order_monitor.db'"

# 원격 임시 백업본에서 사람이 보기 쉬운 CSV도 같이 뽑아둔다.
# walls: 시간 필드가 없는 현재 상태 스냅샷이라 전체를 내보낸다 (필터 대상 아님).
sqlite3 -header -csv "$REMOTE_TMP/order_monitor.db" \
  "SELECT * FROM walls;" > "$REMOTE_TMP/walls_snapshot.csv" 2>/dev/null || true

# alerts_outbox: recorded_at(unix epoch)이 최근 DAYS일 범위인 행만 CSV로 필터
CUTOFF="$(date -d "-${DAYS} day" +%s)"
sqlite3 -header -csv "$REMOTE_TMP/order_monitor.db" \
  "SELECT * FROM alerts_outbox WHERE recorded_at >= ${CUTOFF};" \
  > "$REMOTE_TMP/alerts_outbox_last_${DAYS}d.csv" 2>/dev/null || true

gzip -f "$REMOTE_TMP/order_monitor.db"
tar -czf "$REMOTE_TMP/bundle.tar.gz" -C "$REMOTE_TMP" \
  order_monitor.filtered.log.gz journalctl.log.gz order_monitor.db.gz \
  walls_snapshot.csv "alerts_outbox_last_${DAYS}d.csv"
REMOTE_SCRIPT

echo "[2/4] 번들 다운로드 중..."
scp "${SSH_HOST}:${REMOTE_TMP}/bundle.tar.gz" "$LOCAL_DIR/bundle.tar.gz"

echo "[3/4] 원격 임시 파일 정리 중..."
ssh "$SSH_HOST" "rm -rf '$REMOTE_TMP'"

echo "[4/4] 로컬 압축 해제 중..."
tar -xzf "$LOCAL_DIR/bundle.tar.gz" -C "$LOCAL_DIR"
rm "$LOCAL_DIR/bundle.tar.gz"
gunzip -f "$LOCAL_DIR"/*.gz

echo "완료: $LOCAL_DIR"
ls -la "$LOCAL_DIR"
