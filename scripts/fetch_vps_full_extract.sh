#!/usr/bin/env bash
# VPS(order-monitor)에서 운영 시작 이후 전체 로그·DB 데이터를 로컬로 추출한다.
# fetch_vps_daily_extract.sh(최근 N일 스팟체크용)와 달리 날짜로 자르지 않고 전량을 가져온다 —
# RotatingFileHandler(10MB x 5, logging_setup.py)로 로그가 여러 파일에 걸쳐 롤오버돼 있을 수 있어서
# 회전된 파일(order_monitor.log.1 등)까지 전부 포함한다.
#
# 사용법:
#   scripts/fetch_vps_full_extract.sh [output_dir]
#
# 원격에서 /var/lib/order-monitor 읽기 권한이 없으면 REMOTE_SUDO=1 환경변수를 붙인다:
#   REMOTE_SUDO=1 scripts/fetch_vps_full_extract.sh

set -euo pipefail

SSH_HOST="root@72.61.125.198"
OUTPUT_ROOT="${1:-./extracts}"

REMOTE_DATA_DIR="/var/lib/order-monitor"
REMOTE_DB="${REMOTE_DATA_DIR}/order_monitor.db"
REMOTE_TMP="/tmp/order-monitor-full-extract.$$"
SERVICE_UNIT="order-monitor"

SUDO=""
if [ "${REMOTE_SUDO:-0}" = "1" ]; then
  SUDO="sudo"
fi

STAMP="$(date +%Y%m%d_%H%M%S)"
LOCAL_DIR="${OUTPUT_ROOT}/full_${STAMP}"
mkdir -p "$LOCAL_DIR"

echo "[1/4] 원격($SSH_HOST)에서 전체 데이터 준비 중..."

ssh "$SSH_HOST" "REMOTE_DATA_DIR='$REMOTE_DATA_DIR' REMOTE_DB='$REMOTE_DB' REMOTE_TMP='$REMOTE_TMP' SUDO='$SUDO' bash -s" <<'REMOTE_SCRIPT'
set -euo pipefail
mkdir -p "$REMOTE_TMP/logs"

# 로그: 현재 파일 + 회전된 파일(order_monitor.log, .log.1 ... .log.5) 전부
$SUDO bash -c "cp ${REMOTE_DATA_DIR}/order_monitor.log* '$REMOTE_TMP/logs/' 2>/dev/null" || true
$SUDO chmod -R a+r "$REMOTE_TMP/logs"

# journald: 서비스 설치 이후 전체 이력 (--since 없이 전체)
$SUDO journalctl -u order-monitor -u order-monitor-watchdog --no-pager \
  > "$REMOTE_TMP/journalctl_full.log" 2>/dev/null || true

# DB: 라이브 중에도 안전한 온라인 백업 (WAL 대응), 필터 없이 전체
$SUDO sqlite3 "$REMOTE_DB" ".backup '$REMOTE_TMP/order_monitor.db'"

# 사람이 보기 쉬운 CSV도 테이블별 전체를 같이 뽑아둔다 (필터 없음)
sqlite3 -header -csv "$REMOTE_TMP/order_monitor.db" \
  "SELECT * FROM walls;" > "$REMOTE_TMP/walls_snapshot.csv" 2>/dev/null || true
sqlite3 -header -csv "$REMOTE_TMP/order_monitor.db" \
  "SELECT * FROM alerts_outbox;" > "$REMOTE_TMP/alerts_outbox_full.csv" 2>/dev/null || true

gzip -f "$REMOTE_TMP"/logs/*
gzip -f "$REMOTE_TMP/journalctl_full.log"
gzip -f "$REMOTE_TMP/order_monitor.db"

tar -czf "$REMOTE_TMP/bundle.tar.gz" -C "$REMOTE_TMP" \
  logs journalctl_full.log.gz order_monitor.db.gz walls_snapshot.csv alerts_outbox_full.csv
REMOTE_SCRIPT

echo "[2/4] 번들 다운로드 중..."
scp "${SSH_HOST}:${REMOTE_TMP}/bundle.tar.gz" "$LOCAL_DIR/bundle.tar.gz"

echo "[3/4] 원격 임시 파일 정리 중..."
ssh "$SSH_HOST" "rm -rf '$REMOTE_TMP'"

echo "[4/4] 로컬 압축 해제 중..."
tar -xzf "$LOCAL_DIR/bundle.tar.gz" -C "$LOCAL_DIR"
rm "$LOCAL_DIR/bundle.tar.gz"
find "$LOCAL_DIR" -name "*.gz" -exec gunzip -f {} \;

echo "완료: $LOCAL_DIR"
find "$LOCAL_DIR" -type f -exec ls -la {} \;
