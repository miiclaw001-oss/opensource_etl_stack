#!/usr/bin/env bash
# ============================================================================
# run_etl.sh  —  One-shot ETL starter
#
# Usage:
#   ./run_etl.sh                          # use existing /tmp/etl_test_data.csv
#   ./run_etl.sh --generate               # generate synthetic data
#   ./run_etl.sh --csv /path/to/file.csv  # custom source CSV
#   ./run_etl.sh --generate --with-minio  # start MinIO first, then run ETL
#   ./run_etl.sh --dry-run                # local only, no MinIO uploads
#
# Environment:
#   MINIO_ENDPOINT   default: http://localhost:9000
#   MINIO_ACCESS_KEY default: minioadmin
#   MINIO_SECRET_KEY default: minioadmin123
# ============================================================================
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="$DIR/scripts/etl_pipeline.js"

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; RESET='\033[0m'

log()  { echo -e "${CYAN}[run_etl]${RESET} $*"; }
ok()   { echo -e "${GREEN}  ✓${RESET} $*"; }
warn() { echo -e "${YELLOW}  ⚠${RESET} $*"; }
err()  { echo -e "${RED}  ✗${RESET} $*"; }

# ── Parse flags ───────────────────────────────────────────────────────────────
WITH_MINIO=false
ETL_ARGS=()

for arg in "$@"; do
  case "$arg" in
    --with-minio) WITH_MINIO=true ;;
    *)            ETL_ARGS+=("$arg") ;;
  esac
done

echo ""
echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${CYAN}   ETL Stack — run_etl.sh                         ${RESET}"
echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════${RESET}"
echo ""

# ── Node.js check ─────────────────────────────────────────────────────────────
if ! command -v node &>/dev/null; then
  err "node.js not found — install Node.js 18+"
  exit 1
fi
ok "Node.js: $(node --version)"

# ── parquetjs-lite install check ──────────────────────────────────────────────
PARQUET_PATHS=(
  "$DIR/node_modules/parquetjs-lite"
  "/tmp/node_modules/parquetjs-lite"
)

PARQUET_FOUND=false
for p in "${PARQUET_PATHS[@]}"; do
  [[ -d "$p" ]] && { PARQUET_FOUND=true; ok "parquetjs-lite: $p"; break; }
done

if ! $PARQUET_FOUND; then
  warn "parquetjs-lite not found — installing in project root…"
  cd "$DIR" && npm install parquetjs-lite --save-dev 2>&1 | tail -3
  ok "parquetjs-lite installed"
fi

# ── Optional: start MinIO via Docker Compose ──────────────────────────────────
if $WITH_MINIO; then
  if ! command -v docker &>/dev/null; then
    warn "Docker not found — cannot start MinIO automatically"
    warn "Install Docker or start MinIO manually, then re-run without --with-minio"
  else
    log "Starting MinIO…"
    cd "$DIR"
    docker compose up -d minio 2>&1 | tail -5
    log "Initialising MinIO buckets…"
    docker compose up minio-init 2>&1 | tail -5
    ok "MinIO ready — adding --wait flag to ETL args"
    ETL_ARGS+=("--wait")
  fi
fi

# ── Run pipeline ──────────────────────────────────────────────────────────────
log "Launching ETL pipeline…"
echo ""
node "$SCRIPT" "${ETL_ARGS[@]}"

EXIT=$?
echo ""
if [[ $EXIT -eq 0 ]]; then
  ok "run_etl.sh complete"
else
  err "Pipeline exited with code $EXIT"
  exit $EXIT
fi
