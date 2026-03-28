#!/usr/bin/env bash
# =============================================================================
# setup.sh - One-shot setup for the local Iceberg ETL stack
# Usage: ./scripts/setup.sh [--mode k8s|docker] [--skip-kind] [--skip-deps]
# =============================================================================

set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

log()  { echo -e "${BLUE}[$(date +%H:%M:%S)] ${NC}$*"; }
ok()   { echo -e "${GREEN}[$(date +%H:%M:%S)] ✓ ${NC}$*"; }
warn() { echo -e "${YELLOW}[$(date +%H:%M:%S)] ⚠ ${NC}$*"; }
err()  { echo -e "${RED}[$(date +%H:%M:%S)] ✗ ${NC}$*" >&2; }
step() { echo -e "\n${BOLD}${CYAN}═══ $* ═══${NC}\n"; }

# ── Defaults ──────────────────────────────────────────────────────────────────
MODE="${MODE:-docker}"            # docker | k8s
SKIP_KIND="${SKIP_KIND:-false}"
SKIP_DEPS="${SKIP_DEPS:-false}"
CLUSTER_NAME="etl-stack"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# ── Parse args ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)       MODE="$2"; shift 2 ;;
        --skip-kind)  SKIP_KIND="true"; shift ;;
        --skip-deps)  SKIP_DEPS="true"; shift ;;
        --help|-h)
            echo "Usage: $0 [--mode docker|k8s] [--skip-kind] [--skip-deps]"
            exit 0
            ;;
        *) err "Unknown argument: $1"; exit 1 ;;
    esac
done

# ── Banner ────────────────────────────────────────────────────────────────────
echo -e "${BOLD}${CYAN}"
cat << 'EOF'
  _____ _____ _       ____  _             _
 | ____|_   _| |     / ___|| |_ __ _  ___| | __
 |  _|   | | | |     \___ \| __/ _` |/ __| |/ /
 | |___  | | | |___   ___) | || (_| | (__|   <
 |_____| |_| |_____| |____/ \__\__,_|\___|_|\_\

  Local Iceberg ETL Stack
  MinIO + Nessie + Spark + Airflow + Airbyte + dbt
EOF
echo -e "${NC}"
log "Mode: ${BOLD}${MODE}${NC}"
log "Root: ${ROOT_DIR}"

# ── Dependency checks ─────────────────────────────────────────────────────────
step "Checking dependencies"

check_cmd() {
    if command -v "$1" &>/dev/null; then
        ok "$1 found: $(command -v "$1")"
    else
        warn "$1 not found"
        return 1
    fi
}

MISSING=()

check_cmd docker   || MISSING+=(docker)
check_cmd docker   && docker info &>/dev/null || { err "Docker daemon not running"; exit 1; }

if [[ "$MODE" == "k8s" ]]; then
    check_cmd kind    || MISSING+=(kind)
    check_cmd kubectl || MISSING+=(kubectl)
    check_cmd helm    || MISSING+=(helm)
fi

check_cmd python3  || MISSING+=(python3)

if [[ ${#MISSING[@]} -gt 0 ]]; then
    err "Missing dependencies: ${MISSING[*]}"
    echo ""
    echo "Install missing tools:"
    echo "  Docker:  https://docs.docker.com/get-docker/"
    if [[ "$MODE" == "k8s" ]]; then
        echo "  kind:    go install sigs.k8s.io/kind@v0.20.0"
        echo "  kubectl: https://kubernetes.io/docs/tasks/tools/"
        echo "  helm:    https://helm.sh/docs/intro/install/"
    fi
    exit 1
fi

ok "All required dependencies found"

# ── Load .env ─────────────────────────────────────────────────────────────────
if [[ -f "${ROOT_DIR}/.env" ]]; then
    log "Loading .env"
    set -a
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.env"
    set +a
elif [[ -f "${ROOT_DIR}/.env.example" ]]; then
    warn ".env not found, using .env.example defaults"
    cp "${ROOT_DIR}/.env.example" "${ROOT_DIR}/.env"
    set -a
    source "${ROOT_DIR}/.env"
    set +a
fi

# ── Create data directories ────────────────────────────────────────────────────
step "Creating local data directories"
mkdir -p /tmp/etl-data/{raw,silver,gold,spark-logs}
ok "Data directories ready at /tmp/etl-data"

# =============================================================================
# DOCKER MODE
# =============================================================================
if [[ "$MODE" == "docker" ]]; then
    step "Starting Docker Compose stack"

    cd "${ROOT_DIR}"

    log "Pulling images (this may take a while on first run)..."
    docker compose pull --quiet 2>/dev/null || warn "Some images could not be pre-pulled"

    log "Starting core services (MinIO, Nessie, Postgres)..."
    docker compose up -d minio postgres nessie
    sleep 5

    log "Initializing MinIO buckets..."
    docker compose up minio-init --wait || warn "minio-init may have already run"

    log "Starting Spark cluster..."
    docker compose up -d spark-master spark-worker
    sleep 10

    log "Initializing Airflow database..."
    docker compose run --rm airflow-init

    log "Starting Airflow services..."
    docker compose up -d airflow-webserver airflow-scheduler

    log "Starting Airbyte..."
    docker compose up -d airbyte-db airbyte-server airbyte-webapp

    step "Waiting for services to be healthy"
    wait_healthy() {
        local service=$1
        local url=$2
        local max_wait=${3:-120}
        local waited=0
        log "Waiting for ${service}..."
        until curl -sf "$url" &>/dev/null; do
            sleep 5
            waited=$((waited + 5))
            if [[ $waited -ge $max_wait ]]; then
                warn "${service} did not become healthy after ${max_wait}s"
                return 1
            fi
        done
        ok "${service} is healthy"
    }

    wait_healthy "MinIO"          "http://localhost:9000/minio/health/live"
    wait_healthy "Nessie"         "http://localhost:19120/q/health/ready"
    wait_healthy "Airflow"        "http://localhost:8080/health" 180
    wait_healthy "Airbyte Webapp" "http://localhost:8000" 180 || true

    step "Docker Compose Stack Ready!"
    echo ""
    echo -e "${BOLD}Service URLs:${NC}"
    echo -e "  MinIO Console:  ${CYAN}http://localhost:9001${NC}  (minioadmin / minioadmin123)"
    echo -e "  MinIO API:      ${CYAN}http://localhost:9000${NC}"
    echo -e "  Nessie API:     ${CYAN}http://localhost:19120/api/v1${NC}"
    echo -e "  Airflow UI:     ${CYAN}http://localhost:8080${NC}  (admin / admin123)"
    echo -e "  Airbyte UI:     ${CYAN}http://localhost:8000${NC}"
    echo -e "  Spark Master:   ${CYAN}http://localhost:8090${NC}"
    echo ""
    echo -e "${GREEN}Stack is ready! Run tests with: ${BOLD}./scripts/test_pipeline.sh --mode docker${NC}"
    exit 0
fi

# =============================================================================
# KUBERNETES MODE
# =============================================================================
step "Setting up Kubernetes (kind) cluster"

# ── Create kind cluster ───────────────────────────────────────────────────────
if [[ "$SKIP_KIND" != "true" ]]; then
    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        warn "Kind cluster '${CLUSTER_NAME}' already exists"
        read -rp "  Delete and recreate? [y/N] " answer
        if [[ "$answer" =~ ^[Yy]$ ]]; then
            log "Deleting existing cluster..."
            kind delete cluster --name "${CLUSTER_NAME}"
        fi
    fi

    if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        log "Creating kind cluster '${CLUSTER_NAME}'..."
        kind create cluster \
            --name "${CLUSTER_NAME}" \
            --config "${ROOT_DIR}/kind-config.yaml" \
            --wait 5m
        ok "Kind cluster created"
    fi
else
    log "Skipping kind cluster creation (--skip-kind)"
fi

# Set kubectl context
kubectl config use-context "kind-${CLUSTER_NAME}"
ok "kubectl context: kind-${CLUSTER_NAME}"

# ── Apply Kubernetes manifests ─────────────────────────────────────────────────
step "Applying Kubernetes manifests"

log "Creating namespaces..."
kubectl apply -f "${ROOT_DIR}/k8s/namespaces/namespaces.yaml"

log "Deploying MinIO..."
kubectl apply -f "${ROOT_DIR}/k8s/minio/minio.yaml"

log "Deploying Nessie..."
kubectl apply -f "${ROOT_DIR}/k8s/nessie/nessie.yaml"

log "Deploying Spark..."
kubectl apply -f "${ROOT_DIR}/k8s/spark/spark.yaml"

log "Deploying Airflow Postgres..."
kubectl apply -f "${ROOT_DIR}/k8s/airflow/postgres.yaml"

log "Deploying Airflow..."
kubectl apply -f "${ROOT_DIR}/k8s/airflow/airflow.yaml"

log "Deploying Airbyte..."
kubectl apply -f "${ROOT_DIR}/k8s/airbyte/airbyte.yaml"

# ── Wait for deployments ───────────────────────────────────────────────────────
step "Waiting for deployments to be ready"

wait_deploy() {
    local ns=$1
    local deploy=$2
    local timeout=${3:-300}
    log "Waiting for ${ns}/${deploy}..."
    if kubectl wait deployment/"${deploy}" \
        --for=condition=available \
        --timeout="${timeout}s" \
        -n "${ns}" 2>/dev/null; then
        ok "${ns}/${deploy} ready"
    else
        warn "${ns}/${deploy} not ready after ${timeout}s - check: kubectl get pods -n ${ns}"
    fi
}

wait_deploy minio   minio     300
wait_deploy nessie  nessie    300
wait_deploy spark   spark-master 300
wait_deploy airflow airflow-webserver 300

# ── Upload DAGs to Airflow ─────────────────────────────────────────────────────
step "Uploading Airflow DAGs"

AIRFLOW_POD=$(kubectl get pod -n airflow -l component=webserver -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [[ -n "$AIRFLOW_POD" ]]; then
    for dag in "${ROOT_DIR}/airflow/dags/"*.py; do
        log "Uploading $(basename "$dag")..."
        kubectl cp "$dag" "airflow/${AIRFLOW_POD}:/opt/airflow/dags/$(basename "$dag")"
    done
    ok "DAGs uploaded"
else
    warn "No Airflow pod found, skipping DAG upload"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
step "Kubernetes Stack Ready!"
echo ""
echo -e "${BOLD}Service URLs (via NodePort):${NC}"
echo -e "  MinIO Console:    ${CYAN}http://localhost:9001${NC}  (minioadmin / minioadmin123)"
echo -e "  MinIO API:        ${CYAN}http://localhost:9000${NC}"
echo -e "  Nessie API:       ${CYAN}http://localhost:19120/api/v1${NC}"
echo -e "  Airflow UI:       ${CYAN}http://localhost:8080${NC}  (admin / admin123)"
echo -e "  Airbyte UI:       ${CYAN}http://localhost:8000${NC}"
echo -e "  Spark History:    ${CYAN}http://localhost:18080${NC}"
echo ""
echo -e "${BOLD}Useful commands:${NC}"
echo -e "  kubectl get pods --all-namespaces"
echo -e "  kubectl logs -n minio deployment/minio -f"
echo -e "  kubectl logs -n nessie deployment/nessie -f"
echo ""
echo -e "${GREEN}Stack is ready! Run tests with: ${BOLD}./scripts/test_pipeline.sh --mode k8s${NC}"
