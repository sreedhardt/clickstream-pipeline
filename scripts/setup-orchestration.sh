#!/usr/bin/env bash
# =============================================================================
# ORCHESTRATION: Airflow
# Builds the custom Airflow image (includes our DAG), deploys via Helm,
# and opens the Airflow UI.
# =============================================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }
step()    { echo -e "\n${BOLD}──────────────────────────────────────────${NC}"; \
            echo -e "${BOLD} $*${NC}"; \
            echo -e "${BOLD}──────────────────────────────────────────${NC}"; }

NAMESPACE="streaming"
AIRFLOW_RELEASE="streaming-airflow"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# ── 1. Verify prerequisites ───────────────────────────────────────────────────
step "Step 1: Verifying prerequisites"

if ! minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
  error "minikube is not running. Run setup-infrastructure.sh and setup-streaming-job.sh first."
fi
success "minikube is running"

# ── 2. Build Airflow image ────────────────────────────────────────────────────
step "Step 2: Building custom Airflow image"

info "Switching to minikube's Docker daemon…"
eval "$(minikube docker-env)"

info "Building clickstream-airflow image (from project root for DAG COPY)…"
docker build \
  -f airflow/Dockerfile \
  -t clickstream-airflow:latest \
  .
success "clickstream-airflow:latest built"

eval "$(minikube docker-env -u)"

# ── 3. Create Airflow metadata DB in our existing PostgreSQL ──────────────────
step "Step 3: Creating airflow_metadata database"

# Use instance label to target only streaming-postgresql (not any leftover pods)
PG_POD=$(kubectl get pods -n "$NAMESPACE" \
  -l "app.kubernetes.io/name=postgresql,app.kubernetes.io/instance=streaming-postgresql" \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [[ -z "$PG_POD" ]]; then
  error "streaming-postgresql pod not found or not Running. Did the infrastructure step complete successfully?"
fi

info "Using pod: $PG_POD"

PG_ADMIN_PASS=$(kubectl get secret postgres-credentials -n "$NAMESPACE" -o jsonpath='{.data.postgres-password}' | base64 -d)

# Create database idempotently (do not fail if already exists)
kubectl exec -n "$NAMESPACE" "$PG_POD" -c postgresql -- \
  env PGPASSWORD="$PG_ADMIN_PASS" psql -U postgres -tc \
  "SELECT 1 FROM pg_database WHERE datname='airflow_metadata';" \
  | grep -q 1 || \
kubectl exec -n "$NAMESPACE" "$PG_POD" -c postgresql -- \
  env PGPASSWORD="$PG_ADMIN_PASS" psql -U postgres -c \
  "CREATE DATABASE airflow_metadata;"

success "airflow_metadata database ready"

# ── 3b. Secrets ────────────────────────────────────────────────────────────────
step "Step 3b: Ensuring Airflow Secrets exist"

# Idempotent — reuses the postgres-credentials created during infrastructure setup, only creates
# the Airflow-specific secrets (fernet key, webserver key, connections) here.
"$SCRIPT_DIR/create-secrets.sh"

# ── 4. Deploy Airflow via Helm ────────────────────────────────────────────────
step "Step 4: Deploying Airflow (this takes 8–15 minutes on first run)"

# The official Apache Airflow chart needs to be added if not present
helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
helm repo update apache-airflow

# Pin to chart 1.15.0 (Airflow 2.x).
# Chart 2.x+ deploys Airflow 3.x with a completely different architecture
# (api-server, dag-processor) that is incompatible with our 2.8.1 image/values.
helm upgrade --install "$AIRFLOW_RELEASE" apache-airflow/airflow \
  --version 1.15.0 \
  --namespace "$NAMESPACE" \
  --values helm/airflow-values.yaml \
  --wait --timeout=15m

success "Airflow deployed"
kubectl get pods -n "$NAMESPACE" | grep airflow

# ── 5. Verify the DAG is loaded ───────────────────────────────────────────────
step "Step 5: Verifying DAG is registered"

info "Waiting 30 seconds for Airflow scheduler to pick up DAGs…"
sleep 30

SCHEDULER_POD=$(kubectl get pods -n "$NAMESPACE" \
  -l "component=scheduler,release=$AIRFLOW_RELEASE" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || \
  kubectl get pods -n "$NAMESPACE" \
  -l "component=scheduler" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [[ -n "$SCHEDULER_POD" ]]; then
  info "DAGs visible to scheduler:"
  kubectl exec -n "$NAMESPACE" "$SCHEDULER_POD" -- \
    airflow dags list 2>/dev/null | grep -E "dag_id|clickstream" || \
    warn "Could not list DAGs yet — scheduler may still be loading."
else
  warn "Scheduler pod not found. Check: kubectl get pods -n streaming"
fi

# ── 6. Trigger DAG manually ───────────────────────────────────────────────────
step "Step 6: Triggering DAG for a test run"

if [[ -n "$SCHEDULER_POD" ]]; then
  info "Triggering clickstream_pipeline_monitor DAG…"
  kubectl exec -n "$NAMESPACE" "$SCHEDULER_POD" -- \
    airflow dags trigger clickstream_pipeline_monitor 2>/dev/null || \
    warn "Could not trigger DAG automatically. Do it manually in the UI."
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}${BOLD}║  Orchestration complete! The full pipeline is running.   ║${NC}"
echo -e "${GREEN}${BOLD}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Open service UIs (run each in a separate terminal):"
echo "  Flink UI:    ./scripts/port-forward.sh flink     → http://localhost:8081"
echo "  Airflow UI:  ./scripts/port-forward.sh airflow   → http://localhost:8080"
echo "               Login: admin / admin"
echo ""
echo "Explore results:"
echo "  ./scripts/query-postgres.sh"
echo ""
echo "Tear everything down:"
echo "  ./scripts/teardown.sh"
echo ""
