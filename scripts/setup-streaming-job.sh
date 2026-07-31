#!/usr/bin/env bash
# =============================================================================
# STREAMING JOB: Flink + Producer
# Builds Docker images into minikube, deploys the Flink session cluster,
# submits the PyFlink analytics job, and starts the data producer.
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
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# ── 1. Verify minikube is running ─────────────────────────────────────────────
step "Step 1: Checking minikube status"

if ! minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
  error "minikube is not running. Run ./scripts/setup-infrastructure.sh first."
fi
success "minikube is running"

# ── 2. Build Docker images inside minikube ────────────────────────────────────
step "Step 2: Building Docker images inside minikube"
# By pointing Docker at minikube's daemon we avoid needing a registry.

info "Switching to minikube's Docker daemon…"
eval "$(minikube docker-env)"

info "Building clickstream-producer image…"
docker build -t clickstream-producer:latest ./producer
success "clickstream-producer:latest built"

info "Building clickstream-flink image (downloads JARs — may take a few minutes)…"
# --platform linux/amd64: pre-built PyFlink wheels only exist for x86_64.
# Docker Desktop with Rosetta runs amd64 images transparently on Apple Silicon.
docker build --platform linux/amd64 -t clickstream-flink:latest ./flink_jobs
success "clickstream-flink:latest built"

# Restore host Docker env
eval "$(minikube docker-env -u)"

# ── 3. Deploy Flink session cluster ───────────────────────────────────────────
step "Step 3: Deploying Flink session cluster"

kubectl apply -f k8s/flink/flink-session-cluster.yaml

info "Waiting for JobManager to be ready…"
kubectl rollout status deployment/flink-jobmanager \
  -n "$NAMESPACE" --timeout=3m
info "Waiting for TaskManager to be ready…"
kubectl rollout status deployment/flink-taskmanager \
  -n "$NAMESPACE" --timeout=3m

success "Flink session cluster is up"
kubectl get pods -n "$NAMESPACE" -l "app=flink"

# ── 4. Submit PyFlink analytics job ───────────────────────────────────────────
step "Step 4: Submitting PyFlink analytics job"

# Delete stale submission job if it exists
kubectl delete job flink-job-submit -n "$NAMESPACE" --ignore-not-found=true 2>/dev/null
sleep 2

kubectl apply -f k8s/flink/flink-job-submit.yaml
info "Job submitted — waiting for completion (this runs flink submit, not the streaming job itself)…"

# Wait up to 3 minutes for the submission Job to complete
kubectl wait --for=condition=complete \
  job/flink-job-submit \
  -n "$NAMESPACE" \
  --timeout=180s || {
    warn "Job submission did not complete in time. Showing logs:"
    kubectl logs -n "$NAMESPACE" -l job-name=flink-job-submit --tail=50
  }

# Show submission logs
info "Submission job logs:"
kubectl logs -n "$NAMESPACE" -l job-name=flink-job-submit --tail=30

success "Flink analytics job submitted"

# Verify job is RUNNING via REST API
info "Checking Flink job status via REST API…"
JM_POD=$(kubectl get pods -n "$NAMESPACE" -l "component=jobmanager" \
  -o jsonpath='{.items[0].metadata.name}')
JOB_STATUS=$(kubectl exec -n "$NAMESPACE" "$JM_POD" -- \
  curl -s http://localhost:8081/jobs/overview 2>/dev/null || echo "{}")
echo "Flink jobs: $JOB_STATUS"

# ── 5. Deploy clickstream producer ────────────────────────────────────────────
step "Step 5: Deploying clickstream event producer"

kubectl apply -f k8s/producer/producer.yaml
kubectl rollout status deployment/clickstream-producer \
  -n "$NAMESPACE" --timeout=2m

success "Producer deployed — events are flowing into Kafka"

# Tail a few producer logs to verify
info "First 20 log lines from producer:"
sleep 5
kubectl logs -n "$NAMESPACE" \
  -l app=clickstream-producer --tail=20 || true

# ── 6. Wait and verify end-to-end ─────────────────────────────────────────────
step "Step 6: End-to-end verification"

info "Waiting 75 seconds for the first 1-minute Flink window to close…"
sleep 75

PG_POD=$(kubectl get pods -n "$NAMESPACE" \
  -l "app.kubernetes.io/name=postgresql" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [[ -n "$PG_POD" ]]; then
  info "Querying PostgreSQL for aggregated results…"
  PG_PASS=$(kubectl get secret postgres-credentials -n "$NAMESPACE" -o jsonpath='{.data.password}' | base64 -d)
  kubectl exec -n "$NAMESPACE" "$PG_POD" -- \
    env PGPASSWORD="$PG_PASS" psql -U analytics -d analytics -c \
    "SELECT window_start, category, event_count, purchase_count, ROUND(total_revenue::numeric,2) AS revenue FROM analytics_summary ORDER BY window_start DESC LIMIT 10;" \
    2>/dev/null || warn "Could not query PostgreSQL."
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}${BOLD}║  Streaming job complete! Kafka → Flink → PostgreSQL is running.  ║${NC}"
echo -e "${GREEN}${BOLD}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Useful commands:"
echo "  Open Flink UI:      ./scripts/port-forward.sh flink"
echo "  Query PostgreSQL:   ./scripts/query-postgres.sh"
echo "  Watch producer:     kubectl logs -n streaming -l app=clickstream-producer -f"
echo "  Run next:            ./scripts/setup-orchestration.sh"
echo ""
