#!/usr/bin/env bash
# =============================================================================
# INFRASTRUCTURE SETUP
# Starts minikube, installs Helm, deploys Kafka + PostgreSQL, verifies them.
# =============================================================================
set -euo pipefail

# ── Colours ──────────────────────────────────────────────────────────────────
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
POSTGRES_RELEASE="streaming-postgresql"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

# ── 1. Prerequisites ──────────────────────────────────────────────────────────
step "Step 1: Checking prerequisites"

for cmd in minikube kubectl docker; do
  if command -v "$cmd" &>/dev/null; then
    success "$cmd found ($(which $cmd))"
  else
    error "$cmd is not installed or not in PATH. Please install it first."
  fi
done

# Install Helm if missing
if ! command -v helm &>/dev/null; then
  info "Helm not found — installing via official script..."
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  success "Helm installed: $(helm version --short)"
else
  success "helm found: $(helm version --short)"
fi

# ── 2. Start minikube ─────────────────────────────────────────────────────────
step "Step 2: Starting minikube"

if minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
  warn "minikube is already running. Skipping start."
  minikube status
else
  info "Starting minikube with 4 CPUs, 8 GB RAM, 20 GB disk…"
  minikube start \
    --cpus=4 \
    --memory=7168 \
    --disk-size=20g \
    --driver=docker \
    --kubernetes-version=v1.28.0
  success "minikube started"
fi

# Enable ingress addon (useful later)
minikube addons enable ingress 2>/dev/null || true

# ── 3. Helm repos ─────────────────────────────────────────────────────────────
step "Step 3: Adding Helm repositories"

helm repo add bitnami        https://charts.bitnami.com/bitnami        2>/dev/null || true
helm repo add apache-airflow https://airflow.apache.org                2>/dev/null || true
helm repo update
success "Helm repos updated"

# ── 4. Namespace ──────────────────────────────────────────────────────────────
step "Step 4: Creating namespace '${NAMESPACE}'"

kubectl apply -f k8s/namespace.yaml
success "Namespace '${NAMESPACE}' ready"

# ── 4b. Secrets ────────────────────────────────────────────────────────────────
step "Step 4b: Generating credentials as K8s Secrets"

./scripts/create-secrets.sh
success "Secrets ready (generated randomly, never written to git)"

# ── 5. PostgreSQL ─────────────────────────────────────────────────────────────
step "Step 5: Deploying PostgreSQL"

helm upgrade --install "$POSTGRES_RELEASE" bitnami/postgresql \
  --namespace "$NAMESPACE" \
  --values helm/postgres-values.yaml \
  --wait --timeout=5m

success "PostgreSQL deployed"
kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/name=postgresql"

# ── 6. Kafka ──────────────────────────────────────────────────────────────────
step "Step 6: Deploying Kafka (apache/kafka:3.7.0 via plain K8s manifests)"

kubectl apply -f k8s/kafka/kafka.yaml

info "Waiting for Kafka StatefulSet to be ready…"
kubectl rollout status statefulset/kafka -n "$NAMESPACE" --timeout=3m

success "Kafka deployed"
kubectl get pods -n "$NAMESPACE" -l "app=kafka"

# ── 7. Verify Kafka topic ─────────────────────────────────────────────────────
step "Step 7: Creating Kafka topic 'clickstream-events'"

info "Waiting for topic-init Job to complete…"
kubectl wait --for=condition=complete job/kafka-topic-init \
  -n "$NAMESPACE" --timeout=120s || {
    warn "Topic init job timed out. Showing logs:"
    kubectl logs -n "$NAMESPACE" -l job-name=kafka-topic-init --tail=30
  }

info "Topic init logs:"
kubectl logs -n "$NAMESPACE" -l job-name=kafka-topic-init --tail=20 2>/dev/null || true

# ── 8. Verify PostgreSQL tables ───────────────────────────────────────────────
step "Step 8: Verifying PostgreSQL tables"

PG_POD=$(kubectl get pods -n "$NAMESPACE" \
  -l "app.kubernetes.io/name=postgresql" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [[ -n "$PG_POD" ]]; then
  info "Checking tables in 'analytics' database…"
  PG_PASS=$(kubectl get secret postgres-credentials -n "$NAMESPACE" -o jsonpath='{.data.password}' | base64 -d)
  kubectl exec -n "$NAMESPACE" "$PG_POD" -- \
    env PGPASSWORD="$PG_PASS" psql -U analytics -d analytics -c "\dt" || \
    warn "Could not query PostgreSQL yet — it may still be initialising."
else
  warn "No PostgreSQL pod found."
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}${BOLD}║  Infrastructure complete! Kafka + PostgreSQL are up.║${NC}"
echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Next steps:"
echo "  • Check pods:    kubectl get pods -n streaming"
echo "  • Run next:      ./scripts/setup-streaming-job.sh"
echo ""
