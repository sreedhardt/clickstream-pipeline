#!/usr/bin/env bash
# =============================================================================
# Teardown — removes all deployed resources.
# Optionally stops minikube entirely.
# =============================================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; NC='\033[0m'
info()    { echo -e "\033[0;34m[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }

NAMESPACE="streaming"

echo -e "${RED}${BOLD}WARNING: This will delete all resources in the '$NAMESPACE' namespace.${NC}"
read -rp "Continue? [y/N] " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }

info "Uninstalling Helm releases…"
helm uninstall streaming-airflow    -n "$NAMESPACE" 2>/dev/null || warn "streaming-airflow not found"
helm uninstall streaming-postgresql -n "$NAMESPACE" 2>/dev/null || warn "streaming-postgresql not found"

info "Deleting K8s manifests…"
kubectl delete -f k8s/kafka/kafka.yaml                 --ignore-not-found 2>/dev/null || true
kubectl delete -f k8s/flink/flink-session-cluster.yaml --ignore-not-found 2>/dev/null || true
kubectl delete -f k8s/flink/flink-job-submit.yaml      --ignore-not-found 2>/dev/null || true
kubectl delete -f k8s/producer/producer.yaml            --ignore-not-found 2>/dev/null || true

info "Deleting namespace…"
kubectl delete namespace "$NAMESPACE" --ignore-not-found

success "All resources deleted."

read -rp "Also stop minikube? [y/N] " stop_mk
if [[ "$stop_mk" =~ ^[Yy]$ ]]; then
  minikube stop
  success "minikube stopped."
fi
