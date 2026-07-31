#!/usr/bin/env bash
# =============================================================================
# Create the Kubernetes Secrets the Helm releases read credentials from.
#
# Values are randomly generated on first run and stored ONLY as K8s Secrets
# in the cluster — they are never written to disk or committed to git.
# Re-running is safe: existing secrets are left untouched, so
# setup-infrastructure.sh and setup-orchestration.sh can both call this
# without stepping on each other.
#
# Override any value by exporting it before running, e.g.:
#   ANALYTICS_DB_PASSWORD=mypass ./scripts/create-secrets.sh
# =============================================================================
set -euo pipefail

NAMESPACE="streaming"

RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }

kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl apply -f "$(dirname "${BASH_SOURCE[0]}")/../k8s/namespace.yaml"

secret_exists() { kubectl get secret "$1" -n "$NAMESPACE" >/dev/null 2>&1; }
rand_hex()      { openssl rand -hex 24; }
# A Fernet key is 32 random bytes, URL-safe base64 encoded.
fernet_key()    { openssl rand -base64 32 | tr '+/' '-_'; }

# ── PostgreSQL credentials (also consumed by the Flink JDBC sink) ────────────
if secret_exists postgres-credentials; then
  info "postgres-credentials already exists — reusing it"
  ANALYTICS_PW=$(kubectl get secret postgres-credentials -n "$NAMESPACE" -o jsonpath='{.data.password}' | base64 -d)
  ADMIN_PW=$(kubectl get secret postgres-credentials -n "$NAMESPACE" -o jsonpath='{.data.postgres-password}' | base64 -d)
else
  ANALYTICS_PW="${ANALYTICS_DB_PASSWORD:-$(rand_hex)}"
  ADMIN_PW="${POSTGRES_ADMIN_PASSWORD:-$(rand_hex)}"
  kubectl create secret generic postgres-credentials -n "$NAMESPACE" \
    --from-literal=password="$ANALYTICS_PW" \
    --from-literal=postgres-password="$ADMIN_PW"
  success "created secret/postgres-credentials"
fi

# ── Airflow Fernet key (encrypts connection passwords in Airflow's metadata DB) ──
if secret_exists airflow-fernet-key; then
  info "airflow-fernet-key already exists — reusing it"
else
  kubectl create secret generic airflow-fernet-key -n "$NAMESPACE" \
    --from-literal=fernet-key="$(fernet_key)"
  success "created secret/airflow-fernet-key"
fi

# ── Airflow webserver secret key (Flask session signing) ─────────────────────
if secret_exists airflow-webserver-secret-key; then
  info "airflow-webserver-secret-key already exists — reusing it"
else
  kubectl create secret generic airflow-webserver-secret-key -n "$NAMESPACE" \
    --from-literal=webserver-secret-key="$(rand_hex)"
  success "created secret/airflow-webserver-secret-key"
fi

# ── Airflow metadata DB connection (points at our existing PostgreSQL) ───────
if secret_exists airflow-metadata-connection; then
  info "airflow-metadata-connection already exists — reusing it"
else
  kubectl create secret generic airflow-metadata-connection -n "$NAMESPACE" \
    --from-literal=connection="postgresql://postgres:${ADMIN_PW}@streaming-postgresql.streaming.svc.cluster.local:5432/airflow_metadata?sslmode=disable"
  success "created secret/airflow-metadata-connection"
fi

# ── Airflow connection used by the DAG's PostgresHook ─────────────────────────
if secret_exists airflow-connections; then
  info "airflow-connections already exists — reusing it"
else
  kubectl create secret generic airflow-connections -n "$NAMESPACE" \
    --from-literal=postgres-analytics-uri="postgresql://analytics:${ANALYTICS_PW}@streaming-postgresql.streaming.svc.cluster.local:5432/analytics"
  success "created secret/airflow-connections"
fi

echo ""
success "All secrets ready in namespace '$NAMESPACE'."
