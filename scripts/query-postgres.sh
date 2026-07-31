#!/usr/bin/env bash
# =============================================================================
# Query the analytics PostgreSQL database directly from your laptop.
# Runs common queries to explore the streaming results.
# =============================================================================
NAMESPACE="streaming"

PG_POD=$(kubectl get pods -n "$NAMESPACE" \
  -l "app.kubernetes.io/name=postgresql" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [[ -z "$PG_POD" ]]; then
  echo "ERROR: No PostgreSQL pod found in namespace '$NAMESPACE'."
  exit 1
fi

PG_PASS=$(kubectl get secret postgres-credentials -n "$NAMESPACE" -o jsonpath='{.data.password}' | base64 -d)
if [[ -z "$PG_PASS" ]]; then
  echo "ERROR: secret/postgres-credentials not found in namespace '$NAMESPACE'. Run ./scripts/setup-infrastructure.sh first."
  exit 1
fi

run_query() {
  local label="$1"
  local sql="$2"
  echo ""
  echo "──────────────────────────────────────────────────────────────"
  echo " $label"
  echo "──────────────────────────────────────────────────────────────"
  kubectl exec -n "$NAMESPACE" "$PG_POD" -- \
    env PGPASSWORD="$PG_PASS" psql -U analytics -d analytics -c "$sql"
}

run_query "Latest 10 windows (most recent first)" \
  "SELECT window_start, window_end, category, event_count, purchase_count, ROUND(total_revenue::numeric,2) AS revenue FROM analytics_summary ORDER BY window_start DESC LIMIT 10;"

run_query "Revenue by category (all time)" \
  "SELECT category, SUM(event_count) AS total_events, SUM(purchase_count) AS total_purchases, ROUND(SUM(total_revenue)::numeric,2) AS total_revenue FROM analytics_summary GROUP BY category ORDER BY total_revenue DESC;"

run_query "Events per minute (last 10 windows)" \
  "SELECT window_start, SUM(event_count) AS total_events, SUM(purchase_count) AS purchases FROM analytics_summary GROUP BY window_start ORDER BY window_start DESC LIMIT 10;"

echo ""
echo "Done. Tip: connect interactively with:"
echo "  kubectl exec -it -n $NAMESPACE $PG_POD -- env PGPASSWORD=\"\$PG_PASS\" psql -U analytics -d analytics"
