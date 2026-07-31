#!/usr/bin/env bash
# =============================================================================
# Port-forward a service to localhost.
# Usage: ./scripts/port-forward.sh [flink|airflow|postgres|all]
# =============================================================================
NAMESPACE="streaming"
AIRFLOW_RELEASE="streaming-airflow"

case "${1:-all}" in
  flink)
    echo "Forwarding Flink UI → http://localhost:8081"
    kubectl port-forward svc/flink-jobmanager 8081:8081 -n "$NAMESPACE"
    ;;
  airflow)
    echo "Forwarding Airflow UI → http://localhost:8080  (admin/admin)"
    kubectl port-forward "svc/${AIRFLOW_RELEASE}-webserver" 8080:8080 -n "$NAMESPACE"
    ;;
  postgres)
    echo "Forwarding PostgreSQL → localhost:5433"
    echo "  psql -h localhost -p 5433 -U analytics -d analytics"
    kubectl port-forward svc/streaming-postgresql 5433:5432 -n "$NAMESPACE"
    ;;
  all)
    echo "Forwarding all services — open separate terminals or use tmux."
    echo "  Flink:    http://localhost:8081"
    echo "  Airflow:  http://localhost:8080"
    echo "  Postgres: localhost:5433"
    kubectl port-forward svc/flink-jobmanager 8081:8081 -n "$NAMESPACE" &
    kubectl port-forward "svc/${AIRFLOW_RELEASE}-webserver" 8080:8080 -n "$NAMESPACE" &
    kubectl port-forward svc/streaming-postgresql 5433:5432 -n "$NAMESPACE" &
    echo ""
    echo "Press Ctrl+C to stop all port-forwards."
    wait
    ;;
  *)
    echo "Usage: $0 [flink|airflow|postgres|all]"
    exit 1
    ;;
esac
