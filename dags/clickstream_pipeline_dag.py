"""
Clickstream Pipeline Monitor DAG
----------------------------------
Runs hourly to:
  1. Health-check Kafka (TCP connectivity)
  2. Health-check the Flink job (REST API)
  3. Query PostgreSQL and log an analytics summary report

Note: The streaming pipeline (Kafka → Flink → PostgreSQL) runs continuously
as K8s Deployments. Airflow's role here is operational monitoring and reporting,
not triggering each message.

Connections required (set in Airflow UI or via environment variable):
  - postgres_analytics : PostgreSQL connection to the analytics DB
    (Pre-configured via AIRFLOW_CONN_POSTGRES_ANALYTICS in helm/airflow-values.yaml)
"""

import socket
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------------------------------------------------------------------
# Service addresses (resolvable inside the K8s cluster)
# ---------------------------------------------------------------------------
KAFKA_HOST      = "kafka.streaming.svc.cluster.local"
KAFKA_PORT      = 9092
FLINK_REST_URL  = "http://flink-jobmanager.streaming.svc.cluster.local:8081"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "streaming-team",
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry":   False,
}


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def check_kafka_health(**_):
    """Verify Kafka broker is reachable via TCP."""
    try:
        sock = socket.create_connection((KAFKA_HOST, KAFKA_PORT), timeout=10)
        sock.close()
        print(f"[OK] Kafka is reachable at {KAFKA_HOST}:{KAFKA_PORT}")
    except (socket.timeout, ConnectionRefusedError, OSError) as exc:
        raise RuntimeError(f"Kafka health check failed: {exc}") from exc


def check_flink_health(**_):
    """
    Call the Flink REST API to verify the JobManager is up and
    check whether the analytics job is RUNNING.
    """
    try:
        resp = requests.get(f"{FLINK_REST_URL}/jobs/overview", timeout=15)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Flink REST API unreachable: {exc}") from exc

    jobs = resp.json().get("jobs", [])
    running = [j for j in jobs if j.get("state") == "RUNNING"]

    print(f"[OK] Flink REST API is up. Total jobs: {len(jobs)}, running: {len(running)}")

    if not running:
        # Don't fail the DAG — just warn. The job may have been intentionally stopped.
        print("[WARN] No Flink jobs are currently RUNNING. Check the Flink UI.")
    else:
        for job in running:
            print(f"  → {job['name']} (id={job['jid']}, started={job.get('start-time')})")


def generate_analytics_report(**_):
    """
    Pull the last hour of aggregated data from PostgreSQL and print a summary.
    In a real deployment you'd email/Slack this or write it to a dashboard.
    """
    hook    = PostgresHook(postgres_conn_id="postgres_analytics")
    records = hook.get_records("""
        SELECT
            window_start,
            window_end,
            category,
            event_count,
            purchase_count,
            ROUND(total_revenue::NUMERIC, 2) AS total_revenue
        FROM analytics_summary
        WHERE window_start >= NOW() - INTERVAL '1 hour'
        ORDER BY window_start DESC, total_revenue DESC
        LIMIT 100;
    """)

    sep = "=" * 75
    print(f"\n{sep}")
    print("  CLICKSTREAM ANALYTICS REPORT — Last 60 Minutes")
    print(sep)

    if not records:
        print("  No data yet. The Flink job may still be warming up.")
        print(sep)
        return

    header = f"{'Window Start':<22} {'Category':<14} {'Events':>8} {'Purchases':>10} {'Revenue ($)':>12}"
    print(header)
    print("-" * 75)

    total_events    = 0
    total_purchases = 0
    total_revenue   = 0.0

    for row in records:
        window_start, _, category, events, purchases, revenue = row
        print(
            f"{str(window_start)[:19]:<22} {category:<14} "
            f"{events:>8,} {purchases:>10,} {revenue:>12,.2f}"
        )
        total_events    += events
        total_purchases += purchases
        total_revenue   += revenue

    print("-" * 75)
    print(
        f"{'TOTAL':<22} {'':<14} "
        f"{total_events:>8,} {total_purchases:>10,} {total_revenue:>12,.2f}"
    )
    print(sep)
    print(f"  Conversion rate: {total_purchases/total_events*100:.1f}%  |  "
          f"Avg order value: ${total_revenue/max(total_purchases, 1):.2f}")
    print(f"{sep}\n")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="clickstream_pipeline_monitor",
    default_args=default_args,
    description="Hourly health checks and analytics report for the streaming pipeline",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["streaming", "kafka", "flink", "analytics"],
) as dag:

    t_check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )

    t_check_flink = PythonOperator(
        task_id="check_flink_health",
        python_callable=check_flink_health,
    )

    t_report = PythonOperator(
        task_id="generate_analytics_report",
        python_callable=generate_analytics_report,
    )

    # Health checks run in parallel, then generate the report
    [t_check_kafka, t_check_flink] >> t_report
