"""
Clickstream Analytics Flink Job
---------------------------------
Reads clickstream events from Kafka and computes per-minute aggregations
(event counts, purchase counts, total revenue) grouped by product category.
Results are written to PostgreSQL via the JDBC connector.

Uses the Flink Table API (SQL) which is the recommended API for analytics.

Run locally (outside K8s) for testing:
    python analytics_job.py

Deploy to Flink session cluster:
    flink run --target remote \
        --jobmanager flink-jobmanager:8081 \
        --python analytics_job.py
"""

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# ---------------------------------------------------------------------------
# Configuration (injected via environment variables in Kubernetes)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka.streaming.svc.cluster.local:9092",
)
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "clickstream-events")
KAFKA_GROUP_ID  = os.environ.get("KAFKA_GROUP_ID", "flink-analytics-group")

POSTGRES_URL    = os.environ.get(
    "POSTGRES_URL",
    "jdbc:postgresql://streaming-postgresql.streaming.svc.cluster.local:5432/analytics",
)
POSTGRES_USER   = os.environ.get("POSTGRES_USER", "analytics")
# No fallback default — the password must come from the environment (in K8s,
# from the postgres-credentials Secret via flink-job-submit.yaml) so nothing
# sensitive is hardcoded here.
POSTGRES_PASS   = os.environ["POSTGRES_PASSWORD"]

WINDOW_MINUTES  = int(os.environ.get("WINDOW_MINUTES", "1"))


def main() -> None:
    # ------------------------------------------------------------------
    # Environment setup
    # ------------------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # Periodic checkpoints let the job resume from the last committed Kafka
    # offset after a TaskManager restart instead of replaying (or skipping)
    # the whole topic. See README "Delivery Semantics" for what this does
    # and does not guarantee for the JDBC sink.
    env.enable_checkpointing(60000)  # 60s interval

    t_env = StreamTableEnvironment.create(
        env,
        environment_settings=EnvironmentSettings.in_streaming_mode(),
    )

    # ------------------------------------------------------------------
    # Kafka source table
    # ------------------------------------------------------------------
    # The timestamp field comes as an ISO-8601 string from the producer.
    # We parse it and declare a watermark for event-time processing.
    t_env.execute_sql(f"""
        CREATE TABLE clickstream (
            user_id        STRING,
            session_id     STRING,
            product_id     STRING,
            category       STRING,
            event_type     STRING,
            amount         DOUBLE,
            `timestamp`    STRING,
            -- Derived event-time column and watermark
            event_time     AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSSXXX'),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector'                           = 'kafka',
            'topic'                               = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers'        = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'                 = '{KAFKA_GROUP_ID}',
            'format'                              = 'json',
            'json.fail-on-missing-field'          = 'false',
            'json.ignore-parse-errors'            = 'true',
            'scan.startup.mode'                   = 'latest-offset'
        )
    """)

    # ------------------------------------------------------------------
    # PostgreSQL sink table
    # ------------------------------------------------------------------
    t_env.execute_sql(f"""
        CREATE TABLE analytics_summary (
            window_start   TIMESTAMP(3),
            window_end     TIMESTAMP(3),
            category       STRING,
            event_count    BIGINT,
            purchase_count BIGINT,
            total_revenue  DOUBLE,
            PRIMARY KEY (window_start, category) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{POSTGRES_URL}',
            'table-name' = 'analytics_summary',
            'username'   = '{POSTGRES_USER}',
            'password'   = '{POSTGRES_PASS}',
            'sink.buffer-flush.max-rows'     = '100',
            'sink.buffer-flush.interval'     = '5s'
        )
    """)

    # ------------------------------------------------------------------
    # Streaming aggregation query
    # ------------------------------------------------------------------
    # Tumbling window of WINDOW_MINUTES minutes; group by category.
    # COALESCE handles the case where no purchases occur in a window
    # (SUM of empty set → NULL without the guard).
    print(f"Submitting job: {WINDOW_MINUTES}-minute tumbling window analytics")

    t_env.execute_sql(f"""
        INSERT INTO analytics_summary
        SELECT
            TUMBLE_START(event_time, INTERVAL '{WINDOW_MINUTES}' MINUTE)  AS window_start,
            TUMBLE_END(event_time,   INTERVAL '{WINDOW_MINUTES}' MINUTE)  AS window_end,
            category,
            COUNT(*)                                                        AS event_count,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)            AS purchase_count,
            COALESCE(SUM(amount), 0.0)                                     AS total_revenue
        FROM clickstream
        GROUP BY
            TUMBLE(event_time, INTERVAL '{WINDOW_MINUTES}' MINUTE),
            category
    """)


if __name__ == "__main__":
    main()
