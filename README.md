# Real-Time Clickstream Analytics Pipeline

A local data streaming project built on **Apache Kafka**, **Apache Flink**, and **Apache Airflow**, deployed on **minikube** (Kubernetes).

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  minikube (local Kubernetes)     namespace: streaming                        │
│                                                                              │
│  ┌─────────────┐   clickstream-events   ┌──────────────────────────────┐   │
│  │  Producer   │ ──────────────────────▶│  Apache Kafka (KRaft)        │   │
│  │  (Python)   │                        │  Bitnami Helm chart          │   │
│  │  ~10 ev/sec │                        └──────────┬───────────────────┘   │
│  └─────────────┘                                   │                        │
│                                                     ▼                        │
│                                  ┌──────────────────────────────────┐       │
│                                  │  Apache Flink (Session Cluster)  │       │
│                                  │  PyFlink Table API               │       │
│                                  │  1-min tumbling window agg.      │       │
│                                  └──────────────┬───────────────────┘       │
│                                                  │                           │
│                                                  ▼                           │
│                              ┌───────────────────────────────────┐          │
│                              │  PostgreSQL                        │          │
│                              │  table: analytics_summary          │          │
│                              │  (window_start, category, ...)     │          │
│                              └───────────────────────────────────┘          │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │  Apache Airflow (hourly DAG)                                 │           │
│  │   ├─ check_kafka_health ─┐                                   │           │
│  │   └─ check_flink_health ─┴─▶ generate_analytics_report       │           │
│  └─────────────────────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### What the pipeline does

1. **Producer** (`producer/producer.py`) — generates simulated e-commerce events (views, cart-adds, purchases) at ~10 events/sec across 7 product categories.
2. **Kafka** — receives and buffers events in the `clickstream-events` topic (3 partitions, 1-day retention).
3. **Flink** — reads from Kafka with event-time processing, applies a 1-minute tumbling window, and writes aggregated stats (event count, purchase count, total revenue) per category to PostgreSQL. **Delivery semantics:** the job checkpoints every 60s so it resumes from the last committed Kafka offset after a failure, but the JDBC sink writes are non-transactional — so end-to-end delivery is **at-least-once**, not exactly-once (a restart can re-emit or adjust rows for the in-flight window rather than lose data).
4. **PostgreSQL** — stores the `analytics_summary` table. Query it any time for real-time analytics.
5. **Airflow** — runs an hourly monitoring DAG: health-checks Kafka and Flink, then logs an analytics report from PostgreSQL.

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | 4.x+ | Must be running |
| kubectl | 1.27+ | `brew install kubectl` |
| minikube | 1.31+ | `brew install minikube` |
| Helm | 3.x | Installed automatically by the infrastructure script if missing |

**Minimum system resources:** 4 CPU cores, 8 GB RAM free for minikube.

---

## Quick Start

### 1. Infrastructure (Kafka + PostgreSQL)

```bash
./scripts/setup-infrastructure.sh
```

This will:
- Start minikube (4 CPUs, 8 GB RAM)
- Install Helm if missing
- Generate credentials and store them as K8s Secrets (`scripts/create-secrets.sh`) — random values, never written to git
- Deploy Kafka (Bitnami, KRaft mode, single broker) with the `clickstream-events` topic pre-created
- Deploy PostgreSQL with the `analytics_summary` table initialised

**Verify:**
```bash
kubectl get pods -n streaming
# Expected: kafka pod Running, postgresql pod Running
```

---

### 2. Streaming job (Flink + Data Producer)

```bash
./scripts/setup-streaming-job.sh
```

This will:
- Build `clickstream-flink` and `clickstream-producer` Docker images inside minikube
- Deploy the Flink session cluster (JobManager + TaskManager)
- Submit the PyFlink analytics job to Flink
- Deploy the event producer

**Verify data is flowing end-to-end:**
```bash
# Watch producer logs
kubectl logs -n streaming -l app=clickstream-producer -f

# After ~90 seconds, query results in PostgreSQL
./scripts/query-postgres.sh
```

**Open Flink UI** (in a separate terminal):
```bash
./scripts/port-forward.sh flink
# Open: http://localhost:8081
```

---

### 3. Orchestration (Airflow)

```bash
./scripts/setup-orchestration.sh
```

This will:
- Build a custom Airflow image with the monitoring DAG baked in
- Deploy Airflow via the official Helm chart (LocalExecutor)
- Trigger the `clickstream_pipeline_monitor` DAG

**Open Airflow UI** (in a separate terminal):
```bash
./scripts/port-forward.sh airflow
# Open: http://localhost:8080  (admin / admin — local-only demo login, see helm/airflow-values.yaml)
```

In the UI, find the `clickstream_pipeline_monitor` DAG and trigger it manually to run all health checks immediately.

---

## Project Structure

```
.
├── airflow/
│   └── Dockerfile                  # Extends apache/airflow with our DAG
├── dags/
│   └── clickstream_pipeline_dag.py # Hourly health-check + report DAG
├── flink_jobs/
│   ├── analytics_job.py            # PyFlink Table API streaming job
│   └── Dockerfile                  # Flink image with Python + connector JARs
├── helm/
│   ├── kafka-values.yaml           # Bitnami Kafka chart overrides
│   ├── postgres-values.yaml        # Bitnami PostgreSQL chart overrides
│   └── airflow-values.yaml         # Official Airflow chart overrides
├── k8s/
│   ├── namespace.yaml
│   ├── flink/
│   │   ├── flink-session-cluster.yaml   # JobManager + TaskManager Deployments
│   │   └── flink-job-submit.yaml        # K8s Job to submit the PyFlink job
│   └── producer/
│       └── producer.yaml
├── producer/
│   ├── producer.py                 # Generates fake clickstream events
│   ├── requirements.txt
│   └── Dockerfile
└── scripts/
    ├── setup-infrastructure.sh     # 1: Kafka + PostgreSQL
    ├── setup-streaming-job.sh      # 2: Flink + producer
    ├── setup-orchestration.sh      # 3: Airflow
    ├── create-secrets.sh           # Generates K8s Secrets (called by setup-infrastructure/orchestration)
    ├── port-forward.sh             # Port-forward helper
    ├── query-postgres.sh           # Run analytics queries
    └── teardown.sh                 # Delete everything
```

---

## Useful Commands

### Cluster status
```bash
kubectl get pods -n streaming          # All pods
kubectl get pods -n streaming -w       # Watch for changes
```

### Kafka
```bash
# Describe the topic
kubectl exec -n streaming \
  $(kubectl get pods -n streaming -l app.kubernetes.io/name=kafka -o name | head -1) \
  -- kafka-topics.sh --bootstrap-server localhost:9092 \
     --describe --topic clickstream-events

# Consume messages live (sanity check)
kubectl exec -it -n streaming \
  $(kubectl get pods -n streaming -l app.kubernetes.io/name=kafka -o name | head -1) \
  -- kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic clickstream-events \
     --from-beginning --max-messages 5
```

### Flink
```bash
# REST API — list running jobs
kubectl exec -n streaming \
  $(kubectl get pods -n streaming -l component=jobmanager -o name | head -1) \
  -- curl -s http://localhost:8081/jobs/overview | python3 -m json.tool

# Re-submit the analytics job (e.g., after a config change)
kubectl delete job flink-job-submit -n streaming --ignore-not-found
kubectl apply  -f k8s/flink/flink-job-submit.yaml
```

### PostgreSQL
```bash
# Interactive psql session
kubectl exec -it -n streaming \
  $(kubectl get pods -n streaming -l app.kubernetes.io/name=postgresql -o name | head -1) \
  -- psql -U analytics -d analytics

# Quick analytics query
./scripts/query-postgres.sh
```

### Airflow
```bash
# List DAGs
kubectl exec -n streaming \
  $(kubectl get pods -n streaming -l component=scheduler -o name | head -1) \
  -- airflow dags list

# Trigger DAG manually
kubectl exec -n streaming \
  $(kubectl get pods -n streaming -l component=scheduler -o name | head -1) \
  -- airflow dags trigger clickstream_pipeline_monitor
```

---

## Tear Down

```bash
./scripts/teardown.sh
```

Removes all Helm releases, K8s resources, and the namespace. Optionally stops minikube.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| Flink job not starting | Check logs: `kubectl logs -n streaming -l component=jobmanager` |
| No data in PostgreSQL | Verify the submission job completed: `kubectl logs -n streaming -l job-name=flink-job-submit` |
| Kafka connection refused | Wait 30–60s for Kafka to fully start; check: `kubectl get pods -n streaming` |
| Image pull errors | Make sure you ran `eval $(minikube docker-env)` before `docker build` |
| minikube OOMKilled | Increase memory: `minikube delete && minikube start --memory=10240` |
| Airflow DAG not showing | Wait 60s for scheduler; check: `kubectl logs -n streaming -l component=scheduler --tail=50` |

---

## Key Configuration

| Setting | Value | Where to change |
|---|---|---|
| Events per second | 10 | `k8s/producer/producer.yaml` → `EVENTS_PER_SECOND` |
| Window size | 1 minute | `k8s/flink/flink-job-submit.yaml` → `WINDOW_MINUTES` |
| Kafka topic | `clickstream-events` | `helm/kafka-values.yaml` |
| Airflow schedule | `@hourly` | `dags/clickstream_pipeline_dag.py` |
| DB credentials | generated randomly per-deploy, stored as a K8s Secret | `scripts/create-secrets.sh` (secret `postgres-credentials`) |
