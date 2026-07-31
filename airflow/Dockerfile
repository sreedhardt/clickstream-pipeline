# Extend the official Airflow image with our DAG and provider packages.
# Build context must be the project root:
#   docker build -f airflow/Dockerfile -t clickstream-airflow:latest .

FROM apache/airflow:2.8.1-python3.11

# Install extra providers (Postgres hook, HTTP operator, etc.)
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.10.2 \
    apache-airflow-providers-http==4.10.0 \
    requests==2.31.0

# Copy our DAGs into the image
COPY dags/ /opt/airflow/dags/
