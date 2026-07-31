# Force linux/amd64 so pip uses pre-built manylinux_x86_64 wheels for
# apache-flink and pemja (no arm64 wheels exist on PyPI; building from source
# requires cmake + maven and fails on JRE-based images).
# Docker Desktop with Rosetta handles amd64 emulation on Apple Silicon.
FROM --platform=linux/amd64 flink:1.18-scala_2.12-java11

# ---------------------------------------------------------------------------
# Install Python 3 and PyFlink
# ---------------------------------------------------------------------------
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev wget curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Pre-built x86_64 wheel installs cleanly (no compilation needed)
RUN pip3 install --no-cache-dir apache-flink==1.18.1

# ---------------------------------------------------------------------------
# Download connector JARs into Flink's lib directory
# ---------------------------------------------------------------------------
# Kafka SQL connector (fat JAR — includes Kafka client)
RUN wget -q \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" \
    -O /opt/flink/lib/flink-sql-connector-kafka.jar

# JDBC connector
RUN wget -q \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar" \
    -O /opt/flink/lib/flink-connector-jdbc.jar

# PostgreSQL JDBC driver
RUN wget -q \
    "https://jdbc.postgresql.org/download/postgresql-42.7.2.jar" \
    -O /opt/flink/lib/postgresql-jdbc.jar

# ---------------------------------------------------------------------------
# Copy job script
# ---------------------------------------------------------------------------
COPY analytics_job.py /opt/flink/jobs/

# Flink's own entrypoint handles starting JM / TM based on CMD args
ENTRYPOINT ["/docker-entrypoint.sh"]
