"""
Clickstream Event Producer
--------------------------
Generates simulated e-commerce clickstream events and publishes them to Kafka.

Events represent user interactions: page views, add-to-cart, and purchases.
Designed to run as a long-lived Kubernetes Deployment.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ---------------------------------------------------------------------------
# Config (from environment variables)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka.streaming.svc.cluster.local:9092"
)
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "clickstream-events")
EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "10"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data fixtures
# ---------------------------------------------------------------------------
CATEGORIES = ["electronics", "clothing", "books", "sports", "home", "beauty", "toys"]

PRODUCTS = {
    "electronics": ["laptop", "smartphone", "headphones", "tablet", "smartwatch"],
    "clothing":    ["jacket", "jeans", "sneakers", "dress", "hoodie"],
    "books":       ["novel", "textbook", "cookbook", "biography", "comic"],
    "sports":      ["yoga_mat", "dumbbells", "bike_helmet", "running_shoes", "water_bottle"],
    "home":        ["lamp", "pillow", "coffee_maker", "candle", "plant_pot"],
    "beauty":      ["moisturizer", "lipstick", "perfume", "shampoo", "serum"],
    "toys":        ["lego_set", "board_game", "action_figure", "puzzle", "rc_car"],
}

PRICE_RANGES = {
    "electronics": (50, 1500),
    "clothing":    (15, 250),
    "books":       (5, 80),
    "sports":      (10, 400),
    "home":        (8, 300),
    "beauty":      (5, 150),
    "toys":        (10, 200),
}

# Event type distribution: 60% views, 25% cart, 15% purchase
EVENT_TYPES   = ["view", "cart", "purchase"]
EVENT_WEIGHTS = [0.60,   0.25,   0.15]

USER_COUNT    = 500   # Simulate N unique users
SESSION_COUNT = 100   # Active sessions at once


def generate_event() -> dict:
    category   = random.choice(CATEGORIES)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    product    = random.choice(PRODUCTS[category])
    low, high  = PRICE_RANGES[category]

    # Only purchases have non-zero amount; cart has 0 (intent, not revenue)
    amount = round(random.uniform(low, high), 2) if event_type == "purchase" else 0.0

    return {
        "user_id":    f"user_{random.randint(1, USER_COUNT):04d}",
        "session_id": f"sess_{random.randint(1, SESSION_COUNT):04d}",
        "product_id": f"{category}_{product}_{random.randint(1, 50):03d}",
        "category":   category,
        "event_type": event_type,
        "amount":     amount,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


def on_send_error(exc: Exception) -> None:
    log.error("Failed to deliver message: %s", exc)


def connect_producer(max_retries: int = 10, retry_delay: int = 5) -> KafkaProducer:
    """Connect to Kafka with retries (handles slow broker startup)."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                max_block_ms=10_000,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning(
                "Kafka not ready (attempt %d/%d). Retrying in %ds...",
                attempt, max_retries, retry_delay,
            )
            time.sleep(retry_delay)
    raise RuntimeError(f"Could not connect to Kafka after {max_retries} attempts")


def main() -> None:
    log.info("Starting clickstream producer")
    log.info("  Kafka:  %s", KAFKA_BOOTSTRAP_SERVERS)
    log.info("  Topic:  %s", KAFKA_TOPIC)
    log.info("  Rate:   %.1f events/sec", EVENTS_PER_SECOND)

    producer = connect_producer()
    sleep_interval = 1.0 / EVENTS_PER_SECOND
    sent = 0
    shutdown = False

    def handle_sigterm(*_):
        nonlocal shutdown
        log.info("Received shutdown signal, flushing and exiting...")
        shutdown = True

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    try:
        while not shutdown:
            event = generate_event()
            producer.send(KAFKA_TOPIC, value=event).add_errback(on_send_error)
            sent += 1

            if sent % 1000 == 0:
                log.info("Sent %d events so far (latest: %s)", sent, event["event_type"])

            time.sleep(sleep_interval)
    finally:
        log.info("Flushing producer... (total sent: %d)", sent)
        producer.flush()
        producer.close()
        log.info("Producer shut down cleanly.")


if __name__ == "__main__":
    main()
