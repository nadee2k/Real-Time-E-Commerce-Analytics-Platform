"""Kafka event simulator for e-commerce clickstream."""

from __future__ import annotations

import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()
ACTIONS = ["click", "add_to_cart", "purchase"]
ACTION_WEIGHTS = [0.65, 0.25, 0.10]


def build_event(users: int, products: int) -> dict:
    action = random.choices(ACTIONS, weights=ACTION_WEIGHTS, k=1)[0]
    price = round(random.uniform(5, 500), 2)
    quantity = 1 if action != "purchase" else random.randint(1, 4)

    return {
        "event_id": fake.uuid4(),
        "user_id": random.randint(1, users),
        "product_id": random.randint(1, products),
        "action": action,
        "price": price,
        "quantity": quantity,
        "session_id": fake.uuid4(),
        "country": fake.country_code(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    topic = os.getenv("KAFKA_TOPIC", "user_activity")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    interval_ms = int(os.getenv("EVENT_INTERVAL_MS", "500"))
    users = int(os.getenv("USERS", "500"))
    products = int(os.getenv("PRODUCTS", "250"))

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return

    logger.info(f"Producing events to {topic} on {bootstrap} every {interval_ms}ms")
    while True:
        event = build_event(users=users, products=products)
        try:
            producer.send(topic, event)
            producer.flush()
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
        time.sleep(interval_ms / 1000)


if __name__ == "__main__":
    main()
