"""Simple streaming processor using kafka-python and pandas."""

import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer

# Configuration
KAFKA_SERVERS = ["localhost:9092"]
TOPIC = "user_activity"
DATA_DIR = Path("data")
BRONZE_DIR = DATA_DIR / "bronze" / "events"
SILVER_DIR = DATA_DIR / "silver" / "events"
GOLD_DIR = DATA_DIR / "gold" / "kpis"

# Create directories
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)

# In-memory storage for aggregation
bronze_data = []
silver_data = []
gold_aggregates = defaultdict(lambda: {"active_users": 0, "orders": 0, "revenue": 0.0})

def process_event(event):
    """Process a single event."""
    # Bronze: raw events
    bronze_data.append(event)

    # Silver: with derived fields
    silver_event = event.copy()
    silver_event["event_ts"] = pd.to_datetime(event["timestamp"])
    if event["action"] == "purchase":
        silver_event["order_revenue"] = event["price"] * event["quantity"]
    else:
        silver_event["order_revenue"] = 0.0
    silver_data.append(silver_event)

    # Gold: 5-minute aggregations
    window_start = silver_event["event_ts"].floor("5min")
    window_end = window_start + timedelta(minutes=5)
    key = window_start

    gold_aggregates[key]["active_users"] = len(set(e["user_id"] for e in silver_data if window_start <= pd.to_datetime(e["timestamp"]) < window_end))
    gold_aggregates[key]["orders"] = sum(1 for e in silver_data if window_start <= pd.to_datetime(e["timestamp"]) < window_end and e["action"] == "purchase")
    gold_aggregates[key]["revenue"] = sum(e["order_revenue"] for e in silver_data if window_start <= pd.to_datetime(e["timestamp"]) < window_end)

def save_data():
    """Save data to parquet files."""
    if bronze_data:
        df_bronze = pd.DataFrame(bronze_data)
        df_bronze.to_parquet(BRONZE_DIR / f"events_{int(time.time())}.parquet")

    if silver_data:
        df_silver = pd.DataFrame(silver_data)
        df_silver.to_parquet(SILVER_DIR / f"events_{int(time.time())}.parquet")

    if gold_aggregates:
        gold_rows = []
        for window_start, agg in gold_aggregates.items():
            gold_rows.append({
                "window": {"start": window_start, "end": window_start + timedelta(minutes=5)},
                "active_users": agg["active_users"],
                "orders": agg["orders"],
                "revenue": agg["revenue"]
            })
        if gold_rows:
            df_gold = pd.DataFrame(gold_rows)
            df_gold.to_parquet(GOLD_DIR / f"kpis_{int(time.time())}.parquet")

def main():
    """Main processing loop."""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    print("Starting simple stream processor...")

    try:
        for message in consumer:
            event = message.value
            print(f"Processing event: {event}")
            process_event(event)

            # Save periodically (every 10 events for demo)
            if len(bronze_data) % 10 == 0:
                save_data()
                print(f"Saved {len(bronze_data)} bronze events, {len(gold_aggregates)} gold aggregates")

    except KeyboardInterrupt:
        print("Stopping processor...")
        save_data()
        print("Final save completed.")

if __name__ == "__main__":
    main()
