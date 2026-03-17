"""Load Gold KPIs from parquet into PostgreSQL."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import psycopg

GOLD_PATH = Path(os.getenv("GOLD_PATH", "data/gold/kpis"))
DB_DSN = os.getenv("DB_DSN", "postgresql://analytics:analytics@localhost:5432/ecommerce")


def latest_gold() -> pd.DataFrame:
    files = sorted(GOLD_PATH.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found under {GOLD_PATH}")
    return pd.read_parquet(files)


def main() -> None:
    df = latest_gold()
    if df.empty:
        print("No rows to load.")
        return

    rows = []
    for _, row in df.iterrows():
        start = row["window"]["start"]
        end = row["window"]["end"]
        active_users = int(row["active_users"])
        orders = int(row["orders"])
        revenue = float(row["revenue"] or 0)
        conversion_rate = (orders / active_users) if active_users else 0.0
        avg_order_value = (revenue / orders) if orders else 0.0
        rows.append((start, end, active_users, orders, revenue, conversion_rate, avg_order_value))

    upsert = """
    INSERT INTO gold_kpis_5m
      (window_start, window_end, active_users, orders, revenue, conversion_rate, avg_order_value)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (window_start) DO UPDATE
      SET window_end = EXCLUDED.window_end,
          active_users = EXCLUDED.active_users,
          orders = EXCLUDED.orders,
          revenue = EXCLUDED.revenue,
          conversion_rate = EXCLUDED.conversion_rate,
          avg_order_value = EXCLUDED.avg_order_value,
          loaded_at = NOW();
    """

    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.executemany(upsert, rows)
        conn.commit()

    print(f"Loaded {len(rows)} KPI rows into warehouse")


if __name__ == "__main__":
    main()
