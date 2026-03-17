from __future__ import annotations

import os
from typing import Any

import psycopg
from fastapi import FastAPI

DB_DSN = os.getenv("DB_DSN", "postgresql://analytics:analytics@localhost:5432/ecommerce")
app = FastAPI(title="E-Commerce Analytics API", version="0.1.0")


def query_all(sql: str) -> list[dict[str, Any]]:
    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/kpis/latest")
def latest_kpis() -> dict[str, Any]:
    sql = """
    SELECT window_start, window_end, active_users, orders, revenue,
           conversion_rate, avg_order_value
    FROM gold_kpis_5m
    ORDER BY window_start DESC
    LIMIT 1;
    """
    rows = query_all(sql)
    return {"data": rows[0] if rows else None}


@app.get("/kpis/timeseries")
def kpi_timeseries(limit: int = 100) -> dict[str, Any]:
    sql = f"""
    SELECT window_start, active_users, orders, revenue
    FROM gold_kpis_5m
    ORDER BY window_start DESC
    LIMIT {int(limit)};
    """
    rows = query_all(sql)
    return {"data": rows}
