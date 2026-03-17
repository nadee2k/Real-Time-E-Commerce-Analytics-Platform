# Real-Time E-Commerce Analytics Platform

A Linux-friendly, portfolio-grade data platform showing end-to-end analytics engineering:

- **Streaming ingestion** with Kafka
- **Real-time transformation** with Spark Structured Streaming
- **Lakehouse layers** (Bronze/Silver/Gold) on local filesystem (S3-compatible design)
- **Warehouse serving** in PostgreSQL with star schema
- **Data quality checks** via a lightweight GE-style validator job
- **Orchestration** via Airflow DAGs
- **BI & monitoring** with Superset + Grafana
- **API serving** with FastAPI

## Architecture

```text
User Events -> Kafka -> Spark Streaming -> Bronze/Silver/Gold -> PostgreSQL Warehouse
                                                     |                |
                                                     v                v
                                                Grafana RT      Superset BI
                                                                      |
                                                                      v
                                                                  FastAPI
```

## Repository Layout

- `docker-compose.yml`: local stack bootstrapping
- `streaming/producer.py`: event simulator for clicks/carts/purchases
- `spark/stream_processor.py`: stream processing + KPI aggregation
- `warehouse/sql/star_schema.sql`: dimensions/facts + KPI mart table
- `jobs/load_gold_to_warehouse.py`: load Gold aggregates into PostgreSQL
- `quality/validate_gold.py`: data quality checks for Gold layer
- `airflow/dags/ecommerce_pipeline.py`: orchestrated daily/batch flow
- `api/main.py`: analytics API endpoints
- `tests/test_api_contract.py`: API contract tests

## Quickstart

1. Start services:
   ```bash
   docker compose up -d
   ```
2. Initialize warehouse schema:
   ```bash
   docker compose exec -T postgres psql -U analytics -d ecommerce -f /workspace/warehouse/sql/star_schema.sql
   ```
3. Start event producer:
   ```bash
   python streaming/producer.py
   ```
4. Run streaming processor (from Spark container or local Spark runtime):
   ```bash
   python spark/stream_processor.py
   ```
5. Load Gold data to warehouse:
   ```bash
   python jobs/load_gold_to_warehouse.py
   ```
6. Run API:
   ```bash
   uvicorn api.main:app --reload --port 8000
   ```

## Key KPIs

- Active users (5-min rolling window)
- Orders count and conversion rate
- Revenue and average order value
- Trending products by event count

## Notes

- This repo is designed to be **cloud-portable**: swap local lake paths for S3 and PostgreSQL for cloud warehouse.
- The producer supports deterministic generation with configurable rates and product/user pools.
