# Real-Time E-Commerce Analytics Platform

**Real-time e-commerce analytics pipeline**, built for visibility from event ingress to KPI dashboards.

- 🚀 Streaming ingestion: Kafka event bus
- 🔄 Transformation: Spark Structured Streaming
- 🏭 Lakehouse layers: Bronze / Silver / Gold
- 🧩 Warehouse: PostgreSQL star schema
- 🧪 Quality: validator + data checks
- ⚙️ Orchestration: Airflow DAG
- 📊 Monitoring: Superset + Grafana
- 🌐 API: FastAPI endpoint package

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

- `docker-compose.yml` — local stack bootstrap
- `streaming/producer.py` — event simulator (clicks / carts / purchases)
- `spark/stream_processor.py` — structured stream transformation + KPI aggregation
- `warehouse/sql/star_schema.sql` — star-schema model for dimensions + facts
- `jobs/load_gold_to_warehouse.py` — Gold layer load into Postgres
- `quality/validate_gold.py` — quality checks for Gold layer processing
- `airflow/dags/ecommerce_pipeline.py` — DAG-driven workflow orchestration
- `api/main.py` — FastAPI analytics endpoints
- `tests/test_api_contract.py` — API contract tests

## Quickstart

1. Start the stack:
   ```bash
   docker compose up -d
   ```
2. Initialize warehouse schema:
   ```bash
   docker compose exec -T postgres psql -U analytics -d ecommerce -f /workspace/warehouse/sql/star_schema.sql
   ```
3. Start the event generator:
   ```bash
   python streaming/producer.py
   ```
4. Start streaming ETL:
   ```bash
   python spark/stream_processor.py
   ```
5. Push Gold aggregates to warehouse:
   ```bash
   python jobs/load_gold_to_warehouse.py
   ```
6. Start API server:
   ```bash
   uvicorn api.main:app --reload --port 8000
   ```
7. Open dashboards
   - Grafana (port from `docker-compose`)
   - Superset (port from `docker-compose`)

## Key KPIs

- Active users (5-min rolling window)
- Orders count and conversion rate
- Revenue and average order value
- Top product trends by event count

## Best Practices

- Keep raw events in Bronze, cleaned metrics in Silver, business aggregates in Gold
- Use `quality/validate_gold.py` as part of pipeline validation
- Keep local paths in Spark config to simplify cloud substitution

## Next Enhancements

- Cloud storage support (S3 / MinIO)
- Managed warehouse sink (Amazon Redshift / BigQuery)
- Alerting (Prometheus + Grafana)
- CI for data quality and contract tests
- Authentication and multi-tenant analytics API
