# Data Reliability & Analytics Platform

Medallion (Bronze/Silver/Gold) lakehouse that detects, blocks and recovers from bad data before it reaches analytics.
Research question: can automated data-quality checks and pipeline recovery reduce downstream data errors and improve analytics reliability?

Progress: see `docs/BLUEPRINT.md` (checklist) and `HANDOFF.md` (latest state).

## Quickstart
```bash
docker compose up -d --build          # MinIO, Postgres, app container (first build takes a few minutes)
docker compose exec app pytest -q     # unit tests
docker compose exec app python -m data_generator.generate --profile medium
docker compose exec app python scripts/smoke_spark_delta.py --local   # Spark + Delta check, no MinIO
docker compose exec app python scripts/smoke_spark_delta.py           # Spark + Delta + MinIO
```
MinIO console: http://localhost:9001 (minioadmin / minioadmin). Without `make` (e.g. Windows), use the commands above.

## Synthetic data
Three sources (POS CSV, web JSONL, partner CSV with different column names), 12 injectable fault types, and a ground-truth log
of every injected fault so detection rate can be measured exactly. Profiles: `none | low | medium | high`.
