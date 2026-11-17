## HANDOFF (after chat #1)
Built so far: Phase 1 (all) and Phase 2 (all). See docs/BLUEPRINT.md for the checklist.
- Docker: docker-compose.yml (MinIO + buckets landing/bronze/silver/gold/quarantine, Postgres drp/drp/drp, `app` container with Java+PySpark 3.5.1+delta-spark 3.2.0+DuckDB; Airflow behind `--profile airflow`), Dockerfile, Makefile
- common/config.py (YAML + DRP__SECTION__KEY env overrides), common/logging_utils.py, common/spark.py (get_spark: Delta + S3A to MinIO)
- scripts/smoke_spark_delta.py (--local skips MinIO)
- data_generator/: sources.py (pos CSV, web JSONL, partner CSV; partner uses different column names order_ref/sold_at/sku/units/price_usd/region), profiles.py (none/low/medium/high), generate.py
- Outputs: data/clean/, data/landing/<source>/<delivered_date>/<source>_<batch_date>.<ext>, data/ground_truth/ground_truth.csv + batches.csv
- tests/ (9 passing), CI workflow (ruff + pytest)
Verified working: pytest 9/9 and ruff clean in a plain Python env; generator output inspected.
NOT yet verified: Docker stack and Spark/Delta/MinIO smoke test (needs to be run on the user's machine: `docker compose up -d --build`, then `make smoke`).
Known issues / deviations: none. Airflow is optional until Phase 7.
Next up: Phase 3 (connectors, Bronze Delta writer with batch_id/file_hash/ingest_ts, idempotent ingestion, Postgres run log).
Commands: docker compose up -d --build | docker compose exec app pytest -q | docker compose exec app python -m data_generator.generate --profile medium | docker compose exec app python scripts/smoke_spark_delta.py [--local]
Open decisions: none.
