.PHONY: up down shell test gen smoke airflow-up lint ingest quality

up:        ## build + start MinIO, Postgres, app container
	docker compose up -d --build

down:
	docker compose down

shell:
	docker compose exec app bash

test:
	docker compose exec app pytest -q

lint:
	docker compose exec app ruff check .

gen:       ## generate synthetic multi-source data with injected faults
	docker compose exec app python -m data_generator.generate

smoke:     ## Spark + Delta + MinIO smoke test
	docker compose exec app python scripts/smoke_spark_delta.py

ingest:    ## ingest landing files into Bronze (optional SOURCE=pos|web|partner)
	docker compose exec app python -m ingestion.pipeline $(if $(SOURCE),--source $(SOURCE),)

quality:   ## run quality checks on pending ingestion runs (optional SOURCE=)
	docker compose exec app python -m quality.runner $(if $(SOURCE),--source $(SOURCE),)

airflow-up:
	docker compose --profile airflow up -d