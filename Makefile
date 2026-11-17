.PHONY: up down shell test gen smoke airflow-up lint ingest quality transform replay rollback refresh checks backfill lineage feeds impact metrics dashboard

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

transform: ## Bronze to Silver to Gold (ALL=1 recomputes every date)
	docker compose exec app python -m transform.runner $(if $(ALL),--all-dates,)

replay:    ## re-check runs after a rule change (RUN=<run_id>, or ALL=1)
	docker compose exec app python -m recovery.replay $(if $(RUN),--run $(RUN),--all)

rollback:  ## restore Silver to a Delta version (LIST=1 to list, VERSION=n to restore)
	docker compose exec app python -m recovery.rollback $(if $(LIST),--list,--version $(VERSION))

refresh:   ## ingest new files, check, load Silver, supersede, refresh Gold
	docker compose exec app python -m recovery.refresh

checks:    ## alert checks (LIST=1 to show open alerts)
	docker compose exec app python -m orchestration.checks $(if $(LIST),--list,)

backfill:  ## re-run a date range: make backfill START=2025-01-01 END=2025-01-07
	docker compose exec app python -m orchestration.backfill --start $(START) --end $(END)

lineage:   ## rebuild lineage edges from Silver
	docker compose exec app python -m lineage.record

feeds:     ## which sources feed a Gold day: make feeds DATE=2025-01-03
	docker compose exec app python -m lineage.query feeds --date $(DATE)

impact:    ## what a run affects: make impact RUN=<id> or SOURCE=pos DATE=2025-01-01
	docker compose exec app python -m lineage.query impact $(if $(RUN),--run $(RUN),--source $(SOURCE) --date $(DATE))

metrics:   ## collect pipeline metrics into pipeline_metrics
	docker compose exec app python -m monitoring.collect

dashboard: ## start the Streamlit dashboard on http://localhost:8501
	docker compose up -d dashboard

airflow-up:
	docker compose --profile airflow up -d --build airflow