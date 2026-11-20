# Blueprint: Data Reliability & Analytics Platform (master checklist)

Rules for every chat: read this + HANDOFF.md first, build only the next unchecked items, give complete runnable code with file paths, don't change locked decisions without a stated reason, end with an updated HANDOFF block.

Locked stack: Delta Lake on MinIO (S3 API; swap to AWS S3 in Phase 11) | PySpark local mode + DuckDB | Airflow | custom rule engine (Great Expectations optional comparison) | custom/OpenLineage lineage in Postgres | Postgres metadata | Streamlit | Docker Compose, GitHub Actions, pytest.
Scope: Module A (Phases 1-9, 11-12) core platform. Module B (Phase 10) experimentation engine on Gold, only after A is done.

## Phase 1 Foundation
- [x] 1.1 Repo + structure  - [x] 1.2 Docker Compose (MinIO, Postgres, app; Airflow optional profile)
- [x] 1.3 Config + logging  - [x] 1.4 Makefile  - [x] 1.5 CI (ruff + pytest)
## Phase 2 Synthetic data + fault injector
- [x] 2.1 3 sources (pos CSV, web JSONL, partner CSV with different column names)
- [x] 2.2 Fault injector (duplicate_exact, duplicate_key_conflict, null_values, out_of_range, corrupt_rows, column_rename, column_dropped, extra_column, type_change, timestamp_format_change, empty_file, late_arriving)
- [x] 2.3 Ground truth (ground_truth.csv, batches.csv) + clean copies
## Phase 3 Ingestion + Bronze
- [x] 3.1 Connectors  - [x] 3.2 Bronze writer (append-only, metadata: source, ingest_ts, batch_id, file_hash)
- [x] 3.3 Idempotent ingestion  - [x] 3.4 Run log in Postgres
## Phase 4 Data quality engine
- [x] 4.1 Schema registry  - [x] 4.2 Schema validation + drift classification  - [x] 4.3 Evolution policy (auto-merge safe, quarantine breaking)
- [x] 4.4 Duplicate detection  - [x] 4.5 Missing data / completeness / volume vs history  - [x] 4.6 Validity rules
- [x] 4.7 Statistical anomaly checks  - [x] 4.8 YAML rules with warn/block severity  - [x] 4.9 Results stored per batch per rule
## Phase 5 Silver + Gold
- [x] 5.1 Bronze->Silver (clean, dedupe, standardize, map 3 source schemas to one canonical schema)
- [x] 5.2 Silver->Gold aggregates  - [x] 5.3 Gate: failed batch blocks Gold  - [x] 5.4 Incremental merge/upsert
## Phase 6 Recovery
- [x] 6.1 Quarantine with reason codes  - [x] 6.2 Retry with backoff  - [x] 6.3 Replay after fix
- [x] 6.4 Rollback via Delta time travel  - [x] 6.5 Recovery outcomes logged
## Phase 7 Lineage + orchestration
- [x] 7.1 Lineage capture  - [x] 7.2 Lineage queries ("what does this bad batch affect?")
- [x] 7.3 Airflow DAGs + failure alerts  - [x] 7.4 Backfill
## Phase 8 Monitoring
- [x] 8.1 Metrics collector (freshness, runtime, resources, quality score, failures)
- [x] 8.2 Streamlit dashboard  - [x] 8.3 Alerts
## Phase 9 Research evaluation
- [x] 9.1 Baseline (no checks) vs Proposed on identical fault-injected data
- [x] 9.2 Metrics: detection precision/recall/F1, runtime overhead, recovery success/time, duplicates prevented, freshness, resource use, Gold downstream error rate
- [x] 9.3 Several fault rates, repeated runs, mean +- std  - [x] 9.4 Significance tests  - [x] 9.5 docs/results.md with tables/charts
## Phase 10 Module B: experimentation engine (on Gold)
- [x] 10.1 Data model  - [x] 10.2 Tests (t, Welch, chi-square, Mann-Whitney, z-prop)
- [x] 10.3 Effect size, CIs, multiple-testing correction  - [x] 10.4 Power / sample size
- [x] 10.5 Sequential testing (stretch)  - [x] 10.6 Dashboard page
- [x] 10.7 Show how bad data distorts experiment conclusions (links A and B)
## Phase 11 Cloud
- [ ] 11.1 MinIO -> AWS S3  - [ ] 11.2 Deploy (EC2 or Glue/EMR Serverless) + schedule  - [ ] 11.3 IAM/secrets  - [ ] 11.4 Cost note
## Phase 12 Finish
- [x] 12.1 Tests 70%+  - [x] 12.2 README + architecture diagram + demo GIF  - [x] 12.3 Demo script  - [x] 12.4 Paper-style writeup  - [x] 12.5 Resume bullets

## Done means
`make demo` runs the full scenario; injected faults are caught and Gold stays clean; recovery works; dashboard shows health/quality/lineage; benchmarks produce result tables; experimentation runs on Gold; deployed on AWS; CI green.
