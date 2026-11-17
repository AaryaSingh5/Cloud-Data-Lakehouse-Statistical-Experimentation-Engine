CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source        TEXT        NOT NULL,
    batch_id      UUID,
    file_path     TEXT        NOT NULL,
    file_hash     TEXT        NOT NULL,
    batch_date    DATE        NOT NULL,
    status        TEXT        NOT NULL
                  CHECK (status IN ('success', 'skipped_duplicate', 'empty', 'failed')),
    rows_written  BIGINT      NOT NULL DEFAULT 0,
    error         TEXT,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ingestion_runs_source_hash
    ON ingestion_runs (source, file_hash);
    