CREATE TABLE IF NOT EXISTS lineage_edges (
    run_id       UUID        NOT NULL REFERENCES ingestion_runs (run_id) ON DELETE CASCADE,
    sale_date    DATE        NOT NULL,
    rows_loaded  BIGINT      NOT NULL,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, sale_date)
);
CREATE INDEX IF NOT EXISTS ix_lineage_sale_date ON lineage_edges (sale_date);