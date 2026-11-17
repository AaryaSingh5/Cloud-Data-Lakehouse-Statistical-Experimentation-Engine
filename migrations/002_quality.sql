CREATE TABLE IF NOT EXISTS quality_verdicts (
    run_id          UUID        PRIMARY KEY REFERENCES ingestion_runs (run_id),
    source          TEXT        NOT NULL,
    batch_id        UUID,
    batch_date      DATE        NOT NULL,
    schema_version  INT,
    schema_hash     TEXT,
    row_count       BIGINT      NOT NULL,
    bad_rows        BIGINT      NOT NULL,
    mean_unit_price DOUBLE PRECISION,
    mean_quantity   DOUBLE PRECISION,
    verdict         TEXT        NOT NULL CHECK (verdict IN ('PASS', 'WARN', 'BLOCK')),
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_quality_verdicts_history
    ON quality_verdicts (source, batch_date);

CREATE TABLE IF NOT EXISTS quality_rule_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID      NOT NULL REFERENCES quality_verdicts (run_id) ON DELETE CASCADE,
    rule_id     TEXT      NOT NULL,
    severity    TEXT      NOT NULL,
    triggered   BOOLEAN   NOT NULL,
    observed    DOUBLE PRECISION,
    threshold   TEXT,
    detail      TEXT
);

CREATE TABLE IF NOT EXISTS quality_row_findings (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID      NOT NULL REFERENCES quality_verdicts (run_id) ON DELETE CASCADE,
    row_index   INT       NOT NULL,
    rule_id     TEXT      NOT NULL,
    field       TEXT,
    detail      TEXT
);
CREATE INDEX IF NOT EXISTS ix_quality_row_findings_run
    ON quality_row_findings (run_id);