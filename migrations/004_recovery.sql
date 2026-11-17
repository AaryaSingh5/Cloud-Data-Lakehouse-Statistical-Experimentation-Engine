ALTER TABLE silver_loads DROP CONSTRAINT IF EXISTS silver_loads_status_check;
ALTER TABLE silver_loads ADD CONSTRAINT silver_loads_status_check
    CHECK (status IN ('loaded', 'excluded', 'superseded'));

CREATE TABLE IF NOT EXISTS quarantine_items (
    id           BIGSERIAL PRIMARY KEY,
    run_id       UUID        NOT NULL REFERENCES ingestion_runs (run_id) ON DELETE CASCADE,
    source       TEXT        NOT NULL,
    batch_date   DATE        NOT NULL,
    line_no      INT         NOT NULL,
    reason_codes TEXT        NOT NULL,
    raw_payload  TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'held'
                 CHECK (status IN ('held', 'released', 'superseded')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, line_no)
);
CREATE INDEX IF NOT EXISTS ix_quarantine_status
    ON quarantine_items (status, source, batch_date);

CREATE TABLE IF NOT EXISTS recovery_log (
    id          BIGSERIAL PRIMARY KEY,
    action      TEXT        NOT NULL CHECK (action IN ('retry', 'reconcile', 'replay', 'rollback')),
    target      TEXT        NOT NULL,
    run_id      UUID,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    duration_ms INT         NOT NULL,
    outcome     TEXT        NOT NULL CHECK (outcome IN ('success', 'recovered', 'failed')),
    detail      TEXT
);