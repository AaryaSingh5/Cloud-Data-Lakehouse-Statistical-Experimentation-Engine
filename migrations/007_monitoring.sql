ALTER TABLE alerts ADD COLUMN IF NOT EXISTS acknowledged_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id           BIGSERIAL        PRIMARY KEY,
    captured_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    metric       TEXT             NOT NULL,
    source       TEXT,
    value        DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_pipeline_metrics_metric_time
    ON pipeline_metrics (metric, captured_at);