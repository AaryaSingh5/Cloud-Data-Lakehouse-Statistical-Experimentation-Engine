CREATE TABLE IF NOT EXISTS alerts (
    id          BIGSERIAL   PRIMARY KEY,
    kind        TEXT        NOT NULL,
    severity    TEXT        NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    subject     TEXT        NOT NULL,
    message     TEXT        NOT NULL,
    occurrences INT         NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_alerts_open
    ON alerts (kind, subject) WHERE resolved_at IS NULL;