CREATE TABLE IF NOT EXISTS benchmark_results (
    id           BIGSERIAL        PRIMARY KEY,
    seed         INT              NOT NULL,
    profile      TEXT             NOT NULL,
    arm          TEXT             NOT NULL CHECK (arm IN ('proposed', 'baseline')),
    metric       TEXT             NOT NULL,
    value        DOUBLE PRECISION,
    recorded_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    UNIQUE (seed, profile, arm, metric)
);