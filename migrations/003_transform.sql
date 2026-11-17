CREATE TABLE IF NOT EXISTS silver_loads (
    run_id      UUID        PRIMARY KEY REFERENCES ingestion_runs (run_id),
    batch_id    UUID,
    source      TEXT        NOT NULL,
    batch_date  DATE        NOT NULL,
    verdict     TEXT        NOT NULL,
    status      TEXT        NOT NULL CHECK (status IN ('loaded', 'excluded')),
    rows_in     BIGINT      NOT NULL,
    rows_kept   BIGINT      NOT NULL,
    rows_held   BIGINT      NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_publications (
    sale_date       DATE        PRIMARY KEY,
    status          TEXT        NOT NULL CHECK (status IN ('published', 'held')),
    sources_ok      TEXT        NOT NULL,
    sources_not_ok  TEXT        NOT NULL,
    orders          BIGINT,
    revenue         DOUBLE PRECISION,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);