CREATE TABLE IF NOT EXISTS experiments (
    name          TEXT             PRIMARY KEY,
    start_date    DATE             NOT NULL,
    end_date      DATE             NOT NULL,
    planted_lift  DOUBLE PRECISION NOT NULL,
    created_at    TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS experiment_runs (
    run_id       BIGSERIAL   PRIMARY KEY,
    experiment   TEXT        NOT NULL REFERENCES experiments (name) ON DELETE CASCADE,
    data_source  TEXT        NOT NULL CHECK (data_source IN ('truth', 'platform', 'baseline')),
    n_units      INT         NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS experiment_results (
    run_id          BIGINT           NOT NULL REFERENCES experiment_runs (run_id) ON DELETE CASCADE,
    metric          TEXT             NOT NULL,
    test            TEXT             NOT NULL,
    statistic       DOUBLE PRECISION,
    p_value         DOUBLE PRECISION,
    p_adjusted      DOUBLE PRECISION,
    effect          DOUBLE PRECISION,
    ci_low          DOUBLE PRECISION,
    ci_high         DOUBLE PRECISION,
    n_control       INT              NOT NULL,
    n_treatment     INT              NOT NULL,
    mean_control    DOUBLE PRECISION,
    mean_treatment  DOUBLE PRECISION,
    PRIMARY KEY (run_id, metric, test)
);