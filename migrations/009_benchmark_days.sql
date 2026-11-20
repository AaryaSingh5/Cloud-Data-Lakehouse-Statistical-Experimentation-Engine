CREATE TABLE IF NOT EXISTS benchmark_days (
    seed           INT              NOT NULL,
    profile        TEXT             NOT NULL,
    arm            TEXT             NOT NULL CHECK (arm IN ('proposed', 'baseline')),
    sale_date      DATE             NOT NULL,
    published      BOOLEAN          NOT NULL,
    pred_orders    BIGINT,
    pred_revenue   DOUBLE PRECISION,
    truth_orders   BIGINT           NOT NULL,
    truth_revenue  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (seed, profile, arm, sale_date)
);