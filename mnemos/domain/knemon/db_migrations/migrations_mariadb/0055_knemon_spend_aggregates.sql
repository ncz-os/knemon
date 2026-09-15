-- Durable KNEMON spend-window aggregate (MySQL/MariaDB).
-- ``provider = '*'`` is the sentinel for "all providers" — see
-- the PostgreSQL migration for the rationale.
CREATE TABLE IF NOT EXISTS knemon_spend_window_aggregates (
    caller_subsystem        VARCHAR(64) NOT NULL,
    provider                VARCHAR(64) NOT NULL DEFAULT '*',
    window_start_ts         DATETIME(6) NOT NULL,
    committed_usd           DECIMAL(18, 8) NOT NULL DEFAULT 0
        CHECK (committed_usd >= 0),
    pending_usd             DECIMAL(18, 8) NOT NULL DEFAULT 0
        CHECK (pending_usd >= 0),
    last_reconciled_at      DATETIME(6) NULL,
    last_updated_at         DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (caller_subsystem, provider),
    INDEX idx_knemon_spend_window_aggregates_window (window_start_ts)
) ENGINE=InnoDB;
