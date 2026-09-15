-- Durable KNEMON spend-window aggregate (SQLite).
-- ``provider = '*'`` is the sentinel for "all providers" because
-- SQLite does NOT enforce UNIQUE on NULL values, so a NULLable
-- provider column would allow the unfiltered row to be inserted
-- an unbounded number of times.
CREATE TABLE IF NOT EXISTS knemon_spend_window_aggregates (
    caller_subsystem        TEXT NOT NULL,
    provider                TEXT NOT NULL DEFAULT '*',
    window_start_ts         TEXT NOT NULL,
    committed_usd           REAL NOT NULL DEFAULT 0 CHECK (committed_usd >= 0),
    pending_usd             REAL NOT NULL DEFAULT 0 CHECK (pending_usd >= 0),
    last_reconciled_at      TEXT,
    last_updated_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (caller_subsystem, provider)
);
CREATE INDEX IF NOT EXISTS idx_knemon_spend_window_aggregates_window
    ON knemon_spend_window_aggregates (window_start_ts);
