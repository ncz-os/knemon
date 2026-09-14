-- Durable KNEMON cost-budget reservations (SQLite).
CREATE TABLE IF NOT EXISTS knemon_cost_reservations (
    attempt_id          TEXT PRIMARY KEY,
    provider            TEXT NOT NULL,
    model               TEXT NOT NULL,
    caller_subsystem    TEXT NOT NULL,
    estimated_cost_usd  REAL NOT NULL CHECK (estimated_cost_usd >= 0),
    actual_cost_usd     REAL CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    reserved_at         TEXT NOT NULL,
    expires_at          TEXT NOT NULL,
    settled_at          TEXT,
    released_at         TEXT,
    ledger_id           INTEGER,
    status              TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'settled', 'released'))
);
CREATE TABLE IF NOT EXISTS knemon_cost_reservation_locks (
    caller_subsystem    TEXT PRIMARY KEY,
    touched_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_knemon_cost_reservations_pending_lookup
    ON knemon_cost_reservations (caller_subsystem, provider, status, expires_at);
CREATE INDEX IF NOT EXISTS idx_knemon_cost_reservations_attempt
    ON knemon_cost_reservations (attempt_id);
CREATE INDEX IF NOT EXISTS idx_knemon_cost_reservations_retention
    ON knemon_cost_reservations (status, settled_at, released_at, expires_at);