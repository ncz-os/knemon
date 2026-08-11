-- Durable KNEMON session-burn reservations (SQLite).
CREATE TABLE IF NOT EXISTS knemon_burn_sessions (
    session_id TEXT PRIMARY KEY,
    touched_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS knemon_burn_reservations (
    reservation_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES knemon_burn_sessions(session_id),
    reserved_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 1 CHECK (request_count > 0),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'reconciled', 'released')),
    reconciled_at TEXT,
    ledger_id INTEGER
);
CREATE INDEX IF NOT EXISTS idx_knemon_burn_reservations_active
    ON knemon_burn_reservations (session_id, status, expires_at);
CREATE INDEX IF NOT EXISTS idx_knemon_burn_reservations_retention
    ON knemon_burn_reservations (status, reconciled_at, expires_at);
