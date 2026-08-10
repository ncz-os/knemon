-- Durable KNEMON session-burn reservations (PostgreSQL).
-- Route creates a pending row; the ledger write reconciles it atomically.
CREATE TABLE IF NOT EXISTS knemon_burn_sessions (
    session_id VARCHAR(128) PRIMARY KEY,
    touched_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS knemon_burn_reservations (
    reservation_id VARCHAR(36) PRIMARY KEY,
    session_id VARCHAR(128) NOT NULL,
    reserved_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 1 CHECK (request_count > 0),
    status VARCHAR(16) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'reconciled', 'released')),
    reconciled_at TIMESTAMPTZ,
    ledger_id BIGINT,
    CONSTRAINT fk_knemon_burn_reservation_session
        FOREIGN KEY (session_id) REFERENCES knemon_burn_sessions(session_id)
);

CREATE INDEX IF NOT EXISTS idx_knemon_burn_reservations_active
    ON knemon_burn_reservations (session_id, status, expires_at);
