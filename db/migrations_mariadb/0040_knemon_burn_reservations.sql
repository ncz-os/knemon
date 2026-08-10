-- Durable KNEMON session-burn reservations (MySQL/MariaDB).
CREATE TABLE IF NOT EXISTS knemon_burn_sessions (
    session_id VARCHAR(128) PRIMARY KEY,
    touched_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS knemon_burn_reservations (
    reservation_id VARCHAR(36) PRIMARY KEY,
    session_id VARCHAR(128) NOT NULL,
    reserved_at DATETIME(6) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 1 CHECK (request_count > 0),
    status VARCHAR(16) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'reconciled', 'released')),
    reconciled_at DATETIME(6) NULL,
    ledger_id BIGINT NULL,
    CONSTRAINT fk_knemon_burn_reservation_session
        FOREIGN KEY (session_id) REFERENCES knemon_burn_sessions(session_id),
    INDEX idx_knemon_burn_reservations_active (session_id, status, expires_at)
) ENGINE=InnoDB;
