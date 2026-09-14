-- Durable KNEMON cost-budget reservations (MySQL/MariaDB).
CREATE TABLE IF NOT EXISTS knemon_cost_reservations (
    attempt_id          VARCHAR(128) NOT NULL PRIMARY KEY,
    provider            VARCHAR(64) NOT NULL,
    model               VARCHAR(128) NOT NULL,
    caller_subsystem    VARCHAR(64) NOT NULL,
    estimated_cost_usd  DECIMAL(18, 8) NOT NULL CHECK (estimated_cost_usd >= 0),
    actual_cost_usd     DECIMAL(18, 8) NULL CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    reserved_at         DATETIME(6) NOT NULL,
    expires_at          DATETIME(6) NOT NULL,
    settled_at          DATETIME(6) NULL,
    released_at         DATETIME(6) NULL,
    ledger_id           BIGINT NULL,
    status              VARCHAR(16) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'settled', 'released')),
    INDEX idx_knemon_cost_reservations_pending_lookup
        (caller_subsystem, provider, status, expires_at),
    INDEX idx_knemon_cost_reservations_retention
        (status, settled_at, released_at, expires_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS knemon_cost_reservation_locks (
    caller_subsystem    VARCHAR(64) NOT NULL PRIMARY KEY,
    touched_at          DATETIME(6) NOT NULL
) ENGINE=InnoDB;