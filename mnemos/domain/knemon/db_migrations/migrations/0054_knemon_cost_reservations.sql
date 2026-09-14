-- Durable KNEMON cost-budget reservations (PostgreSQL).
--
-- F04: KNEMON now reserves a pending reservation row keyed by the
-- caller's per-attempt id BEFORE admitting the request, so concurrent
-- admission checks across processes correctly count both committed
-- ledger spend AND outstanding (unsettled) reservations against the
-- budget cap. ``settle_cost`` transitions a pending row to settled and
-- writes the ACTUAL cost; ``release_cost`` returns the headroom for a
-- pending row when the call is aborted before completion. Both settle
-- and release are idempotent on the same attempt_id.
CREATE TABLE IF NOT EXISTS knemon_cost_reservations (
    attempt_id          VARCHAR(128) PRIMARY KEY,
    provider            VARCHAR(64) NOT NULL,
    model               VARCHAR(128) NOT NULL,
    caller_subsystem    VARCHAR(64) NOT NULL,
    estimated_cost_usd  NUMERIC(18, 8) NOT NULL CHECK (estimated_cost_usd >= 0),
    actual_cost_usd     NUMERIC(18, 8) CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    reserved_at         TIMESTAMPTZ NOT NULL,
    expires_at          TIMESTAMPTZ NOT NULL,
    settled_at          TIMESTAMPTZ,
    released_at         TIMESTAMPTZ,
    ledger_id           BIGINT,
    status              VARCHAR(16) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'settled', 'released'))
);
-- Lock-row for atomic reserve/settle/release serialization per caller.
CREATE TABLE IF NOT EXISTS knemon_cost_reservation_locks (
    caller_subsystem    VARCHAR(64) PRIMARY KEY,
    touched_at          TIMESTAMPTZ NOT NULL
);
-- Pending-reservation lookup for admission accounting. Status filtered
-- to 'pending' so settled / released rows do NOT count against the cap.
CREATE INDEX IF NOT EXISTS idx_knemon_cost_reservations_pending_lookup
    ON knemon_cost_reservations (caller_subsystem, provider, status, expires_at);
CREATE INDEX IF NOT EXISTS idx_knemon_cost_reservations_attempt
    ON knemon_cost_reservations (attempt_id);
-- Retention: drop settled / released rows past retention; expire stale
-- pending rows so a crashed caller doesn't permanently burn headroom.
CREATE INDEX IF NOT EXISTS idx_knemon_cost_reservations_retention
    ON knemon_cost_reservations (status, settled_at, released_at, expires_at);