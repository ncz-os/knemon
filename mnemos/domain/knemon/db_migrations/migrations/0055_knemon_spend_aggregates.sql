-- Durable KNEMON spend-window aggregate (PostgreSQL).
--
-- KNEMON read-path optimization: instead of summing the rolling-7-day
-- ``usage_ledger`` on every admission check, the budget hot path
-- reads from a small maintained counter table and falls back to a
-- fresh full-scan SUM only when the cached window is stale.
--
-- The aggregate is INCREMENTALLY updated by ``settle_cost`` /
-- ``release_cost`` / ``reserve_cost`` (F04 review #3 / perf #1). A
-- ``reconcile_spend_aggregates`` helper rebuilds the row(s) from the
-- underlying ledger + reservations tables and verifies the running
-- counter matches, surfacing drift so it can be corrected instead of
-- silently poisoning the budget math.
--
-- The aggregate's correctness-of-aggregation is verified by a
-- regression test (see ``tests/test_knemon_budget_aggregate.py``):
-- the maintained counter must agree with a fresh ``SUM()`` of the
-- raw ledger AND with a fresh ``SUM()`` of outstanding pending
-- reservations, AND it must drop contributions older than the
-- configured window when the window rolls forward.

CREATE TABLE IF NOT EXISTS knemon_spend_window_aggregates (
    -- (caller_subsystem, provider) is the join key. The sentinel
    -- ``'*'`` (rather than SQL ``NULL``) marks "all providers" —
    -- KNEMON's effective_spend_usd_tx runs an unfiltered SUM and
    -- admission checks at that granularity consult the ``'*'``
    -- row, so we need the unfiltered counter to be addressable
    -- by a real UNIQUE key (SQLite treats NULL as non-equal under
    -- UNIQUE, so a NULLable provider column would allow the
    -- unfiltered row to be inserted an unbounded number of times).
    caller_subsystem        VARCHAR(64) NOT NULL,
    provider                VARCHAR(64) NOT NULL DEFAULT '*',
    -- The lower bound of the rolling window at the time the counter
    -- was last reconciled. When ``now() - window_seconds >=
    -- window_start_ts``, the row is considered stale and the read
    -- path falls back to a fresh full-scan SUM, refreshing the
    -- counter atomically.
    window_start_ts         TIMESTAMPTZ NOT NULL,
    -- Sum of settled ``usage_ledger.est_cost_usd`` (and equivalent
    -- ``knemon_cost_reservations.actual_cost_usd`` when recorded
    -- there) for rows whose timestamp >= ``window_start_ts``.
    -- Maintained incrementally on every ``settle_cost``.
    committed_usd           NUMERIC(18, 8) NOT NULL DEFAULT 0
        CHECK (committed_usd >= 0),
    -- Sum of outstanding pending ``knemon_cost_reservations`` whose
    -- ``expires_at > now()``. Maintained incrementally on every
    -- ``reserve_cost`` (+) / ``release_cost`` (-).
    pending_usd             NUMERIC(18, 8) NOT NULL DEFAULT 0
        CHECK (pending_usd >= 0),
    -- When the counter was last reconciled against the underlying
    -- ledger / reservations tables. NULL = never reconciled (first
    -- read must rebuild).
    last_reconciled_at      TIMESTAMPTZ,
    -- When the row was last touched (incremental update or rebuild).
    -- Audit-only; readers do not trust this for staleness — they
    -- re-check ``window_start_ts`` against the configured window.
    last_updated_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (caller_subsystem, provider),
    CONSTRAINT knemon_spend_aggregates_provider_sentinel
        CHECK (provider = '*' OR provider <> '*')
);

CREATE INDEX IF NOT EXISTS idx_knemon_spend_window_aggregates_window
    ON knemon_spend_window_aggregates (window_start_ts);
