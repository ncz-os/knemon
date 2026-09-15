-- Recovery provenance and indexed rolling budget reads.
CREATE TABLE IF NOT EXISTS knemon_cost_recovery (attempt_id VARCHAR(128) PRIMARY KEY);
CREATE INDEX IF NOT EXISTS idx_knemon_ledger_budget_window ON usage_ledger (caller_subsystem, julianday(ts));
CREATE INDEX IF NOT EXISTS idx_knemon_settled_budget_window ON knemon_cost_reservations (caller_subsystem, status, julianday(settled_at));
