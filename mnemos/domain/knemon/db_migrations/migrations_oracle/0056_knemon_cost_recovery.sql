-- Recovery provenance and indexed rolling budget reads.
BEGIN
 EXECUTE IMMEDIATE 'CREATE TABLE knemon_cost_recovery (attempt_id VARCHAR2(128 CHAR) PRIMARY KEY)';
EXCEPTION WHEN OTHERS THEN
 IF SQLCODE != -955 THEN RAISE; END IF;
END;
/
BEGIN
 EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_ledger_budget_window ON usage_ledger (caller_subsystem, ts)';
EXCEPTION WHEN OTHERS THEN
 IF SQLCODE != -955 THEN RAISE; END IF;
END;
/
BEGIN
 EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_settled_budget_window ON knemon_cost_reservations (caller_subsystem, status, settled_at)';
EXCEPTION WHEN OTHERS THEN
 IF SQLCODE != -955 THEN RAISE; END IF;
END;
/
