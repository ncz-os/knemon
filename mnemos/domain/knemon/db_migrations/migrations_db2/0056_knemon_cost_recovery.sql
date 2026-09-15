-- Recovery provenance and indexed rolling budget reads.
--#SET TERMINATOR @
BEGIN
 DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
 EXECUTE IMMEDIATE 'CREATE TABLE knemon_cost_recovery (attempt_id VARCHAR(128) PRIMARY KEY)';
END@
BEGIN
 DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
 EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_ledger_budget_window ON usage_ledger (caller_subsystem, ts)';
END@
BEGIN
 DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
 EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_settled_budget_window ON knemon_cost_reservations (caller_subsystem, status, settled_at)';
END@
