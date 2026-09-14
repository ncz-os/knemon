--#SET TERMINATOR @
-- Durable KNEMON cost-budget reservations (IBM Db2).
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE TABLE knemon_cost_reservations (
    attempt_id VARCHAR(128) NOT NULL PRIMARY KEY,
    provider VARCHAR(64) NOT NULL,
    model VARCHAR(128) NOT NULL,
    caller_subsystem VARCHAR(64) NOT NULL,
    estimated_cost_usd DECIMAL(18, 8) NOT NULL CHECK (estimated_cost_usd >= 0),
    actual_cost_usd DECIMAL(18, 8) CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    reserved_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    settled_at TIMESTAMP,
    released_at TIMESTAMP,
    ledger_id BIGINT,
    status VARCHAR(16) NOT NULL DEFAULT ''pending''
      CHECK (status IN (''pending'', ''settled'', ''released'')))';
END@
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE TABLE knemon_cost_reservation_locks (
    caller_subsystem VARCHAR(64) NOT NULL PRIMARY KEY,
    touched_at TIMESTAMP NOT NULL)';
END@
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_cost_reservations_pending_lookup
    ON knemon_cost_reservations (caller_subsystem, provider, status, expires_at)';
END@
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_cost_reservations_retention
    ON knemon_cost_reservations (status, settled_at, released_at, expires_at)';
END@