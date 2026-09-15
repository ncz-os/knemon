--#SET TERMINATOR @
-- Durable KNEMON spend-window aggregate (IBM Db2).
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE TABLE knemon_spend_window_aggregates (
    caller_subsystem VARCHAR(64) NOT NULL,
    provider VARCHAR(64) DEFAULT ''*'' NOT NULL,
    window_start_ts TIMESTAMP NOT NULL,
    committed_usd DECIMAL(18, 8) DEFAULT 0 NOT NULL CHECK (committed_usd >= 0),
    pending_usd DECIMAL(18, 8) DEFAULT 0 NOT NULL CHECK (pending_usd >= 0),
    last_reconciled_at TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (caller_subsystem, provider))';
END@
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_spend_window_aggregates_window
    ON knemon_spend_window_aggregates (window_start_ts)';
END@
