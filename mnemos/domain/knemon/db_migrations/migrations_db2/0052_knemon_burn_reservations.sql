--#SET TERMINATOR @
-- Durable KNEMON session-burn reservations (IBM Db2).
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE TABLE knemon_burn_sessions (
    session_id VARCHAR(128) NOT NULL PRIMARY KEY,
    touched_at TIMESTAMP NOT NULL)';
END@
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE TABLE knemon_burn_reservations (
    reservation_id VARCHAR(36) NOT NULL PRIMARY KEY,
    session_id VARCHAR(128) NOT NULL,
    reserved_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 1 CHECK (request_count > 0),
    status VARCHAR(16) NOT NULL DEFAULT ''pending''
      CHECK (status IN (''pending'', ''reconciled'', ''released'')),
    reconciled_at TIMESTAMP,
    ledger_id BIGINT,
    CONSTRAINT fk_knemon_burn_res_session
      FOREIGN KEY (session_id) REFERENCES knemon_burn_sessions(session_id))';
END@
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_burn_res_retention
    ON knemon_burn_reservations (status, reconciled_at, expires_at)';
END@
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_burn_res_active
    ON knemon_burn_reservations (session_id, status, expires_at)';
END@
