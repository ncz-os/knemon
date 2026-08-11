--#SET TERMINATOR @
-- Trusted burn-enforcement principals for usage-ledger rows (IBM Db2).
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE TABLE knemon_ledger_principals (
    ledger_id BIGINT NOT NULL PRIMARY KEY,
    principal_id VARCHAR(128) NOT NULL,
    CONSTRAINT fk_knemon_ledger_principal
      FOREIGN KEY (ledger_id) REFERENCES usage_ledger(id) ON DELETE CASCADE)';
END@
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END;
  EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_ledger_principal
    ON knemon_ledger_principals (principal_id, ledger_id)';
END@
MERGE INTO knemon_ledger_principals AS target
USING (SELECT id AS ledger_id, session_id AS principal_id
       FROM usage_ledger WHERE session_id IS NOT NULL) AS source
ON target.ledger_id = source.ledger_id
WHEN NOT MATCHED THEN
  INSERT (ledger_id, principal_id) VALUES (source.ledger_id, source.principal_id)@
