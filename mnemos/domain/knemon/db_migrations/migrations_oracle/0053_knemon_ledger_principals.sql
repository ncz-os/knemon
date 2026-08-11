-- Trusted burn-enforcement principals for usage-ledger rows (Oracle).
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE knemon_ledger_principals (
      ledger_id NUMBER(19) PRIMARY KEY,
      principal_id VARCHAR2(128 CHAR) NOT NULL,
      CONSTRAINT fk_knemon_ledger_principal
        FOREIGN KEY (ledger_id) REFERENCES usage_ledger(id) ON DELETE CASCADE)';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;
END;
/
BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_ledger_principal
      ON knemon_ledger_principals (principal_id, ledger_id)';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;
END;
/
MERGE INTO knemon_ledger_principals target
USING (SELECT id AS ledger_id, session_id AS principal_id
       FROM usage_ledger WHERE session_id IS NOT NULL) source
ON (target.ledger_id = source.ledger_id)
WHEN NOT MATCHED THEN
  INSERT (ledger_id, principal_id) VALUES (source.ledger_id, source.principal_id);
