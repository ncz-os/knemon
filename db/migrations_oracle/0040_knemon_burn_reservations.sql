-- Durable KNEMON session-burn reservations (Oracle).
BEGIN
    EXECUTE IMMEDIATE '
      CREATE TABLE knemon_burn_sessions (
        session_id VARCHAR2(128 CHAR) PRIMARY KEY,
        touched_at TIMESTAMP WITH TIME ZONE NOT NULL
      )';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE '
      CREATE TABLE knemon_burn_reservations (
        reservation_id VARCHAR2(36 CHAR) PRIMARY KEY,
        session_id VARCHAR2(128 CHAR) NOT NULL,
        reserved_at TIMESTAMP WITH TIME ZONE NOT NULL,
        expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
        request_count NUMBER(10) DEFAULT 1 NOT NULL CHECK (request_count > 0),
        status VARCHAR2(16 CHAR) DEFAULT ''pending'' NOT NULL
          CHECK (status IN (''pending'', ''reconciled'', ''released'')),
        reconciled_at TIMESTAMP WITH TIME ZONE,
        ledger_id NUMBER(19),
        CONSTRAINT fk_knemon_burn_res_session
          FOREIGN KEY (session_id) REFERENCES knemon_burn_sessions(session_id)
      )';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_burn_res_active
      ON knemon_burn_reservations (session_id, status, expires_at)';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;
END;
/
