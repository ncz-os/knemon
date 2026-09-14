-- Durable KNEMON cost-budget reservations (Oracle).
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE knemon_cost_reservations (
      attempt_id VARCHAR2(128 CHAR) PRIMARY KEY,
      provider VARCHAR2(64 CHAR) NOT NULL,
      model VARCHAR2(128 CHAR) NOT NULL,
      caller_subsystem VARCHAR2(64 CHAR) NOT NULL,
      estimated_cost_usd NUMBER(18, 8) NOT NULL CHECK (estimated_cost_usd >= 0),
      actual_cost_usd NUMBER(18, 8) CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
      reserved_at TIMESTAMP WITH TIME ZONE NOT NULL,
      expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
      settled_at TIMESTAMP WITH TIME ZONE,
      released_at TIMESTAMP WITH TIME ZONE,
      ledger_id NUMBER(19),
      status VARCHAR2(16 CHAR) DEFAULT ''pending'' NOT NULL
        CHECK (status IN (''pending'', ''settled'', ''released'')))';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;
END;
/
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE knemon_cost_reservation_locks (
      caller_subsystem VARCHAR2(64 CHAR) PRIMARY KEY,
      touched_at TIMESTAMP WITH TIME ZONE NOT NULL)';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;
END;
/
BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_cost_reservations_pending_lookup
      ON knemon_cost_reservations (caller_subsystem, provider, status, expires_at)';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;
END;
/
BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_cost_reservations_retention
      ON knemon_cost_reservations (status, settled_at, released_at, expires_at)';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;
END;
/