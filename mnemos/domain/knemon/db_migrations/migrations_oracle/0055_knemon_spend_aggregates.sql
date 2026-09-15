-- Durable KNEMON spend-window aggregate (Oracle).
-- ``provider = '*'`` is the sentinel for "all providers" — see
-- the PostgreSQL migration for the rationale (NULL is not UNIQUE
-- comparable in some dialects).
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE knemon_spend_window_aggregates (
      caller_subsystem VARCHAR2(64 CHAR) NOT NULL,
      provider VARCHAR2(64 CHAR) DEFAULT ''*'' NOT NULL,
      window_start_ts TIMESTAMP WITH TIME ZONE NOT NULL,
      committed_usd NUMBER(18, 8) DEFAULT 0 NOT NULL
        CHECK (committed_usd >= 0),
      pending_usd NUMBER(18, 8) DEFAULT 0 NOT NULL
        CHECK (pending_usd >= 0),
      last_reconciled_at TIMESTAMP WITH TIME ZONE,
      last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
      PRIMARY KEY (caller_subsystem, provider))';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;
END;
/
BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_knemon_spend_window_aggregates_window
      ON knemon_spend_window_aggregates (window_start_ts)';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;
END;
/
