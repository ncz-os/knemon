-- Trusted burn-enforcement principals for usage-ledger rows (PostgreSQL).
CREATE TABLE IF NOT EXISTS knemon_ledger_principals (
    ledger_id BIGINT PRIMARY KEY REFERENCES usage_ledger(id) ON DELETE CASCADE,
    principal_id VARCHAR(128) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_knemon_ledger_principals_principal
    ON knemon_ledger_principals (principal_id, ledger_id);
INSERT INTO knemon_ledger_principals (ledger_id, principal_id)
SELECT id, session_id FROM usage_ledger WHERE session_id IS NOT NULL
ON CONFLICT (ledger_id) DO NOTHING;
