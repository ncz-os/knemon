-- Trusted burn-enforcement principals for usage-ledger rows (MySQL/MariaDB).
CREATE TABLE IF NOT EXISTS knemon_ledger_principals (
    ledger_id BIGINT PRIMARY KEY,
    principal_id VARCHAR(128) NOT NULL,
    CONSTRAINT fk_knemon_ledger_principal_ledger
        FOREIGN KEY (ledger_id) REFERENCES usage_ledger(id) ON DELETE CASCADE,
    INDEX idx_knemon_ledger_principals_principal (principal_id, ledger_id)
) ENGINE=InnoDB;
INSERT IGNORE INTO knemon_ledger_principals (ledger_id, principal_id)
SELECT id, session_id FROM usage_ledger WHERE session_id IS NOT NULL;
