"""Runtime migration hook for KNEMON-owned persistence objects.

Core cannot discover files from a separately installed add-on until the files
have been installed into its own distribution.  KNEMON therefore owns and
applies its small, idempotent schema before using it.
"""

from __future__ import annotations

import asyncio
from importlib.resources import files
from typing import Any


MIGRATION_NAME = "0052_knemon_burn_reservations.sql"
_MIGRATION_LOCK = asyncio.Lock()
_MIGRATED_BACKENDS: dict[int, Any] = {}


async def _schema_present(backend: Any) -> bool:
    from mnemos.domain.knemon.router import _conn_from_tx, _tx_rows

    try:
        async with backend.transactional() as tx:
            await _tx_rows(
                _conn_from_tx(tx),
                "SELECT reservation_id FROM knemon_burn_reservations WHERE 1 = 0",
            )
        return True
    except Exception as exc:
        from mnemos.domain.knemon.router import _is_burn_reservation_schema_missing

        if _is_burn_reservation_schema_missing(exc):
            return False
        raise


def _statements(sql: str, dialect: str) -> list[str]:
    from mnemos.persistence.schema import (
        split_db2_statements,
        split_oracle_statements,
        split_postgres_statements,
    )

    if dialect == "postgres":
        return split_postgres_statements(sql)
    if dialect == "oracle":
        return split_oracle_statements(sql)
    if dialect == "db2":
        return split_db2_statements(sql)
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


async def ensure_knemon_schema(backend: Any) -> None:
    """Apply the packaged migration when an installed database predates it."""
    backend_key = id(backend)
    if _MIGRATED_BACKENDS.get(backend_key) is backend:
        return
    async with _MIGRATION_LOCK:
        if _MIGRATED_BACKENDS.get(backend_key) is backend:
            return
        from mnemos.domain.knemon.router import (
            _conn_from_tx,
            _connection_dialect,
            _tx_execute,
        )

        async with backend.transactional() as tx:
            conn = _conn_from_tx(tx)
            dialect = _connection_dialect(conn)
            if dialect not in {"postgres", "sqlite", "mysql", "oracle", "db2"}:
                raise RuntimeError(f"unsupported KNEMON migration backend: {dialect}")
            directory = "migrations_mariadb" if dialect == "mysql" else f"migrations_{dialect}"
            if dialect == "postgres":
                directory = "migrations"
            resource = files("mnemos.domain.knemon.db_migrations").joinpath(directory, MIGRATION_NAME)
            sql = resource.read_text(encoding="utf-8")
            for statement in _statements(sql, dialect):
                try:
                    await _tx_execute(conn, statement)
                except Exception as exc:
                    message = str(exc).lower()
                    if dialect == "mysql" and (
                        getattr(exc, "errno", None) == 1061 or "duplicate key name" in message
                    ):
                        continue
                    raise

        if not await _schema_present(backend):
            raise RuntimeError(f"KNEMON migration {MIGRATION_NAME} completed without creating its schema")
        _MIGRATED_BACKENDS[backend_key] = backend


__all__ = ["MIGRATION_NAME", "ensure_knemon_schema"]
