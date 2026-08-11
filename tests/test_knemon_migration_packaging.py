from __future__ import annotations

import subprocess
import shutil
import sys
import zipfile
from importlib.resources import files
from pathlib import Path

from mnemos.domain.knemon.migrations import _statements


def test_built_wheel_contains_every_knemon_migration(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parent.parent
    build_root = tmp_path / "source"
    shutil.copytree(
        root,
        build_root,
        ignore=shutil.ignore_patterns(".git", "build", "*.egg-info", "__pycache__", "*.pyc"),
    )
    build_python = shutil.which("python") or sys.executable
    subprocess.run(
        [
            build_python,
            "-m",
            "pip",
            "wheel",
            ".",
            "--no-deps",
            "--no-build-isolation",
            "--wheel-dir",
            str(tmp_path),
        ],
        cwd=build_root,
        check=True,
        capture_output=True,
        text=True,
    )
    wheel = next(tmp_path.glob("mnemos_knemon-*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        packaged = {
            name for name in archive.namelist() if name.endswith("0052_knemon_burn_reservations.sql")
        }

    assert packaged == {
        "mnemos/domain/knemon/db_migrations/migrations/0052_knemon_burn_reservations.sql",
        "mnemos/domain/knemon/db_migrations/migrations_sqlite/0052_knemon_burn_reservations.sql",
        "mnemos/domain/knemon/db_migrations/migrations_mariadb/0052_knemon_burn_reservations.sql",
        "mnemos/domain/knemon/db_migrations/migrations_oracle/0052_knemon_burn_reservations.sql",
        "mnemos/domain/knemon/db_migrations/migrations_db2/0052_knemon_burn_reservations.sql",
    }


def test_each_backend_migration_splits_with_tables_before_indexes() -> None:
    root = files("mnemos.domain.knemon.db_migrations")
    resources = {
        "postgres": root / "migrations/0052_knemon_burn_reservations.sql",
        "sqlite": root / "migrations_sqlite/0052_knemon_burn_reservations.sql",
        "mysql": root / "migrations_mariadb/0052_knemon_burn_reservations.sql",
        "oracle": root / "migrations_oracle/0052_knemon_burn_reservations.sql",
        "db2": root / "migrations_db2/0052_knemon_burn_reservations.sql",
    }

    for dialect, resource in resources.items():
        statements = _statements(resource.read_text(encoding="utf-8"), dialect)
        reservation_table = next(
            index
            for index, statement in enumerate(statements)
            if "CREATE TABLE" in statement and "knemon_burn_reservations (" in statement
        )
        retention_index = next(
            index
            for index, statement in enumerate(statements)
            if "retention" in statement and "CREATE INDEX" in statement
        )
        assert reservation_table < retention_index
