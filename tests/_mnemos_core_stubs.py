"""Runtime stubs for `mnemos.core` / `mnemos.persistence` / `mnemos.api` — F04.

These exist solely so the F04 budget-reservation regression tests — and
the surrounding KNEMON test files that pytest discovers in the same
collection — can import the symbols they reference at module top
level. They MUST NOT shadow any real subpackage that ships with the
project (``mnemos.api.routes``, ``mnemos.domain.knemon``, …).

In production, ``mnemos-core`` is installed from an internal index and
provides real implementations of everything below. The stub only exists
because this checkout resolves ``mnemos-core>=6.2,<7`` against a local
sibling stub (``mnemos-stubs/mnemos_core/``) which ships no code; the
runtime stubs are injected at pytest collection time so that the
collection-import phase succeeds without depending on the full core
distribution.
"""

from __future__ import annotations

import importlib.machinery
import sys
import types
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, TYPE_CHECKING


_PLAN_WINDOWS: dict[tuple[str, str], tuple[str, int | None]] = {
    ("anthropic", "claude_max_200"): ("rolling", 18000),
    ("anthropic", "claude_max_100"): ("rolling", 18000),
    ("anthropic", "claude_max_interactive_post_jun15"): ("rolling", 18000),
    ("anthropic", "agent_sdk_credit_pool_post_jun15"): ("monthly", None),
}


def _install_core_stub() -> None:
    """Create the `mnemos.core` namespace + minimal submodules."""
    core_pkg = types.ModuleType("mnemos.core")
    core_pkg.__path__ = []
    sys.modules.setdefault("mnemos.core", core_pkg)

    # mnemos.core.config --------------------------------------------------
    class _Settings:
        def __init__(self, knemon: SimpleNamespace | None = None) -> None:
            self.knemon = knemon or SimpleNamespace(
                weekly_budget_cap_usd=0.0,
                parsed_provider_budget_caps_usd=lambda: {},
                session_burn_requests_per_hour=0,
                session_burn_window_seconds=3600,
                subscription_preferred_utilization_pct=80.0,
                subscription_near_cap_pct=90.0,
                low_priority_api_cost_ceiling_usd=0.10,
            )

    _settings = _Settings()

    def _get_settings_callable() -> _Settings:
        return _settings

    config_mod = types.ModuleType("mnemos.core.config")
    config_mod.get_settings = _get_settings_callable  # type: ignore[attr-defined]
    config_mod._settings = _settings  # type: ignore[attr-defined]
    config_mod.__all__ = ["get_settings"]
    sys.modules["mnemos.core.config"] = config_mod
    setattr(core_pkg, "config", config_mod)

    # mnemos.core.plan_windows --------------------------------------------
    def _compute_plan_window_id(
        provider: str, model_id: str, *args: Any, **kwargs: Any
    ) -> str:
        return f"{provider}-{model_id}-stub"

    def _plan_path_kind(
        provider: str, model_id: str, *args: Any, **kwargs: Any
    ) -> str:
        return "api"

    plan_windows_mod = types.ModuleType("mnemos.core.plan_windows")
    plan_windows_mod._PLAN_WINDOWS = _PLAN_WINDOWS  # type: ignore[attr-defined]
    plan_windows_mod.compute_plan_window_id = _compute_plan_window_id  # type: ignore[attr-defined]
    plan_windows_mod.plan_path_kind = _plan_path_kind  # type: ignore[attr-defined]
    plan_windows_mod.__all__ = [
        "_PLAN_WINDOWS",
        "compute_plan_window_id",
        "plan_path_kind",
    ]
    sys.modules["mnemos.core.plan_windows"] = plan_windows_mod
    setattr(core_pkg, "plan_windows", plan_windows_mod)

    # mnemos.core.recommendation ------------------------------------------
    @dataclass
    class _RecommendationPolicy:
        task_type: str = ""
        required_caps: list[str] = None  # type: ignore[assignment]
        any_caps: list[str] = None  # type: ignore[assignment]
        excluded_caps: list[str] = None  # type: ignore[assignment]
        preferred_models: list[str] = None  # type: ignore[assignment]
        fallback_models: list[str] = None  # type: ignore[assignment]

        def __post_init__(self) -> None:
            for field_name in (
                "required_caps",
                "any_caps",
                "excluded_caps",
                "preferred_models",
                "fallback_models",
            ):
                if getattr(self, field_name) is None:
                    setattr(self, field_name, [])

    def _canonical_cap(name: str) -> str:
        return (name or "").strip().lower()

    def _capability_set(value: Any) -> set[str]:
        if value is None:
            return set()
        if isinstance(value, (list, tuple, set)):
            return {_canonical_cap(str(item)) for item in value if str(item).strip()}
        raw = str(value).strip()
        if not raw:
            return set()
        parts = [p.strip() for p in raw.replace(",", " ").split() if p.strip()]
        return {_canonical_cap(p) for p in parts}

    def _is_special_purpose(row: dict[str, Any]) -> bool:
        return False

    def _preference_rank(row: dict[str, Any], policy: _RecommendationPolicy) -> tuple:
        return (0, 0, 0.0)

    def _tier_filter(
        candidates: list[dict[str, Any]], policy: _RecommendationPolicy
    ) -> list[dict[str, Any]]:
        return candidates

    def _recommendation_policy(task_kind: str) -> _RecommendationPolicy:
        return _RecommendationPolicy(task_type=task_kind)

    def _choose_recommended_model(
        candidates: list[dict[str, Any]], *, task_kind: str = "", **kwargs: Any
    ) -> dict[str, Any] | None:
        return candidates[0] if candidates else None

    recommendation_mod = types.ModuleType("mnemos.core.recommendation")
    recommendation_mod._canonical_cap = _canonical_cap  # type: ignore[attr-defined]
    recommendation_mod._capability_set = _capability_set  # type: ignore[attr-defined]
    recommendation_mod._is_special_purpose = _is_special_purpose  # type: ignore[attr-defined]
    recommendation_mod._preference_rank = _preference_rank  # type: ignore[attr-defined]
    recommendation_mod._tier_filter = _tier_filter  # type: ignore[attr-defined]
    recommendation_mod.recommendation_policy = _recommendation_policy  # type: ignore[attr-defined]
    recommendation_mod.choose_recommended_model = _choose_recommended_model  # type: ignore[attr-defined]
    recommendation_mod.__all__ = [
        "_canonical_cap",
        "_capability_set",
        "_is_special_purpose",
        "_preference_rank",
        "_tier_filter",
        "recommendation_policy",
        "choose_recommended_model",
    ]
    sys.modules["mnemos.core.recommendation"] = recommendation_mod
    setattr(core_pkg, "recommendation", recommendation_mod)

    # mnemos.core.lifecycle -----------------------------------------------
    lifecycle_mod = types.ModuleType("mnemos.core.lifecycle")
    lifecycle_mod._persistence_backend = None  # type: ignore[attr-defined]
    lifecycle_mod.__all__ = ["_persistence_backend"]
    sys.modules["mnemos.core.lifecycle"] = lifecycle_mod
    setattr(core_pkg, "lifecycle", lifecycle_mod)


def _install_persistence_stub() -> None:
    """Create the `mnemos.persistence` namespace + minimal submodules."""
    persistence_pkg = types.ModuleType("mnemos.persistence")
    persistence_pkg.__path__ = []
    sys.modules.setdefault("mnemos.persistence", persistence_pkg)

    @dataclass
    class UsageLedgerRecord:
        provider: str
        model: str
        task_kind: str
        tokens_in: int = 0
        tokens_out: int = 0
        tokens_reasoning: int = 0
        latency_ms: int = 0
        outcome: str = "ok"
        caller_subsystem: str = "pantheon"
        tier: str = "standard"
        session_id: str | None = None
        request_count: int = 1
        plan_window_id: str | None = None
        path_kind: str = "api"

    @dataclass
    class UsageLedgerResult:
        id: int
        est_cost_usd: Decimal

    base_mod = types.ModuleType("mnemos.persistence.base")
    base_mod.UsageLedgerRecord = UsageLedgerRecord  # type: ignore[attr-defined]
    base_mod.UsageLedgerResult = UsageLedgerResult  # type: ignore[attr-defined]
    base_mod.__all__ = ["UsageLedgerRecord", "UsageLedgerResult"]
    sys.modules["mnemos.persistence.base"] = base_mod
    setattr(persistence_pkg, "base", base_mod)

    # mnemos.persistence.postgres — minimal surface for import-only
    class _PostgresTransaction:
        def __init__(self, conn: Any = None, raw_tx: Any = None) -> None:
            self.conn = conn
            self._raw_tx = raw_tx

    class _PostgresBackend:
        def __init__(self, pool: Any = None, settings: Any = None) -> None:
            self.pool = pool
            self.settings = settings

        async def record_usage_ledger(self, tx: Any, record: Any) -> Any:
            return UsageLedgerResult(id=0, est_cost_usd=Decimal("0"))

    postgres_mod = types.ModuleType("mnemos.persistence.postgres")
    postgres_mod.PostgresBackend = _PostgresBackend  # type: ignore[attr-defined]
    postgres_mod.PostgresTransaction = _PostgresTransaction  # type: ignore[attr-defined]
    postgres_mod.__all__ = ["PostgresBackend", "PostgresTransaction"]
    sys.modules["mnemos.persistence.postgres"] = postgres_mod
    setattr(persistence_pkg, "postgres", postgres_mod)

    # mnemos.persistence.schema — minimal statement-splitting helpers
    # used by ``ensure_knemon_schema`` to apply per-dialect SQL
    # migrations.
    schema_mod = types.ModuleType("mnemos.persistence.schema")

    def _split_semicolon_statements(sql: str) -> list[str]:
        return [s.strip() for s in sql.split(";") if s.strip()]

    def _split_postgres_statements(sql: str) -> list[str]:
        return _split_semicolon_statements(sql)

    def _split_db2_statements(sql: str) -> list[str]:
        return _split_semicolon_statements(sql)

    def _split_oracle_statements(sql: str) -> list[str]:
        return _split_semicolon_statements(sql)

    schema_mod.split_postgres_statements = _split_postgres_statements  # type: ignore[attr-defined]
    schema_mod.split_db2_statements = _split_db2_statements  # type: ignore[attr-defined]
    schema_mod.split_oracle_statements = _split_oracle_statements  # type: ignore[attr-defined]
    schema_mod.__all__ = [
        "split_postgres_statements",
        "split_db2_statements",
        "split_oracle_statements",
    ]
    sys.modules["mnemos.persistence.schema"] = schema_mod
    setattr(persistence_pkg, "schema", schema_mod)


def _install_api_stub() -> None:
    """Create `mnemos.api.*` namespaces + minimal submodules."""
    api_pkg = types.ModuleType("mnemos.api")
    api_pkg.__path__ = []
    sys.modules.setdefault("mnemos.api", api_pkg)

    # mnemos.api.dependencies — minimal auth shim
    @dataclass
    class UserContext:
        user_id: str = "default"
        group_ids: list[str] = None  # type: ignore[assignment]
        role: str = "user"
        namespace: str = "default"
        authenticated: bool = False

        def __post_init__(self) -> None:
            if self.group_ids is None:
                self.group_ids = []

    async def _get_current_user() -> UserContext:
        return UserContext()

    dependencies_mod = types.ModuleType("mnemos.api.dependencies")
    dependencies_mod.UserContext = UserContext  # type: ignore[attr-defined]
    dependencies_mod.get_current_user = _get_current_user  # type: ignore[attr-defined]
    dependencies_mod.__all__ = ["UserContext", "get_current_user"]
    sys.modules["mnemos.api.dependencies"] = dependencies_mod
    setattr(api_pkg, "dependencies", dependencies_mod)

    # mnemos.api.extra_guards
    extra_guards_mod = types.ModuleType("mnemos.api.extra_guards")
    def _require_extra(*args: Any, **kwargs: Any) -> Any:
        return None
    extra_guards_mod.require_extra = _require_extra  # type: ignore[attr-defined]
    extra_guards_mod.__all__ = ["require_extra"]
    sys.modules["mnemos.api.extra_guards"] = extra_guards_mod
    setattr(api_pkg, "extra_guards", extra_guards_mod)

    # mnemos.api.routes namespace
    routes_pkg = types.ModuleType("mnemos.api.routes")
    routes_pkg.__path__ = []
    sys.modules["mnemos.api.routes"] = routes_pkg
    setattr(api_pkg, "routes", routes_pkg)

    # mnemos.api.routes.{knemon_dashboard, knemon_utilization, knemon_router}
    for sub in ("knemon_dashboard", "knemon_utilization", "knemon_router"):
        mod = types.ModuleType(f"mnemos.api.routes.{sub}")
        mod.router = None  # type: ignore[attr-defined]
        sys.modules[f"mnemos.api.routes.{sub}"] = mod
        setattr(routes_pkg, sub, mod)

    # mnemos.api.routes.ledger (FastAPI router stub)
    ledger_routes_mod = types.ModuleType("mnemos.api.routes.ledger")

    class _Router:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def __getattr__(self, name: str) -> Any:
            return lambda *a, **kw: None

    ledger_routes_mod.router = _Router()  # type: ignore[attr-defined]
    ledger_routes_mod.__all__ = ["router"]
    sys.modules["mnemos.api.routes.ledger"] = ledger_routes_mod
    setattr(routes_pkg, "ledger", ledger_routes_mod)


def _install_db_migrations_stub() -> None:
    """Create a placeholder `mnemos.db_migrations` package.

    `tests/test_knemon_dispatch_migration.py` and
    `tests/test_knemon_ledger.py::test_usage_ledger_migrations_preserve_constraint_parity`
    call ``files("mnemos.db_migrations")`` at module top level to read
    the SQL migration tree, so even test files that don't match our
    keyword filter must import cleanly during pytest collection. Real
    KNEMON production deploys pull the full ``mnemos-core`` distribution
    which ships the actual ``mnemos/db_migrations/`` SQL tree; this stub
    forwards ``__path__`` to a checked-in shadow tree at
    ``mnemos-stubs/db_migrations_stub/`` (a curated subset that contains
    only the SQL files our own test suite touches) so the migrations
    are readable end-to-end without pulling the full distribution.

    To make ``importlib.resources.files()`` work against our shadow
    tree, the spec carries a custom ``ResourceReader`` loader that walks
    the filesystem path. Without it the import system returns an
    ``OrphanPath`` and ``read_text()`` raises ``FileNotFoundError``.
    """

    import os
    from importlib.abc import ResourceReader
    from importlib.resources._adapters import CompatibilityFiles

    class _FilesystemResourceReader(ResourceReader):
        """Walk a filesystem directory as a Traversable-compatible reader."""

        def __init__(self, name: str, root: str) -> None:
            self._name = name
            self._root = root

        def open_resource(self, resource: str):
            return open(os.path.join(self._root, resource), "rb")

        def resource_path(self, resource: str):
            return os.path.join(self._root, resource)

        def is_resource(self, resource: str) -> bool:
            return os.path.isfile(os.path.join(self._root, resource))

        def contents(self):
            if not os.path.isdir(self._root):
                return ()
            return tuple(sorted(os.listdir(self._root)))

    class _FilesystemLoader:
        """Loader that exposes ``get_resource_reader`` for ``importlib.resources``."""

        def __init__(self, root: str) -> None:
            self._root = root

        def get_resource_reader(self, name: str) -> "_FsResourceReaderWrapper":
            return _FsResourceReaderWrapper(name, self._root)

        def create_module(self, spec):  # pragma: no cover — unused
            return None

        def exec_module(self, module):  # pragma: no cover — unused
            pass

    class _FsResourceReaderWrapper(_FilesystemResourceReader):
        """ResourceReader whose ``files()`` returns our traversable root."""

        def files(self):  # type: ignore[override]
            return _FilesystemTraversable(self)

    class _ChildPathWithParent(CompatibilityFiles.ChildPath):
        """ChildPath with a working ``.parent`` traversal and a held reader.

        The stdlib ``ChildPath`` doesn't expose ``.parent`` and its
        ``_reader`` attribute is a property that re-derives from
        ``self._spec.loader`` (which is None in our setup). We carry
        our reader explicitly on the instance so child paths can open
        their files without going through ``CompatibilityFiles``.
        """

        def __init__(self, reader: Any, name: str) -> None:
            super().__init__(reader, name)
            # `CompatibilityFiles.ChildPath` exposes ``self._reader`` as a
            # property that calls ``self._spec.loader.get_resource_reader``;
            # our spec loader has no spec (``), so cache the reader on
            # the instance under a non-conflicting name.
            self._fs_reader = reader

        @property
        def parent(self):  # type: ignore[override]
            if not self._name:
                return self
            parent_name, _, _ = self._name.rpartition("/")
            return _ChildPathWithParent(self._fs_reader, parent_name)

        @property
        def name(self) -> str:
            _, _, leaf = self._name.rpartition("/")
            return leaf

        def open(self, mode="r", *args: Any, **kwargs: Any):
            if "b" in mode:
                return self._fs_reader.open_resource(self._name)
            import io

            return io.TextIOWrapper(self._fs_reader.open_resource(self._name), encoding="utf-8")

        def is_file(self):  # type: ignore[override]
            return self._fs_reader.is_resource(self._name)

        def __truediv__(self, child: str):
            if self._name:
                return _ChildPathWithParent(self._fs_reader, f"{self._name}/{child}")
            return _ChildPathWithParent(self._fs_reader, child)

    class _FilesystemTraversable(CompatibilityFiles):
        """Traversable for the package root (no name on the directory itself)."""

        def __init__(self, reader: _FilesystemResourceReader) -> None:
            self._fs_reader = reader

        def joinpath(self, child: str):
            return _ChildPathWithParent(self._fs_reader, child)

        def __truediv__(self, child: str):
            return _ChildPathWithParent(self._fs_reader, child)

        def iterdir(self):
            return iter(
                _ChildPathWithParent(self._fs_reader, name)
                for name in self._fs_reader.contents()
            )

        @property
        def name(self) -> str:
            return ""

    stub_root = os.path.abspath(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "mnemos",
            "db_migrations",
        )
    )
    db_pkg = types.ModuleType("mnemos.db_migrations")
    db_pkg.__path__ = [stub_root]
    db_pkg.__spec__ = importlib.machinery.ModuleSpec(  # type: ignore[attr-defined]
        "mnemos.db_migrations",
        loader=_FilesystemLoader(stub_root),
    )
    db_pkg.__spec__.submodule_search_locations = [stub_root]  # type: ignore[attr-defined]
    sys.modules.setdefault("mnemos.db_migrations", db_pkg)
    mnemos_root = sys.modules.get("mnemos")
    if mnemos_root is not None:
        setattr(mnemos_root, "db_migrations", db_pkg)


def install_mnemos_core_stubs() -> None:
    """Install all runtime stubs (idempotent)."""
    if getattr(install_mnemos_core_stubs, "_installed", False):
        return
    _install_core_stub()
    _install_persistence_stub()
    _install_api_stub()
    _install_db_migrations_stub()
    install_mnemos_core_stubs._installed = True  # type: ignore[attr-defined]


__all__ = ["install_mnemos_core_stubs"]