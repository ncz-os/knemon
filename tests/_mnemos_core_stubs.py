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

When ``mnemos-core`` is already importable (typical for CI / dev
checkouts where it is installed at ``/Users/jasonperlow/mnemos-core-item7``
or via the internal index), the stubs below MUST defer to the real
modules so that callers (and tests) get the real
``PostgresBackend.record_usage_ledger`` / ``mnemos.api.routes.ledger``
implementations rather than a stubbed-out placeholder. This module
guards each ``sys.modules[...]`` registration behind an ``already
imported?`` check; if the real module is present we leave it alone.
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


def _already_imported(name: str) -> bool:
    """Return True if a real ``name`` module is already importable.

    When ``mnemos-core`` is installed at runtime the real subpackage is
    importable and shadows would clobber it. We probe the path lazily so
    this module is safe to import even when ``mnemos-core`` is NOT
    installed (in which case every probe falls back to the stub).
    """
    return name in sys.modules or _can_import(name)


def _can_import(name: str) -> bool:
    """Cheap, side-effect-free import probe."""
    import importlib.util

    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False


def _safe_register(name: str, module: types.ModuleType, parent_pkg: types.ModuleType | None = None) -> bool:
    """Register a stub ``module`` under ``name`` only if no real one is importable.

    Returns True if the stub was actually registered; False if a real
    module was found and left alone. Also attaches the stub to the
    optional ``parent_pkg`` namespace package so ``from
    parent_pkg.submodule import …`` keeps working for tests that
    imported the stub parent.
    """
    if _already_imported(name):
        return False
    sys.modules[name] = module
    if parent_pkg is not None:
        setattr(parent_pkg, name.rpartition(".")[-1], module)
    return True


def _install_core_stub() -> None:
    """Create the `mnemos.core` namespace + minimal submodules.

    Mirrors the ``_install_api_stub`` fix: if the real ``mnemos.core``
    namespace package is importable (production install), wire the
    project source path onto it AND leave it alone — pre-registering
    an empty-``__path__`` stub here would shadow the real namespace
    package and make every ``mnemos.core.<submodule>`` import return
    ``None`` or raise ``ModuleNotFoundError`` for real submodules like
    ``auth_context`` / ``lifecycle`` / ``config``.

    In the stub-only fallback (no real ``mnemos-core`` installed), the
    legacy empty ``mnemos.core`` package is registered so the stub
    submodules below can still attach to it.
    """
    core_pkg = sys.modules.get("mnemos.core")
    if core_pkg is None:
        try:
            import mnemos.core as core_pkg  # noqa: F401 — namespace pkg
        except Exception:
            core_pkg = types.ModuleType("mnemos.core")
            core_pkg.__path__ = []
            sys.modules.setdefault("mnemos.core", core_pkg)
    else:
        # The real namespace package is loaded. Make sure the project
        # source path is included so any mnemos/core/*.py the project
        # itself ships is discoverable (mirrors what an editable
        # install would do). Today the project ships nothing under
        # mnemos/core/, but wiring it defensively avoids future
        # surprises when a sibling repo adds mnemos/core helpers.
        import os

        project_core = os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "mnemos", "core")
        )
        if project_core not in getattr(core_pkg, "__path__", []) and os.path.isdir(project_core):
            core_pkg.__path__.append(project_core)

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
    _safe_register("mnemos.core.config", config_mod, core_pkg)

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
    _safe_register("mnemos.core.plan_windows", plan_windows_mod, core_pkg)

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
    _safe_register("mnemos.core.recommendation", recommendation_mod, core_pkg)

    # mnemos.core.lifecycle -----------------------------------------------
    lifecycle_mod = types.ModuleType("mnemos.core.lifecycle")
    lifecycle_mod._persistence_backend = None  # type: ignore[attr-defined]
    lifecycle_mod.__all__ = ["_persistence_backend"]
    _safe_register("mnemos.core.lifecycle", lifecycle_mod, core_pkg)


def _install_persistence_stub() -> None:
    """Create the `mnemos.persistence` namespace + minimal submodules.

    If the real ``mnemos.persistence`` namespace package is importable
    (production install), use it as-is and let the project contribute
    any future ``mnemos/persistence/*.py`` source files via its own
    ``__path__`` entry. Otherwise fall back to the legacy empty stub
    so the F04-only ``UsageLedgerRecord`` / ``PostgresBackend`` shims
    below still have a parent to attach to.
    """
    persistence_pkg = sys.modules.get("mnemos.persistence")
    if persistence_pkg is None:
        try:
            import mnemos.persistence as persistence_pkg  # noqa: F401
        except Exception:
            persistence_pkg = types.ModuleType("mnemos.persistence")
            persistence_pkg.__path__ = []
            sys.modules.setdefault("mnemos.persistence", persistence_pkg)
    else:
        # Make sure the project source path is on the namespace
        # package's __path__ so KNEMON's own mnemos/persistence/*.py
        # files (none today, but defensively future-proof) are
        # discoverable when the real mnemos-core namespace package
        # was imported first.
        import os

        project_persistence = os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "mnemos", "persistence")
        )
        if project_persistence not in getattr(persistence_pkg, "__path__", []) and os.path.isdir(project_persistence):
            persistence_pkg.__path__.append(project_persistence)

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
    _safe_register("mnemos.persistence.base", base_mod, persistence_pkg)

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
    _safe_register("mnemos.persistence.postgres", postgres_mod, persistence_pkg)

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
    _safe_register("mnemos.persistence.schema", schema_mod, persistence_pkg)


def _install_api_stub() -> None:
    """Create `mnemos.api.*` namespaces + minimal submodules.

    When ``mnemos-core`` is importable, the real ``mnemos.api``
    namespace package exists (the ``mnemos-core`` distribution
    contributes ``/Users/jasonperlow/mnemos-core-item7/mnemos/api``)
    AND the project contributes KNEMON's own source files
    (``mnemos/api/routes/{knemon_*, ledger}.py``). The import system
    will discover both automatically as long as NEITHER the stub
    ``mnemos.api`` placeholder nor a stub ``mnemos.api.routes``
    placeholder has been pre-registered in ``sys.modules``.

    The previous stub version pre-registered empty-``__path__`` modules
    for both, which silently swallowed the real namespace package and
    every KNEMON module under it.

    Behaviour summary:

    * If ``mnemos.api`` is importable: do NOT register any stub; let
      the import system discover the real namespace package and KNEMON
      submodules naturally on first use.
    * If ``mnemos.api`` is NOT importable (stub-only fallback): create
      an empty placeholder so the legacy F04 submodules below can
      attach.
    """
    api_pkg = sys.modules.get("mnemos.api")
    if api_pkg is not None and getattr(api_pkg, "__path__", None):
        # Real namespace package already loaded. Make sure the project
        # source path is wired into its __path__ (an editable install
        # would do this automatically; explicit here so the stub
        # works on non-editable internal-index installs too).
        import os

        project_api = os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "mnemos", "api")
        )
        if project_api not in api_pkg.__path__ and os.path.isdir(project_api):
            api_pkg.__path__.append(project_api)
        return  # Real namespace package is fine — nothing to stub.

    # Stub-only fallback path: ``mnemos.api`` either does not exist or
    # is the legacy empty-stub module. Install a placeholder and let
    # the F04-era submodules below register against it.
    if api_pkg is None:
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
    _safe_register("mnemos.api.dependencies", dependencies_mod, api_pkg)

    # mnemos.api.extra_guards
    extra_guards_mod = types.ModuleType("mnemos.api.extra_guards")
    def _require_extra(*args: Any, **kwargs: Any) -> Any:
        return None
    extra_guards_mod.require_extra = _require_extra  # type: ignore[attr-defined]
    extra_guards_mod.__all__ = ["require_extra"]
    _safe_register("mnemos.api.extra_guards", extra_guards_mod, api_pkg)

    # mnemos.api.routes namespace
    # We deliberately do NOT create a stub ``mnemos.api.routes`` module
    # here — the project ships real ``mnemos/api/routes/{knemon_*,
    # ledger}.py`` files that tests need to import (the real
    # ``_limited_sql`` / ``_timeline_sql`` helpers in
    # ``knemon_dashboard``, the real ``router`` instance, the real
    # PostgresBackend etc.). Pre-registering a placeholder here would
    # shadow them.
    #
    # When the real ``mnemos.api.routes`` namespace package exists, we
    # import it eagerly so that ``from mnemos.api.routes import
    # knemon_dashboard`` works on the first try (otherwise the implicit
    # namespace materialisation on first attribute access would happen
    # at test-collection time, AFTER the parent ``mnemos.api`` stub was
    # pre-registered with empty ``__path__``, which leaves the routes
    # namespace without the project source path entry).
    if not _already_imported("mnemos.api.routes"):
        # Force the real namespace package to materialise by importing
        # it. This wires ``api_pkg.__path__`` to include the project
        # source directory that contains ``knemon_dashboard.py`` etc.
        try:
            import mnemos.api.routes as _real_routes  # noqa: F401

            # Replace the empty-stub ``api_pkg`` attribute with the
            # real namespace package so subsequent ``import
            # mnemos.api.routes`` finds it AND ``from mnemos.api.routes
            # import knemon_dashboard`` works.
            real_routes = sys.modules.get("mnemos.api.routes")
            if real_routes is not None:
                # If our stub pre-registered an empty ``__path__``
                # package under the same name, replace it in the
                # parent's attribute dict.
                if real_routes is not api_pkg or real_routes.__path__ != api_pkg.__path__:
                    setattr(api_pkg, "routes", real_routes)
        except ImportError:
            # No real namespace package — fall back to the empty
            # placeholder so the import machinery still has *some*
            # module to attach child submodules to.
            routes_pkg = types.ModuleType("mnemos.api.routes")
            routes_pkg.__path__ = []
            sys.modules.setdefault("mnemos.api.routes", routes_pkg)
            setattr(api_pkg, "routes", routes_pkg)


def _install_pantheon_stubs() -> None:
    """Install stub modules for PANTHEON and graeae collaborators.

    PANTHEON (``mnemos.domain.pantheon.*``) and graeae
    (``mnemos.domain.graeae.*``) are sibling packages in the
    ``mnemos-os`` distribution. They are NOT installed in this KNEMON
    checkout — ``pyproject.toml`` declares ``pythonpath = ["../pantheon"]``
    so a sibling ``pantheon`` checkout (a different repo) is the
    expected source. When the sibling is absent (dev checkout, CI
    runner that doesn't have ``../pantheon``) the tests would fail at
    collection time with ``ModuleNotFoundError``.

    The stubs below provide the MINIMAL surface the KNEMON test files
    actually touch:

    * ``mnemos.domain.pantheon.catalog.list_models`` (async, returns
      a list) — monkeypatched by ``test_triage_*`` tests.
    * ``mnemos.domain.pantheon.triage.recommend`` (async) +
      ``_PRICE_ABSENT_LOGGED`` (bool) — monkeypatched by
      ``test_triage_*`` tests AND imported by
      ``test_llm_wrapper_*`` tests that monkeypatch
      ``triage.recommend``.
    * ``mnemos.domain.graeae.engine.get_graeae_engine`` (callable
      returning an object with ``.route()``) — monkeypatched by
      ``test_llm_wrapper_*`` tests.

    The actual implementations in production come from the sibling
    ``pantheon`` / ``mnemos-os`` repo, so these stubs are NOT used in
    production — they exist purely to keep the test-collection phase
    green when the sibling repo is not available.

    If the real ``mnemos.domain.pantheon`` package IS importable (e.g.
    via the sibling overlay) the stub defers to it and installs
    nothing.
    """
    if _already_imported("mnemos.domain.pantheon") or _can_import("mnemos.domain.pantheon"):
        return
    if _already_imported("mnemos.domain.graeae") or _can_import("mnemos.domain.graeae"):
        return

    domain_pkg = sys.modules.get("mnemos.domain")
    if domain_pkg is None:
        try:
            import mnemos.domain as domain_pkg  # noqa: F401
        except Exception:
            domain_pkg = types.ModuleType("mnemos.domain")
            domain_pkg.__path__ = []
            sys.modules.setdefault("mnemos.domain", domain_pkg)

    # mnemos.domain.graeae.engine --------------------------------------
    graeae_pkg = types.ModuleType("mnemos.domain.graeae")
    graeae_pkg.__path__ = []
    _safe_register("mnemos.domain.graeae", graeae_pkg, domain_pkg)

    graeae_engine_mod = types.ModuleType("mnemos.domain.graeae.engine")

    async def _stub_get_graeae_engine():
        raise RuntimeError(
            "mnemos.domain.graeae.engine is not available in this checkout; "
            "tests must monkeypatch get_graeae_engine before calling mnemos.llm.call"
        )

    graeae_engine_mod.get_graeae_engine = _stub_get_graeae_engine  # type: ignore[attr-defined]
    graeae_engine_mod.__all__ = ["get_graeae_engine"]
    _safe_register("mnemos.domain.graeae.engine", graeae_engine_mod, graeae_pkg)

    # mnemos.domain.pantheon -------------------------------------------
    pantheon_pkg = types.ModuleType("mnemos.domain.pantheon")
    pantheon_pkg.__path__ = []
    _safe_register("mnemos.domain.pantheon", pantheon_pkg, domain_pkg)

    catalog_mod = types.ModuleType("mnemos.domain.pantheon.catalog")

    async def _stub_list_models():
        return []

    catalog_mod.list_models = _stub_list_models  # type: ignore[attr-defined]
    catalog_mod.__all__ = ["list_models"]
    _safe_register("mnemos.domain.pantheon.catalog", catalog_mod, pantheon_pkg)

    triage_mod = types.ModuleType("mnemos.domain.pantheon.triage")

    async def _stub_recommend(*args, **kwargs):
        raise RuntimeError(
            "mnemos.domain.pantheon.triage.recommend is not available in this "
            "checkout; tests must monkeypatch triage.recommend before calling it"
        )

    triage_mod.recommend = _stub_recommend  # type: ignore[attr-defined]
    triage_mod._PRICE_ABSENT_LOGGED = False  # type: ignore[attr-defined]
    triage_mod.__all__ = ["recommend", "_PRICE_ABSENT_LOGGED"]
    _safe_register("mnemos.domain.pantheon.triage", triage_mod, pantheon_pkg)


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
    """Install all runtime stubs (idempotent).

    The stub installer guards every ``sys.modules[name]`` registration
    behind ``_already_imported(name)`` so the stubs below cannot shadow
    a real ``mnemos-core`` subpackage when one is importable. When the
    real package IS installed the stubs become no-ops and tests get the
    real ``PostgresBackend`` / ``mnemos.api.routes.ledger`` / etc.

    We also pre-register the project-source ``mnemos.api.routes.*`` /
    ``mnemos.domain.knemon.*`` modules in ``sys.modules`` so that the
    import system does not re-resolve them via the namespace-package
    path machinery. Namespace packages re-derive their ``__path__`` from
    the finders every time a child module is imported, which can
    silently drop the project source path and surface as
    ``ModuleNotFoundError`` for KNEMON's own route/domain modules.
    Pre-registering the leaf modules avoids the re-derivation entirely.
    """
    if getattr(install_mnemos_core_stubs, "_installed", False):
        return

    import os

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    project_mnemos = os.path.abspath(os.path.join(project_root, "mnemos"))

    # Force-import the parent ``mnemos`` namespace package so that
    # subsequent sub-attribute lookups wire up correctly.
    try:
        import mnemos  # noqa: F401
    except Exception:
        pass

    # Wire the project source directory onto the ``mnemos`` namespace
    # package's ``__path__`` so KNEMON's own mnemos/{domain/knemon,
    # api/routes/knemon_*, etc.} files are discoverable. In an editable
    # install of mnemos-knemon, setuptools usually wires this
    # automatically, but on internal-index installs where the project
    # is installed as a regular site-packages package, the namespace
    # contribution needs an explicit hint.
    if os.path.isdir(project_mnemos):
        mnemos_root = sys.modules.get("mnemos")
        if mnemos_root is not None:
            mnemos_paths = getattr(mnemos_root, "__path__", [])
            if project_mnemos not in mnemos_paths:
                mnemos_paths.append(project_mnemos)

    # Wire the local ``mnemos-core`` checkout (if any) onto the
    # ``mnemos`` namespace package's ``__path__`` so the real
    # ``mnemos.api.dependencies`` / ``mnemos.persistence.postgres`` /
    # ``mnemos.llm`` modules are importable. Production nodes install
    # ``mnemos-core`` from an internal index that ships with full
    # sources; the venv in this checkout resolves the dep against the
    # local ``mnemos-stubs/mnemos_core`` placeholder which has no code,
    # so tests would otherwise have to rely on the in-memory stub
    # shims below. Auto-detecting a sibling checkout via
    # ``MNEMOS_CORE_PATH`` env var OR a well-known ``../`` location
    # makes the validation work either way without forcing the
    # developer to edit uv config.
    mnemos_core_path = os.environ.get("MNEMOS_CORE_PATH", "").strip()
    if not mnemos_core_path:
        candidates = [
            os.path.abspath(os.path.join(project_root, "..", "mnemos-core-item7", "mnemos")),
            os.path.abspath(os.path.join(project_root, "..", "mnemos-core", "mnemos")),
        ]
        for candidate in candidates:
            if os.path.isdir(candidate) and os.path.isfile(
                os.path.join(candidate, "api", "dependencies.py")
            ):
                mnemos_core_path = candidate
                break
    if mnemos_core_path and os.path.isdir(mnemos_core_path):
        mnemos_root = sys.modules.get("mnemos")
        if mnemos_root is not None:
            mnemos_paths = getattr(mnemos_root, "__path__", [])
            if mnemos_core_path not in mnemos_paths:
                mnemos_paths.append(mnemos_core_path)

    # Pre-register KNEMON's own source modules in ``sys.modules``
    # BEFORE any other code (including conftest, pytest, or the
    # individual tests) has a chance to import them through the
    # namespace-package path machinery. Once a module is in
    # ``sys.modules`` by its dotted name, the import system returns it
    # directly without re-resolving any parent namespace package.
    _register_knemon_modules(project_mnemos)

    _install_core_stub()
    _install_persistence_stub()
    _install_api_stub()
    _install_pantheon_stubs()
    _install_db_migrations_stub()
    install_mnemos_core_stubs._installed = True  # type: ignore[attr-defined]


def _register_knemon_modules(project_mnemos: str) -> None:
    """Pre-register KNEMON's own Python modules under their dotted names.

    The project contributes ``mnemos.domain.knemon.*`` and
    ``mnemos.api.routes.{knemon_*, ledger}.py`` as namespace-package
    members. Python's namespace-package machinery re-derives the
    parent ``__path__`` from the finders every time a child module is
    imported, which can drop the project source path silently — and
    surface as ``ModuleNotFoundError`` for KNEMON's own modules when
    the mnemos-core namespace package's finders do not include our
    checkout.

    By eagerly importing each KNEMON Python file under its dotted name
    into ``sys.modules`` we sidestep the re-derivation entirely. This
    is safe even when the modules would be importable through the
    regular path machinery (the regular import is just a no-op once
    the module is cached in ``sys.modules``).
    """
    import importlib.util
    import os

    routes_dir = os.path.join(project_mnemos, "api", "routes")
    domain_knemon_dir = os.path.join(project_mnemos, "domain", "knemon")

    for directory, parent in (
        (routes_dir, "mnemos.api.routes"),
        (domain_knemon_dir, "mnemos.domain.knemon"),
    ):
        if not os.path.isdir(directory):
            continue
        # Make sure the parent namespace package is in sys.modules
        # first so the import system has something to attach the
        # children to.
        try:
            importlib.import_module(parent)
        except Exception:
            pass

        for entry in sorted(os.listdir(directory)):
            if not entry.endswith(".py") or entry == "__init__.py":
                continue
            if entry.startswith("_") and entry not in {"_plan_windows.py"}:
                continue
            module_name = f"{parent}.{entry[:-3]}"
            if module_name in sys.modules:
                continue
            file_path = os.path.join(directory, entry)
            try:
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                if spec is None or spec.loader is None:
                    continue
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                # Also attach to the parent so attribute lookup works.
                parent_mod = sys.modules.get(parent)
                if parent_mod is not None:
                    setattr(parent_mod, entry[:-3], module)
            except Exception:
                # If a particular module fails to import (e.g. it has
                # its own missing dependency), leave it out of
                # sys.modules so a later import attempt can surface the
                # real error.
                sys.modules.pop(module_name, None)


__all__ = ["install_mnemos_core_stubs"]