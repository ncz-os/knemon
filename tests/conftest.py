"""pytest configuration for KNEMON's F04 budget-reservation regression suite.

The conftest injects `mnemos.core` and `mnemos.persistence` runtime
stubs BEFORE any test module imports them. This is necessary because
`mnemos-core` is not published to PyPI as an independently installable
distribution in this checkout — production nodes install it from an
internal index. The stub lives at ``tests/_mnemos_core_stubs.py`` and
exposes only the surface that the F04 budget-reservation tests actually
exercise (settings shim, plan-window helper, recommendation helpers,
and persistence record dataclasses).

Important: the pythonpath in pyproject.toml adds the sibling
``pantheon/tests`` directory for cross-repo F04/F05 integration tests,
which means ``import tests`` resolves to ``pantheon/tests`` rather than
our own directory. We therefore load the stub by *file path* (not via
``import tests._mnemos_core_stubs``) so we get THIS repo's stubs and
not anything from the sibling overlay.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_STUB_PATH = Path(__file__).resolve().parent / "_mnemos_core_stubs.py"


def _ensure_stubs_installed() -> None:
    """Import (or re-import) the runtime stub installer.

    Loading by file path is required because the pythonpath entry
    ``../pantheon`` exposes a sibling ``tests`` package that would
    shadow our local module under a normal ``import tests`` lookup.
    """
    spec = importlib.util.spec_from_file_location(
        "_mnemos_core_stubs_runtime", _STUB_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"could not load runtime stub from {_STUB_PATH!s}"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("_mnemos_core_stubs_runtime", module)
    spec.loader.exec_module(module)
    module.install_mnemos_core_stubs()


@pytest.hookimpl(tryfirst=True)
def pytest_load_initial_conftests(
    early_config: pytest.Config,
    parser: pytest.Parser,
    args: list[str],
) -> None:
    """Install stubs even earlier — before any rootdir conftest loads."""
    _ensure_stubs_installed()


@pytest.hookimpl(tryfirst=True)
def pytest_collection(session: pytest.Session) -> None:
    """Reaffirm the stubs after any subsequent collection-time imports.

    The mnemos-core stubs are idempotent, so this is cheap.
    """
    _ensure_stubs_installed()