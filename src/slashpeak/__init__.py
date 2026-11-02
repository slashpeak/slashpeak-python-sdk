"""SlashPeak Job Router: run each warehouse job where it costs less.

    pip install "slashpeak[bigquery]"

    import slashpeak
    slashpeak.install()   # hooks every supported library that is installed

Reads ``SLASHPEAK_API_KEY``, ``SLASHPEAK_MODE`` (off | shadow | active),
``SLASHPEAK_ENDPOINT`` and ``SLASHPEAK_SEND_PARAM_VALUES``. Without a key it
does nothing. On any error, or no answer within 500 ms, jobs run unchanged.
"""

from __future__ import annotations

import importlib.util
from typing import List, Optional

from slashpeak._core.settings import Settings
from slashpeak._version import __version__

__all__ = ["__version__", "install", "uninstall", "Settings"]

#: Warehouse hook → the library it needs.
_HOOKS = {"bigquery": "google.cloud.bigquery"}


def _has(module: str) -> bool:
    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):
        return False


def install(settings: Optional[Settings] = None) -> List[str]:
    """Hooks every supported library that is installed. Returns the hooks that are on."""
    on = []
    for name, library in _HOOKS.items():
        if not _has(library):
            continue
        hook = importlib.import_module(f"slashpeak.{name}")
        if hook.install(settings):
            on.append(name)
    return on


def uninstall() -> None:
    """Removes every hook and flushes pending events (at most 1 s)."""
    import sys

    for name in _HOOKS:
        hook = sys.modules.get(f"slashpeak.{name}")
        if hook is not None:
            hook.uninstall()
