"""SlashPeak Job Router hook for google-cloud-bigquery.

    import slashpeak.bigquery
    slashpeak.bigquery.install()          # every BigQuery client in this process
    # or: slashpeak.bigquery.wrap(client)  # one client

``slashpeak.install()`` does the same for every supported library installed.
"""

from __future__ import annotations

import contextlib
import threading
from typing import Any, Callable, Iterator, Optional

from slashpeak._core import runtime
from slashpeak._core.settings import Settings
from slashpeak.bigquery.hook import Hook, _guard, _Pending

__all__ = ["install", "uninstall", "wrap", "bypass", "Settings"]

NAME = "bigquery"
_lock = threading.Lock()
_hook = Hook(runtime.current)
_patched: Optional[tuple] = None  # (class, had_own_attribute, original)


def _reset_after_fork() -> None:
    global _lock
    _lock = threading.Lock()
    _hook.pending = _Pending()


runtime.on_fork(_reset_after_fork)


def _wrapper(original: Callable[..., Any]) -> Callable[..., Any]:
    def api_request(self: Any, *args: Any, **kwargs: Any) -> Any:
        return _hook.handle(original, self, args, kwargs)

    api_request.__wrapped__ = original  # type: ignore[attr-defined]
    api_request._slashpeak = True  # type: ignore[attr-defined]
    return api_request


def _patch_class(cls: type) -> None:
    global _patched
    current = getattr(cls, "api_request")
    if getattr(current, "_slashpeak", False):
        return
    had_own = "api_request" in cls.__dict__
    cls.api_request = _wrapper(current)  # type: ignore[attr-defined]
    _patched = (cls, had_own, current)


def install(settings: Optional[Settings] = None) -> bool:
    """Hooks every BigQuery client in this process. True when the hook is on."""
    with _lock:
        router = runtime.acquire(NAME, settings)
        if not router.settings.usable:
            return False
        from google.cloud.bigquery import _http  # imported late: `slashpeak` installs without it

        _patch_class(_http.Connection)
        router.start()
        return True


def wrap(client: Any, settings: Optional[Settings] = None) -> Any:
    """Hooks one ``bigquery.Client`` only. Returns the same client."""
    with _lock:
        router = runtime.acquire(NAME, settings)
        if not router.settings.usable:
            return client
        connection = client._connection
        if not getattr(connection.api_request, "_slashpeak", False):
            original = type(connection).api_request
            connection.api_request = _wrapper(original).__get__(connection, type(connection))
        router.start()
        return client


def uninstall() -> None:
    """Removes the hook. The shared router closes (events flushed, at most 1 s) when no hook is left."""
    global _patched
    with _lock:
        if _patched is not None:
            cls, had_own, original = _patched
            if had_own:
                cls.api_request = original  # type: ignore[attr-defined]
            elif "api_request" in cls.__dict__:
                delattr(cls, "api_request")
            _patched = None
    runtime.release(NAME)


@contextlib.contextmanager
def bypass() -> Iterator[None]:
    """Runs the block with the hook off on this thread (for example, a job that must never move)."""
    previous = getattr(_guard, "active", False)
    _guard.active = True
    try:
        yield
    finally:
        _guard.active = previous
