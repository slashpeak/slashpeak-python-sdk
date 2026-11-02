"""SlashPeak Job Router for google-cloud-bigquery.

    import slashpeak.bigquery
    slashpeak.bigquery.install()          # every client in this process
    # or: slashpeak.bigquery.wrap(client)  # one client

Reads ``SLASHPEAK_API_KEY``, ``SLASHPEAK_MODE`` (off | shadow | active),
``SLASHPEAK_ENDPOINT`` and ``SLASHPEAK_SEND_PARAM_VALUES``. Without a key it
does nothing. On any error, or no answer within 500 ms, jobs run unchanged.
"""

from __future__ import annotations

import contextlib
import os
import threading
from typing import Any, Callable, Iterator, Optional

from slashpeak.bigquery._version import __version__
from slashpeak.bigquery.hook import Hook, _guard, _Pending
from slashpeak.bigquery.router import Router, register_exit
from slashpeak.bigquery.settings import Settings, load_settings

__all__ = ["__version__", "install", "uninstall", "wrap", "bypass", "Settings"]

_lock = threading.Lock()
_router: Optional[Router] = None
_hook = Hook(lambda: _router)
_patched: Optional[tuple] = None  # (class, had_own_attribute, original)
_FORK_HOOKED = False


def _after_fork_in_child() -> None:
    global _lock
    _lock = threading.Lock()
    # A lock held by another thread at fork time stays locked in the child: rebuild every one.
    _hook.pending = _Pending()
    router = _router
    if router is not None:
        router.after_fork_in_child()


def _ensure_router(settings: Optional[Settings]) -> Router:
    global _router, _FORK_HOOKED
    if _router is None or settings is not None:
        if _router is not None:
            _router.close(flush_s=0.0)
        _router = Router(settings or load_settings())
        register_exit(lambda: _router)
    if not _FORK_HOOKED and hasattr(os, "register_at_fork"):
        os.register_at_fork(after_in_child=_after_fork_in_child)
        _FORK_HOOKED = True
    return _router


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
        router = _ensure_router(settings)
        if not router.settings.usable:
            return False
        from google.cloud.bigquery import _http  # imported late: the package works without it installed

        _patch_class(_http.Connection)
        router.start()
        return True


def wrap(client: Any, settings: Optional[Settings] = None) -> Any:
    """Hooks one ``bigquery.Client`` only. Returns the same client."""
    with _lock:
        router = _ensure_router(settings)
        if not router.settings.usable:
            return client
        connection = client._connection
        current = connection.api_request
        if not getattr(current, "_slashpeak", False):
            original = type(connection).api_request
            connection.api_request = _wrapper(original).__get__(connection, type(connection))
        router.start()
        return client


def uninstall() -> None:
    """Removes the process-wide hook and flushes pending events (at most 1 s)."""
    global _patched, _router
    with _lock:
        if _patched is not None:
            cls, had_own, original = _patched
            if had_own:
                cls.api_request = original  # type: ignore[attr-defined]
            elif "api_request" in cls.__dict__:
                delattr(cls, "api_request")
            _patched = None
        if _router is not None:
            _router.close()
            _router = None


@contextlib.contextmanager
def bypass() -> Iterator[None]:
    """Runs the block with the hook off on this thread (for example, a job that must never move)."""
    previous = getattr(_guard, "active", False)
    _guard.active = True
    try:
        yield
    finally:
        _guard.active = previous
