"""The one router per process, shared by every warehouse hook.

A process that uses BigQuery and Snowflake keeps one key, one connection pool,
one config poll and one event queue.
"""

from __future__ import annotations

import os
import threading
from typing import Callable, List, Optional, Set

from slashpeak._core.router import Router, register_exit
from slashpeak._core.settings import Settings, load_settings

_lock = threading.Lock()
_router: Optional[Router] = None
_users: Set[str] = set()
_fork_callbacks: List[Callable[[], None]] = []
_fork_hooked = False


def current() -> Optional[Router]:
    return _router


def _after_fork_in_child() -> None:
    global _lock
    # A lock held by another thread at fork time stays locked in the child: rebuild every one.
    _lock = threading.Lock()
    for callback in list(_fork_callbacks):
        callback()
    router = _router
    if router is not None:
        router.after_fork_in_child()


def on_fork(callback: Callable[[], None]) -> None:
    """Runs ``callback`` in a forked child, before the router resets."""
    if callback not in _fork_callbacks:
        _fork_callbacks.append(callback)


def acquire(user: str, settings: Optional[Settings] = None) -> Router:
    """The process router for one hook (``bigquery``, …). New settings replace the router."""
    global _router, _fork_hooked
    with _lock:
        # Same settings from a second hook keep the router (one pool, one poll, one queue).
        if _router is None or (settings is not None and settings != _router.settings):
            if _router is not None:
                _router.close(flush_s=0.0)
            _router = Router(settings or load_settings())
            register_exit(current)
        if not _fork_hooked and hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=_after_fork_in_child)
            _fork_hooked = True
        _users.add(user)
        return _router


def release(user: str) -> None:
    """Closes the router (flushing events, at most 1 s) when its last hook goes."""
    global _router
    with _lock:
        _users.discard(user)
        if _users or _router is None:
            return
        router, _router = _router, None
    router.close()
