"""The SDK's view of the router: decide (with breaker and cache), config poll, events.

Everything here fails open. Any doubt means "no decision", and the job runs as
the caller wrote it.
"""

from __future__ import annotations

import atexit
import hashlib
import logging
import os
import queue
import random
import threading
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Tuple

from slashpeak.bigquery._version import __version__
from slashpeak.bigquery.mask import Masker, MaskError, builtin_masker, masker_from_config
from slashpeak.bigquery.settings import Settings, stricter_mode
from slashpeak.bigquery.transport import Reply, Transport

logger = logging.getLogger("slashpeak.bigquery")

BREAKER_FAILURES = 5
BREAKER_OPEN_S = 30.0
CACHE_MAX = 2048
EVENT_BATCH = 100
EVENT_INTERVAL_S = 2.0
EVENT_QUEUE_MAX = 5000
EXIT_FLUSH_S = 1.0
POLL_MIN_S = 10.0
POLL_MAX_S = 600.0
DEFAULT_TTL_S = 60.0


def _version_tuple(value: str) -> Tuple[int, int, int]:
    parts = []
    for piece in value.replace("+", ".").replace("-", ".").split(".")[:3]:
        digits = "".join(ch for ch in piece if ch.isdigit())
        parts.append(int(digits) if digits else 0)
    while len(parts) < 3:
        parts.append(0)
    return parts[0], parts[1], parts[2]


def client_at_least(version: str, minimum: Optional[str]) -> bool:
    """Same rule as the server's ``clientAtLeast``."""
    return not minimum or _version_tuple(version) >= _version_tuple(minimum)


class Breaker:
    """5 failures in a row → open 30 s → one probe. A good probe closes it."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._failures = 0
        self._opened_at: Optional[float] = None
        self._probing = False

    def allow(self) -> bool:
        with self._lock:
            if self._opened_at is None:
                return True
            if self._probing or self._clock() - self._opened_at < BREAKER_OPEN_S:
                return False
            self._probing = True
            return True

    def record(self, ok: bool) -> None:
        with self._lock:
            if ok:
                self._failures = 0
                self._opened_at = None
            else:
                self._failures += 1
                if self._probing or self._failures >= BREAKER_FAILURES:
                    self._opened_at = self._clock()
            self._probing = False


class _Flight:
    __slots__ = ("done", "value")

    def __init__(self) -> None:
        self.done = threading.Event()
        self.value: Optional[Dict[str, Any]] = None


class DecisionCache:
    """Per-process cache of decisions, key = sha256(project, location, SQL). Single-flight per key."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: "OrderedDict[str, Tuple[float, Dict[str, Any]]]" = OrderedDict()
        self._flights: Dict[str, _Flight] = {}

    @staticmethod
    def key(project: str, location: Optional[str], sql: str) -> str:
        return hashlib.sha256("\x00".join((project, location or "", sql)).encode("utf-8")).hexdigest()

    def get_or_fetch(self, key: str, fetch: Callable[[], Optional[Dict[str, Any]]], ttl_cap_s: float, wait_s: float) -> Optional[Dict[str, Any]]:
        with self._lock:
            hit = self._entries.get(key)
            if hit is not None:
                if hit[0] > self._clock():
                    self._entries.move_to_end(key)
                    return hit[1]
                del self._entries[key]
            flight = self._flights.get(key)
            leader = flight is None
            if leader:
                flight = _Flight()
                self._flights[key] = flight
        assert flight is not None
        if not leader:
            # Another thread is asking for the same query: wait for its answer, within our deadline.
            return flight.value if flight.done.wait(wait_s) else None
        value: Optional[Dict[str, Any]] = None
        try:
            value = fetch()
        finally:
            with self._lock:
                self._flights.pop(key, None)
                ttl = _cache_ttl(value, ttl_cap_s)
                if value is not None and ttl > 0:
                    self._entries[key] = (self._clock() + ttl, value)
                    while len(self._entries) > CACHE_MAX:
                        self._entries.popitem(last=False)
            flight.value = value
            flight.done.set()
        return value

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._flights.clear()


def _cache_ttl(decision: Optional[Dict[str, Any]], cap_s: float) -> float:
    if not decision or decision.get("cacheScope") != "sql":
        return 0.0
    ttl = decision.get("ttlSeconds")
    if not isinstance(ttl, (int, float)) or isinstance(ttl, bool) or ttl <= 0:
        return 0.0
    return float(min(ttl, cap_s))


class Router:
    """Process-wide router state. Thread-safe; ``after_fork_in_child`` rebuilds it in a child."""

    def __init__(self, settings: Settings, transport: Optional[Transport] = None, clock: Callable[[], float] = time.monotonic) -> None:
        self.settings = settings
        self._clock = clock
        self.transport = transport or Transport(settings.endpoint, settings.api_key or "")
        self.breaker = Breaker(clock)
        self.cache = DecisionCache(clock)
        self._config: Optional[Dict[str, Any]] = None
        self._etag: Optional[str] = None
        self._masker: Optional[Masker] = builtin_masker()
        self._state_lock = threading.Lock()
        self._events: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=EVENT_QUEUE_MAX)
        self._stop = threading.Event()
        self._threads_pid = -1
        self._threads_lock = threading.Lock()
        self._closed = False

    # -- background threads ----------------------------------------------------------------
    def start(self) -> None:
        """Starts the config and event threads once per process (again in a forked child)."""
        if self._threads_pid == os.getpid() or self._closed:
            return
        with self._threads_lock:
            if self._threads_pid == os.getpid():
                return
            self._threads_pid = os.getpid()
            self._stop = threading.Event()
            threading.Thread(target=self._config_loop, name="slashpeak-config", daemon=True).start()
            threading.Thread(target=self._event_loop, name="slashpeak-events", daemon=True).start()

    def after_fork_in_child(self) -> None:
        # Locks held by another thread at fork time stay locked forever in the child: make new ones.
        self._state_lock = threading.Lock()
        self._threads_lock = threading.Lock()
        self.breaker = Breaker(self._clock)
        self.cache = DecisionCache(self._clock)
        self._events = queue.Queue(maxsize=EVENT_QUEUE_MAX)
        self._threads_pid = -1
        self.transport.after_fork_in_child()

    def close(self, flush_s: float = EXIT_FLUSH_S) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        if self._threads_pid == os.getpid():
            self.flush_events(deadline_s=flush_s)
        self.transport.close()

    # -- config ----------------------------------------------------------------------------
    def poll_config(self) -> bool:
        """One ``GET /v1/config`` with ETag. True when the call worked (200 or 304)."""
        headers = {"If-None-Match": self._etag} if self._etag else None
        try:
            reply = self.transport.request("GET", "/v1/config", headers=headers)
        except Exception:  # noqa: BLE001
            logger.debug("slashpeak config fetch failed", extra={"code": "network"})
            return False
        if reply.status == 304:
            return True
        if reply.status != 200 or reply.body is None:
            logger.debug("slashpeak config fetch failed", extra={"code": f"http_{reply.status}"})
            return False
        self._apply_config(reply)
        return True

    def _apply_config(self, reply: Reply) -> None:
        config = reply.body or {}
        masker: Optional[Masker]
        try:
            masker = masker_from_config(config.get("mask"))
        except MaskError:
            masker = None  # A rule Python cannot compile: send nothing until it changes.
        else:
            if masker is None:
                masker = builtin_masker()
        with self._state_lock:
            self._config = config
            self._etag = reply.etag
            self._masker = masker

    def _poll_seconds(self) -> float:
        config = self._config or {}
        value = config.get("pollSeconds")
        seconds = float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 60.0
        return max(POLL_MIN_S, min(POLL_MAX_S, seconds))

    def _config_loop(self) -> None:
        while not self._stop.is_set():
            ok = self.poll_config()
            wait = self._poll_seconds() if ok else POLL_MIN_S
            # Jitter, so a fleet of workers started together does not poll together.
            self._stop.wait(wait * random.uniform(0.9, 1.1))

    def masker(self) -> Optional[Masker]:
        return self._masker

    def config(self) -> Optional[Dict[str, Any]]:
        return self._config

    # -- gating ----------------------------------------------------------------------------
    def _policies_for(self, project: str) -> Optional[List[Dict[str, Any]]]:
        """Policies that may cover ``project``; None when no config has arrived yet."""
        config = self._config
        if config is None:
            return None
        policies = [p for p in config.get("policies") or [] if isinstance(p, dict)]
        out = []
        for policy in policies:
            projects = policy.get("projects") or []
            # An empty list covers every project of that connection (the server's rule).
            if not projects or project in projects:
                out.append(policy)
        return out

    def may_decide(self, project: str) -> bool:
        if self._closed or not self.settings.usable:
            return False
        config = self._config
        if config is None:
            return True  # First calls before the config arrives: the server still applies every rule.
        if config.get("killSwitch") is True:
            return False
        minimum = (config.get("minClient") or {}).get("python") if isinstance(config.get("minClient"), dict) else None
        if not client_at_least(__version__, minimum):
            return False
        policies = self._policies_for(project) or []
        return any(stricter_mode(self.settings.mode, p.get("mode")) != "off" for p in policies)

    def send_param_values(self, project: str) -> bool:
        if not self.settings.send_param_values:
            return False
        policies = self._policies_for(project)
        # Off if any policy that may cover the project says off.
        return all(p.get("sendParamValues") is not False for p in policies or [])

    def ttl_cap_s(self) -> float:
        value = (self._config or {}).get("decisionTtlSeconds")
        return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0 else DEFAULT_TTL_S

    # -- decide ----------------------------------------------------------------------------
    def decide(self, cache_key: str, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.start()
        return self.cache.get_or_fetch(cache_key, lambda: self._decide_call(request), self.ttl_cap_s(), wait_s=0.5)

    def _decide_call(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.breaker.allow():
            return None
        reply = self.transport.request_with_deadline("POST", "/v1/decide", request)
        ok = reply is not None and reply.status == 200 and isinstance(reply.body, dict)
        self.breaker.record(ok)
        if not ok:
            logger.debug("slashpeak decide skipped", extra={"code": "timeout" if reply is None else f"http_{reply.status}"})
            return None
        return reply.body

    # -- events ----------------------------------------------------------------------------
    def emit(self, event: Dict[str, Any]) -> None:
        if not event.get("decisionId") or self._closed:
            return
        self.start()
        try:
            self._events.put_nowait(event)
        except queue.Full:
            pass  # Bounded: events are best effort and never slow a job.

    def _drain(self, limit: int) -> List[Dict[str, Any]]:
        batch: List[Dict[str, Any]] = []
        while len(batch) < limit:
            try:
                batch.append(self._events.get_nowait())
            except queue.Empty:
                break
        return batch

    def _send_events(self, batch: List[Dict[str, Any]], timeout: Tuple[float, float]) -> None:
        if not batch:
            return
        try:
            self.transport.request("POST", "/v1/events", {"events": batch}, timeout=timeout)
        except Exception:  # noqa: BLE001
            logger.debug("slashpeak events dropped", extra={"code": "network", "count": len(batch)})

    def _event_loop(self) -> None:
        while not self._stop.wait(EVENT_INTERVAL_S):
            while True:
                batch = self._drain(EVENT_BATCH)
                self._send_events(batch, (2.0, 5.0))
                if len(batch) < EVENT_BATCH:
                    break

    def flush_events(self, deadline_s: float = EXIT_FLUSH_S) -> None:
        end = time.monotonic() + deadline_s
        while time.monotonic() < end:
            batch = self._drain(EVENT_BATCH)
            if not batch:
                return
            left = max(0.05, end - time.monotonic())
            self._send_events(batch, (min(0.5, left), left))


_EXIT_HOOKED = False


def register_exit(router_getter: Callable[[], Optional[Router]]) -> None:
    """Flush events for at most 1 s at interpreter exit."""
    global _EXIT_HOOKED
    if _EXIT_HOOKED:
        return
    _EXIT_HOOKED = True

    def _on_exit() -> None:
        router = router_getter()
        if router is not None:
            router.close()

    atexit.register(_on_exit)
