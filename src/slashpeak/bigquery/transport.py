"""HTTPS to the router: one pooled keep-alive session per process.

Connection rules:

* **One session, one pool.** A ``requests.Session`` with one ``HTTPAdapter``
  keeps TLS connections open between calls (HTTP/1.1 keep-alive), so a warm
  decide pays no TCP or TLS handshake. The pool holds up to ``POOL_SIZE``
  sockets: the decide workers plus the event and config threads. It never
  blocks waiting for a free socket (``pool_block=False``); an extra call opens
  a short-lived socket instead.
* **No hidden retries.** ``max_retries`` is a ``Retry`` with every count at 0,
  so urllib3 never retries or follows a redirect behind our deadline. A failed
  call is a failed call; the job runs unchanged.
* **Split timeouts.** Connect and read timeouts are separate. A socket timeout
  does not cover DNS, so decide also runs on a small thread pool and the caller
  waits at most ``DECIDE_DEADLINE_S`` (DNS included). A late answer is dropped.
* **Background calls** (events, config) use longer timeouts on the same pool;
  they never hold up a job.
* **Fork safe.** A child process must not use the parent's sockets (two
  processes writing one TLS stream corrupts it). After a fork the child drops
  the parent's session and threads without closing them (closing would send
  a TLS close on the parent's stream), and builds its own on first use.
* **No cookies, TLS verified, proxies from the environment** (``HTTPS_PROXY``,
  ``NO_PROXY``), bounded response size, the key only in the
  ``Authorization`` header, never in a URL or a log.
"""

from __future__ import annotations

import json
import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from http.cookiejar import DefaultCookiePolicy
from typing import Any, Callable, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from slashpeak.bigquery._version import __version__

DECIDE_WORKERS = 4
POOL_SIZE = DECIDE_WORKERS + 2
#: Hard stop for one decide, DNS included (plan: 500 ms end to end).
DECIDE_DEADLINE_S = 0.5
#: (connect, read) for decide. The deadline above still wins.
DECIDE_TIMEOUT: Tuple[float, float] = (0.2, 0.5)
#: (connect, read) for events and config, which run off the job's path.
BACKGROUND_TIMEOUT: Tuple[float, float] = (2.0, 5.0)
#: Router answers are small; a bigger body is not ours.
MAX_RESPONSE_BYTES = 1024 * 1024


class _NoCookies(DefaultCookiePolicy):
    def set_ok(self, cookie: Any, request: Any) -> bool:  # noqa: ARG002
        return False


def _user_agent() -> str:
    try:
        from google.cloud.bigquery import __version__ as bq_version  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        bq_version = "unknown"
    return f"slashpeak-bigquery-python/{__version__} google-cloud-bigquery/{bq_version}"


def build_session(api_key: str) -> requests.Session:
    session = requests.Session()
    no_retries = Retry(total=0, connect=0, read=0, redirect=0, status=0, other=0, raise_on_redirect=False, raise_on_status=False)
    adapter = HTTPAdapter(pool_connections=1, pool_maxsize=POOL_SIZE, max_retries=no_retries, pool_block=False)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.cookies.set_policy(_NoCookies())
    session.headers.update(
        {
            "Authorization": f"Bearer {api_key}",
            "User-Agent": _user_agent(),
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "Connection": "keep-alive",
        }
    )
    return session


class Reply:
    __slots__ = ("status", "body", "etag")

    def __init__(self, status: int, body: Optional[Dict[str, Any]], etag: Optional[str]) -> None:
        self.status = status
        self.body = body
        self.etag = etag


def _read_reply(response: requests.Response) -> Reply:
    body: Optional[Dict[str, Any]] = None
    try:
        raw = response.raw.read(MAX_RESPONSE_BYTES + 1, decode_content=True) if response.raw is not None else b""
        if raw and len(raw) <= MAX_RESPONSE_BYTES:
            parsed = json.loads(raw)
            body = parsed if isinstance(parsed, dict) else None
    except (ValueError, OSError, requests.RequestException):
        body = None
    finally:
        # Releases the socket back to the pool (it stays open for the next call).
        response.close()
    return Reply(response.status_code, body, response.headers.get("ETag"))


class Transport:
    """Thread-safe. Rebuilds its session and thread pool in a forked child."""

    def __init__(self, endpoint: str, api_key: str, session_factory: Callable[[str], requests.Session] = build_session) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._api_key = api_key
        self._session_factory = session_factory
        self._lock = threading.Lock()
        self._pid = -1
        self._session: Optional[requests.Session] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._closed = False

    # -- process-local state ---------------------------------------------------------------
    def _ensure(self) -> Tuple[requests.Session, ThreadPoolExecutor]:
        pid = os.getpid()
        session, executor = self._session, self._executor
        if session is not None and executor is not None and self._pid == pid:
            return session, executor
        with self._lock:
            if self._pid != pid:
                # A fork, or the first call: never reuse the parent's sockets or threads.
                self._session = None
                self._executor = None
                self._pid = pid
            if self._session is None:
                self._session = self._session_factory(self._api_key)
            if self._executor is None:
                self._executor = ThreadPoolExecutor(max_workers=DECIDE_WORKERS, thread_name_prefix="slashpeak-decide")
            return self._session, self._executor

    def after_fork_in_child(self) -> None:
        """Drop the parent's session and threads without closing them (see module doc)."""
        self._lock = threading.Lock()
        self._session = None
        self._executor = None
        self._pid = os.getpid()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            session, executor = self._session, self._executor
            self._session = None
            self._executor = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        if session is not None:
            session.close()

    # -- calls -----------------------------------------------------------------------------
    def request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Tuple[float, float] = BACKGROUND_TIMEOUT,
    ) -> Reply:
        """One call on the calling thread. Raises ``requests.RequestException`` on a network error."""
        if self._closed:
            raise requests.ConnectionError("transport closed")
        session, _ = self._ensure()
        data = None
        all_headers = dict(headers or {})
        if body is not None:
            data = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            all_headers["Content-Type"] = "application/json"
        response = session.request(
            method,
            self._endpoint + path,
            data=data,
            headers=all_headers,
            timeout=timeout,
            allow_redirects=False,
            stream=True,
        )
        return _read_reply(response)

    def request_with_deadline(self, method: str, path: str, body: Optional[Dict[str, Any]], deadline_s: float = DECIDE_DEADLINE_S) -> Optional[Reply]:
        """The call on the decide pool; None when it fails or misses the deadline (DNS included)."""
        if self._closed:
            return None
        try:
            _, executor = self._ensure()
            future: Future[Reply] = executor.submit(self.request, method, path, body, None, DECIDE_TIMEOUT)
        except RuntimeError:
            # The pool is shutting down (interpreter exit).
            return None
        try:
            return future.result(timeout=deadline_s)
        except FutureTimeout:
            # Not started yet → never runs. Already running → its own timeouts end it; the answer is dropped.
            future.cancel()
            return None
        except Exception:  # noqa: BLE001  (network errors: the job runs unchanged)
            return None
