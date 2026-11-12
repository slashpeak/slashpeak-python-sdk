"""The hook on ``google.cloud.bigquery._http.Connection.api_request``.

One point covers ``Client.query``, ``query_and_wait``, the DB-API, SQLAlchemy,
pandas-gbq and bigframes. Rules (plans: "SDKs"):

* Never make a job fail that would have run. Any error in the SDK → the
  caller's request, unchanged.
* Never override an explicit ``reservation``. Never touch dry runs, load,
  copy, extract or continuous jobs.
* Set a client job id when none is set, so retries dedupe and the outcome
  check can find the job.
* After a 4xx at insert on a changed request (never 408, 409 or 429), run the
  original request once, then report ``fallback``. Never re-send a changed
  request after a 5xx or a timeout. jobs.insert can also answer 200 with a job
  that failed at once because the reservation is not found (nothing ran):
  run the original once under a new job id.
* A move to on-demand (``reservation: none``, jobs.query only) carries a bytes
  cap. If BigQuery stops the job on that cap (before it bills), run the
  original once, then report ``fallback``.
* Never change the caller's dict: the hook sends copies.
* Read job state only from responses the library already fetches. No extra
  BigQuery calls.
"""

from __future__ import annotations

import copy
import logging
import re
import threading
import time
import uuid
from collections import OrderedDict
from typing import Any, Callable, Dict, Optional, Tuple

from slashpeak.bigquery.requests_body import INSERT_PATH, QUERY_PATH, build_decide_request
from slashpeak._core import runtime
from slashpeak._core.router import DecisionCache, Router
from slashpeak._core.settings import stricter_mode

logger = logging.getLogger("slashpeak.bigquery")

LABEL_KEY = "slashpeak_route"
MAX_LABELS = 64
_LABEL_VALUE = re.compile(r"^[a-z0-9_-]{1,63}$")
_RESERVATION = re.compile(r"^projects/[^/]+/locations/[^/]+/reservations/[^/]+$")
_JOB_GET = re.compile(r"^/projects/[^/]+/jobs/([^/]+)/?$")
_RESULTS_GET = re.compile(r"^/projects/[^/]+/queries/([^/]+)/?$")
_REASON = re.compile(r"[^A-Za-z0-9_.-]")
#: 4xx codes that do not come from our change: timeout, conflict (job exists), rate limit.
_NO_FALLBACK = {408, 409, 429}
#: jobs.query reports a query that ran and failed as a 4xx too, so a re-run there could bill twice.
#: It re-runs only when BigQuery names a field we changed. The message is read here, never sent or logged.
_OUR_FIELDS = re.compile(r"reservation|label", re.IGNORECASE)
#: The job's ``reservation`` value that runs it on-demand.
ON_DEMAND = "none"
_BYTES_CAP = re.compile(r"^\d{1,19}$")
#: BigQuery's reason when ``maximumBytesBilled`` stops a job; it checks before the job bills.
BYTES_CAP_REASON = "bytesBilledLimitExceeded"
PENDING_MAX = 10000
PENDING_TTL_S = 24 * 3600.0

_ARG_NAMES = ("method", "path", "query_params", "data")
_guard = threading.local()


def _arg(args: Tuple[Any, ...], kwargs: Dict[str, Any], name: str) -> Any:
    if name in kwargs:
        return kwargs[name]
    index = _ARG_NAMES.index(name)
    return args[index] if len(args) > index else None


def _with_data(args: Tuple[Any, ...], kwargs: Dict[str, Any], data: Dict[str, Any]) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
    if "data" in kwargs or len(args) <= 3:
        return args, {**kwargs, "data": data}
    return args[:3] + (data,) + args[4:], kwargs


def _reason_of(error: BaseException) -> str:
    errors = getattr(error, "errors", None)
    reason = None
    if isinstance(errors, list) and errors and isinstance(errors[0], dict):
        reason = errors[0].get("reason")
    if not isinstance(reason, str) or not reason:
        code = getattr(error, "code", None)
        reason = f"http_{code}" if isinstance(code, int) else type(error).__name__
    return _REASON.sub("", reason)[:64]


def _may_fall_back(kind: str, code: Optional[int], error: BaseException, capped: bool = False) -> bool:
    """jobs.insert: a 4xx means no job was made. jobs.query: only when the error names our change,
    or when the bytes cap we set stopped it (BigQuery checks the cap before the job bills)."""
    if code is None or not 400 <= code < 500 or code in _NO_FALLBACK:
        return False
    if capped and _reason_of(error) == BYTES_CAP_REASON:
        return True
    if kind == "insert":
        return True
    try:
        text = str(error)
    except Exception:  # noqa: BLE001
        return False
    return bool(_OUR_FIELDS.search(text))


def _bytes_cap_of(kind: str, body: Dict[str, Any]) -> Any:
    target = body.get("configuration") if kind == "insert" else body
    query = target.get("query") if kind == "insert" and isinstance(target, dict) else target
    return query.get("maximumBytesBilled") if isinstance(query, dict) else None


def _added_bytes_cap(kind: str, base: Dict[str, Any], changed: Optional[Dict[str, Any]]) -> bool:
    """True when the hook set ``maximumBytesBilled`` (the original had none)."""
    return changed is not None and _bytes_cap_of(kind, base) is None and _bytes_cap_of(kind, changed) is not None


def _status_code(error: BaseException) -> Optional[int]:
    code = getattr(error, "code", None)
    return code if isinstance(code, int) and not isinstance(code, bool) else None


class _Pending:
    """Routed jobs waiting for a DONE state, by job id. Bounded and short-lived."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: "OrderedDict[str, Tuple[float, Dict[str, Any]]]" = OrderedDict()

    def __bool__(self) -> bool:
        return bool(self._items)

    def add(self, job_id: str, event: Dict[str, Any]) -> None:
        now = time.monotonic()
        with self._lock:
            self._items[job_id] = (now, event)
            self._items.move_to_end(job_id)
            while self._items and (len(self._items) > PENDING_MAX or now - next(iter(self._items.values()))[0] > PENDING_TTL_S):
                self._items.popitem(last=False)

    def pop(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            item = self._items.pop(job_id, None)
        return item[1] if item else None


class Hook:
    """Wraps one ``api_request`` function. ``handle`` is the whole decision path."""

    def __init__(self, router_getter: Callable[[], Optional[Router]]) -> None:
        self._router = router_getter
        self.pending = _Pending()

    # -- entry -----------------------------------------------------------------------------
    def handle(self, original: Callable[..., Any], conn: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        if getattr(_guard, "active", False):
            return original(conn, *args, **kwargs)
        router = self._router()
        plan = None
        if router is not None:
            try:
                plan = self._plan(router, args, kwargs)
            except Exception:  # noqa: BLE001
                logger.debug("slashpeak hook skipped", extra={"code": "plan_error"})
                plan = None
        if plan is None:
            response = original(conn, *args, **kwargs)
            if router is not None and self.pending:
                self._observe(router, args, kwargs, response)
            return response
        return self._run(router, original, conn, args, kwargs, plan)

    # -- plan ------------------------------------------------------------------------------
    def _plan(self, router: Router, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if str(_arg(args, kwargs, "method") or "").upper() != "POST":
            return None
        path = _arg(args, kwargs, "path")
        data = _arg(args, kwargs, "data")
        if not isinstance(path, str) or not isinstance(data, dict):
            return None
        match = INSERT_PATH.match(path)
        kind = "insert"
        if not match:
            match = QUERY_PATH.match(path)
            kind = "query"
        if not match:
            return None
        project = match.group(1)
        if kind == "insert":
            config = data.get("configuration")
            if not isinstance(config, dict) or not isinstance(config.get("query"), dict):
                return None  # load, copy, extract
            if config.get("dryRun") or config["query"].get("continuous") or config.get("reservation"):
                return None
        elif data.get("dryRun") or data.get("continuous") or data.get("reservation"):
            return None
        if not runtime.job_allowed() or not router.may_decide(project):
            return None
        masker = router.masker()
        if masker is None:
            return None

        base = copy.deepcopy(data)
        job_id: Optional[str] = None
        if kind == "insert":
            ref = base.setdefault("jobReference", {})
            if not isinstance(ref, dict):
                return None
            if not ref.get("jobId"):
                ref["jobId"] = f"slashpeak_{uuid.uuid4().hex}"
            ref.setdefault("projectId", project)
            job_id = ref["jobId"]
            request_id = uuid.uuid4().hex
            location = ref.get("location")
            sql = base["configuration"]["query"].get("query")
        else:
            if not base.get("requestId"):
                base["requestId"] = str(uuid.uuid4())
            request_id = base["requestId"]
            location = base.get("location")
            sql = base.get("query")
        request = build_decide_request(
            kind=kind,
            project=project,
            body=base,
            masker=masker,
            send_values=router.send_param_values(project),
            request_id=str(request_id),
            job_id=job_id,
        )
        if request is None or not isinstance(sql, str):
            return None
        return {
            "kind": kind,
            "project": project,
            "base": base,
            "request": request,
            "cache_key": DecisionCache.key(project, location if isinstance(location, str) else None, sql),
        }

    # -- apply -----------------------------------------------------------------------------
    @staticmethod
    def _apply(router: Router, kind: str, base: Dict[str, Any], decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """The changed request body, or None when the decision changes nothing."""
        action = decision.get("action")
        reservation = decision.get("reservation")
        mode = stricter_mode(router.settings.mode, decision.get("mode"))
        # On-demand only on jobs.query: the one path that can re-run a job its bytes cap stopped.
        on_demand = reservation == ON_DEMAND and kind == "query"
        move = (
            action == "set_reservation"
            and mode == "active"
            and isinstance(reservation, str)
            and (bool(_RESERVATION.match(reservation)) or on_demand)
        )
        labels = decision.get("labels")
        route = labels.get(LABEL_KEY) if isinstance(labels, dict) else None
        if action == "set_reservation" and not move:
            route = None  # Not moved: an `m_` label would claim a move that did not happen.
        if not (isinstance(route, str) and _LABEL_VALUE.match(route)):
            route = None
        if not move and route is None:
            return None
        changed = copy.deepcopy(base)
        target = changed["configuration"] if kind == "insert" else changed
        if move:
            target["reservation"] = reservation
        cap = decision.get("maximumBytesBilled")
        if move and on_demand and target.get("maximumBytesBilled") is None:
            # A move to on-demand always comes with the cap; without one, keep the job where it is.
            if not (isinstance(cap, str) and _BYTES_CAP.match(cap)):
                return None
            target["maximumBytesBilled"] = cap
        if route is not None:
            current = target.get("labels")
            current = dict(current) if isinstance(current, dict) else {}
            if LABEL_KEY not in current and len(current) < MAX_LABELS:
                current[LABEL_KEY] = route
                target["labels"] = current
            elif not move:
                return None
        return changed

    def _run(self, router: Router, original: Callable[..., Any], conn: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any], plan: Dict[str, Any]) -> Any:
        kind, base = plan["kind"], plan["base"]
        started = time.monotonic()
        decision = None
        try:
            decision = router.decide(plan["cache_key"], plan["request"])
            changed = self._apply(router, kind, base, decision) if decision else None
        except Exception:  # noqa: BLE001
            logger.debug("slashpeak hook skipped", extra={"code": "decide_error"})
            changed = None
        latency_ms = round((time.monotonic() - started) * 1000.0, 1)
        event = self._event_base(plan, decision, latency_ms)

        body = changed if changed is not None else base
        call_args, call_kwargs = _with_data(args, kwargs, body)
        try:
            response = original(conn, *call_args, **call_kwargs)
        except Exception as error:
            code = _status_code(error)
            if changed is not None and _may_fall_back(kind, code, error, _added_bytes_cap(kind, base, changed)):
                return self._fallback(router, original, conn, args, kwargs, plan, event, error)
            if event:
                router.emit({**event, "state": "insert_error", "errorReason": _reason_of(error)})
            raise
        if changed is not None and kind == "insert" and _refused_reservation(changed, response):
            return self._fallback(router, original, conn, args, kwargs, plan, event, None, new_job_id=True)
        if event:
            self._after_submit(router, kind, event, response)
        return response

    def _fallback(
        self,
        router: Router,
        original: Callable[..., Any],
        conn: Any,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        plan: Dict[str, Any],
        event: Optional[Dict[str, Any]],
        error: Optional[BaseException],
        new_job_id: bool = False,
    ) -> Any:
        """The changed request was refused before it ran: run the original once.

        ``new_job_id``: BigQuery made a failed job under the job id (a reservation
        it could not find), so the original goes under a new id. The library reads
        the job id back from the response.
        """
        body = copy.deepcopy(plan["base"])
        if plan["kind"] == "query":
            # jobs.query dedupes on requestId; a fresh one makes sure BigQuery runs the original.
            body["requestId"] = str(uuid.uuid4())
        if new_job_id:
            ref = body["jobReference"]
            ref["jobId"] = f"{str(ref['jobId'])[:1000]}_fb{uuid.uuid4().hex[:8]}"
        call_args, call_kwargs = _with_data(args, kwargs, body)
        reason = _reason_of(error) if error is not None else "reservation_refused"
        fallback = {**event, "state": "fallback", "errorReason": reason} if event else None
        try:
            response = original(conn, *call_args, **call_kwargs)
        except Exception:
            if fallback:
                router.emit(fallback)
            raise
        if fallback:
            job_id = (response.get("jobReference") or {}).get("jobId") if isinstance(response, dict) else None
            if isinstance(job_id, str):
                fallback["jobId"] = job_id
            router.emit(fallback)
        return response

    # -- events ----------------------------------------------------------------------------
    @staticmethod
    def _event_base(plan: Dict[str, Any], decision: Optional[Dict[str, Any]], latency_ms: float) -> Optional[Dict[str, Any]]:
        decision_id = decision.get("decisionId") if isinstance(decision, dict) else None
        if not isinstance(decision_id, str) or not decision_id:
            return None
        event: Dict[str, Any] = {
            "decisionId": decision_id[:64],
            "requestId": plan["request"]["requestId"],
            "decideLatencyMs": max(0.0, min(latency_ms, 600000.0)),
        }
        job_id = plan["request"].get("jobId")
        if job_id:
            event["jobId"] = job_id
        return event

    def _after_submit(self, router: Router, kind: str, event: Dict[str, Any], response: Any) -> None:
        try:
            if not isinstance(response, dict):
                router.emit({**event, "state": "submitted"})
                return
            job_id = (response.get("jobReference") or {}).get("jobId")
            if not job_id and kind == "query":
                # jobs.query with optional job creation: no job, and INFORMATION_SCHEMA.JOBS lists the run under its queryId.
                job_id = response.get("queryId")
            if isinstance(job_id, str) and job_id:
                event = {**event, "jobId": job_id[:256]}
            final = _final_state(kind, response)
            if final is not None:
                router.emit({**event, **final})
                return
            router.emit({**event, "state": "submitted"})
            if isinstance(job_id, str) and job_id:
                self.pending.add(job_id, event)
        except Exception:  # noqa: BLE001
            logger.debug("slashpeak event skipped", extra={"code": "event_error"})

    def _observe(self, router: Router, args: Tuple[Any, ...], kwargs: Dict[str, Any], response: Any) -> None:
        """A GET the library made anyway: report a routed job's final state."""
        try:
            if str(_arg(args, kwargs, "method") or "").upper() != "GET" or not isinstance(response, dict):
                return
            path = _arg(args, kwargs, "path")
            if not isinstance(path, str):
                return
            match = _JOB_GET.match(path)
            kind = "insert"
            if not match:
                match = _RESULTS_GET.match(path)
                kind = "results"
            if not match:
                return
            final = _final_state(kind, response)
            if final is None:
                return
            event = self.pending.pop(match.group(1))
            if event is not None:
                router.emit({**event, **final})
        except Exception:  # noqa: BLE001
            logger.debug("slashpeak event skipped", extra={"code": "observe_error"})


def _refused_reservation(changed: Dict[str, Any], response: Any) -> bool:
    """jobs.insert answered 200 with a job that failed at once because of the reservation we set.

    BigQuery makes such a job when the reservation is not found (checked 2026-11-03,
    plans/JOB_ROUTER_SPIKES.md spike 1): state DONE, an error naming the reservation,
    and no query statistics, so nothing ran or billed. The message is read here, never
    sent or logged.
    """
    if not changed.get("configuration", {}).get("reservation") or not isinstance(response, dict):
        return False
    status = response.get("status")
    if not isinstance(status, dict) or status.get("state") != "DONE":
        return False
    error = status.get("errorResult")
    if not isinstance(error, dict) or error.get("reason") != "invalid":
        return False
    if not re.search(r"\breservation\b", str(error.get("message") or ""), re.IGNORECASE):
        return False
    stats = response.get("statistics") or {}
    query_stats = stats.get("query") if isinstance(stats, dict) else None
    return not (isinstance(query_stats, dict) and (query_stats.get("totalBytesProcessed") or query_stats.get("totalSlotMs")))


def _final_state(kind: str, response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """done / failed from a response, or None while the job still runs."""
    if kind == "insert":
        status = response.get("status")
        if not isinstance(status, dict) or status.get("state") != "DONE":
            return None
        out: Dict[str, Any] = {"state": "done"}
        error = status.get("errorResult")
        if isinstance(error, dict):
            out = {"state": "failed", "errorReason": _REASON.sub("", str(error.get("reason") or "error"))[:64]}
        observed = (response.get("statistics") or {}).get("reservation_id")
        if isinstance(observed, str) and observed:
            out["observedReservation"] = observed[:512]
        return out
    # jobs.query and getQueryResults: `jobComplete`; a failed query raises in the library instead.
    if response.get("jobComplete") is True:
        return {"state": "done"}
    return None
