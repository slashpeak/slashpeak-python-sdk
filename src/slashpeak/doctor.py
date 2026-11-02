"""``slashpeak doctor``: checks the key, the network path and reservation access.

Prints a fix for each problem. Never prints the key, SQL or values.
"""

from __future__ import annotations

import re
import statistics
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from slashpeak._version import __version__
from slashpeak._core.router import client_at_least
from slashpeak._core.settings import Settings, endpoint_is_safe, load_settings
from slashpeak._core.transport import Transport

RESERVATION_API = "https://bigqueryreservation.googleapis.com/v1"
USE_PERMISSION = "bigquery.reservations.use"
LATENCY_BUDGET_MS = 320.0
_REF = re.compile(r"^(.+):([^:.]+)\.([^.]+)$")


class Report:
    def __init__(self, out: Callable[[str], None]) -> None:
        self.out = out
        self.problems = 0

    def ok(self, text: str) -> None:
        self.out(f"  ok    {text}")

    def bad(self, text: str, fix: str) -> None:
        self.problems += 1
        self.out(f"  FAIL  {text}")
        self.out(f"        fix: {fix}")

    def note(self, text: str) -> None:
        self.out(f"  note  {text}")


def reservation_name(ref: str) -> Optional[str]:
    """``project:location.name`` → ``projects/{p}/locations/{l}/reservations/{r}``."""
    match = _REF.match(ref.strip())
    if not match:
        return None
    return f"projects/{match.group(1)}/locations/{match.group(2)}/reservations/{match.group(3)}"


def _timed(call: Callable[[], Any]) -> Tuple[Any, float]:
    started = time.monotonic()
    result = call()
    return result, (time.monotonic() - started) * 1000.0


def _iam_check(names: List[str], report: Report, session_factory: Optional[Callable[[], Any]]) -> None:
    try:
        if session_factory is not None:
            session, principal = session_factory()
        else:
            import google.auth
            from google.auth.transport.requests import AuthorizedSession

            credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            session = AuthorizedSession(credentials)
            principal = getattr(credentials, "service_account_email", None) or "the identity your jobs run as"
    except Exception as error:  # noqa: BLE001
        report.bad(
            f"Google credentials not found ({type(error).__name__})",
            "run this where your jobs run, or run `gcloud auth application-default login`",
        )
        return
    for name in names:
        try:
            response = session.post(
                f"{RESERVATION_API}/{name}:testIamPermissions",
                json={"permissions": [USE_PERMISSION]},
                timeout=(5, 10),
            )
            granted = response.status_code == 200 and USE_PERMISSION in (response.json().get("permissions") or [])
        except Exception:  # noqa: BLE001
            granted = False
            response = None
        if granted:
            report.ok(f"{principal} may use {name}")
        else:
            status = getattr(response, "status_code", "no answer")
            report.bad(
                f"{principal} may not use {name} (HTTP {status})",
                f"grant roles/bigquery.user to {principal} on the reservation itself "
                "(BigQuery > Capacity management > Reservations > the reservation > Permissions). "
                "roles/bigquery.jobUser does not include bigquery.reservations.use.",
            )


def run_doctor(
    settings: Optional[Settings] = None,
    latency: bool = False,
    out: Callable[[str], None] = print,
    transport: Optional[Transport] = None,
    iam_session_factory: Optional[Callable[[], Any]] = None,
) -> int:
    """Exit code: 0 when every check passed, 1 otherwise."""
    settings = settings or load_settings()
    report = Report(out)
    out(f"slashpeak {__version__}")

    try:
        from google.cloud.bigquery import _http
    except ImportError:
        report.bad("no supported warehouse library installed", "pip install 'slashpeak[bigquery]'")
    else:
        if callable(getattr(_http.Connection, "api_request", None)):
            report.ok("google-cloud-bigquery installed; hook point found")
        else:
            report.bad("google-cloud-bigquery has no Connection.api_request", "upgrade google-cloud-bigquery to 3.x")

    if not settings.api_key:
        report.bad("SLASHPEAK_API_KEY is not set", "create a router key on the Router page and set SLASHPEAK_API_KEY")
        return 1
    report.ok("SLASHPEAK_API_KEY is set")
    if not endpoint_is_safe(settings.endpoint):
        report.bad("SLASHPEAK_ENDPOINT is not HTTPS", "use https://router.slashpeak.com")
        return 1
    if settings.mode == "off":
        report.note("SLASHPEAK_MODE=off: the SDK will not call the router")

    transport = transport or Transport(settings.endpoint, settings.api_key)
    try:
        try:
            health, ms = _timed(lambda: transport.request("GET", "/v1/health"))
            if health.status == 200:
                report.ok(f"router reachable ({ms:.0f} ms, first call includes TLS)")
            else:
                report.bad(f"router answered HTTP {health.status}", "check proxies and firewalls for router.slashpeak.com")
        except Exception as error:  # noqa: BLE001
            report.bad(
                f"router not reachable ({type(error).__name__})",
                "allow HTTPS egress to router.slashpeak.com (Cloud NAT for private-IP Composer); set HTTPS_PROXY if you use one",
            )
            return 1

        config_reply = transport.request("GET", "/v1/config")
        if config_reply.status != 200 or config_reply.body is None:
            code = ((config_reply.body or {}).get("error") or {}).get("code") if config_reply.body else None
            report.bad(f"key refused (HTTP {config_reply.status}{', ' + code if code else ''})", "create a new router key; read-only API keys do not work here")
            return 1
        config: Dict[str, Any] = config_reply.body
        report.ok("key accepted")
        if config.get("killSwitch"):
            report.note("the router is paused for everyone right now (jobs run unchanged)")
        minimum = (config.get("minClient") or {}).get("python")
        if not client_at_least(__version__, minimum):
            report.bad(f"this SDK is older than the minimum {minimum}", "pip install -U slashpeak")
        policies = [p for p in config.get("policies") or [] if isinstance(p, dict)]
        if not policies:
            report.note("no router settings yet: open the Router page and pick shadow or active")
        for policy in policies:
            projects = policy.get("projects") or []
            scope = ", ".join(projects) if projects else "all projects of the connection"
            report.note(f"mode {policy.get('mode')} for {scope}")

        refs = sorted({ref for p in policies for ref in p.get("allowedReservations") or [] if isinstance(ref, str)})
        names = [name for name in (reservation_name(ref) for ref in refs) if name]
        if names:
            _iam_check(names, report, iam_session_factory)
        elif any(p.get("mode") == "active" for p in policies):
            report.bad("active mode with no allowed reservation", "pick a reservation on the Router page")

        if latency:
            samples = []
            for _ in range(20):
                try:
                    _, ms = _timed(lambda: transport.request("GET", "/v1/health"))
                    samples.append(ms)
                except Exception:  # noqa: BLE001
                    pass
            if len(samples) < 2:
                report.bad("latency check failed (no answers)", "check the network path to router.slashpeak.com")
            else:
                samples.sort()
                p95 = samples[min(len(samples) - 1, int(round(0.95 * (len(samples) - 1))))]
                text = f"warm round trip p50 {statistics.median(samples):.0f} ms, p95 {p95:.0f} ms ({len(samples)} calls)"
                if p95 > LATENCY_BUDGET_MS:
                    report.bad(text, "run jobs closer to us-central1, or keep the router in shadow mode")
                else:
                    report.ok(text)
    finally:
        transport.close()

    out("all checks passed" if report.problems == 0 else f"{report.problems} problem(s) found")
    return 0 if report.problems == 0 else 1
