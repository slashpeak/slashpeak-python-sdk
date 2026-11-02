from __future__ import annotations

import json
import pathlib
from typing import Any, Dict, List, Optional

import pytest

from slashpeak._core.router import Router
from slashpeak._core.settings import Settings
from slashpeak._core.transport import Reply

SPEC = pathlib.Path(__file__).resolve().parents[1] / "spec"


def load_spec(name: str) -> Any:
    return json.loads((SPEC / name).read_text(encoding="utf-8"))


class FakeTransport:
    """Records calls; answers from a queue (or a function)."""

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []
        self.decide_replies: List[Optional[Reply]] = []
        self.default_decide: Optional[Reply] = None
        self.closed = False

    def request_with_deadline(self, method, path, body, deadline_s=0.5):
        self.calls.append({"method": method, "path": path, "body": body})
        if self.decide_replies:
            return self.decide_replies.pop(0)
        return self.default_decide

    def request(self, method, path, body=None, headers=None, timeout=(2.0, 5.0)):
        self.calls.append({"method": method, "path": path, "body": body, "headers": headers})
        return Reply(200, {"accepted": 0}, None)

    def after_fork_in_child(self):
        pass

    def close(self):
        self.closed = True

    def decides(self) -> List[Dict[str, Any]]:
        return [c for c in self.calls if c["path"] == "/v1/decide"]


def decision(action="keep", reservation="none", mode="active", label=None, decision_id="d1-k1-abc-0011223344-ffeeddccbbaa", ttl=60, scope="sql"):
    body: Dict[str, Any] = {
        "decisionId": decision_id,
        "action": action,
        "reservation": reservation,
        "ttlSeconds": ttl,
        "cacheScope": scope,
        "mode": mode,
        "explanation": {},
    }
    if label:
        body["labels"] = {"slashpeak_route": label}
    return Reply(200, body, None)


RES = "projects/admin/locations/US/reservations/etl"


class FakeError(Exception):
    def __init__(self, code: int, reason: str = "invalid", message: str = "") -> None:
        super().__init__(f"HTTP {code} {message}".strip())
        self.code = code
        self.errors = [{"reason": reason, "message": "secret value 'ann@example.com'"}]


class FakeConnection:
    """Stands in for google.cloud.bigquery._http.Connection."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []
        self.fail_with: List[Exception] = []
        self.responses: List[Any] = []

    def api_request(self, method, path, query_params=None, data=None, **kwargs):
        self.sent.append({"method": method, "path": path, "data": json.loads(json.dumps(data)) if data is not None else None})
        if self.fail_with:
            raise self.fail_with.pop(0)
        if self.responses:
            return self.responses.pop(0)
        if path.endswith("/jobs"):
            return {"jobReference": data["jobReference"], "status": {"state": "RUNNING"}}
        return {"jobReference": {"projectId": "p", "jobId": "q1"}, "jobComplete": False}


@pytest.fixture
def transport() -> FakeTransport:
    return FakeTransport()


@pytest.fixture
def router(transport: FakeTransport) -> Router:
    r = Router(Settings(api_key="k", endpoint="https://router.example.com"), transport=transport)  # type: ignore[arg-type]
    r.start = lambda: None  # type: ignore[method-assign]  # no background threads in unit tests
    yield r
    r._closed = True


def insert_body(sql="SELECT 1 FROM t", **config):
    body: Dict[str, Any] = {
        "jobReference": {"projectId": "p", "jobId": "job_1", "location": "US"},
        "configuration": {"query": {"query": sql, "useLegacySql": False}},
    }
    body["configuration"].update(config)
    return body
