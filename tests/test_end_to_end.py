"""The real google-cloud-bigquery client → our hook → a local router and a local BigQuery."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

bigquery = pytest.importorskip("google.cloud.bigquery")
from google.api_core.client_options import ClientOptions  # noqa: E402
from google.auth.credentials import AnonymousCredentials  # noqa: E402

import slashpeak.bigquery as sdk  # noqa: E402
from slashpeak._core import runtime  # noqa: E402
from slashpeak._core.settings import Settings  # noqa: E402

RES = "projects/admin/locations/US/reservations/etl"


def serve(handle):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _do(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length)) if length else None
            status, reply = handle(self.command, self.path.split("?")[0], body)
            data = json.dumps(reply).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = _do
        do_POST = _do

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.daemon_threads = True
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


@pytest.fixture
def world():
    state = {"router": [], "bq": [], "reject_reservation": False}

    def router(method, path, body):
        state["router"].append((method, path, body))
        if path == "/v1/config":
            return 200, {"policies": [{"projects": [], "mode": "active", "sendParamValues": True}], "pollSeconds": 60}
        if path == "/v1/decide":
            return 200, {
                "decisionId": "d1-k1-abc-001122334455-aabbccddeeff",
                "action": "set_reservation",
                "reservation": RES,
                "labels": {"slashpeak_route": "m_d1-k1-abc-001122334455-aabbccddeeff"},
                "ttlSeconds": 60,
                "cacheScope": "sql",
                "mode": "active",
                "explanation": {},
            }
        return 200, {"accepted": 1}

    def bq(method, path, body):
        state["bq"].append((method, path, body))
        if method == "POST" and path.endswith("/jobs"):
            if state["reject_reservation"] and body["configuration"].get("reservation"):
                return 403, {"error": {"code": 403, "message": "no", "errors": [{"reason": "accessDenied", "message": "no"}]}}
            return 200, {**body, "status": {"state": "DONE"}, "statistics": {"reservation_id": "admin:US.etl"}}
        if method == "POST" and path.endswith("/queries"):
            return 200, {"jobComplete": True, "totalRows": "0", "rows": [], "schema": {"fields": []},
                         "jobReference": {"projectId": "p", "jobId": "qj1", "location": "US"}}
        if method == "GET" and "/jobs/" in path:
            job_id = path.split("/jobs/")[1].split("?")[0]
            return 200, {
                "jobReference": {"projectId": "p", "jobId": job_id, "location": "US"},
                "configuration": {"query": {"query": "SELECT 1"}},
                "status": {"state": "DONE"},
            }
        if method == "GET" and "/queries/" in path:
            return 200, {"jobComplete": True, "totalRows": "0", "rows": [], "schema": {"fields": []}, "jobReference": {"projectId": "p", "jobId": "x"}}
        return 404, {"error": {"code": 404, "message": "nope"}}

    router_httpd, router_url = serve(router)
    bq_httpd, bq_url = serve(bq)
    sdk.install(Settings(api_key="k", endpoint=router_url))
    client = bigquery.Client(project="p", credentials=AnonymousCredentials(), client_options=ClientOptions(api_endpoint=bq_url))
    yield state, client
    sdk.uninstall()
    router_httpd.shutdown()
    bq_httpd.shutdown()


def inserts(state):
    return [body for method, path, body in state["bq"] if method == "POST" and path.endswith("/jobs")]


def test_client_query_is_routed(world):
    state, client = world
    client.query("SELECT email FROM t WHERE email = 'ann@example.com'", location="US").result()
    (insert,) = inserts(state)
    assert insert["configuration"]["reservation"] == RES
    assert insert["configuration"]["labels"]["slashpeak_route"].startswith("m_")
    decide = [body for _, path, body in state["router"] if path == "/v1/decide"][0]
    assert "ann@example.com" not in json.dumps(decide)
    assert decide["jobId"] == insert["jobReference"]["jobId"]


def test_fallback_runs_the_original_job(world):
    state, client = world
    state["reject_reservation"] = True
    client.query("SELECT 2", location="US").result()
    first, second = inserts(state)
    assert first["configuration"]["reservation"] == RES
    assert "reservation" not in second["configuration"]
    assert first["jobReference"]["jobId"] == second["jobReference"]["jobId"]
    runtime.current().flush_events(1.0)
    sent = [e for _, path, body in state["router"] if path == "/v1/events" for e in body["events"]]
    assert [e["state"] for e in sent] == ["fallback"]


def test_explicit_reservation_is_never_overridden(world):
    state, client = world
    config = bigquery.QueryJobConfig()
    config._properties["reservation"] = "projects/me/locations/US/reservations/mine"
    client.query("SELECT 3", location="US", job_config=config).result()
    (insert,) = inserts(state)
    assert insert["configuration"]["reservation"] == "projects/me/locations/US/reservations/mine"
    assert not [p for _, p, _ in state["router"] if p == "/v1/decide"]


def test_query_and_wait_is_routed(world):
    state, client = world
    if not hasattr(client, "query_and_wait"):
        pytest.skip("query_and_wait needs google-cloud-bigquery 3.15+")
    client.query_and_wait("SELECT 4", location="US")
    (body,) = [b for m, p, b in state["bq"] if m == "POST" and p.endswith("/queries")]
    assert body["reservation"] == RES and body["labels"]["slashpeak_route"].startswith("m_")
    runtime.current().flush_events(1.0)
    sent = [e for _, path, b in state["router"] if path == "/v1/events" for e in b["events"]]
    assert [(e["state"], e.get("jobId")) for e in sent] == [("done", "qj1")]
