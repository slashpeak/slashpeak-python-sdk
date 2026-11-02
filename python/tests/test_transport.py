"""Connection behaviour against a real local HTTP/1.1 server."""

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from slashpeak.bigquery import transport as transport_module
from slashpeak.bigquery.transport import DECIDE_DEADLINE_S, MAX_RESPONSE_BYTES, Transport


class Server:
    def __init__(self):
        self.hits = []
        self.connections = 0
        self.sleep = 0.0
        self.status = 200
        self.body = b'{"ok":true}'
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"  # keep-alive

            def log_message(self, *args):
                pass

            def setup(self):
                # One handler per TCP connection: this counts connections, not requests.
                super().setup()
                outer.connections += 1

            def _reply(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                outer.hits.append({"path": self.path, "headers": dict(self.headers), "body": body})
                if outer.sleep:
                    time.sleep(outer.sleep)
                self.send_response(outer.status)
                if outer.status in (301, 302):
                    self.send_header("Location", "http://127.0.0.1:1/elsewhere")
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(outer.body)))
                self.send_header("Set-Cookie", "track=1")
                self.end_headers()
                self.wfile.write(outer.body)

            do_GET = _reply
            do_POST = _reply

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


@pytest.fixture
def server():
    s = Server()
    yield s
    s.close()


def test_keep_alive_reuses_one_connection(server):
    t = Transport(server.url, "secret-key")
    for _ in range(10):
        assert t.request("GET", "/v1/health").status == 200
    assert len(server.hits) == 10
    assert server.connections == 1  # one TCP + TLS handshake for all calls
    t.close()


def test_decide_pool_reuses_connections(server):
    t = Transport(server.url, "k")
    for _ in range(10):
        assert t.request_with_deadline("POST", "/v1/decide", {"a": 1}).status == 200
    assert server.connections <= transport_module.DECIDE_WORKERS
    t.close()


def test_key_only_in_the_header_and_no_cookies(server):
    t = Transport(server.url, "secret-key")
    t.request("GET", "/v1/config")
    t.request("GET", "/v1/config")
    first, second = server.hits
    assert first["headers"]["Authorization"] == "Bearer secret-key"
    assert "secret-key" not in first["path"]
    assert "Cookie" not in second["headers"]
    assert second["headers"]["User-Agent"].startswith("slashpeak-bigquery-python/")
    t.close()


def test_deadline_bounds_a_slow_router(server):
    server.sleep = 2.0
    t = Transport(server.url, "k")
    started = time.monotonic()
    assert t.request_with_deadline("POST", "/v1/decide", {"a": 1}) is None
    assert time.monotonic() - started < DECIDE_DEADLINE_S + 0.2
    t.close()


def test_deadline_covers_a_stalled_connect(monkeypatch):
    # A stall before any socket exists (like slow DNS): only the pool deadline can end it.
    t = Transport("https://router.example.invalid", "k")
    monkeypatch.setattr(t, "request", lambda *a, **k: time.sleep(3))
    started = time.monotonic()
    assert t.request_with_deadline("POST", "/v1/decide", {}) is None
    assert time.monotonic() - started < DECIDE_DEADLINE_S + 0.2


def test_no_retries_on_5xx(server):
    server.status = 503
    t = Transport(server.url, "k")
    assert t.request("POST", "/v1/decide", {}).status == 503
    assert len(server.hits) == 1
    t.close()


def test_no_redirects(server):
    server.status = 302
    t = Transport(server.url, "k")
    assert t.request("GET", "/v1/config").status == 302
    assert len(server.hits) == 1
    t.close()


def test_big_response_is_dropped(server):
    server.body = b'{"x":"' + b"a" * MAX_RESPONSE_BYTES + b'"}'
    t = Transport(server.url, "k")
    reply = t.request("GET", "/v1/config")
    assert reply.status == 200 and reply.body is None
    t.close()


def test_json_body_is_sent(server):
    t = Transport(server.url, "k")
    t.request("POST", "/v1/events", {"events": [{"decisionId": "d", "state": "done"}]})
    assert json.loads(server.hits[0]["body"]) == {"events": [{"decisionId": "d", "state": "done"}]}
    assert server.hits[0]["headers"]["Content-Type"] == "application/json"
    t.close()


def test_closed_transport_fails_open(server):
    t = Transport(server.url, "k")
    t.close()
    assert t.request_with_deadline("POST", "/v1/decide", {}) is None


def test_new_session_after_pid_change(server, monkeypatch):
    t = Transport(server.url, "k")
    t.request("GET", "/v1/health")
    parent_session = t._session
    monkeypatch.setattr(os, "getpid", lambda: -42)  # as seen in a forked child
    t.request("GET", "/v1/health")
    assert t._session is not parent_session
    parent_session.close()
    t.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX only")
def test_real_fork_child_uses_its_own_connection(server):
    t = Transport(server.url, "k")
    t.request("GET", "/v1/health")
    assert server.connections == 1
    pid = os.fork()
    if pid == 0:  # child
        try:
            t.after_fork_in_child()
            ok = t.request("GET", "/v1/health").status == 200
            os._exit(0 if ok else 1)
        except BaseException:
            os._exit(2)
    _, status = os.waitpid(pid, 0)
    assert os.WEXITSTATUS(status) == 0
    assert server.connections == 2  # the child opened its own
    assert t.request("GET", "/v1/health").status == 200
    assert server.connections == 2  # the parent's socket still works after the fork
    t.close()
