import copy

import pytest

from conftest import RES, FakeConnection, FakeError, decision, insert_body
from slashpeak.bigquery.hook import Hook
from slashpeak._core.settings import Settings
from slashpeak._core.transport import Reply


def run(router, conn, method="POST", path="/projects/p/jobs", data=None):
    hook = Hook(lambda: router)
    original = FakeConnection.api_request
    return hook, hook.handle(original, conn, (), {"method": method, "path": path, "data": data})


def events(router):
    out = []
    while not router._events.empty():
        out.append(router._events.get_nowait())
    return out


def test_active_move_sets_reservation_and_label(router, transport):
    transport.decide_replies = [decision("set_reservation", RES, label="m_d1-x")]
    conn = FakeConnection()
    body = insert_body()
    before = copy.deepcopy(body)
    run(router, conn, data=body)
    sent = conn.sent[0]["data"]
    assert sent["configuration"]["reservation"] == RES
    assert sent["configuration"]["labels"]["slashpeak_route"] == "m_d1-x"
    assert body == before  # the caller's dict is never changed
    assert [e["state"] for e in events(router)] == ["submitted"]


def test_shadow_label_only(router, transport):
    transport.decide_replies = [decision("keep", mode="shadow", label="s_d1-x")]
    conn = FakeConnection()
    run(router, conn, data=insert_body())
    sent = conn.sent[0]["data"]["configuration"]
    assert "reservation" not in sent and sent["labels"] == {"slashpeak_route": "s_d1-x"}


def test_sdk_shadow_never_moves_and_drops_move_label(transport):
    from slashpeak._core.router import Router

    router = Router(Settings(api_key="k", endpoint="https://x", mode="shadow"), transport=transport)
    router.start = lambda: None
    transport.decide_replies = [decision("set_reservation", RES, label="m_d1-x")]
    conn = FakeConnection()
    run(router, conn, data=insert_body())
    assert conn.sent[0]["data"] == insert_body()


@pytest.mark.parametrize(
    "body",
    [
        insert_body(reservation="projects/a/locations/US/reservations/mine"),
        insert_body(dryRun=True),
        {"jobReference": {"jobId": "j"}, "configuration": {"load": {}}},
        insert_body("/* slashpeak:off */ SELECT 1"),
    ],
    ids=["explicit-reservation", "dry-run", "load-job", "opt-out"],
)
def test_untouched_jobs_skip_the_router(router, transport, body):
    conn = FakeConnection()
    run(router, conn, data=body)
    assert transport.decides() == []
    assert conn.sent[0]["data"] == body


def test_job_id_set_when_missing(router, transport):
    transport.decide_replies = [None]
    body = insert_body()
    del body["jobReference"]["jobId"]
    conn = FakeConnection()
    run(router, conn, data=body)
    job_id = conn.sent[0]["data"]["jobReference"]["jobId"]
    assert job_id.startswith("slashpeak_")
    assert transport.decides()[0]["body"]["jobId"] == job_id


def test_no_answer_runs_original(router, transport):
    transport.decide_replies = [None]
    conn = FakeConnection()
    run(router, conn, data=insert_body())
    assert conn.sent[0]["data"] == insert_body()
    assert events(router) == []


def test_4xx_on_changed_request_falls_back_once_with_same_job_id(router, transport):
    transport.decide_replies = [decision("set_reservation", RES, label="m_d1-x")]
    conn = FakeConnection()
    conn.fail_with = [FakeError(403, "accessDenied")]
    run(router, conn, data=insert_body())
    assert len(conn.sent) == 2
    assert conn.sent[1]["data"] == insert_body()
    assert conn.sent[0]["data"]["jobReference"]["jobId"] == conn.sent[1]["data"]["jobReference"]["jobId"]
    (event,) = events(router)
    assert event["state"] == "fallback" and event["errorReason"] == "accessDenied"
    assert "ann@example.com" not in str(event)


def _refused_job(body, message="User specified reservation projects/admin/locations/US/reservations/etl is not found."):
    """What BigQuery answers (200) for a reservation it cannot find (spike 1, 2026-11-03)."""
    return {
        "jobReference": body["jobReference"],
        "status": {"state": "DONE", "errorResult": {"reason": "invalid", "message": message}},
        "statistics": {"creationTime": "1", "startTime": "2", "endTime": "2", "query": {}},
    }


def test_job_failed_for_missing_reservation_reruns_original_under_new_job_id(router, transport):
    transport.decide_replies = [decision("set_reservation", RES, label="m_d1-x")]
    conn = FakeConnection()
    conn.responses = [_refused_job(insert_body())]
    _, response = run(router, conn, data=insert_body())
    assert len(conn.sent) == 2
    retry = conn.sent[1]["data"]
    assert "reservation" not in retry["configuration"] and "labels" not in retry["configuration"]
    assert retry["jobReference"]["jobId"].startswith("job_1_fb")
    assert response["jobReference"]["jobId"] == retry["jobReference"]["jobId"]
    (event,) = events(router)
    assert event["state"] == "fallback" and event["errorReason"] == "reservation_refused"
    assert event["jobId"] == retry["jobReference"]["jobId"]


@pytest.mark.parametrize(
    "response",
    [
        # The query ran and failed on its own (a SQL error): never re-run, it could bill twice.
        {"status": {"state": "DONE", "errorResult": {"reason": "invalidQuery", "message": "Syntax error"}}, "statistics": {}},
        # An error that names the reservation but the query ran (statistics filled).
        {"status": {"state": "DONE", "errorResult": {"reason": "invalid", "message": "reservation busy"}}, "statistics": {"query": {"totalBytesProcessed": "10"}}},
        # Other refusals that do not name the reservation (for example max_slots) are left alone.
        {"status": {"state": "DONE", "errorResult": {"reason": "invalid", "message": "max_slots field is not yet supported."}}, "statistics": {}},
    ],
    ids=["sql-error", "ran", "other-invalid"],
)
def test_other_failed_jobs_are_not_rerun(router, transport, response):
    transport.decide_replies = [decision("set_reservation", RES, label="m_d1-x")]
    conn = FakeConnection()
    conn.responses = [{"jobReference": insert_body()["jobReference"], **response}]
    run(router, conn, data=insert_body())
    assert len(conn.sent) == 1
    assert [e["state"] for e in events(router)] == ["failed"]


def test_refused_reservation_on_unchanged_request_is_left_alone(router, transport):
    transport.decide_replies = [decision("keep")]
    conn = FakeConnection()
    conn.responses = [_refused_job(insert_body())]
    run(router, conn, data=insert_body())
    assert len(conn.sent) == 1


def test_query_without_job_reports_query_id(router, transport):
    transport.decide_replies = [decision("keep", mode="shadow", label="s_d1-x")]
    conn = FakeConnection()
    conn.responses = [{"queryId": "abc%1a1a22f7106", "jobComplete": True}]
    run(router, conn, path="/projects/p/queries", data={"query": "SELECT 1", "location": "US", "jobCreationMode": "JOB_CREATION_OPTIONAL"})
    (event,) = events(router)
    assert event["state"] == "done" and event["jobId"] == "abc%1a1a22f7106"


@pytest.mark.parametrize("code", [409, 429, 500, 503])
def test_no_fallback_on_conflict_rate_limit_or_5xx(router, transport, code):
    transport.decide_replies = [decision("set_reservation", RES, label="m_d1-x")]
    conn = FakeConnection()
    conn.fail_with = [FakeError(code)]
    with pytest.raises(FakeError):
        run(router, conn, data=insert_body())
    assert len(conn.sent) == 1
    assert [e["state"] for e in events(router)] == ["insert_error"]


def test_4xx_on_unchanged_request_is_raised(router, transport):
    transport.decide_replies = [decision("keep")]
    conn = FakeConnection()
    conn.fail_with = [FakeError(400)]
    with pytest.raises(FakeError):
        run(router, conn, data=insert_body())
    assert len(conn.sent) == 1


def test_query_path_sets_top_level_fields_and_new_request_id_on_fallback(router, transport):
    transport.decide_replies = [decision("set_reservation", RES, label="m_d1-x")]
    conn = FakeConnection()
    conn.fail_with = [FakeError(403, "accessDenied", "Access Denied: Reservation projects/admin/locations/US/reservations/etl")]
    body = {"query": "SELECT 1", "requestId": "req-1", "location": "US"}
    run(router, conn, path="/projects/p/queries", data=body)
    assert conn.sent[0]["data"]["reservation"] == RES
    assert conn.sent[0]["data"]["labels"] == {"slashpeak_route": "m_d1-x"}
    assert "reservation" not in conn.sent[1]["data"]
    assert conn.sent[1]["data"]["requestId"] != "req-1"
    (event,) = events(router)
    assert event["state"] == "fallback" and event["requestId"] == "req-1" and event["jobId"] == "q1"


def test_label_not_added_when_64_labels(router, transport):
    transport.decide_replies = [decision("keep", mode="shadow", label="s_d1-x")]
    labels = {f"k{i}": "v" for i in range(64)}
    conn = FakeConnection()
    run(router, conn, data=insert_body(labels=labels))
    assert conn.sent[0]["data"]["configuration"]["labels"] == labels


def test_bad_reservation_or_label_is_ignored(router, transport):
    transport.decide_replies = [decision("set_reservation", "none", label="M_BAD LABEL")]
    conn = FakeConnection()
    run(router, conn, data=insert_body())
    assert conn.sent[0]["data"] == insert_body()


def test_done_state_from_a_get_the_library_makes(router, transport):
    transport.decide_replies = [decision("set_reservation", RES, label="m_d1-x")]
    conn = FakeConnection()
    hook, _ = run(router, conn, data=insert_body())
    assert [e["state"] for e in events(router)] == ["submitted"]
    done = {"status": {"state": "DONE"}, "statistics": {"reservation_id": "admin:US.etl"}}
    conn.responses = [done]
    hook.handle(FakeConnection.api_request, conn, (), {"method": "GET", "path": "/projects/p/jobs/job_1"})
    (event,) = events(router)
    assert event == {**event, "state": "done", "observedReservation": "admin:US.etl", "jobId": "job_1"}
    # Reported once.
    conn.responses = [done]
    hook.handle(FakeConnection.api_request, conn, (), {"method": "GET", "path": "/projects/p/jobs/job_1"})
    assert events(router) == []


def test_failed_state(router, transport):
    transport.decide_replies = [decision("keep", mode="shadow", label="s_d1-x")]
    conn = FakeConnection()
    conn.responses = [{"jobReference": {"jobId": "job_1"}, "status": {"state": "DONE", "errorResult": {"reason": "invalid query!"}}}]
    run(router, conn, data=insert_body())
    (event,) = events(router)
    assert event["state"] == "failed" and event["errorReason"] == "invalidquery"


def test_kill_switch_decision_has_no_events(router, transport):
    transport.decide_replies = [decision("keep", decision_id=None, scope="none")]
    conn = FakeConnection()
    run(router, conn, data=insert_body())
    assert events(router) == []


def test_positional_arguments_are_kept(router, transport):
    transport.decide_replies = [decision("set_reservation", RES)]
    conn = FakeConnection()
    hook = Hook(lambda: router)
    hook.handle(FakeConnection.api_request, conn, ("POST", "/projects/p/jobs", None, insert_body()), {})
    assert conn.sent[0]["data"]["configuration"]["reservation"] == RES


def test_router_error_never_breaks_the_job(router, transport, monkeypatch):
    monkeypatch.setattr(router, "decide", lambda *a: (_ for _ in ()).throw(RuntimeError("boom")))
    conn = FakeConnection()
    run(router, conn, data=insert_body())
    assert conn.sent[0]["data"] == insert_body()


def test_bypass_skips_the_hook(router, transport):
    import slashpeak.bigquery as sdk

    transport.decide_replies = [decision("set_reservation", RES)]
    conn = FakeConnection()
    with sdk.bypass():
        run(router, conn, data=insert_body())
    assert transport.decides() == []


def test_cache_reuses_a_decision(router, transport):
    transport.default_decide = decision("set_reservation", RES, label="m_d1-x")
    conn = FakeConnection()
    run(router, conn, data=insert_body())
    run(router, conn, data=insert_body())
    assert len(transport.decides()) == 1
    assert conn.sent[1]["data"]["configuration"]["reservation"] == RES


def test_install_patches_the_real_connection(monkeypatch):
    pytest.importorskip("google.cloud.bigquery")
    import slashpeak.bigquery as sdk
    from google.cloud.bigquery import _http

    before = _http.Connection.api_request
    assert sdk.install(Settings(api_key="k", endpoint="https://x")) is True
    assert getattr(_http.Connection.api_request, "_slashpeak", False)
    assert sdk.install(Settings(api_key="k", endpoint="https://x")) is True  # twice is fine
    assert _http.Connection.api_request.__wrapped__ is before
    sdk.uninstall()
    assert _http.Connection.api_request is before


def test_install_without_key_does_nothing():
    import slashpeak.bigquery as sdk

    assert sdk.install(Settings(api_key=None)) is False
    assert sdk.install(Settings(api_key="k", endpoint="http://evil.example.com")) is False
    sdk.uninstall()


def test_fork_child_gets_fresh_locks():
    import slashpeak.bigquery as sdk
    from slashpeak._core import runtime

    sdk.install(Settings(api_key="k", endpoint="https://x"))
    old_pending, old_router = sdk._hook.pending, runtime.current()
    old_pending._lock.acquire()  # as if another thread held it at fork time
    try:
        runtime._after_fork_in_child()
        assert sdk._hook.pending is not old_pending
        sdk._hook.pending.add("j", {"decisionId": "d"})  # would hang on the old lock
        assert runtime.current() is old_router and old_router._threads_pid == -1
    finally:
        old_pending._lock.release()
        sdk.uninstall()


@pytest.mark.parametrize(
    "message",
    ["Division by zero: 1 / 0", "Access Denied: Table p:d.t", "Syntax error: Unexpected keyword"],
)
def test_query_path_never_reruns_a_query_that_ran(router, transport, message):
    # jobs.query returns a 4xx for a query that ran and failed; a re-run would bill it twice.
    transport.decide_replies = [decision("set_reservation", RES, label="m_d1-x")]
    conn = FakeConnection()
    conn.fail_with = [FakeError(400, "invalidQuery", message)]
    with pytest.raises(FakeError):
        run(router, conn, path="/projects/p/queries", data={"query": "SELECT 1/0", "requestId": "r", "location": "US"})
    assert len(conn.sent) == 1
    (event,) = events(router)
    assert event["state"] == "insert_error" and "Division" not in str(event)


def test_top_level_install_hooks_every_installed_library():
    pytest.importorskip("google.cloud.bigquery")
    import slashpeak
    from google.cloud.bigquery import _http

    assert slashpeak.install(Settings(api_key="k", endpoint="https://x")) == ["bigquery"]
    assert getattr(_http.Connection.api_request, "_slashpeak", False)
    slashpeak.uninstall()
    assert not getattr(_http.Connection.api_request, "_slashpeak", False)
    assert slashpeak.install(Settings(api_key=None)) == []
    slashpeak.uninstall()


def test_hooks_share_one_router_until_the_last_leaves():
    from slashpeak._core import runtime

    first = runtime.acquire("bigquery", Settings(api_key="k", endpoint="https://x"))
    assert runtime.acquire("snowflake") is first  # same pool, config poll and event queue
    runtime.release("bigquery")
    assert runtime.current() is first and not first._closed
    runtime.release("snowflake")
    assert runtime.current() is None and first._closed


def test_missing_library_is_skipped_not_an_error():
    import slashpeak

    assert slashpeak._has("no_such_parent.child") is False
    assert slashpeak._has("json") is True


def test_same_settings_keep_the_router_new_settings_replace_it():
    from slashpeak._core import runtime

    settings = Settings(api_key="k", endpoint="https://x")
    first = runtime.acquire("bigquery", settings)
    assert runtime.acquire("snowflake", Settings(api_key="k", endpoint="https://x")) is first
    second = runtime.acquire("snowflake", Settings(api_key="k2", endpoint="https://x"))
    assert second is not first and first._closed
    runtime.release("bigquery")
    runtime.release("snowflake")
    assert runtime.current() is None


def _on_demand(cap="20971520"):
    reply = decision("set_reservation", "none", label="m_d1-x")
    if cap is not None:
        reply.body["maximumBytesBilled"] = cap
    return reply


def test_on_demand_move_sets_none_and_bytes_cap_on_jobs_query_only(router, transport):
    transport.decide_replies = [_on_demand(), _on_demand()]
    conn = FakeConnection()
    run(router, conn, path="/projects/p/queries", data={"query": "SELECT 1", "requestId": "r1", "location": "US"})
    sent = conn.sent[0]["data"]
    assert sent["reservation"] == "none" and sent["maximumBytesBilled"] == "20971520"
    assert transport.decides()[0]["body"]["client"]["capabilities"] == ["rerun_bytes_cap"]
    # jobs.insert never moves to on-demand and never says it can re-run.
    conn = FakeConnection()
    run(router, conn, data=insert_body("SELECT 2"))
    assert conn.sent[0]["data"] == insert_body("SELECT 2")
    assert "capabilities" not in transport.decides()[-1]["body"]["client"]


def test_on_demand_move_without_a_cap_keeps_the_job(router, transport):
    transport.decide_replies = [_on_demand(cap=None)]
    conn = FakeConnection()
    body = {"query": "SELECT 1", "requestId": "r1", "location": "US"}
    run(router, conn, path="/projects/p/queries", data=body)
    assert conn.sent[0]["data"]["requestId"] == "r1" and "reservation" not in conn.sent[0]["data"]


def test_on_demand_move_keeps_the_callers_own_cap(router, transport):
    transport.decide_replies = [_on_demand()]
    conn = FakeConnection()
    run(router, conn, path="/projects/p/queries", data={"query": "SELECT 1", "requestId": "r1", "maximumBytesBilled": "5"})
    assert conn.sent[0]["data"]["maximumBytesBilled"] == "5"


def test_our_bytes_cap_stop_reruns_the_original_once(router, transport):
    transport.decide_replies = [_on_demand()]
    conn = FakeConnection()
    conn.fail_with = [FakeError(400, "bytesBilledLimitExceeded", "Query exceeded limit for bytes billed")]
    run(router, conn, path="/projects/p/queries", data={"query": "SELECT 1", "requestId": "r1", "location": "US"})
    assert len(conn.sent) == 2
    assert "reservation" not in conn.sent[1]["data"] and "maximumBytesBilled" not in conn.sent[1]["data"]
    (event,) = events(router)
    assert event["state"] == "fallback" and event["errorReason"] == "bytesBilledLimitExceeded"


def test_callers_own_cap_stop_is_not_rerun(router, transport):
    transport.decide_replies = [_on_demand()]
    conn = FakeConnection()
    conn.fail_with = [FakeError(400, "bytesBilledLimitExceeded", "Query exceeded limit for bytes billed")]
    with pytest.raises(FakeError):
        run(router, conn, path="/projects/p/queries", data={"query": "SELECT 1", "requestId": "r1", "maximumBytesBilled": "5"})
    assert len(conn.sent) == 1
