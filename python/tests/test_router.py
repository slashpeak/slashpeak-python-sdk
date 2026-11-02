import threading

from conftest import decision
from slashpeak.bigquery.router import BREAKER_FAILURES, BREAKER_OPEN_S, Breaker, DecisionCache, Router, client_at_least
from slashpeak.bigquery.settings import Settings, load_settings, stricter_mode
from slashpeak.bigquery.transport import Reply


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def test_breaker_opens_then_probes_once():
    clock = Clock()
    b = Breaker(clock)
    for _ in range(BREAKER_FAILURES):
        assert b.allow()
        b.record(False)
    assert not b.allow()
    clock.t += BREAKER_OPEN_S
    assert b.allow()  # the probe
    assert not b.allow()  # only one probe at a time
    b.record(False)
    assert not b.allow()  # reopened
    clock.t += BREAKER_OPEN_S
    assert b.allow()
    b.record(True)
    assert b.allow() and b.allow()


def test_decide_trips_breaker(router, transport):
    transport.default_decide = None
    for i in range(BREAKER_FAILURES + 3):
        router.cache.clear()
        router.decide(f"k{i}", {"x": 1})
    assert len(transport.decides()) == BREAKER_FAILURES


def test_non_200_is_a_failure_and_not_cached(router, transport):
    transport.decide_replies = [Reply(429, {"error": {"code": "rate_limited"}}, None), decision()]
    assert router.decide("k", {}) is None
    assert router.decide("k", {})["action"] == "keep"


def test_cache_ttl_scope_and_cap():
    clock = Clock()
    cache = DecisionCache(clock)
    calls = []

    def fetch(value):
        def go():
            calls.append(1)
            return value
        return go

    body = decision(ttl=600).body
    cache.get_or_fetch("a", fetch(body), ttl_cap_s=60, wait_s=0.5)
    clock.t += 59
    cache.get_or_fetch("a", fetch(body), ttl_cap_s=60, wait_s=0.5)
    assert len(calls) == 1
    clock.t += 2  # capped at 60 s, not the 600 the reply asked for
    cache.get_or_fetch("a", fetch(body), ttl_cap_s=60, wait_s=0.5)
    assert len(calls) == 2
    for bad in (decision(scope="none").body, decision(ttl=0).body, decision(ttl=-5).body, None):
        calls.clear()
        cache.get_or_fetch("b", fetch(bad), 60, 0.5)
        cache.get_or_fetch("b", fetch(bad), 60, 0.5)
        assert len(calls) == 2


def test_cache_single_flight():
    cache = DecisionCache()
    gate = threading.Event()
    calls = []

    def slow():
        calls.append(1)
        gate.wait(1)
        return decision().body

    results = []
    threads = [threading.Thread(target=lambda: results.append(cache.get_or_fetch("k", slow, 60, 1.0))) for _ in range(5)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join()
    assert len(calls) == 1 and len(results) == 5 and all(r and r["action"] == "keep" for r in results)


def config(router, body):
    router._apply_config(Reply(200, body, '"e1"'))


def test_gating_from_config(router):
    assert router.may_decide("p")  # no config yet: the server decides
    config(router, {"policies": []})
    assert not router.may_decide("p")
    config(router, {"policies": [{"projects": ["a"], "mode": "active"}]})
    assert router.may_decide("a") and not router.may_decide("p")
    config(router, {"policies": [{"projects": [], "mode": "shadow"}]})
    assert router.may_decide("p")
    config(router, {"policies": [{"projects": [], "mode": "off"}]})
    assert not router.may_decide("p")
    config(router, {"killSwitch": True, "policies": [{"projects": [], "mode": "active"}]})
    assert not router.may_decide("p")
    config(router, {"minClient": {"python": "99.0.0"}, "policies": [{"projects": [], "mode": "active"}]})
    assert not router.may_decide("p")


def test_send_param_values(router):
    assert router.send_param_values("p")
    config(router, {"policies": [{"projects": [], "mode": "active", "sendParamValues": False}]})
    assert not router.send_param_values("p")
    off = Router(Settings(api_key="k", send_param_values=False), transport=router.transport)
    assert not off.send_param_values("p")


def test_bad_mask_rule_in_config_stops_sending(router):
    config(router, {"policies": [], "mask": {"revision": "r", "rules": [{"pattern": "(", "token": "x"}]}})
    assert router.masker() is None
    config(router, {"policies": [], "mask": {"revision": "r2", "rules": [{"pattern": "a", "token": "x"}]}})
    assert router.masker().revision == "r2"


def test_poll_config_uses_etag(router, transport):
    replies = [Reply(200, {"policies": [], "pollSeconds": 30}, '"e1"'), Reply(304, None, '"e1"')]
    transport.request = lambda method, path, body=None, headers=None, timeout=None: (transport.calls.append(headers), replies.pop(0))[1]
    assert router.poll_config()
    assert router.poll_config()
    assert transport.calls == [None, {"If-None-Match": '"e1"'}]
    assert router.config()["pollSeconds"] == 30 and router._poll_seconds() == 30


def test_poll_seconds_bounded(router):
    for value, want in ((0, 10.0), (-1, 10.0), (10**6, 600.0), ("x", 60.0), (True, 60.0)):
        config(router, {"pollSeconds": value})
        assert router._poll_seconds() == want


def test_events_batch_and_flush(router, transport):
    for i in range(250):
        router.emit({"decisionId": f"d{i}", "state": "done"})
    router.emit({"decisionId": None, "state": "done"})  # no id: dropped
    router.flush_events(deadline_s=1.0)
    sizes = [len(c["body"]["events"]) for c in transport.calls if c["path"] == "/v1/events"]
    assert sizes == [100, 100, 50]


def test_settings_and_modes():
    s = load_settings({"SLASHPEAK_API_KEY": " k ", "SLASHPEAK_MODE": "SHADOW", "SLASHPEAK_SEND_PARAM_VALUES": "false"})
    assert s.api_key == "k" and s.mode == "shadow" and not s.send_param_values and s.usable
    assert load_settings({"SLASHPEAK_API_KEY": "k", "SLASHPEAK_MODE": "typo"}).mode == "off"
    assert not load_settings({}).usable
    assert not load_settings({"SLASHPEAK_API_KEY": "k", "SLASHPEAK_ENDPOINT": "http://router.example.com"}).usable
    assert load_settings({"SLASHPEAK_API_KEY": "k", "SLASHPEAK_ENDPOINT": "http://127.0.0.1:9"}).usable
    assert stricter_mode("active", "shadow") == "shadow"
    assert stricter_mode("shadow", None) == "off"
    assert stricter_mode("active", "weird") == "off"


def test_client_at_least():
    assert client_at_least("0.1.0", "0.1.0")
    assert client_at_least("0.10.0", "0.9.9")
    assert not client_at_least("0.1.0", "0.2.0")
    assert client_at_least("1.0.0rc1", None)
