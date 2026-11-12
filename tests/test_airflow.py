"""The Airflow plugin, against a fake ``airflow`` package (no Airflow in the unit tests)."""

import importlib.metadata
import logging
import sys
import threading
import time
import types

import pytest

import slashpeak
import slashpeak.airflow as sp_airflow
import slashpeak.bigquery
from conftest import RES, FakeConnection, decision, insert_body
from slashpeak._core import runtime
from slashpeak.bigquery.hook import Hook

SECRET = "sk_live_conn_secret_123"


class FakeAirflow:
    """What the fake Airflow answers. An Exception value is raised; None means "not found"."""

    def __init__(self) -> None:
        self.connection = None
        self.variable = None
        self.sdk_connection = None
        self.sdk_variable = None
        self.calls = {"connection": 0, "variable": 0}


class Conn:
    def __init__(self, password):
        self.password = password


def _answer(value, missing):
    if isinstance(value, BaseException):
        raise value
    if value is None:
        raise missing
    return value


@pytest.fixture
def airflow(monkeypatch):
    """Airflow 2 shape: ``airflow.hooks.base.BaseHook`` and ``airflow.models.Variable``; no ``airflow.sdk``."""
    state = FakeAirflow()
    root = types.ModuleType("airflow")
    plugins_manager = types.ModuleType("airflow.plugins_manager")
    hooks = types.ModuleType("airflow.hooks")
    base = types.ModuleType("airflow.hooks.base")
    models = types.ModuleType("airflow.models")

    class AirflowPlugin:
        name = None

        @classmethod
        def on_load(cls, *args, **kwargs):
            pass

    class BaseHook:
        @classmethod
        def get_connection(cls, conn_id):
            assert conn_id == "slashpeak_default"
            state.calls["connection"] += 1
            return _answer(state.connection, LookupError("The conn_id `slashpeak_default` isn't defined"))

    class Variable:
        @classmethod
        def get(cls, key):
            assert key == "slashpeak_router"
            state.calls["variable"] += 1
            return _answer(state.variable, KeyError(key))

    plugins_manager.AirflowPlugin = AirflowPlugin
    base.BaseHook = BaseHook
    models.Variable = Variable
    root.plugins_manager, root.hooks, root.models = plugins_manager, hooks, models
    hooks.base = base
    for name, module in {
        "airflow": root,
        "airflow.plugins_manager": plugins_manager,
        "airflow.hooks": hooks,
        "airflow.hooks.base": base,
        "airflow.models": models,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.delitem(sys.modules, "airflow.sdk", raising=False)
    monkeypatch.delitem(sys.modules, "slashpeak.airflow._plugin", raising=False)
    state.root = root
    state.monkeypatch = monkeypatch
    return state


def add_sdk(state):
    """Airflow 3 shape: ``airflow.sdk.Connection`` and ``airflow.sdk.Variable`` as well."""
    sdk = types.ModuleType("airflow.sdk")

    class Connection:
        @classmethod
        def get(cls, conn_id):
            return _answer(state.sdk_connection, RuntimeError("CONNECTION_NOT_FOUND"))

    class Variable:
        @classmethod
        def get(cls, key):
            return _answer(state.sdk_variable, RuntimeError("VARIABLE_NOT_FOUND"))

    sdk.Connection, sdk.Variable = Connection, Variable
    state.root.sdk = sdk
    state.monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)


@pytest.fixture
def bq(monkeypatch):
    """Stands in for ``slashpeak.bigquery.install``: records the settings, no real patch or threads."""
    calls = {"install": [], "uninstall": 0, "raise": None}

    def install(settings=None):
        if calls["raise"]:
            raise calls["raise"]
        calls["install"].append(settings)
        return settings.usable

    def uninstall():
        calls["uninstall"] += 1

    monkeypatch.setattr(slashpeak.bigquery, "install", install)
    monkeypatch.setattr(slashpeak.bigquery, "uninstall", uninstall)
    monkeypatch.setattr(slashpeak, "_has", lambda module: True)
    return calls


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    for name in ("SLASHPEAK_API_KEY", "SLASHPEAK_MODE", "SLASHPEAK_ENDPOINT", "SLASHPEAK_SEND_PARAM_VALUES",
                 "AIRFLOW_CTX_DAG_ID", "AIRFLOW_CTX_TASK_ID", "AIRFLOW_CTX_DAG_RUN_ID"):
        monkeypatch.delenv(name, raising=False)
    yield
    sp_airflow._installed = False
    sp_airflow._by_plugin = False
    sp_airflow._dags = frozenset()
    runtime.set_job_filter(None)


def plugin_class():
    """The class Airflow gets from the ``airflow.plugins`` entry point."""
    eps = importlib.metadata.entry_points()
    group = eps.select(group="airflow.plugins") if hasattr(eps, "select") else eps.get("airflow.plugins", [])
    (ep,) = [e for e in group if e.name == "slashpeak"]
    assert ep.value == "slashpeak.airflow:SlashPeakPlugin"
    return ep.load()


# -- plugin -----------------------------------------------------------------------------------
def test_entry_point_loads_the_plugin(airflow, bq):
    cls = plugin_class()
    assert issubclass(cls, sys.modules["airflow.plugins_manager"].AirflowPlugin)
    assert cls.name == "slashpeak"


def test_on_load_installs_once(airflow, bq, monkeypatch):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    cls = plugin_class()
    cls.on_load()
    cls.on_load()
    assert sp_airflow.install() is True
    assert len(bq["install"]) == 1
    assert airflow.calls == {"connection": 1, "variable": 1}  # read once, cached


def test_module_imports_without_airflow(monkeypatch, bq):
    monkeypatch.setitem(sys.modules, "airflow", None)  # `import airflow` raises
    monkeypatch.delitem(sys.modules, "slashpeak.airflow._plugin", raising=False)
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    assert sp_airflow.install() is True
    assert bq["install"][0].api_key == "env-key"
    with pytest.raises(ImportError):
        sp_airflow.SlashPeakPlugin  # noqa: B018


# -- key --------------------------------------------------------------------------------------
def test_key_from_connection_wins_over_env(airflow, bq, monkeypatch):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    airflow.connection = Conn(f"  {SECRET} ")
    assert sp_airflow.install() is True
    assert bq["install"][0].api_key == SECRET


@pytest.mark.parametrize("connection", [None, RuntimeError("db down"), Conn(None), Conn("  "), object()], ids=["missing", "error", "no-password", "blank", "odd-object"])
def test_key_falls_back_to_env(airflow, bq, monkeypatch, connection):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    airflow.connection = connection
    assert sp_airflow.install() is True
    assert bq["install"][0].api_key == "env-key"


def test_no_key_anywhere_does_nothing_and_retries_later(airflow, bq, monkeypatch):
    assert sp_airflow.install() is False
    assert runtime.job_allowed()
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    assert sp_airflow.install() is True
    assert len(bq["install"]) == 2


def test_airflow_3_sdk_lookups_come_first(airflow, bq):
    add_sdk(airflow)
    airflow.sdk_connection = Conn("sdk-key")
    airflow.sdk_variable = '{"mode": "shadow", "dags": ["a"]}'
    airflow.connection = Conn("old-key")
    assert sp_airflow.install() is True
    assert bq["install"][0].api_key == "sdk-key"
    assert bq["install"][0].mode == "shadow"
    assert airflow.calls == {"connection": 0, "variable": 0}


def test_airflow_3_falls_back_to_the_airflow_2_api(airflow, bq):
    add_sdk(airflow)  # outside a task the SDK lookups fail (no supervisor)
    airflow.connection = Conn("db-key")
    airflow.variable = '{"mode": "shadow"}'
    assert sp_airflow.install() is True
    assert bq["install"][0].api_key == "db-key" and bq["install"][0].mode == "shadow"


# -- Variable ---------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw, expected",
    [
        (None, None),
        ("", None),
        ("{not json", None),
        ('["a"]', None),
        ("42", None),
        ('{"dags": "a"}', None),
        ('{"dags": ["a", 1]}', None),
        ("{}", sp_airflow.RouterVariable(None, frozenset())),
        ('{"mode": "SHADOW "}', sp_airflow.RouterVariable("shadow", frozenset())),
        ('{"mode": "activ"}', sp_airflow.RouterVariable("off", frozenset())),
        ('{"mode": 1}', sp_airflow.RouterVariable("off", frozenset())),
        ('{"mode": "active", "dags": []}', sp_airflow.RouterVariable("active", frozenset())),
        ('{"dags": [" a ", "b", ""]}', sp_airflow.RouterVariable(None, frozenset({"a", "b"}))),
        (b'{"mode": "off"}', sp_airflow.RouterVariable("off", frozenset())),
        ({"mode": "shadow", "dags": ["a"]}, sp_airflow.RouterVariable("shadow", frozenset({"a"}))),
    ],
)
def test_parse_variable(raw, expected):
    assert sp_airflow.parse_variable(raw) == expected


@pytest.mark.parametrize("variable", [None, RuntimeError("db down"), "{bad json", '"shadow"'], ids=["missing", "error", "bad-json", "not-object"])
def test_bad_or_missing_variable_falls_back_to_env(airflow, bq, monkeypatch, variable):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    monkeypatch.setenv("SLASHPEAK_MODE", "shadow")
    airflow.variable = variable
    assert sp_airflow.install() is True
    assert bq["install"][0].mode == "shadow"
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "any_dag")
    assert runtime.job_allowed()  # no allowlist: every DAG


@pytest.mark.parametrize(
    "env_mode, var_mode, expected",
    [(None, "shadow", "shadow"), ("shadow", "active", "shadow"), ("active", "off", "off"), ("off", "active", "off"), (None, None, "active")],
)
def test_stricter_of_env_and_variable_mode(airflow, bq, monkeypatch, env_mode, var_mode, expected):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    if env_mode:
        monkeypatch.setenv("SLASHPEAK_MODE", env_mode)
    airflow.variable = f'{{"mode": "{var_mode}"}}' if var_mode else "{}"
    on = sp_airflow.install()
    assert on is (expected != "off")
    assert bq["install"][0].mode == expected


def test_variable_off_turns_nothing_on(airflow, bq, monkeypatch):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    airflow.variable = '{"mode": "off", "dags": ["a"]}'
    assert sp_airflow.install() is False
    assert sp_airflow._dags == frozenset() and runtime.job_allowed()


def test_plugin_reads_again_after_an_early_install(airflow, bq, monkeypatch):
    """airflow_local_settings.py runs before Airflow opens its database; the plugin reads once more."""
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    airflow.variable = RuntimeError("Session must be set before!")
    assert sp_airflow.install() is True
    assert bq["install"][-1].mode == "active"
    airflow.variable = '{"mode": "shadow", "dags": ["a"]}'
    airflow.connection = Conn("db-key")
    plugin_class().on_load()
    assert (bq["install"][-1].mode, bq["install"][-1].api_key) == ("shadow", "db-key")
    assert sp_airflow._dags == frozenset({"a"})
    plugin_class().on_load()
    sp_airflow.install()
    assert len(bq["install"]) == 2


def test_plugin_second_read_can_turn_it_off(airflow, bq, monkeypatch):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    assert sp_airflow.install() is True
    airflow.variable = '{"mode": "off"}'
    plugin_class().on_load()
    assert bq["uninstall"] == 1 and sp_airflow._installed is False


def test_lookups_run_on_their_own_thread(airflow, bq, monkeypatch):
    """Airflow keeps one DB session per thread; a lookup must never close the caller's."""
    seen = []
    monkeypatch.setattr(sp_airflow, "VARIABLE_GETTERS", [lambda: seen.append(threading.current_thread()) or '{"mode": "shadow"}'])
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    assert sp_airflow.install() is True
    assert seen and seen[0] is not threading.current_thread()
    assert bq["install"][0].mode == "shadow"


def test_lookups_inline_while_airflow_imports(airflow, bq, monkeypatch):
    """airflow_local_settings.py runs inside `import airflow`: a second thread would wait on the import lock."""
    seen = []
    airflow.root.__spec__ = types.SimpleNamespace(_initializing=True)
    monkeypatch.setattr(sp_airflow, "VARIABLE_GETTERS", [lambda: seen.append(threading.current_thread()) or "{}"])
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    assert sp_airflow.install() is True
    assert seen == [threading.current_thread()]


def test_slow_lookup_is_dropped(airflow, bq, monkeypatch):
    release = threading.Event()
    monkeypatch.setattr(sp_airflow, "LOOKUP_TIMEOUT_S", 0.05)
    monkeypatch.setattr(sp_airflow, "VARIABLE_GETTERS", [lambda: release.wait(5) and '{"mode": "off"}'])
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    started = time.monotonic()
    try:
        assert sp_airflow.install() is True
    finally:
        release.set()
    assert time.monotonic() - started < 2
    assert bq["install"][0].mode == "active" and bq["install"][0].api_key == "env-key"


# -- per job: allowlist and source ------------------------------------------------------------
def run_job(router, data):
    conn = FakeConnection()
    Hook(lambda: router).handle(FakeConnection.api_request, conn, (), {"method": "POST", "path": "/projects/p/jobs", "data": data})
    return conn


def test_dag_not_in_allowlist_skips_the_router(airflow, bq, monkeypatch, router, transport):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    airflow.variable = '{"mode": "active", "dags": ["dag_a"]}'
    assert sp_airflow.install() is True
    transport.default_decide = decision("set_reservation", RES)
    for dag in ("dag_b", None):
        if dag:
            monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", dag)
        else:
            monkeypatch.delenv("AIRFLOW_CTX_DAG_ID")
        conn = run_job(router, insert_body())
        assert transport.decides() == []
        assert conn.sent[0]["data"] == insert_body()
    # Read at job time: the same process now runs a task of an allowed DAG.
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_a")
    conn = run_job(router, insert_body())
    assert len(transport.decides()) == 1
    assert conn.sent[0]["data"]["configuration"]["reservation"] == RES


def test_source_context_sent(airflow, bq, monkeypatch, router, transport):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    assert sp_airflow.install() is True
    transport.decide_replies = [decision(), decision()]
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "daily")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "load")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled__2026-11-12T00:00:00+00:00")
    run_job(router, insert_body("SELECT 1"))
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "report")
    run_job(router, insert_body("SELECT 2"))
    sources = [call["body"]["source"] for call in transport.decides()]
    assert sources == [
        {"kind": "airflow", "dagId": "daily", "taskId": "load", "runId": "scheduled__2026-11-12T00:00:00+00:00"},
        {"kind": "airflow", "dagId": "daily", "taskId": "report", "runId": "scheduled__2026-11-12T00:00:00+00:00"},
    ]


def test_failing_job_filter_runs_the_job_unchanged(router, transport):
    runtime.set_job_filter(lambda: 1 / 0)
    transport.default_decide = decision("set_reservation", RES)
    conn = run_job(router, insert_body())
    assert transport.decides() == []
    assert conn.sent[0]["data"] == insert_body()


# -- never raises, never logs secrets ---------------------------------------------------------
def test_errors_never_raise(airflow, bq, monkeypatch):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    bq["raise"] = RuntimeError("boom")
    assert sp_airflow.install() is False
    plugin_class().on_load()
    monkeypatch.setattr(sp_airflow, "_on_plugin_load", lambda: 1 / 0)
    plugin_class().on_load()
    monkeypatch.setattr(sp_airflow, "_first", lambda *a: 1 / 0)
    assert sp_airflow.install() is False
    monkeypatch.setattr(slashpeak.bigquery, "uninstall", lambda: 1 / 0)
    sp_airflow._installed = True
    sp_airflow.uninstall()


def test_password_property_that_raises(airflow, bq, monkeypatch):
    class Broken:
        @property
        def password(self):
            raise ValueError("cannot decrypt")

    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    airflow.connection = Broken()
    assert sp_airflow.install() is True
    assert bq["install"][0].api_key == "env-key"


def test_no_bigquery_library(airflow, bq, monkeypatch):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    monkeypatch.setattr(slashpeak, "_has", lambda module: False)
    assert sp_airflow.install() is False
    assert bq["install"] == []


def test_logs_hold_no_secrets(airflow, bq, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="slashpeak")
    monkeypatch.setenv("SLASHPEAK_API_KEY", SECRET)
    airflow.connection = RuntimeError(f"bad password {SECRET}")
    airflow.variable = '{"mode": "shadow", "dags": ["secret_dag_name"'
    sp_airflow.install()
    assert caplog.records
    for record in caplog.records:
        text = " ".join(str(v) for v in record.__dict__.values())
        assert SECRET not in text and "secret_dag_name" not in text


def test_uninstall_removes_hook_and_filter(airflow, bq, monkeypatch):
    monkeypatch.setenv("SLASHPEAK_API_KEY", "env-key")
    airflow.variable = '{"dags": ["dag_a"]}'
    assert sp_airflow.install() is True
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_b")
    assert not runtime.job_allowed()
    sp_airflow.uninstall()
    assert runtime.job_allowed() and bq["uninstall"] == 1
    assert sp_airflow.install() is True  # can be turned on again


def test_lock_rebuilt_after_fork():
    assert sp_airflow._reset_after_fork in runtime._fork_callbacks
    old = sp_airflow._lock
    sp_airflow._reset_after_fork()
    assert sp_airflow._lock is not old
