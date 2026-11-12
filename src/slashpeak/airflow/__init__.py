"""SlashPeak Job Router for Apache Airflow.

    pip install "slashpeak[airflow]"

The ``slashpeak`` plugin (entry point group ``airflow.plugins``) turns on the
BigQuery hook when Airflow loads plugins. A worker that skips plugins can do
the same from ``airflow_local_settings.py``:

    import slashpeak.airflow
    slashpeak.airflow.install()

Key: the password of Connection ``slashpeak_default``, else ``SLASHPEAK_API_KEY``.
Mode and DAG allowlist: Variable ``slashpeak_router``, JSON
``{"mode": "off|shadow|active", "dags": ["dag_a", ...]}`` (no ``dags``, or an
empty list, means every DAG). Both are read once per process, when the plugin
loads (and once more by the plugin when ``airflow_local_settings.py`` ran first,
since Airflow has not opened its database at that point).

The DAG, task and run ids are read per job from ``AIRFLOW_CTX_*``, since one
worker process runs many tasks. Nothing here ever raises into Airflow.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import sys
import threading
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

from slashpeak._core import runtime
from slashpeak._core.settings import MODES, Settings, load_settings, stricter_mode

__all__ = ["install", "uninstall", "CONN_ID", "VARIABLE"]  # and SlashPeakPlugin, loaded on first use

logger = logging.getLogger("slashpeak.airflow")

#: Airflow Connection whose password is the API key.
CONN_ID = "slashpeak_default"
#: Airflow Variable with ``{"mode": ..., "dags": [...]}``.
VARIABLE = "slashpeak_router"
DAG_ENV = "AIRFLOW_CTX_DAG_ID"
MAX_DAGS = 10000
LOOKUP_TIMEOUT_S = 5.0

_lock = threading.Lock()
_installed = False
_by_plugin = False  # installed from the plugin's on_load (Airflow fully started)
_dags: FrozenSet[str] = frozenset()  # empty: every DAG


def _reset_after_fork() -> None:
    global _lock
    _lock = threading.Lock()


runtime.on_fork(_reset_after_fork)


@dataclasses.dataclass(frozen=True)
class RouterVariable:
    """The parsed ``slashpeak_router`` Variable."""

    mode: Optional[str]
    dags: FrozenSet[str]


# -- Airflow lookups (Airflow 3 Task SDK first, then the Airflow 2 API) ---------------------
def _sdk_connection() -> Any:
    from airflow.sdk import Connection  # type: ignore[import-not-found]

    return Connection.get(CONN_ID)


def _hook_connection() -> Any:
    from airflow.hooks.base import BaseHook  # type: ignore[import-not-found]

    return BaseHook.get_connection(CONN_ID)


def _sdk_variable() -> Any:
    from airflow.sdk import Variable  # type: ignore[import-not-found]

    return Variable.get(VARIABLE)


def _model_variable() -> Any:
    from airflow.models import Variable  # type: ignore[import-not-found]

    return Variable.get(VARIABLE)


CONNECTION_GETTERS: List[Callable[[], Any]] = [_sdk_connection, _hook_connection]
VARIABLE_GETTERS: List[Callable[[], Any]] = [_sdk_variable, _model_variable]


def _first(getters: List[Callable[[], Any]], what: str) -> Any:
    """The first value a getter returns. Errors are logged by type only (messages may hold values)."""
    for getter in getters:
        try:
            value = getter()
        except Exception as error:  # noqa: BLE001
            logger.debug("slashpeak airflow lookup failed", extra={"code": f"{what}_unavailable", "error": type(error).__name__})
            continue
        if value is not None:
            return value
    return None


def _password(connection: Any) -> Optional[str]:
    try:
        password = getattr(connection, "password", None) if connection is not None else None
    except Exception:  # noqa: BLE001
        return None
    if isinstance(password, str) and password.strip():
        return password.strip()
    return None


def _lookup() -> Tuple[Any, Optional[str]]:
    return _first(VARIABLE_GETTERS, "variable"), _password(_first(CONNECTION_GETTERS, "connection"))


def _airflow_importing() -> bool:
    """True while ``import airflow`` runs: ``airflow_local_settings.py``, or plugins loaded at import."""
    spec = getattr(sys.modules.get("airflow"), "__spec__", None)
    return bool(getattr(spec, "_initializing", False))


def read_airflow() -> Tuple[Any, Optional[str]]:
    """The raw ``slashpeak_router`` Variable and the Connection key, each None when not found.

    Airflow keeps one database session per thread and closes it after a lookup, so a
    lookup on the thread that is loading plugins can detach that code's objects (seen
    on Airflow 3.4: DetachedInstanceError on the first ``airflow tasks test``). So the
    lookups run on a short-lived thread, and one that takes over 5 s is dropped.

    While ``import airflow`` still runs, the lookups run inline instead: a second thread
    would wait on the import lock, and no caller holds database objects yet.
    """
    if _airflow_importing():
        return _lookup()
    box: Dict[str, Tuple[Any, Optional[str]]] = {}

    def run() -> None:
        try:
            box["value"] = _lookup()
        except Exception as error:  # noqa: BLE001
            logger.debug("slashpeak airflow lookup failed", extra={"code": "lookup_error", "error": type(error).__name__})

    thread = threading.Thread(target=run, name="slashpeak-airflow-lookup", daemon=True)
    thread.start()
    thread.join(LOOKUP_TIMEOUT_S)
    if thread.is_alive():
        logger.debug("slashpeak airflow lookup failed", extra={"code": "lookup_timeout"})
        return None, None
    return box.get("value", (None, None))


def parse_variable(raw: Any) -> Optional[RouterVariable]:
    """The Variable's mode and DAG list, or None when it is missing or not valid (then env settings apply)."""
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    if isinstance(raw, str):
        if not raw.strip():
            return None
        try:
            raw = json.loads(raw)
        except ValueError:
            logger.debug("slashpeak airflow variable ignored", extra={"code": "bad_json"})
            return None
    if not isinstance(raw, dict):
        logger.debug("slashpeak airflow variable ignored", extra={"code": "not_an_object"})
        return None
    mode: Optional[str] = None
    if raw.get("mode") is not None:
        value = raw["mode"].strip().lower() if isinstance(raw["mode"], str) else ""
        # An unknown mode is a typo: do nothing rather than guess (same rule as SLASHPEAK_MODE).
        mode = value if value in MODES else "off"
    dags = raw.get("dags")
    if dags is None:
        names: FrozenSet[str] = frozenset()
    elif isinstance(dags, list) and all(isinstance(d, str) for d in dags):
        names = frozenset(d.strip() for d in dags[:MAX_DAGS] if d.strip())
    else:
        logger.debug("slashpeak airflow variable ignored", extra={"code": "bad_dags"})
        return None
    return RouterVariable(mode=mode, dags=names)


def resolve_settings(variable: Optional[RouterVariable], connection_key: Optional[str]) -> Settings:
    """Env settings, with the Connection key and the stricter of the env and Variable modes."""
    base = load_settings()
    key = connection_key or base.api_key
    mode = stricter_mode(base.mode, variable.mode) if variable is not None and variable.mode else base.mode
    return dataclasses.replace(base, api_key=key, mode=mode)


def _dag_allowed() -> bool:
    """Read at job time: one worker process runs tasks of many DAGs."""
    dags = _dags
    return not dags or os.environ.get(DAG_ENV, "") in dags


# -- install ----------------------------------------------------------------------------------
def install() -> bool:
    """Turns on the BigQuery hook with Airflow settings, once per process. True when the hook is on.

    Never raises. Without a key, with mode ``off``, or without google-cloud-bigquery,
    it does nothing and returns False (a later call tries again).
    """
    return _safe_install(from_plugin=False)


def _on_plugin_load() -> bool:
    """The plugin's ``on_load``. When ``airflow_local_settings.py`` installed first, Airflow had
    not opened its database yet, so the Connection and Variable are read once more here."""
    return _safe_install(from_plugin=True)


def _safe_install(from_plugin: bool) -> bool:
    try:
        return _install(from_plugin)
    except Exception as error:  # noqa: BLE001
        logger.debug("slashpeak airflow install skipped", extra={"code": "install_error", "error": type(error).__name__})
        return False


def _install(from_plugin: bool) -> bool:
    global _installed, _by_plugin, _dags
    with _lock:
        if _installed and (_by_plugin or not from_plugin):
            return True
        import slashpeak
        import slashpeak.bigquery

        if not slashpeak._has("google.cloud.bigquery"):
            logger.debug("slashpeak airflow install skipped", extra={"code": "no_bigquery"})
            return False
        raw_variable, connection_key = read_airflow()
        variable = parse_variable(raw_variable)
        settings = resolve_settings(variable, connection_key)
        # The filter goes in before the hook, so no job of another DAG reaches the router.
        _dags = variable.dags if variable is not None else frozenset()
        runtime.set_job_filter(_dag_allowed if _dags else None)
        on = bool(slashpeak.bigquery.install(settings))
        if not on:
            runtime.set_job_filter(None)
            _dags = frozenset()
            if _installed:
                slashpeak.bigquery.uninstall()  # the second read turned it off
        _installed, _by_plugin = on, from_plugin
        logger.debug("slashpeak airflow install", extra={"code": "on" if on else "off", "mode": settings.mode, "dags": len(_dags)})
        return on


def uninstall() -> None:
    """Removes the hook and the DAG filter. Never raises."""
    global _installed, _by_plugin, _dags
    try:
        with _lock:
            was_installed, _installed, _by_plugin = _installed, False, False
            _dags = frozenset()
            runtime.set_job_filter(None)
        if was_installed:
            import slashpeak.bigquery

            slashpeak.bigquery.uninstall()
    except Exception as error:  # noqa: BLE001
        logger.debug("slashpeak airflow uninstall failed", extra={"code": "uninstall_error", "error": type(error).__name__})


def __getattr__(name: str) -> Any:
    # Imported only when Airflow asks for it, so ``airflow_local_settings.py`` can import this
    # module while Airflow itself is still starting.
    if name == "SlashPeakPlugin":
        from slashpeak.airflow._plugin import SlashPeakPlugin

        return SlashPeakPlugin
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
