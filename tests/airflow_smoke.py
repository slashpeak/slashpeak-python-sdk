"""Loads the plugin in a real Airflow install (CI job `airflow`). Not a pytest file.

    SLASHPEAK_API_KEY=ci SLASHPEAK_ENDPOINT=http://localhost:9 \\
    AIRFLOW_VAR_SLASHPEAK_ROUTER='{"mode": "shadow", "dags": ["dag_a"]}' \\
    python tests/airflow_smoke.py
"""

import importlib.metadata
import os

import airflow
from airflow import plugins_manager
from google.cloud.bigquery import _http

import slashpeak.airflow
from slashpeak._core import runtime

eps = importlib.metadata.entry_points()
group = eps.select(group="airflow.plugins") if hasattr(eps, "select") else eps.get("airflow.plugins", [])
(ep,) = [e for e in group if e.name == "slashpeak"]
assert ep.value == "slashpeak.airflow:SlashPeakPlugin", ep.value
assert issubclass(ep.load(), plugins_manager.AirflowPlugin)

plugins_manager.ensure_plugins_loaded()
if hasattr(plugins_manager, "plugins"):
    loaded = plugins_manager.plugins or []
else:  # newer Airflow 3
    loaded = plugins_manager._get_plugins()[0]
names = [p.name for p in loaded]
assert "slashpeak" in names, names
assert getattr(_http.Connection.api_request, "_slashpeak", False), "BigQuery hook not on"

router = runtime.current()
assert router is not None and router.settings.mode == "shadow", router and router.settings.mode
assert slashpeak.airflow._dags == frozenset({"dag_a"}), slashpeak.airflow._dags
os.environ["AIRFLOW_CTX_DAG_ID"] = "dag_b"
assert not runtime.job_allowed()
os.environ["AIRFLOW_CTX_DAG_ID"] = "dag_a"
assert runtime.job_allowed()
print(f"airflow {airflow.__version__}: slashpeak plugin loaded, hook on")
