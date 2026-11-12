"""DAGs for the CI job `airflow`: `airflow tasks test dag_a check` and `dag_b check`. Not a pytest file.

Expects Variable `slashpeak_router` = {"mode": "shadow", "dags": ["dag_a"]} in the metadata database.
"""

import datetime

try:
    from airflow.sdk import DAG
except ImportError:  # Airflow 2
    from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2
    from airflow.operators.python import PythonOperator


def check(dag_id):
    import os

    from google.cloud.bigquery import _http

    from slashpeak._core import runtime
    from slashpeak.bigquery.requests_body import _source

    assert getattr(_http.Connection.api_request, "_slashpeak", False), "BigQuery hook not on"
    router = runtime.current()
    assert router is not None and router.settings.mode == "shadow", "Variable not read"
    assert runtime.job_allowed() is (dag_id == "dag_a"), "DAG allowlist"
    source = _source()
    assert source == {"kind": "airflow", "dagId": dag_id, "taskId": "check", "runId": os.environ["AIRFLOW_CTX_DAG_RUN_ID"]}, source
    print(f"slashpeak smoke ok: {dag_id}")


for _dag_id in ("dag_a", "dag_b"):
    with DAG(_dag_id, start_date=datetime.datetime(2026, 1, 1), schedule=None, catchup=False) as _dag:
        PythonOperator(task_id="check", python_callable=check, op_args=[_dag_id])
    globals()[_dag_id] = _dag
