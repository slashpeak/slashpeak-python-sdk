"""Builds the ``/v1/decide`` body from a BigQuery REST request body.

Two request shapes reach the hook:

* ``POST /projects/{p}/jobs`` (jobs.insert): ``configuration.query`` …
* ``POST /projects/{p}/queries`` (jobs.query): ``query``, ``location`` … at the top level.

Only masked SQL and masked string parameter values leave the process.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

from slashpeak._version import __version__
from slashpeak._core.mask import Masker

#: This call path re-runs the original job once when the bytes cap the router set stops it.
CAPABILITY_RERUN_BYTES_CAP = "rerun_bytes_cap"

API_VERSION = 1
#: SQL over this is never sent: the router keeps it unread (masking cost grows with size; spike 11, 2026-11-03).
MAX_SQL_BYTES = 256 * 1024
MAX_PARAMS = 1000
#: ``/* slashpeak:off */`` anywhere in the SQL opts the job out (same rule as the server).
OPT_OUT = re.compile(r"/\*\s*slashpeak\s*:\s*off\s*\*/", re.IGNORECASE)

INSERT_PATH = re.compile(r"^/projects/([^/]+)/jobs/?$")
QUERY_PATH = re.compile(r"^/projects/([^/]+)/queries/?$")


def _library() -> str:
    try:
        from google.cloud.bigquery import __version__ as bq_version  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return "google-cloud-bigquery"
    return f"google-cloud-bigquery/{bq_version}"


def _short(value: Any, limit: int) -> Optional[str]:
    return value if isinstance(value, str) and 0 < len(value) <= limit else None


def _source() -> Optional[Dict[str, str]]:
    dag = os.environ.get("AIRFLOW_CTX_DAG_ID")
    if not dag:
        return None
    out = {"kind": "airflow", "dagId": dag[:256]}
    task = os.environ.get("AIRFLOW_CTX_TASK_ID")
    run = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
    if task:
        out["taskId"] = task[:256]
    if run:
        out["runId"] = run[:256]
    return out


def _param_value(parameter: Dict[str, Any], masker: Masker) -> Any:
    value = parameter.get("parameterValue")
    if not isinstance(value, dict):
        return None
    if "value" in value:
        raw = value["value"]
        return masker.mask(raw) if isinstance(raw, str) else raw
    items = value.get("arrayValues")
    if isinstance(items, list):
        out = []
        for item in items[:MAX_PARAMS]:
            raw = item.get("value") if isinstance(item, dict) else None
            out.append(masker.mask(raw) if isinstance(raw, str) else raw)
        return out
    return None  # STRUCT values are not sent.


def _parameters(raw: Any, masker: Masker, send_values: bool) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(raw, list) or not raw:
        return None
    out = []
    for parameter in raw[:MAX_PARAMS]:
        if not isinstance(parameter, dict):
            continue
        entry: Dict[str, Any] = {}
        name = _short(parameter.get("name"), 128)
        kind = _short((parameter.get("parameterType") or {}).get("type"), 64)
        if name:
            entry["name"] = name
        if kind:
            entry["type"] = kind
        if send_values:
            entry["value"] = _param_value(parameter, masker)
        out.append(entry)
    return out


def _labels(raw: Any) -> Optional[Dict[str, str]]:
    if not isinstance(raw, dict):
        return None
    out = {k: v for k, v in raw.items() if isinstance(k, str) and isinstance(v, str) and len(v) <= 63}
    return out or None


def _table(raw: Any, keys: List[str]) -> Optional[Dict[str, str]]:
    if not isinstance(raw, dict):
        return None
    limits = {"projectId": 128, "datasetId": 1024, "tableId": 1024}
    out = {k: raw[k] for k in keys if _short(raw.get(k), limits[k])}
    return out or None


def sql_of(kind: str, body: Dict[str, Any]) -> Optional[str]:
    query = (body.get("configuration") or {}).get("query") if kind == "insert" else body
    sql = query.get("query") if isinstance(query, dict) else None
    return sql if isinstance(sql, str) and sql.strip() else None


def build_decide_request(
    *,
    kind: str,
    project: str,
    body: Dict[str, Any],
    masker: Masker,
    send_values: bool,
    request_id: str,
    job_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    """The decide body, or None when the job is not one to route (or a field is out of bounds)."""
    sql = sql_of(kind, body)
    if sql is None or OPT_OUT.search(sql) or len(sql.encode("utf-8")) > MAX_SQL_BYTES:
        return None
    if not _short(project, 128) or not _short(request_id, 256) or (job_id is not None and not _short(job_id, 256)):
        return None
    if kind == "insert":
        config = body.get("configuration") or {}
        query = config.get("query") or {}
        location = (body.get("jobReference") or {}).get("location")
        labels = config.get("labels")
        dry_run = config.get("dryRun")
    else:
        query = body
        location = body.get("location")
        labels = body.get("labels")
        dry_run = body.get("dryRun")

    masked = masker.mask(sql)
    if len(masked.encode("utf-8")) > MAX_SQL_BYTES:
        return None
    q: Dict[str, Any] = {"sql": masked}
    for key in ("useLegacySql", "useQueryCache", "createSession", "continuous"):
        if isinstance(query.get(key), bool):
            q[key] = query[key]
    if isinstance(dry_run, bool):
        q["dryRun"] = dry_run
    if query.get("priority") in ("INTERACTIVE", "BATCH"):
        q["priority"] = query["priority"]
    mode = _short(query.get("parameterMode"), 16)
    if mode:
        q["parameterMode"] = mode
    params = _parameters(query.get("queryParameters"), masker, send_values)
    if params:
        q["parameters"] = params
    max_bytes = query.get("maximumBytesBilled")
    if isinstance(max_bytes, (str, int)) and not isinstance(max_bytes, bool) and len(str(max_bytes)) <= 32:
        q["maximumBytesBilled"] = str(max_bytes)
    default = _table(query.get("defaultDataset"), ["projectId", "datasetId"])
    if default:
        q["defaultDataset"] = default
    destination = _table(query.get("destinationTable"), ["projectId", "datasetId", "tableId"])
    if destination:
        q["destination"] = destination
    clean_labels = _labels(labels)
    if clean_labels:
        q["labels"] = clean_labels
    sessions = query.get("connectionProperties")
    if isinstance(sessions, list) and any(isinstance(p, dict) and p.get("key") == "session_id" for p in sessions):
        q["hasSessionId"] = True
    if query.get("tableDefinitions"):
        q["hasExternalTables"] = True

    out: Dict[str, Any] = {
        "apiVersion": API_VERSION,
        "requestId": request_id,
        "projectId": project,
        "maskRevision": masker.revision,
        "client": {"sdk": "python", "version": __version__, "library": _library()},
        "query": q,
    }
    if kind == "query":
        # Only jobs.query can re-run a job the bytes cap stopped: the error comes back in the same call.
        out["client"]["capabilities"] = [CAPABILITY_RERUN_BYTES_CAP]
    if job_id:
        out["jobId"] = job_id
    loc = _short(location, 64)
    if loc:
        out["location"] = loc
    source = _source()
    if source:
        out["source"] = source
    return out
