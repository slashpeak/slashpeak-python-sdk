"""Requests the SDK builds must fit the router's OpenAPI contract (spec/openapi.json)."""

from typing import Any, Dict

from conftest import insert_body, load_spec
from slashpeak._core.mask import builtin_masker
from slashpeak.bigquery.requests_body import build_decide_request

OPENAPI = load_spec("openapi.json")


def check(schema: Dict[str, Any], value: Any, where: str = "$") -> None:
    if "const" in schema:
        assert value == schema["const"], where
    if "enum" in schema:
        assert value in schema["enum"], where
    kind = schema.get("type")
    if kind == "object" or "properties" in schema:
        assert isinstance(value, dict), where
        for key in schema.get("required", []):
            assert key in value, f"{where}.{key} missing"
        props = schema.get("properties", {})
        extra = schema.get("additionalProperties")
        for key, item in value.items():
            if key in props:
                check(props[key], item, f"{where}.{key}")
            elif isinstance(extra, dict):
                check(extra, item, f"{where}.{key}")
            else:
                raise AssertionError(f"{where}.{key} is not in the contract")
    if kind == "array":
        assert isinstance(value, list), where
        assert len(value) <= schema.get("maxItems", 10**9), where
        for i, item in enumerate(value):
            check(schema.get("items", {}), item, f"{where}[{i}]")
    if kind == "string":
        assert isinstance(value, str), where
        assert len(value) <= schema.get("maxLength", 10**9), where
        assert len(value) >= schema.get("minLength", 0), where
    if kind == "boolean":
        assert isinstance(value, bool), where


DECIDE = OPENAPI["paths"]["/v1/decide"]["post"]["requestBody"]["content"]["application/json"]["schema"]


def build(kind="insert", body=None, send_values=True, job_id="job_1"):
    return build_decide_request(
        kind=kind,
        project="p",
        body=body or insert_body(),
        masker=builtin_masker(),
        send_values=send_values,
        request_id="r1",
        job_id=job_id,
    )


def test_insert_request_fits_contract_and_is_masked():
    body = insert_body(
        "SELECT * FROM `p.d.t` WHERE email = 'ann@example.com' AND x = @v",
        labels={"team": "bi"},
    )
    q = body["configuration"]["query"]
    q.update(
        parameterMode="NAMED",
        queryParameters=[
            {"name": "v", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "bob@example.com"}},
            {"name": "a", "parameterType": {"type": "ARRAY"}, "parameterValue": {"arrayValues": [{"value": "10.0.0.1"}]}},
        ],
        priority="BATCH",
        defaultDataset={"projectId": "p", "datasetId": "d"},
        destinationTable={"projectId": "p", "datasetId": "d", "tableId": "out"},
        maximumBytesBilled="1000",
        connectionProperties=[{"key": "session_id", "value": "s"}],
    )
    request = build(body=body)
    check(DECIDE, request)
    assert request["query"]["sql"] == "SELECT * FROM `p.d.t` WHERE email = '<email>' AND x = @v"
    assert request["query"]["parameters"][0]["value"] == "<email>"
    assert request["query"]["parameters"][1]["value"] == ["<ip>"]
    assert request["query"]["hasSessionId"] is True
    assert request["location"] == "US" and request["jobId"] == "job_1"


def test_query_path_request_fits_contract():
    body = {"query": "SELECT 1", "location": "EU", "requestId": "r1", "labels": {"a": "b"}, "useLegacySql": False}
    request = build(kind="query", body=body, job_id=None)
    check(DECIDE, request)
    assert "jobId" not in request and request["location"] == "EU"


def test_values_not_sent_when_off():
    body = insert_body("SELECT @v")
    body["configuration"]["query"]["queryParameters"] = [
        {"name": "v", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "x"}}
    ]
    request = build(body=body, send_values=False)
    assert request["query"]["parameters"] == [{"name": "v", "type": "STRING"}]


def test_not_routed_cases():
    assert build(body=insert_body("/* slashpeak:off */ SELECT 1")) is None
    assert build(body=insert_body("   ")) is None
    assert build(body=insert_body("SELECT '" + "x" * (1024 * 1024) + "'")) is None
    # Over 256 KiB the router would keep it unread, so the SDK does not send it (spike 11).
    assert build(body=insert_body("SELECT '" + "x" * (256 * 1024) + "'")) is None
    assert build(body=insert_body("SELECT '" + "x" * (200 * 1024) + "'")) is not None
    assert build_decide_request(kind="insert", project="p" * 200, body=insert_body(), masker=builtin_masker(), send_values=True, request_id="r", job_id="j") is None


def test_airflow_source(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "daily")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "load")
    request = build()
    check(DECIDE, request)
    assert request["source"] == {"kind": "airflow", "dagId": "daily", "taskId": "load"}
