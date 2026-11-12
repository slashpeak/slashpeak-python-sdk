# slashpeak

SlashPeak Job Router for Python. Before each warehouse job starts, the SDK asks the router where the
job costs less. For BigQuery that means on-demand, or a reservation you allowed. The router answers
from your own job history. If the answer is "move", the SDK sets the job's `reservation` field. Your
SQL, project, location, destination, priority and cache settings never change.

Docs: https://slashpeak.com/docs/product/job-router

Supported today: BigQuery ([google-cloud-bigquery](https://pypi.org/project/google-cloud-bigquery/)).
More warehouses will come as add-ons to this same package, sharing one key and one connection pool.

## Install

```bash
pip install "slashpeak[bigquery]"
```

```python
import slashpeak

slashpeak.install()  # hooks every supported library that is installed
```

Or BigQuery only, or one client only:

```python
import slashpeak.bigquery
from google.cloud import bigquery

slashpeak.bigquery.install()                       # every BigQuery client
client = slashpeak.bigquery.wrap(bigquery.Client())  # or just this one
```

Then check your setup:

```bash
slashpeak doctor            # key, network path, reservation access
slashpeak doctor --latency  # also 20 warm round trips
```

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `SLASHPEAK_API_KEY` | none | An API key with the **Job Router** permission (SlashPeak → Settings → API keys). Without it the SDK does nothing. |
| `SLASHPEAK_MODE` | `active` | `off`, `shadow` or `active`. The stricter of this and the Job Router page wins. |
| `SLASHPEAK_ENDPOINT` | `https://router.slashpeak.com` | HTTPS only. |
| `SLASHPEAK_SEND_PARAM_VALUES` | on | Send query parameter values (masked first). Off if this or the Job Router page says off. |
| `HTTPS_PROXY`, `NO_PROXY` | | Honored. |

On Airflow, a Connection and a Variable can set the key, the mode and a DAG allowlist. See [Airflow](#airflow).

## Safety rules

- **Fail open.** Any error, or no answer within 500 ms (DNS included), and the job runs exactly as
  you wrote it. After 5 failures in a row the SDK stops asking for 30 s.
- **Never fails a job that would have run.** If BigQuery refuses a changed job at insert (a 4xx, for
  example no permission on the reservation), the SDK runs your original request once, with the same
  job id, and reports `fallback`. On the `jobs.query` path (`query_and_wait`), BigQuery also returns a
  4xx for a query that ran and failed, so the SDK re-runs only when the error names the reservation or
  a label. When the reservation was deleted, BigQuery answers `jobs.insert` with a job that failed at
  once (nothing ran): the SDK runs your original request once under a new job id (`<your id>_fb` and 8
  characters). A failed query is never run twice.
- **Move to on-demand (0.2.0 and later, off until you turn it on in SlashPeak).** Only on the
  `jobs.query` path. The SDK sets `reservation: none` and a bytes cap (`maximumBytesBilled`) when your
  job has none. If BigQuery stops the job on that cap, nothing was billed: the SDK runs your original
  request once and reports `fallback`. If your job has its own cap, the SDK keeps it and never re-runs.
- **PII is masked before anything leaves your process**: emails, phone numbers, IP addresses, and the
  patterns you add in SlashPeak → Settings → Privacy. Backtick-quoted names are left alone.
- **Never touched:** jobs with an explicit `reservation`, dry runs, load, copy, extract and continuous
  jobs, SQL over 256 KB, and SQL with `/* slashpeak:off */`. To skip one block of code: `with slashpeak.bigquery.bypass(): ...`
- **No extra BigQuery calls.** Job results are read from responses the library already fetches.
- **Never logs** SQL, values or keys.

## Connections

- One pooled HTTPS session per process, HTTP/1.1 keep-alive: a warm call pays no TCP or TLS handshake.
- Separate connect (200 ms) and read (500 ms) timeouts, plus a hard 500 ms deadline on a 4-thread pool.
- No hidden retries and no redirects. No cookies. TLS is always verified.
- Fork safe (Airflow, Celery, gunicorn): a child process builds its own connections and threads.
- Events are batched every 2 s (at most 100 per call, bounded queue) and flushed for at most 1 s at exit.

## Airflow

```bash
pip install "slashpeak[airflow]"
```

Install it wherever tasks run (every worker; with the LocalExecutor, the scheduler). Airflow finds the
`slashpeak` plugin on its own through the `airflow.plugins` entry point. Nothing changes in your DAGs:
when Airflow loads plugins, the plugin turns on the BigQuery hook for the whole process. Tested on
Airflow 2.10 and 3.x.

The `airflow` extra installs the same packages as `bigquery`. It does not list `apache-airflow`: Airflow
is already installed where the plugin runs, and Airflow must be installed with its own constraints file.
A plain `pip install` that pulls Airflow could upgrade or downgrade it, or hundreds of its packages.

**API key.** Put it in the password of the Connection `slashpeak_default` (UI, CLI or your secrets
backend). Without that Connection, the SDK reads `SLASHPEAK_API_KEY`.

```bash
airflow connections add slashpeak_default --conn-type generic --conn-password "<API key>"
```

**Mode and DAG allowlist.** Optional. The Variable `slashpeak_router` holds JSON:

```bash
airflow variables set slashpeak_router '{"mode": "shadow", "dags": ["daily_marts", "hourly_events"]}'
```

- `mode`: `off`, `shadow` or `active`. The strictest of this, `SLASHPEAK_MODE` and the Job Router page
  wins. An unknown value counts as `off`.
- `dags`: only jobs from these DAG ids go to the router. Jobs from other DAGs, and jobs outside a task,
  run exactly as written with no router call. No `dags`, or an empty list, means every DAG.
- If the Variable is not valid JSON, or `dags` is not a list of strings, it is ignored and the
  environment settings apply.
- The Connection and Variable are read once per process, when the plugin loads. Restart the workers to
  apply a change everywhere.

**Per job.** The SDK reads `AIRFLOW_CTX_DAG_ID`, `AIRFLOW_CTX_TASK_ID` and `AIRFLOW_CTX_DAG_RUN_ID` when
each job starts (one worker process runs many tasks) and sends them with the decide call, so the Job
Router page can show decisions per DAG and task.

**Fail open.** The plugin never fails a task or a worker. If Airflow cannot answer for the Connection or
Variable (no database yet, no access, an error, or over 5 s), the SDK uses the environment settings. It
never logs SQL, values or keys.

**Workers that skip plugins.** Add one line to `airflow_local_settings.py`:

```python
import slashpeak.airflow; slashpeak.airflow.install()
```

Airflow runs that file before it opens its database, so there the SDK sees only environment variables
and secrets backends: set `SLASHPEAK_API_KEY` (or `AIRFLOW_CONN_SLASHPEAK_DEFAULT`) and, if you use it,
`AIRFLOW_VAR_SLASHPEAK_ROUTER`. If the plugin loads later in the same process, it reads the Connection
and Variable again.

**Cloud Composer.**

- Install from PyPI: in the environment's **PyPI packages** tab, add `slashpeak` with `[airflow]` as
  extras, or run
  `gcloud composer environments update ENV --location LOCATION --update-pypi-package "slashpeak[airflow]"`.
  Composer images already include google-cloud-bigquery 3.x and requests, so pip normally adds only
  `slashpeak`.
- Set the key in the Connection `slashpeak_default`, or in Secret Manager if the environment uses it as
  the secrets backend.
- Private IP environments have no internet access by default. The workers need egress to
  `https://router.slashpeak.com` (port 443): add Cloud NAT to the environment's VPC subnet. Without egress
  nothing breaks, but no job is routed (each call times out after 500 ms, then the SDK pauses for 30 s
  after 5 failures).
- To check the path from a worker, run `slashpeak doctor` in a task, for example
  `BashOperator(task_id="slashpeak_doctor", bash_command="slashpeak doctor")`.

## Contract

`spec/` holds the router contract this SDK is tested against:

- `openapi.json`: the HTTP API (`GET https://router.slashpeak.com/v1/openapi.json` serves the same file).
- `mask_vectors.json`: the PII masking rules and test vectors.

The SDK never fingerprints queries. The server does that, so a fingerprint change never needs an SDK release.

## Develop

```bash
uv venv && uv pip install -e '.[test]'
.venv/bin/python -m pytest
```

The Airflow tests use a fake `airflow` module. CI also installs real Airflow 2.10 and 3.x, loads the
plugin and runs a task (`tests/airflow_smoke.py`, `tests/airflow_smoke_dag.py`).

## Security

Report a vulnerability to security@slashpeak.com. Please do not open a public issue for it.

## License

Apache-2.0. See [LICENSE](LICENSE).
