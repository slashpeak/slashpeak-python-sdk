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

## Safety rules

- **Fail open.** Any error, or no answer within 500 ms (DNS included), and the job runs exactly as
  you wrote it. After 5 failures in a row the SDK stops asking for 30 s.
- **Never fails a job that would have run.** If BigQuery refuses a changed job at insert (a 4xx, for
  example no permission on the reservation), the SDK runs your original request once, with the same
  job id, and reports `fallback`. On the `jobs.query` path (`query_and_wait`), BigQuery also returns a
  4xx for a query that ran and failed, so the SDK re-runs only when the error names the reservation or
  a label. A failed query is never run twice.
- **PII is masked before anything leaves your process**: emails, phone numbers, IP addresses, and the
  patterns you add in SlashPeak → Settings → Privacy. Backtick-quoted names are left alone.
- **Never touched:** jobs with an explicit `reservation`, dry runs, load, copy, extract and continuous
  jobs, and SQL with `/* slashpeak:off */`. To skip one block of code: `with slashpeak.bigquery.bypass(): ...`
- **No extra BigQuery calls.** Job results are read from responses the library already fetches.
- **Never logs** SQL, values or keys.

## Connections

- One pooled HTTPS session per process, HTTP/1.1 keep-alive: a warm call pays no TCP or TLS handshake.
- Separate connect (200 ms) and read (500 ms) timeouts, plus a hard 500 ms deadline on a 4-thread pool.
- No hidden retries and no redirects. No cookies. TLS is always verified.
- Fork safe (Airflow, Celery, gunicorn): a child process builds its own connections and threads.
- Events are batched every 2 s (at most 100 per call, bounded queue) and flushed for at most 1 s at exit.

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

## Security

Report a vulnerability to security@slashpeak.com. Please do not open a public issue for it.

## License

Apache-2.0. See [LICENSE](LICENSE).
