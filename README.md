# SlashPeak SDKs

Client libraries for the [SlashPeak](https://slashpeak.com) Job Router. The router picks, per BigQuery
job, the cheaper place to run it: on-demand or a reservation you allowed. The SDK asks before each
job starts and sets the job's `reservation` field. On any error, or no answer within 500 ms, the job
runs exactly as written.

| Folder | Package | Status |
|---|---|---|
| [`python/`](python/) | [`slashpeak-bigquery`](https://pypi.org/project/slashpeak-bigquery/) | phase 1 |
| `airflow/` | `slashpeak-airflow` | planned |
| `typescript/` | `@slashpeak/bigquery` | on request |

`spec/` holds the router contract every SDK is tested against:

- `openapi.json`: the HTTP API (`GET https://router.slashpeak.com/v1/openapi.json` serves the same file).
- `mask_vectors.json`: PII masking rules and test vectors. Every SDK must mask each `sql` into `masked`.

SDKs never fingerprint queries. The server does that, so a fingerprint change never needs an SDK release.

## Develop

```bash
cd python
uv venv && uv pip install -e '.[test]'
.venv/bin/python -m pytest
```

## Security

Report a vulnerability to slashpeak@proton.me. Please do not open a public issue for it.

## License

Apache-2.0. See [LICENSE](LICENSE).
