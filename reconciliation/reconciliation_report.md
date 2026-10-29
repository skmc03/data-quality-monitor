# Reconciliation Report

This file documents the reconciliation checks run against each pipeline execution.
The machine-readable version is written to `outputs/reconciliation_report.json`.

## Checks

| Check ID | Description | Expected Result |
|----------|-------------|-----------------|
| `ROW_COUNT_RECONCILIATION` | `source_count == valid_count + rejected_count` — no rows silently dropped | PASS |
| `NO_DUPLICATE_RUN_IDS` | No `run_id` appears more than once in `pipeline_runs` | PASS |
| `QUARANTINE_REASONS_POPULATED` | Every quarantine record has a non-empty `rejection_reason` | PASS |
| `BASELINE_PROFILE_EXISTS` | Baseline column profile was stored in DuckDB before batch comparison | PASS |
| `ALERT_ID_DETERMINISM` | All `alert_id` values equal `SHA-256(run_id::column::metric)` | PASS |
| `QUARANTINE_AGGREGATE_INTEGRITY` | All quarantine rows have `rejection_reason` (aggregate cross-check) | PASS |
| `PIPELINE_RUN_REGISTERED` | Exactly one `pipeline_runs` entry exists for the current `run_id` | PASS |

## How to Run

The reconciliation script runs automatically as the last step of `run_pipeline.py`.
It can also be invoked standalone:

```bash
python reconciliation/reconcile.py \
    --db db/pipeline.duckdb \
    --run-id RUN_001 \
    --input-count 1000 \
    --valid-count 970 \
    --rejected-count 30 \
    --config config.yaml
```

Exit code 0 = all checks passed. Exit code 1 = one or more checks failed.

## Output

Results are written to `outputs/reconciliation_report.json`. Example:

```json
{
  "run_id": "RUN_001",
  "generated_at": "2026-10-29T23:43:26Z",
  "checks": [
    {
      "check_id": "ROW_COUNT_RECONCILIATION",
      "description": "source_count == valid_count + rejected_count (no rows lost)",
      "status": "PASS",
      "detail": {
        "input_count": 1000,
        "valid_count": 970,
        "rejected_count": 30,
        "sum_valid_rejected": 1000,
        "rows_unaccounted_for": 0
      },
      "message": "Row counts balance correctly."
    }
  ],
  "summary": {
    "total_checks": 7,
    "passed": 7,
    "failed": 0,
    "warnings": 0
  }
}
```
