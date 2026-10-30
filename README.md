# Data Quality Monitor

A production-quality, fully local data quality monitoring pipeline for financial transaction data.
Establishes baseline statistical profiles, compares new batches for distribution drift, null-rate
changes, volume anomalies, and freshness issues, then emits explainable alerts — not just pass/fail.

---

## Architecture

```
                     +---------------------+
                     |    Source CSV file  |
                     +----------+----------+
                                |
                         [pipeline/ingest.py]
                         attach run_id + source_file
                                |
                         [pipeline/validate.py]
                      schema + business rules check
                           /            \
                     Valid rows      Rejected rows
                          |               |
                          |        [pipeline/quarantine.py]
                          |         DuckDB quarantine table
                          |         + quarantine.csv
                          |
               +----------+----------+
               |   pipeline/profiler.py   |
               |  Compute column stats:   |
               |  null_rate, min/max/mean |
               |  PSI bins, value_counts  |
               |  freshness, volume       |
               |  -> DuckDB column_profiles|
               +----------+----------+
                          |
               +----------+----------+
               |  pipeline/comparator.py  |
               |  Baseline vs Batch:      |
               |  - Null rate delta       |
               |  - Volume % change       |
               |  - PSI distribution drift|
               |  - Freshness lag         |
               |  - New/missing categories|
               +----------+----------+
                          |
               +----------+----------+
               |    pipeline/alerts.py    |
               |  DriftResult -> Alert    |
               |  - Deterministic ID      |
               |  - Human explanation     |
               |  - Severity: INFO/WARN/  |
               |    CRITICAL              |
               |  -> outputs/alerts.json  |
               +----------+----------+
                          |
               +----------+----------+
               | reconciliation/          |
               | reconcile.py             |
               |  - Row count check       |
               |  - No duplicate runs     |
               |  - Quarantine integrity  |
               |  - Alert ID determinism  |
               +----------+----------+
                          |
               +----------+----------+
               |    pipeline/logger.py    |
               |  DuckDB pipeline_runs    |
               |  Start/Complete/Failed   |
               +---------------------+
```

**5 actors in data flow:**
1. **Ingest** — reads source, stamps metadata
2. **Validate** — applies schema + business rules, splits valid/rejected
3. **Profiler** — builds statistical baseline and batch profiles in DuckDB
4. **Comparator** — diffs profiles, produces `DriftResult` objects
5. **Alerts** — converts drift results into structured, explainable JSON alerts

---

## Setup

### Prerequisites

- Python 3.10+
- No cloud dependencies — runs fully locally

### Install

```bash
git clone https://github.com/skmc03/data-quality-monitor.git
cd data-quality-monitor
python -m pip install -r requirements.txt
```

### Generate sample data

```bash
# Default: 1000 rows baseline + 1000 rows batch with injected drift
python data/generate_sample.py

# Large run mode (100k rows each — tests DuckDB at scale)
python data/generate_sample.py --rows 100000

# Custom output directory
python data/generate_sample.py --out-dir /tmp/my_data/

# Only baseline
python data/generate_sample.py --baseline-only
```

---

## Running the Pipeline

### Basic run (uses config.yaml defaults)

```bash
python run_pipeline.py
```

### Specify files and run ID

```bash
python run_pipeline.py \
    --baseline data/sample_baseline.csv \
    --batch    data/sample_batch.csv \
    --run-id   RUN_001
```

### Force re-run (override idempotency)

```bash
python run_pipeline.py --run-id RUN_001 --force
```

### Skip reconciliation

```bash
python run_pipeline.py --skip-reconcile
```

### All CLI options

```
--baseline         Path to baseline CSV
--batch            Path to batch CSV
--run-id           Unique run identifier (auto-generated if omitted)
--config           Path to config.yaml [default: config.yaml]
--db               Override DuckDB path
--force            Reprocess even if run_id already completed
--baseline-run-id  Explicitly specify which run_id to compare against
--log-level        DEBUG | INFO | WARNING | ERROR [default: INFO]
--skip-reconcile   Skip reconciliation step
```

---

## Running Tests

```bash
# Run all tests
pytest tests/

# With coverage report
pytest tests/ --cov=pipeline --cov-report=term-missing

# Run a specific test file
pytest tests/test_comparator.py -v

# Run a specific test class
pytest tests/test_validate.py::TestDuplicateTransactionId -v
```

Expected output: **121 tests, 0 failures**. See `SUBMISSION_EVIDENCE.md` for recorded results.

---

## Output Files

| File | Description |
|------|-------------|
| `outputs/alerts.json` | All drift alerts with explanations |
| `outputs/quarantine.csv` | Rejected records with rejection reasons |
| `outputs/reconciliation_report.json` | 7-check reconciliation report |
| `db/pipeline.duckdb` | DuckDB with all pipeline state |

### Sample alert structure

```json
{
  "alert_id": "a3f2c1...",
  "run_id": "RUN_001",
  "baseline_run_id": "RUN_001_BASELINE",
  "column": "is_flagged",
  "metric": "null_rate",
  "severity": "CRITICAL",
  "baseline_value": 0.02,
  "current_value": 0.15,
  "delta": 0.13,
  "explanation": "The null rate for column 'is_flagged' has risen by 13.00 percentage points (from 2.00% to 15.00%). This is a large increase and likely indicates an upstream data collection failure...",
  "threshold": 0.05,
  "triggered_at": "2026-10-29T23:43:26Z"
}
```

---

## Idempotency Behaviour

Every pipeline run registers itself in the `pipeline_runs` DuckDB table keyed by `run_id`.

| Scenario | Behaviour |
|----------|-----------|
| New `run_id` | Proceeds normally |
| `run_id` already `COMPLETED` | Skips — returns exit code 2 |
| `run_id` previously `FAILED` | Automatically retries |
| `run_id` marked `RUNNING` (stale crash) | Skipped — use `--force` |
| `--force` flag set | Purges prior state, reprocesses from scratch |

Re-running with the same `run_id` and `--force` is safe: all DuckDB rows for that run_id are deleted before re-insertion (no duplicate data).

---

## Retry / Restart / Backfill

**Retry after failure:**
A `FAILED` run is automatically retriable — no flags needed.

**Restart a crashed run:**
```bash
python run_pipeline.py --run-id RUN_001 --force
```

**Backfill historical data:**
Use distinct run_ids per batch date:
```bash
python run_pipeline.py \
    --batch data/2026-10-01_batch.csv \
    --run-id BACKFILL_20261001

python run_pipeline.py \
    --batch data/2026-10-02_batch.csv \
    --run-id BACKFILL_20261002
```

Each backfill run gets its own profile and alert set, preserving history.

---

## Reconciliation

Run standalone:

```bash
python reconciliation/reconcile.py \
    --db db/pipeline.duckdb \
    --run-id RUN_001 \
    --input-count 1000 \
    --valid-count 970 \
    --rejected-count 30
```

Or it runs automatically as the last step of `run_pipeline.py`.

**7 checks performed:**

1. `ROW_COUNT_RECONCILIATION` — source == valid + rejected (no silent row loss)
2. `NO_DUPLICATE_RUN_IDS` — pipeline_runs has unique run_ids
3. `QUARANTINE_REASONS_POPULATED` — every quarantine record has a reason
4. `BASELINE_PROFILE_EXISTS` — baseline was profiled before comparison
5. `ALERT_ID_DETERMINISM` — alert IDs match SHA-256(run_id::column::metric)
6. `QUARANTINE_AGGREGATE_INTEGRITY` — aggregate quarantine record check
7. `PIPELINE_RUN_REGISTERED` — exactly one pipeline_runs entry per run_id

---

## Known Limitations

1. **Single-node only** — DuckDB is embedded; does not support multi-writer concurrent access.
2. **No incremental profiling** — each run re-profiles the full batch; partial reads not supported.
3. **PSI requires sufficient data** — PSI computation is skipped if either distribution has fewer than 5 non-null values.
4. **Alert deduplication** — alerts.json is overwritten per run; no cross-run deduplication of identical alerts.
5. **Categorical top_n capped at 20** — high-cardinality columns (e.g., merchant_id) are truncated to top 20 values in the profile.
6. **No streaming support** — designed for batch CSV ingestion; not a real-time pipeline.

---

## Next Steps

- Add a Streamlit or FastAPI dashboard to visualise drift over time
- Plug in Great Expectations for richer schema validation
- Add email/Slack alert dispatcher wrapping `alerts.json`
- Implement partition-aware profiling (profile per date bucket)
- Add statistical tests (KS-test, chi-squared) alongside PSI
- Migrate DuckDB state to a shared store (e.g., S3 + Athena) for team-wide monitoring
- Add column-level lineage tracking (source field -> downstream metric)

---

## Data Dictionary Reference

See `data_dictionary.md` for full schema documentation of all source and DuckDB target tables.

---

## Design Tradeoffs

See `design_tradeoffs.md` for the reasoning behind key architectural decisions, including:

- Why DuckDB over Postgres/SQLite
- PSI binning strategy (equal-width, baseline-fixed bins, edge extension, zero-frequency handling)
- Deterministic alert IDs via SHA-256
- Performance benchmarks and applied optimisations
- Error handling philosophy
