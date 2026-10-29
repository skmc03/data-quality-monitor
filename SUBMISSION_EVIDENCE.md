# YottaFlex Data Engineering Challenge — Execution Evidence

## Environment

| | |
|---|---|
| OS | Windows 11 Pro |
| Python | 3.13.7 |
| Test framework | pytest 9.1.1 |
| Database | DuckDB 2.0.0 (embedded, fully local) |

---

## Automated Tests

| Metric | Result |
|---|---|
| Tests collected | 121 |
| Tests passed | **121** |
| Tests failed | **0** |
| Standard test runtime | 31.62 s |
| Coverage test runtime | 31.84 s |
| Statement coverage | **83%** |
| Pipeline modules measured | 8 |

### Coverage by module

| Module | Coverage |
|---|---|
| `pipeline/idempotency.py` | 96% |
| `pipeline/comparator.py` | 93% |
| `pipeline/alerts.py` | 83% |
| `pipeline/quarantine.py` | 82% |
| `pipeline/ingest.py` | 80% |
| `pipeline/profiler.py` | 79% |
| `pipeline/validate.py` | 78% |
| `pipeline/logger.py` | 78% |

---

## Fresh Pipeline Execution

Run ID: `REVIEW_20261030_01`  
Timestamp: `2026-10-30T01:53:05Z`  
Command:

```bash
python run_pipeline.py \
  --baseline data/sample_baseline.csv \
  --batch    data/sample_batch.csv \
  --run-id   REVIEW_20261030_01
```

### Row counts

| Stage | Count |
|---|---|
| Baseline input | 1,000 |
| Baseline valid | 1,000 |
| Batch input | 1,000 |
| Batch valid | 960 |
| Batch rejected (quarantined) | 40 |
| Elapsed | 0.6 s |

### Alerts generated

| Severity | Count |
|---|---|
| CRITICAL | 2 |
| WARN | 20 |
| INFO | 19 |
| **Total** | **41** |

### Detected drift (baseline vs current)

| Metric | Baseline value | Current value | Delta | Severity |
|---|---|---|---|---|
| `is_flagged` null rate | 1.70% | 15.31% | +13.61 pp | WARN |
| `amount` PSI (baseline mean) | 373.50 | (current mean) 9229.51 | PSI score = 4.40 | CRITICAL |
| `transaction_date` freshness | 48h max lag | 572.63h actual lag | +524.63h over threshold | CRITICAL |
| `merchant_id` new categories | — | 20 new merchants | — | WARN ×20 |
| `merchant_id` missing categories | 19 absent | — | — | INFO ×19 |

### Reconciliation (7/7 checks passed)

| Check | Result |
|---|---|
| ROW_COUNT_RECONCILIATION | PASS |
| NO_DUPLICATE_RUN_IDS | PASS |
| QUARANTINE_REASONS_POPULATED | PASS |
| BASELINE_PROFILE_EXISTS | PASS |
| ALERT_ID_DETERMINISM (41 alerts) | PASS |
| QUARANTINE_AGGREGATE_INTEGRITY (40 records) | PASS |
| PIPELINE_RUN_REGISTERED | PASS |

---

## Tested Capabilities

| Capability | Test file |
|---|---|
| Schema validation and business rules | `test_validate.py` |
| Malformed CSV and empty file handling | `test_edge_cases.py`, `test_ingest.py` |
| Null rate and duplicate detection | `test_validate.py` |
| Statistical profiling (null rate, min/max/mean/PSI bins) | `test_profiler.py` |
| Distribution drift via PSI (baseline-fixed bins) | `test_comparator.py` |
| Volume anomaly detection | `test_comparator.py` |
| Freshness and late-arriving data | `test_comparator.py`, `test_edge_cases.py` |
| New and missing category detection | `test_comparator.py` |
| Explainable alerts with SHA-256 deterministic IDs | `test_alerts.py` |
| Quarantine output with rejection reasons | `test_edge_cases.py` |
| Referential integrity (unknown merchant_id) | `test_integration.py` |
| 7-check reconciliation report | `test_integration.py` |
| Idempotency — skip completed run | `test_idempotency.py`, `test_integration.py` |
| Idempotency — auto-retry FAILED run | `test_integration.py` |
| Force reprocess with `--force` | `test_idempotency.py`, `test_integration.py` |
| No duplicate quarantine rows on rerun | `test_integration.py` |
| Separate profiles per run ID | `test_integration.py` |
| End-to-end subprocess execution | `test_integration.py` |

---

## Output Files

| File | Description |
|---|---|
| `outputs/alerts.json` | 41 structured alerts with baseline_run_id, explanation, severity |
| `outputs/quarantine.csv` | 40 rejected records with rejection_reason |
| `outputs/reconciliation_report.json` | 7-check report, all passed |
| `db/pipeline.duckdb` | All pipeline state (profiles, runs, quarantine) |

---

## Generator Determinism Verification

Two independent runs of `python data/generate_sample.py` produce byte-identical files:

| File | SHA-256 |
|---|---|
| `sample_baseline.csv` | `EEDF066E88CE30F542A3FD2969809DD5B152D40CA94AD4D3468CA0EE1CA9AB7B` |
| `sample_batch.csv` | `53691411CFCA859A09E4D9049CAA0CE469D12A0C477B384FD0960BACBB29A9A8` |

Achieved by using a fixed reference timestamp (`2026-10-01T00:00:00Z`) and a seeded NumPy RNG (`seed=42`) instead of `datetime.now()`.

---

## Known Limitations

1. **Single-node only** — DuckDB is embedded; no multi-writer concurrent access.
2. **No incremental profiling** — each run re-profiles the full batch.
3. **PSI requires sufficient data** — skipped if either distribution has fewer than 5 non-null values.
4. **No streaming support** — designed for batch CSV ingestion.
5. **No cloud deployment** — fully local by design; storage interface is abstracted for future adapter.
