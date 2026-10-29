# Data Dictionary

## Source Table: Financial Transactions

This is the external CSV input consumed by the pipeline.

| Column | Type | Nullable | Constraints | Description |
|--------|------|----------|-------------|-------------|
| `transaction_id` | STRING | NOT NULL | PRIMARY KEY | Unique identifier for each transaction. Format: alphanumeric. |
| `customer_id` | STRING | NOT NULL | FK to customer | Identifies the customer who made the transaction. |
| `transaction_date` | TIMESTAMP | NOT NULL | ISO-8601 UTC | When the transaction occurred. Format: `YYYY-MM-DDTHH:MM:SSZ`. |
| `amount` | FLOAT | NOT NULL | > 0 | Monetary value of the transaction in the specified currency. Must be strictly positive. |
| `currency` | STRING | NOT NULL | Enum: USD, EUR, GBP, INR | ISO 4217 currency code. Only the four listed values are accepted. |
| `category` | STRING | NOT NULL | Enum: retail, travel, food, utilities, healthcare | Merchant category classification. |
| `merchant_id` | STRING | NOT NULL | FK to merchant | Identifies the merchant who received the payment. |
| `status` | STRING | NOT NULL | Enum: completed, pending, failed, refunded | Current processing state of the transaction. |
| `country_code` | STRING | NOT NULL | Exactly 2 chars, ISO 3166-1 alpha-2 | Country where the transaction occurred. |
| `is_flagged` | BOOLEAN | NULLABLE | True/False/NULL | Fraud-risk indicator set by the upstream risk engine. NULL indicates the flag was not evaluated. |

---

## DuckDB Target Tables

### Table: `pipeline_runs`

Tracks each pipeline execution lifecycle.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `run_id` | VARCHAR | NOT NULL PK | Unique run identifier (e.g., `RUN_001`, `RUN_20261029_123456_AB12CD`). |
| `start_time` | TIMESTAMP | NOT NULL | UTC timestamp when the run was registered. |
| `end_time` | TIMESTAMP | NULLABLE | UTC timestamp when the run completed or failed. NULL while RUNNING. |
| `status` | VARCHAR | NOT NULL | One of: `RUNNING`, `COMPLETED`, `FAILED`. |
| `input_count` | BIGINT | NULLABLE | Total rows ingested from the source file. |
| `output_count` | BIGINT | NULLABLE | Rows that passed validation (written to valid path). |
| `rejected_count` | BIGINT | NULLABLE | Rows that failed validation (written to quarantine). |
| `failure_reason` | VARCHAR | NULLABLE | Error message if status=FAILED. |
| `source_file` | VARCHAR | NULLABLE | Basename of the batch CSV file. |

**Idempotency:** `run_id` is the primary key. `ON CONFLICT DO UPDATE` prevents duplicates.

---

### Table: `column_profiles`

Stores statistical profiles per column per run.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `run_id` | VARCHAR | NOT NULL PK | Pipeline run identifier. |
| `column_name` | VARCHAR | NOT NULL PK | Source column name or `_volume` for the volume summary. |
| `profile_type` | VARCHAR | NOT NULL | One of: `numeric`, `categorical`, `volume`, `freshness`. |
| `null_rate` | DOUBLE | NULLABLE | Proportion of rows where this column is NULL (0.0 to 1.0). |
| `row_count` | BIGINT | NULLABLE | Total rows in the dataset when profiled. |
| `stat_min` | DOUBLE | NULLABLE | Minimum value (numeric columns only). |
| `stat_max` | DOUBLE | NULLABLE | Maximum value (numeric columns only). |
| `stat_mean` | DOUBLE | NULLABLE | Arithmetic mean (numeric columns only). |
| `stat_median` | DOUBLE | NULLABLE | 50th percentile (numeric columns only). |
| `stat_std` | DOUBLE | NULLABLE | Standard deviation (numeric columns only). |
| `stat_p5` | DOUBLE | NULLABLE | 5th percentile (numeric columns only). |
| `stat_p25` | DOUBLE | NULLABLE | 25th percentile (numeric columns only). |
| `stat_p75` | DOUBLE | NULLABLE | 75th percentile (numeric columns only). |
| `stat_p95` | DOUBLE | NULLABLE | 95th percentile (numeric columns only). |
| `value_counts_json` | VARCHAR | NULLABLE | JSON string: `{"USD": 0.55, "EUR": 0.20, ...}` (categorical columns only, top 20 by frequency). |
| `freshness_min` | TIMESTAMP | NULLABLE | Earliest `transaction_date` in dataset (freshness columns only). |
| `freshness_max` | TIMESTAMP | NULLABLE | Latest `transaction_date` in dataset (freshness columns only). |
| `profiled_at` | TIMESTAMP | NOT NULL | UTC timestamp when the profile was computed. |

**Primary key:** `(run_id, column_name)`

**Idempotency:** Before insert, existing rows for `run_id` are deleted and replaced.

---

### Table: `quarantine`

Records all rows that failed validation.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `transaction_id` | VARCHAR | NULLABLE | Source transaction ID (may be NULL if PK field failed). |
| `customer_id` | VARCHAR | NULLABLE | Source customer ID. |
| `transaction_date` | VARCHAR | NULLABLE | Raw timestamp string from source. |
| `amount` | DOUBLE | NULLABLE | Parsed amount; NULL if non-numeric. |
| `currency` | VARCHAR | NULLABLE | Raw currency string. |
| `category` | VARCHAR | NULLABLE | Raw category string. |
| `merchant_id` | VARCHAR | NULLABLE | Raw merchant ID. |
| `status` | VARCHAR | NULLABLE | Raw status string. |
| `country_code` | VARCHAR | NULLABLE | Raw country code. |
| `is_flagged` | BOOLEAN | NULLABLE | Parsed boolean flag. |
| `run_id` | VARCHAR | NOT NULL | Pipeline run that quarantined this record. |
| `source_file` | VARCHAR | NULLABLE | Basename of the source CSV. |
| `rejection_reason` | VARCHAR | NOT NULL | Semicolon-separated list of all validation failures for this row. |
| `quarantine_timestamp` | TIMESTAMP | NOT NULL | UTC timestamp when the record was quarantined. |

**Idempotency:** On re-run with same `run_id`, existing quarantine rows for that run_id are deleted before re-insert.

---

## Output Files

### `outputs/alerts.json`

Array of alert objects. One alert per (run_id, column, metric) combination that crossed a threshold.

| Field | Type | Description |
|-------|------|-------------|
| `alert_id` | string | SHA-256 hex digest of `run_id::column::metric`. Deterministic across reruns. |
| `run_id` | string | Pipeline run that generated this alert. |
| `column` | string | Column affected (or `_volume` for volume-level alerts). |
| `metric` | string | One of: `null_rate`, `volume`, `distribution_psi`, `freshness`, `new_category`, `missing_category`. |
| `severity` | string | `INFO`, `WARN`, or `CRITICAL`. |
| `baseline_value` | any | Value measured in the baseline profile. |
| `current_value` | any | Value measured in the batch profile. |
| `delta` | number | Change magnitude (signed for null_rate/volume, PSI score for distribution, hours for freshness). |
| `explanation` | string | Human-readable business-context explanation of what changed and why it matters. |
| `threshold` | number | Configured threshold that was crossed. |
| `triggered_at` | string | ISO-8601 UTC timestamp of alert generation. |

### `outputs/quarantine.csv`

CSV version of the quarantine table (append mode across runs). Columns match the DuckDB quarantine table.

### `outputs/reconciliation_report.json`

JSON object with reconciliation check results.

| Field | Description |
|-------|-------------|
| `run_id` | Pipeline run. |
| `generated_at` | ISO-8601 UTC timestamp. |
| `checks` | Array of individual check results (`check_id`, `description`, `status`, `detail`, `message`). |
| `summary.total_checks` | Total number of checks run. |
| `summary.passed` | Count of PASS results. |
| `summary.failed` | Count of FAIL results. |
| `summary.warnings` | Count of WARN results. |
