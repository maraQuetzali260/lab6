# Alumnus Data ETL Pipeline (AWS Glue)

## 1. Overview

This pipeline cleanses a dataset of school alumnus records containing
personal information:

| Field             | Description                                  |
|--------------------|-----------------------------------------------|
| `name`             | Alumnus full name                             |
| `age`              | Alumnus age                                   |
| `date_of_joining`  | Date the alumnus joined the school            |
| `level`            | Program level                                 |
| `mail_address`     | Email address                                 |
| `living_address`   | Physical/mailing address                      |

It runs as a **PySpark AWS Glue job**, reads the raw table from the Glue
Data Catalog, applies data-quality rules to every record, and writes two
outputs to S3: one for records that pass all rules ("curated") and one for
records that fail at least one rule ("quarantine"), tagged with the
specific reason(s) for rejection.

## 2. Files

| File                  | Purpose                                                                 |
|------------------------|--------------------------------------------------------------------------|
| `cleansing_rules.py`   | Pure-Python validation functions implementing the business rules. No Spark/Glue dependency, so it's unit-testable in isolation and reused inside the Glue job as UDFs. |
| `glue_etl_job.py`      | The actual AWS Glue ETL script (Extract → Transform → Load).            |
| `test_etl.py`          | pytest suite covering every business rule and edge cases.               |
| `README.md`            | This document.                                                          |

## 3. Business rules (data quality)

A record is written to the **curated** output only if it passes **all**
of the following. Otherwise it is written to **quarantine** with a
`_rejection_reason` column listing every rule it failed.

1. **Age**: must be numeric and `0 < age <= 100`. Non-numeric, missing, or
   an age over 100 fails this rule.
2. **Date of joining**: must be a string in strict `mm/dd/yy` format
   (2-digit month, 2-digit day, 2-digit year, e.g. `09/01/15`). Any other
   date format (`yyyy-mm-dd`, `mm-dd-yyyy`, etc.) or any free-text /
   non-string value fails this rule. The string is also checked against
   the real calendar (e.g. `02/30/21` fails — no such date).
3. **Level**: must be **exactly** `ASSOCIATE`, `BACHELOR`, or `MASTER`.
   Matching is **case-sensitive/uppercase-only** — `Bachelor`, `master`,
   or `MASTERS` all fail this rule; no normalization is applied.
4. **Name**: must be present after trimming whitespace (empty/blank/`None`
   fails).

`mail_address` and `living_address` are trimmed of surrounding whitespace
but are not subject to a specific validation rule in this version.

## 4. Pipeline architecture

```
                     ┌──────────────────────────┐
                     │   Glue Data Catalog       │
                     │  (source alumnus table)   │
                     └────────────┬─────────────┘
                                  │  Extract
                                  v
                     ┌──────────────────────────┐
                     │   glue_etl_job.py         │
                     │  - trim free-text fields  │
                     │  - apply UDFs from        │
                     │    cleansing_rules.py     │
                     │  - flag valid/invalid     │
                     └───────┬─────────┬─────────┘
                             │         │
                     valid   │         │  invalid
                             v         v
                 ┌───────────────┐ ┌─────────────────────┐
                 │  S3 (curated) │ │  S3 (quarantine)     │
                 │  Parquet      │ │  Parquet +           │
                 │               │ │  _rejection_reason   │
                 └───────────────┘ └─────────────────────┘
```

## 5. Deployment to AWS Glue

### 5.1 Prerequisites
- An S3 bucket to hold: (a) the job scripts, (b) source data / Glue Data
  Catalog table, (c) curated output, (d) quarantine output.
- A Glue Data Catalog database + table pointing at the raw alumnus data
  (e.g. crawled from a CSV/JSON landing zone).
- An IAM role for the Glue job with:
  - `AWSGlueServiceRole` (or equivalent least-privilege policy)
  - S3 read access to the source data/scripts prefix
  - S3 write access to the curated and quarantine prefixes

### 5.2 Upload the scripts
```bash
aws s3 cp glue_etl_job.py   s3://<your-bucket>/scripts/glue_etl_job.py
aws s3 cp cleansing_rules.py s3://<your-bucket>/scripts/cleansing_rules.py
```

### 5.3 Create the Glue job
```bash
aws glue create-job \
  --name alumnus-etl-cleansing \
  --role <GlueServiceRoleArn> \
  --command '{
      "Name": "glueetl",
      "ScriptLocation": "s3://<your-bucket>/scripts/glue_etl_job.py",
      "PythonVersion": "3"
  }' \
  --default-arguments '{
      "--extra-py-files": "s3://<your-bucket>/scripts/cleansing_rules.py",
      "--SOURCE_DATABASE": "alumnus_db",
      "--SOURCE_TABLE": "alumnus_raw",
      "--OUTPUT_PATH": "s3://<your-bucket>/curated/alumnus/",
      "--QUARANTINE_PATH": "s3://<your-bucket>/quarantine/alumnus/",
      "--job-language": "python"
  }' \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X
```

> `--extra-py-files` is what makes `cleansing_rules.py` importable inside
> `glue_etl_job.py` at runtime — both files must live in S3.

### 5.4 Run the job
```bash
aws glue start-job-run --job-name alumnus-etl-cleansing
```

### 5.5 Scheduling (optional)
Attach a Glue trigger (on-demand, scheduled via cron, or event-based off
an S3 landing-zone `PUT` via EventBridge) to `alumnus-etl-cleansing`, e.g.:
```bash
aws glue create-trigger \
  --name alumnus-etl-daily \
  --type SCHEDULED \
  --schedule "cron(0 5 * * ? *)" \
  --actions '[{"JobName": "alumnus-etl-cleansing"}]' \
  --start-on-creation
```

### 5.6 Monitoring
- Job run status/metrics: Glue console → Jobs → Runs, or
  `aws glue get-job-runs --job-name alumnus-etl-cleansing`.
- Logs: CloudWatch Logs group `/aws-glue/jobs/output` (stdout, includes
  the `valid records / invalid records` count printed by the job) and
  `/aws-glue/jobs/error` (stderr).
- Data quality: check the row count and `_rejection_reason` distribution
  under the quarantine S3 prefix to track how much data is failing
  cleansing over time.

## 6. Testing

`cleansing_rules.py` has no Spark/Glue import, so tests run fast and
without any AWS/PySpark environment.

```bash
pip install pytest --break-system-packages
pytest test_etl.py -v
```

The suite (49 tests) covers:
- Age boundaries (0, 100, 101, negative, non-numeric, numeric strings, `None`)
- Date format (valid `mm/dd/yy`, wrong formats, non-string/datetime input,
  invalid calendar dates like Feb 30, out-of-range month/day)
- Level case-sensitivity (`ASSOCIATE`/`BACHELOR`/`MASTER` valid; any other
  casing or unknown value invalid)
- Free-text trimming (`clean_string_field`)
- End-to-end record validation (`validate_record`), including multi-rule
  failures

Before deploying a change to `glue_etl_job.py`, run the test suite against
`cleansing_rules.py` first — since the Glue job wraps those exact
functions as UDFs, a green test suite here guarantees the validation
logic is correct; only the Spark plumbing (I/O, column wiring) still
needs a smoke test in an actual Glue job run or local Glue Docker
container.

## 7. Extending the rules

To add or change a business rule:
1. Update (or add) the validator function in `cleansing_rules.py`.
2. Add/adjust test cases in `test_etl.py`.
3. If it's a new field-level rule, wire it into `glue_etl_job.py`:
   register the UDF, add it to the `_is_valid` condition, and add its
   reason code to `_build_rejection_reason`.
4. Re-run `pytest test_etl.py -v` before redeploying the script to S3.
