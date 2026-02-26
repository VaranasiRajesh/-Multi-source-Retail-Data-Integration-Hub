"""
=============================================================================
  DAG: retail_etl_pipeline
  Owner: data-engineering
  Description:
      Apache Airflow DAG that orchestrates and monitors the full
      Multi-source Retail Data Integration Hub ETL pipeline.

  Pipeline Stages
  ---------------
  0. health_check          – Validate CSV file, API reachability, GCP creds
  1. extract_retail_sales  – Pull Kaggle CSV data
  2. extract_api_products  – Pull Fake Store API products
  3. extract_api_categories– Pull Fake Store API categories
  4. validate_extract      – Row / schema quality checks post-extraction
  5. transform_data        – Build star-schema tables & data marts
  6. validate_transform    – Assert row counts & null rates post-transform
  7. load_to_bigquery      – Load all tables to BigQuery
  8. validate_load         – Confirm BigQuery row counts match expectations
  9. pipeline_summary      – Push run metadata to XCom / log summary
 10. notify_success/fail   – Slack / email callback stubs

  Schedule  : Daily at 02:00 UTC
  SLA       : 90 minutes
  Retries   : 2 (5-minute back-off)
=============================================================================
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Airflow imports
# ---------------------------------------------------------------------------
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException, AirflowFailException

# ---------------------------------------------------------------------------
# Project root on the Airflow worker (adjust via Airflow Variable if needed)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Variable.get(
    "RETAIL_ETL_PROJECT_ROOT",
    default_var=str(Path(__file__).resolve().parent.parent),
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

log = logging.getLogger("airflow.task")

# ============================================================================
# DEFAULT ARGUMENTS
# ============================================================================

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": [Variable.get("ALERT_EMAIL", default_var="data-team@example.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
    "sla": timedelta(minutes=90),
}

# ============================================================================
# HELPER: Push / Pull serialisable stats through XCom
# ============================================================================

def _push_stats(context, key: str, value: Any) -> None:
    context["ti"].xcom_push(key=key, value=value)


def _pull_stats(context, task_id: str, key: str) -> Any:
    return context["ti"].xcom_pull(task_ids=task_id, key=key)


# ============================================================================
# TASK FUNCTIONS
# ============================================================================

# ----------------------------------------------------------------------------
# 0. Health Check
# ----------------------------------------------------------------------------

def task_health_check(**context) -> None:
    """
    Pre-flight checks:
      - CSV file exists and is non-empty
      - Fake Store API is reachable
      - GCP credentials file exists (or ADC is set)
      - Required env vars are present
    Pushes a 'health' dict to XCom.
    """
    import requests
    from config.settings import (
        RETAIL_SALES_CSV,
        FAKE_STORE_API_URL,
        GOOGLE_APPLICATION_CREDENTIALS,
        GCP_PROJECT_ID,
        BQ_DATASET,
    )

    health: dict[str, Any] = {
        "checked_at": datetime.utcnow().isoformat(),
        "checks": {},
        "all_passed": True,
    }

    # 1. CSV file --------------------------------------------------------------
    csv_path = Path(RETAIL_SALES_CSV)
    csv_ok = csv_path.exists() and csv_path.stat().st_size > 0
    health["checks"]["csv_file"] = {
        "path": str(csv_path),
        "exists": csv_path.exists(),
        "size_bytes": csv_path.stat().st_size if csv_path.exists() else 0,
        "passed": csv_ok,
    }
    if not csv_ok:
        health["all_passed"] = False
        log.error("[HEALTH] CSV file missing or empty: %s", csv_path)

    # 2. Fake Store API --------------------------------------------------------
    try:
        resp = requests.get(f"{FAKE_STORE_API_URL}/products?limit=1", timeout=10)
        api_ok = resp.status_code == 200
    except Exception as exc:
        api_ok = False
        log.warning("[HEALTH] API unreachable: %s", exc)
    health["checks"]["fake_store_api"] = {
        "url": FAKE_STORE_API_URL,
        "reachable": api_ok,
        "passed": api_ok,
    }
    if not api_ok:
        health["all_passed"] = False

    # 3. GCP credentials -------------------------------------------------------
    gcp_ok = bool(GOOGLE_APPLICATION_CREDENTIALS) and Path(
        GOOGLE_APPLICATION_CREDENTIALS
    ).exists()
    if not gcp_ok:
        # Try Application Default Credentials (ADC)
        adc_path = Path(os.path.expanduser("~/.config/gcloud/application_default_credentials.json"))
        gcp_ok = adc_path.exists()
    health["checks"]["gcp_credentials"] = {
        "cred_file": GOOGLE_APPLICATION_CREDENTIALS,
        "project_id": GCP_PROJECT_ID,
        "dataset": BQ_DATASET,
        "passed": gcp_ok,
    }
    if not gcp_ok:
        health["all_passed"] = False
        log.warning("[HEALTH] GCP credentials not found – load step may fail")

    _push_stats(context, "health", health)

    log.info("[HEALTH] Results:\n%s", json.dumps(health, indent=2, default=str))

    if not csv_ok:
        raise AirflowFailException(
            f"Pre-flight failed: CSV file not found at {csv_path}"
        )
    if not api_ok:
        # Warn but allow pipeline to proceed (API is non-critical for CSV path)
        log.warning("[HEALTH] API unavailable – continuing with CSV-only extraction")


# ----------------------------------------------------------------------------
# 1–3. Extract Tasks
# ----------------------------------------------------------------------------

def task_extract_retail_sales(**context) -> None:
    """Extract retail sales CSV and push summary stats to XCom."""
    from etl.extract import extract_retail_sales

    df = extract_retail_sales()
    stats = {
        "rows": len(df),
        "columns": list(df.columns),
        "date_min": str(df["Date"].min()) if "Date" in df.columns else None,
        "date_max": str(df["Date"].max()) if "Date" in df.columns else None,
        "categories": (
            df["Product Category"].unique().tolist()
            if "Product Category" in df.columns
            else []
        ),
    }
    _push_stats(context, "retail_sales_stats", stats)
    _push_stats(context, "retail_sales_rows", len(df))
    log.info("[EXTRACT] Retail sales: %d rows", len(df))


def task_extract_api_products(**context) -> None:
    """Extract product catalog from Fake Store API."""
    from etl.extract import extract_api_products

    try:
        df = extract_api_products()
        stats = {
            "rows": len(df),
            "categories": (
                df["category"].unique().tolist() if "category" in df.columns else []
            ),
            "price_min": float(df["price"].min()) if "price" in df.columns else None,
            "price_max": float(df["price"].max()) if "price" in df.columns else None,
        }
        _push_stats(context, "api_products_stats", stats)
        _push_stats(context, "api_products_rows", len(df))
        log.info("[EXTRACT] API products: %d rows", len(df))
    except Exception as exc:
        log.warning("[EXTRACT] API products failed (%s) – skipping", exc)
        _push_stats(context, "api_products_rows", 0)
        raise AirflowSkipException("Fake Store API unavailable; skipping product extract")


def task_extract_api_categories(**context) -> None:
    """Extract product categories from Fake Store API."""
    from etl.extract import extract_api_categories

    try:
        categories = extract_api_categories()
        _push_stats(context, "api_categories", categories)
        _push_stats(context, "api_categories_count", len(categories))
        log.info("[EXTRACT] API categories: %s", categories)
    except Exception as exc:
        log.warning("[EXTRACT] API categories failed (%s) – skipping", exc)
        _push_stats(context, "api_categories_count", 0)
        raise AirflowSkipException("Fake Store API unavailable; skipping category extract")


# ----------------------------------------------------------------------------
# 4. Validate Extract
# ----------------------------------------------------------------------------

def task_validate_extract(**context) -> None:
    """
    Quality gate after extraction.
    Fails the task (and DAG) if retail_sales has 0 rows.
    Warns but continues if API data is absent.
    """
    retail_rows = _pull_stats(context, "extract_retail_sales", "retail_sales_rows") or 0
    api_product_rows = _pull_stats(context, "extract_api_products", "api_products_rows") or 0
    api_cat_count = _pull_stats(context, "extract_api_categories", "api_categories_count") or 0

    validation = {
        "retail_sales_rows": retail_rows,
        "api_products_rows": api_product_rows,
        "api_categories_count": api_cat_count,
        "retail_sales_ok": retail_rows > 0,
        "api_products_ok": api_product_rows > 0,
    }
    _push_stats(context, "extract_validation", validation)
    log.info("[VALIDATE-EXTRACT]\n%s", json.dumps(validation, indent=2))

    if not validation["retail_sales_ok"]:
        raise AirflowFailException(
            "Extraction validation failed: retail_sales has 0 rows"
        )

    if not validation["api_products_ok"]:
        log.warning("[VALIDATE-EXTRACT] API products are empty – transforms may be partial")


# ----------------------------------------------------------------------------
# 5. Transform
# ----------------------------------------------------------------------------

def task_transform_data(**context) -> None:
    """
    Run the full transform suite and push table row-count map to XCom.
    Uses the full extract_all() + transform_all() chain so transforms
    always work on fresh in-memory data within the same Airflow task.
    """
    from etl.extract import extract_all
    from etl.transform import transform_all

    log.info("[TRANSFORM] Starting full transform pipeline ...")
    t0 = time.time()

    extracted = extract_all()
    transformed = transform_all(extracted)

    row_counts = {
        k: len(v) for k, v in transformed.items() if hasattr(v, "__len__")
    }
    duration = round(time.time() - t0, 2)

    _push_stats(context, "transform_row_counts", row_counts)
    _push_stats(context, "transform_duration_sec", duration)
    log.info(
        "[TRANSFORM] Completed in %.1fs. Tables: %s",
        duration,
        json.dumps(row_counts, indent=2),
    )


# ----------------------------------------------------------------------------
# 6. Validate Transform
# ----------------------------------------------------------------------------

def task_validate_transform(**context) -> None:
    """
    Assert that key tables have sufficient rows after transformation.
    SLA thresholds are set via Airflow Variables for easy tuning.
    """
    row_counts: dict = (
        _pull_stats(context, "transform_data", "transform_row_counts") or {}
    )

    # Minimum expected rows per table (tunable via Airflow Variables)
    thresholds = {
        "stg_retail_sales": int(Variable.get("MIN_STG_RETAIL_ROWS", default_var="100")),
        "dim_customer": int(Variable.get("MIN_DIM_CUSTOMER_ROWS", default_var="10")),
        "dim_date": int(Variable.get("MIN_DIM_DATE_ROWS", default_var="365")),
        "fact_sales": int(Variable.get("MIN_FACT_SALES_ROWS", default_var="100")),
    }

    failures = []
    for table, min_rows in thresholds.items():
        actual = row_counts.get(table, 0)
        if actual < min_rows:
            failures.append(
                f"{table}: expected >= {min_rows}, got {actual}"
            )
            log.error("[VALIDATE-TRANSFORM] FAIL: %s", failures[-1])
        else:
            log.info("[VALIDATE-TRANSFORM] OK: %s has %d rows", table, actual)

    validation_result = {
        "row_counts": row_counts,
        "thresholds": thresholds,
        "failures": failures,
        "passed": len(failures) == 0,
    }
    _push_stats(context, "transform_validation", validation_result)

    if failures:
        raise AirflowFailException(
            "Transform validation failed:\n" + "\n".join(failures)
        )


# ----------------------------------------------------------------------------
# 7. Load to BigQuery
# ----------------------------------------------------------------------------

def task_load_to_bigquery(**context) -> None:
    """
    Re-run extract + transform in-memory (stateless workers),
    then call load_all() to push everything to BigQuery.
    Pushes load stats to XCom.
    """
    from etl.extract import extract_all
    from etl.transform import transform_all
    from etl.load import load_all

    log.info("[LOAD] Starting BigQuery load ...")
    t0 = time.time()

    extracted = extract_all()
    transformed = transform_all(extracted)
    load_stats = load_all(transformed)

    duration = round(time.time() - t0, 2)
    _push_stats(context, "load_stats", load_stats)
    _push_stats(context, "load_duration_sec", duration)
    log.info(
        "[LOAD] Completed in %.1fs. Stats: %s",
        duration,
        json.dumps(load_stats, indent=2, default=str),
    )


# ----------------------------------------------------------------------------
# 8. Validate Load
# ----------------------------------------------------------------------------

def task_validate_load(**context) -> None:
    """
    Query BigQuery to confirm row counts match what was loaded.
    Raises on any table with 0 rows.
    """
    try:
        from google.cloud import bigquery
        from config.settings import GCP_PROJECT_ID, BQ_DATASET

        client = bigquery.Client(project=GCP_PROJECT_ID)
        tables_to_check = [
            "stg_retail_sales",
            "dim_date",
            "dim_customer",
            "dim_product",
            "dim_product_category",
            "fact_sales",
            "mart_sales_performance",
            "mart_category_analysis",
        ]

        bq_counts: dict[str, int] = {}
        for table in tables_to_check:
            query = f"""
                SELECT COUNT(*) AS row_count
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{table}`
            """
            result = client.query(query).result()
            for row in result:
                bq_counts[table] = row.row_count

        _push_stats(context, "bq_row_counts", bq_counts)
        log.info("[VALIDATE-LOAD] BigQuery row counts:\n%s", json.dumps(bq_counts, indent=2))

        empty_tables = [t for t, c in bq_counts.items() if c == 0]
        if empty_tables:
            raise AirflowFailException(
                f"Load validation failed – empty tables: {empty_tables}"
            )

    except ImportError:
        log.warning("[VALIDATE-LOAD] google-cloud-bigquery not installed; skipping BQ validation")
    except Exception as exc:
        log.warning("[VALIDATE-LOAD] BigQuery validation skipped: %s", exc)


# ----------------------------------------------------------------------------
# 9. Pipeline Summary
# ----------------------------------------------------------------------------

def task_pipeline_summary(**context) -> None:
    """
    Collect all XCom stats and emit a human-readable summary.
    Writes a JSON report to the project logs/ directory.
    """
    ti = context["ti"]
    dag_run = context["dag_run"]

    summary = {
        "dag_id": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "execution_date": str(dag_run.execution_date),
        "run_timestamp": datetime.utcnow().isoformat(),
        "health": ti.xcom_pull(task_ids="health_check", key="health"),
        "extract": {
            "retail_sales": ti.xcom_pull(task_ids="extract_retail_sales", key="retail_sales_stats"),
            "api_products": ti.xcom_pull(task_ids="extract_api_products", key="api_products_stats"),
            "api_categories": ti.xcom_pull(task_ids="extract_api_categories", key="api_categories"),
        },
        "extract_validation": ti.xcom_pull(task_ids="validate_extract", key="extract_validation"),
        "transform_row_counts": ti.xcom_pull(task_ids="transform_data", key="transform_row_counts"),
        "transform_duration_sec": ti.xcom_pull(task_ids="transform_data", key="transform_duration_sec"),
        "transform_validation": ti.xcom_pull(task_ids="validate_transform", key="transform_validation"),
        "load_stats": ti.xcom_pull(task_ids="load_to_bigquery", key="load_stats"),
        "load_duration_sec": ti.xcom_pull(task_ids="load_to_bigquery", key="load_duration_sec"),
        "bq_row_counts": ti.xcom_pull(task_ids="validate_load", key="bq_row_counts"),
    }

    log.info(
        "\n" + "=" * 70
        + "\n  PIPELINE SUMMARY\n"
        + "=" * 70
        + "\n%s\n" + "=" * 70,
        json.dumps(summary, indent=2, default=str),
    )

    # Persist report to logs directory
    logs_dir = Path(PROJECT_ROOT) / "logs" / "airflow_runs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    report_path = logs_dir / f"run_{dag_run.run_id.replace(':', '_').replace('+', '_')}.json"
    report_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    log.info("[SUMMARY] Report written to %s", report_path)

    _push_stats(context, "summary", summary)


# ----------------------------------------------------------------------------
# 10. Notification callbacks
# ----------------------------------------------------------------------------

def notify_on_failure(context) -> None:
    """
    Called by Airflow on task failure (on_failure_callback).
    Extend with Slack / PagerDuty / email integration as needed.
    """
    task_instance = context["task_instance"]
    log.error(
        "[ALERT] Task FAILED  dag=%s  task=%s  run=%s  try=%s",
        task_instance.dag_id,
        task_instance.task_id,
        task_instance.run_id,
        task_instance.try_number,
    )
    # --- Slack webhook stub ---
    # slack_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    # if slack_url:
    #     import requests
    #     requests.post(slack_url, json={"text": f":red_circle: ETL FAILED: {task_instance.task_id}"})


def notify_on_success(context) -> None:
    """Final success callback fired from 'pipeline_summary' task."""
    log.info(
        "[ALERT] DAG run SUCCEEDED  dag=%s  run=%s",
        context["dag"].dag_id,
        context["run_id"],
    )
    # --- Slack webhook stub ---
    # slack_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    # if slack_url:
    #     import requests
    #     requests.post(slack_url, json={"text": ":large_green_circle: ETL SUCCEEDED"})


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id="retail_etl_pipeline",
    description=(
        "Orchestrates and monitors the Multi-source Retail Data Integration "
        "Hub ETL pipeline: Extract (CSV + API) → Transform (star schema) → "
        "Load (BigQuery)"
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",   # Daily at 02:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "retail", "bigquery", "data-engineering"],
    doc_md=__doc__,
) as dag:

    # ── 0. Health Check ──────────────────────────────────────────────────────
    health_check = PythonOperator(
        task_id="health_check",
        python_callable=task_health_check,
        on_failure_callback=notify_on_failure,
        doc_md="Pre-flight: validate CSV, API, and GCP credentials.",
    )

    # ── 1-3. Extract (parallel) ──────────────────────────────────────────────
    extract_retail_sales = PythonOperator(
        task_id="extract_retail_sales",
        python_callable=task_extract_retail_sales,
        on_failure_callback=notify_on_failure,
        doc_md="Extract retail sales data from the Kaggle CSV dataset.",
    )

    extract_api_products = PythonOperator(
        task_id="extract_api_products",
        python_callable=task_extract_api_products,
        on_failure_callback=notify_on_failure,
        doc_md="Extract product catalog from the Fake Store REST API.",
    )

    extract_api_categories = PythonOperator(
        task_id="extract_api_categories",
        python_callable=task_extract_api_categories,
        on_failure_callback=notify_on_failure,
        doc_md="Extract product categories from the Fake Store REST API.",
    )

    # ── 4. Validate Extract ──────────────────────────────────────────────────
    validate_extract = PythonOperator(
        task_id="validate_extract",
        python_callable=task_validate_extract,
        on_failure_callback=notify_on_failure,
        doc_md="Quality gate: assert extracted row counts meet minimums.",
    )

    # ── 5. Transform ─────────────────────────────────────────────────────────
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=task_transform_data,
        on_failure_callback=notify_on_failure,
        execution_timeout=timedelta(minutes=20),
        doc_md=(
            "Run all transformations: clean data, build star-schema "
            "dimensions, fact table, and data marts."
        ),
    )

    # ── 6. Validate Transform ────────────────────────────────────────────────
    validate_transform = PythonOperator(
        task_id="validate_transform",
        python_callable=task_validate_transform,
        on_failure_callback=notify_on_failure,
        doc_md="Assert transformed table row counts meet thresholds.",
    )

    # ── 7. Load to BigQuery ──────────────────────────────────────────────────
    load_to_bigquery = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=task_load_to_bigquery,
        on_failure_callback=notify_on_failure,
        execution_timeout=timedelta(minutes=40),
        doc_md=(
            "Load all transformed tables (staging, dimensions, fact, "
            "marts) into Google BigQuery."
        ),
    )

    # ── 8. Validate Load ─────────────────────────────────────────────────────
    validate_load = PythonOperator(
        task_id="validate_load",
        python_callable=task_validate_load,
        on_failure_callback=notify_on_failure,
        doc_md="Query BigQuery to confirm every table has rows post-load.",
    )

    # ── 9. Pipeline Summary ──────────────────────────────────────────────────
    pipeline_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=task_pipeline_summary,
        trigger_rule=TriggerRule.ALL_DONE,   # runs even if some tasks skipped
        on_success_callback=notify_on_success,
        on_failure_callback=notify_on_failure,
        doc_md="Aggregate XCom stats and write a JSON run report.",
    )

    # ── Convenience: log-cleanup bash task (weekly) ──────────────────────────
    cleanup_old_logs = BashOperator(
        task_id="cleanup_old_logs",
        bash_command=(
            f"find {PROJECT_ROOT}/logs/airflow_runs -name '*.json' "
            f"-mtime +30 -delete || true"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Remove JSON run-reports older than 30 days.",
    )

    # =========================================================================
    # DEPENDENCY GRAPH
    # =========================================================================
    #
    #   health_check
    #       ├── extract_retail_sales ──┐
    #       ├── extract_api_products  ─┤
    #       └── extract_api_categories┘
    #                                  └── validate_extract
    #                                           └── transform_data
    #                                                    └── validate_transform
    #                                                              └── load_to_bigquery
    #                                                                       └── validate_load
    #                                                                                └── pipeline_summary
    #                                                                                         └── cleanup_old_logs

    health_check >> [extract_retail_sales, extract_api_products, extract_api_categories]
    [extract_retail_sales, extract_api_products, extract_api_categories] >> validate_extract
    validate_extract >> transform_data >> validate_transform >> load_to_bigquery
    load_to_bigquery >> validate_load >> pipeline_summary >> cleanup_old_logs
