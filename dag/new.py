# dags/sample28.py
# ==============================================================
# DAG: plss_test_dag8
# Purpose: Dynamically map JSON → BigQuery columns and perform SCD1 MERGE
#          using CreatedDate / LastModifiedDate from raw JSON.
# ==============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from google.cloud import bigquery
from datetime import datetime
from jinja2 import Template
from typing import List
import json

# ---------------------------
# CONFIG / CONSTANTS
# ---------------------------
PROJECT_ID = "edp-dev-storage"
PROJECT_NAME = "edp-dev-carema"
DATASET = "edp_ent_cma_plss_onboarding_src"
GCP_CONN_ID = "bigquery_plss"
LOCATION = "US"
JSON_COL = "fullvalue"

SOURCE_TABLE = f"{PROJECT_ID}.{DATASET}.source_customer_data"
RAW_TABLE = f"{PROJECT_ID}.{DATASET}.Account1"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET}.SRC_CUSTOMER1"

# GCS paths
CONFIG_FILE = "NO_APP_ACRONYM_PROVIDED-NO_APP_LOB_PROVIDED/plss_onboarding_platform/schema2.json"
BUCKET_NAME = "cma-plss-onboarding-lan-ent-dev"
SQL_TEMPLATE_PATH = "NO_APP_ACRONYM_PROVIDED-NO_APP_LOB_PROVIDED/plss_onboarding_platform/customer.sql"

default_args = {"start_date": datetime(2025, 11, 9)}

# ---------------------------
# MAIN FUNCTION
# ---------------------------
def generate_merge_sql(**kwargs):
    """
    Generate and execute dynamic SCD1 MERGE SQL for BigQuery.
      - Reads mapping config JSON from GCS
      - Retrieves target table schema
      - Builds SELECT expressions dynamically
      - Includes CreatedDate & LastModifiedDate from raw JSON
      - Executes SCD1 MERGE on Sf_account_id
    """

    # Initialize hooks / clients
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=LOCATION)
    client: bigquery.Client = bq_hook.get_client(project_id=PROJECT_NAME)

    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    # ---------------------------
    # Load mapping configuration
    # ---------------------------
    gcs_content = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=CONFIG_FILE)
    if isinstance(gcs_content, bytes):
        gcs_content = gcs_content.decode("utf-8")
    config_data = json.loads(gcs_content)

    mapping = {m["source_field"]: m.get("target_field", "") for m in config_data.get("field_mappings", [])}
    print("Mapping dictionary loaded:", mapping)

    # ---------------------------
    # Fetch target table schema
    # ---------------------------
    schema_query = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'SRC_CUSTOMER1'
    ORDER BY ordinal_position
    """
    schema_result = list(client.query(schema_query).result())
    schema_dict = {row["column_name"]: row["data_type"].upper() for row in schema_result}
    source_cols = list(schema_dict.keys())

    # ---------------------------
    # Build SELECT expressions
    # ---------------------------
    select_exprs: List[str] = []
    for col in source_cols:
        target_field = mapping.get(col, "").strip()
        col_type = schema_dict[col]

        # Skip audit columns (handled separately)
        if col in ("last_process_dts", "source_last_process_dts"):
            continue

        if not target_field:
            expr = f"CAST(NULL AS {col_type}) AS {col}"
        else:
            base_expr = (
                f"COALESCE("
                f"JSON_VALUE({JSON_COL}, '$.{target_field}.value'), "
                f"JSON_VALUE({JSON_COL}, '$.{target_field}')"
                f")"
            )

            if col_type in ("STRING", "BOOL", "BOOLEAN", "INT64", "FLOAT64", "TIMESTAMP", "DATE"):
                expr = f"SAFE_CAST({base_expr} AS {col_type}) AS {col}"
            else:
                expr = f"SAFE_CAST({base_expr} AS STRING) AS {col}"

        select_exprs.append(expr)

    # Add JSON-based timestamps
    select_exprs.append(
        f"SAFE_CAST(JSON_VALUE({JSON_COL}, '$.CreatedDate') AS TIMESTAMP) AS last_process_dts"
    )
    select_exprs.append(
        f"SAFE_CAST(JSON_VALUE({JSON_COL}, '$.LastModifiedDate') AS TIMESTAMP) AS source_last_process_dts"
    )

    select_columns = ",\n    ".join(select_exprs)


    # ---------------------------
    # Load SQL template from GCS
    # ---------------------------
    sql_bytes = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=SQL_TEMPLATE_PATH)
    text_template = sql_bytes.decode("utf-8") if isinstance(sql_bytes, bytes) else str(sql_bytes)

    template = Template(text_template)

    rendered_sql = template.render(
        PROJECT_ID=PROJECT_ID,
        DATASET=DATASET,
        select_columns=select_columns,
        source_columns=source_cols,
        RAW_TABLE=RAW_TABLE,
        TARGET_TABLE=TARGET_TABLE,
    )

    #print("Rendered SQL:\n", rendered_sql[:800], "...\n[truncated]" if len(rendered_sql) > 800 else "")

    # ---------------------------
    # Execute MERGE
    # ---------------------------
    print("Sql to be executed:\n", rendered_sql)
    client.query(rendered_sql).result()
    print("MERGE completed successfully.")

# ---------------------------
# DAG Definition
# ---------------------------
dag = DAG(
    "plss_test_dag8",
    default_args=default_args,
    schedule_interval=None,
    description="Dynamic JSON → BigQuery mapping + SCD1 merge using Sf_account_id",
    tags=[],
)

build_insert_task = PythonOperator(
    task_id="build_and_execute_mapping_query",
    python_callable=generate_merge_sql,
    dag=dag,
)
