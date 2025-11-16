# dags/sample28.py
# ============================================================
# DAG: plss_test_dag10
# Purpose: Dynamically map JSON -> BigQuery columns and perform SCD1 MERGE  
#          using CreatedDate / LastModifiedDate from raw JSON.
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
from datetime import datetime
from jinja2 import Template
from typing import List
import json

# -----------------------------
# CONFIG / CONSTANTS
# -----------------------------
PROJECT_ID = "edp-dev-storage"
PROJECT_NAME = "edp-dev-carema"
DATASET = "edp_ent_cma_plss_onboarding_src"
GCP_CONN_ID = "bigquery_plss"
LOCATION = "US"
JSON_COL = "Payload"

RAW_TABLE = f"`{PROJECT_ID}.{DATASET}.T_PCT_GCP_FIN_ACCOUNTS`"
TARGET_TABLE = f"`{PROJECT_ID}.{DATASET}.SRC_CUSTOMER`"

CONFIG_FILE = "NO_APP_ACRONYM_PROVIDED-NO_APP_LOB_PROVIDED/plss_onboarding_platform/schema2.json"
BUCKET_NAME = "cma-plss-onboarding-lan-ent-dev"
SQL_TEMPLATE_PATH = "NO_APP_ACRONYM_PROVIDED-NO_APP_LOB_PROVIDED/plss_onboarding_platform/customer1.sql"

default_args = {"start_date": datetime(2025, 11, 9)}


# ============================================================
# NEW: CATEGORY-BASED FIELD MAPPING FOR CORRECT JSON STRUCTURE
# ============================================================
def category_based_expr(col: str, col_type: str) -> str | None:

    category_case = (
        "JSON_VALUE(Payload, '$.ClientCategory__c')"
    )

    # -------------------------
    # customer_id logic
    # -------------------------
    if col == "customer_id":
        return (
            f"CASE {category_case} "
            f"WHEN 'Caremark' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Carrier_ID__c') AS {col_type}) "
            f"WHEN 'Aetna' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Plan_Sponsor_ID__c') AS {col_type}) "
            f"WHEN '3PY' THEN SAFE_CAST(JSON_VALUE(Payload, '$.ExternalId__c') AS {col_type}) "
            f"END AS customer_id"
        )

    # -------------------------
    # customer_nm logic
    # -------------------------
    if col == "customer_nm":
        return (
            f"CASE {category_case} "
            f"WHEN '3PY' THEN SAFE_CAST(Name AS {col_type}) "
            f"WHEN 'Caremark' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Carrier_Name__c') AS {col_type}) "
            f"WHEN 'Athena' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Plan_Sponsor_Name__c') AS {col_type}) "
            f"END AS customer_nm"
        )

    # -------------------------
    # account_manager_nm logic
    # -------------------------
    if col == "account_manager_nm":
        return (
            f"CASE {category_case} "
            f"WHEN 'Caremark' THEN SAFE_CAST(JSON_VALUE(Payload, '$.PBM_Account_Manager__c') AS {col_type}) "
            f"WHEN 'Aetna' THEN SAFE_CAST(JSON_VALUE(Payload, '$.Aetna_Account_Manager__c') AS {col_type}) "
            f"END AS account_manager_nm"
        )

    return None


# ============================================================
# MAIN FUNCTION
# ============================================================
def generate_merge_sql(**kwargs):

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=LOCATION)
    client: bigquery.Client = bq_hook.get_client(project_id=PROJECT_NAME)
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    gcs_content = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=CONFIG_FILE)
    config_data = json.loads(gcs_content.decode("utf-8"))

    mapping = {m["target_field"]: m.get("source_field", "") for m in config_data.get("field_mappings", [])}
    non_json_keys = {m["source_field"]: m.get("target_field", "") for m in config_data.get("non_json_keys", [])}

    # -----------------------------
    # Fetch target schema
    # -----------------------------
    schema_query = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA`.COLUMNS
    WHERE table_name = 'SRC_CUSTOMER'
    ORDER BY ordinal_position
    """

    schema_result = list(client.query(schema_query).result())
    schema_dict = {row["column_name"]: row["data_type"].upper() for row in schema_result}
    source_cols = list(schema_dict.keys())

    # -----------------------------
    # Build SELECT expressions
    # -----------------------------
    select_exprs: List[str] = []

    for col in source_cols:

        if col in ("orig_src_pst_dts", "source_last_process_dts"):
            continue

        col_type = schema_dict[col]
        target_field = mapping.get(col, "").strip()

        # category override
        category_expr = category_based_expr(col, col_type)
        if category_expr:
            select_exprs.append(category_expr)
            continue

        # non-json override
        if col in non_json_keys:
            source_key = non_json_keys[col]
            expr = f"SAFE_CAST({source_key} AS {col_type}) AS {col}"
            select_exprs.append(expr)
            continue

        # normal JSON extraction
        if not target_field:
            select_exprs.append(f"CAST(NULL AS {col_type}) AS {col}")
        else:
            base_expr = f"JSON_VALUE(Payload, '$.{target_field}')"
            expr = f"SAFE_CAST({base_expr} AS {col_type}) AS {col}"
            select_exprs.append(expr)

    # timestamps
    select_exprs.append("SAFE_CAST(JSON_VALUE(Payload, '$.CreatedDate') AS TIMESTAMP) AS orig_src_pst_dts")
    select_exprs.append("SAFE_CAST(JSON_VALUE(Payload, '$.LastModifiedDate') AS TIMESTAMP) AS source_last_process_dts")

    select_columns = ",\n    ".join(select_exprs)

    # -----------------------------
    # Render SQL template
    # -----------------------------
    sql_bytes = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=SQL_TEMPLATE_PATH)
    template = Template(sql_bytes.decode("utf-8"))

    rendered_sql = template.render(
        PROJECT_ID=PROJECT_ID,
        DATASET=DATASET,
        select_columns=select_columns,
        source_columns=source_cols,
        RAW_TABLE=RAW_TABLE,
        TARGET_TABLE=TARGET_TABLE,
    )

    print("Executing SQL:\n", rendered_sql)
    client.query(rendered_sql).result()
    print("MERGE completed successfully.")


# -----------------------------
# DAG Definition
# -----------------------------
dag = DAG(
    "plss_test_dag10",
    default_args=default_args,
    schedule_interval=None,
    description="Dynamic JSON → BigQuery mapping + SCD1 merge using sf_account_id",
    tags=[]
)

build_insert_task = PythonOperator(
    task_id="build_and_execute_mapping_query",
    python_callable=generate_merge_sql,
    dag=dag,
)
