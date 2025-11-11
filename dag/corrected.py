from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime
import json

# -------------------------------
# CONFIG
# -------------------------------

PROJECT_ID = "edp-dev-storage"
PROJECT_NAME = "edp-dev-carema"
DATASET = "edp_ent_cma_plss_onboarding_src"
GCP_CONN_ID = "bigquery_plss"
LOCATION = "US"
JSON_COL = "fullvalue"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET}.source_customer_data"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET}.Account"
FINAL_TABLE = f"{PROJECT_ID}.{DATASET}.SRC_CUSTOMER1"
CONFIG_FILE = "NO_APP_ACRONYM_PROVIDED-NO_APP_LOB_PROVIDED/plss_onboarding_platform/schema1.json"
BUCKET_NAME = "plss-onboarding-lan-ent-dev"
SQL_TEMPLATE_PATH = "sql/test.sql"

default_args = {
    "start_date": datetime(2025, 11, 9)
}

dag = DAG(
    "plss_test_dag",
    default_args=default_args,
    schedule_interval=None,
    description="Map JSON target fields to source columns dynamically and insert final data",
    tags=[
        "vss_app_id:APM0017121",
        "project_id:edp-dev-carema"
    ],
)

# -------------------------
# MAIN FUNCTION
# -------------------------
def generate_merge_sql(**kwargs):
    """
    Generate and execute dynamic SCD1 MERGE SQL for BigQuery.
    - Reads schema mapping JSON from GCS
    - Builds SELECT expressions dynamically
    - Renders SQL template from GCS
    - Executes MERGE query using BigQueryHook
    """

    #hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=LOCATION)
    client = bigquery.Client(project=PROJECT_NAME)

    # Load config JSON from GCS
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    gcs_content = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=CONFIG_FILE).decode("utf-8")
    config_data = json.loads(gcs_content)

    # Connect to GCS
    storage_client = storage.Client()
    mapping = {m["source_field"]: m.get("target_field", "") for m in config_data["field_mappings"]}

    # -------------------------
    # Fetch column names & data types from target table schema
    # -------------------------
    schema_query = f"""
        SELECT column_name, data_type
        FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'source_customer_data'
        ORDER BY ordinal_position
    """
    schema_result = list(client.query(schema_query).result())

    # Build datatype lookup
    schema_dict = {row["column_name"]: row["data_type"].upper() for row in schema_result}
    source_cols = list(schema_dict.keys())

    # -------------------------
    # Build SELECT expression per column with type casting
    # -------------------------
    select_exprs = []
    for col in source_cols:
        target_field = mapping.get(col, "").strip()
        col_type = schema_dict[col]

        # Skip audit columns (timestamps handled in template)
        if col in ("created_date", "last_modified_date"):
            continue

        # Determine value expression
        if not target_field:
            expr = f"CAST(NULL AS {col_type}) AS {col}"
        else:
            base_expr = (
                f"COALESCE("
                f"JSON_VALUE(json_string, '$.Account.{target_field}.value'), "
                f"JSON_VALUE(json_string, '$.Account.{target_field}')"
                f")"
            )
            # Apply CAST according to data type
            if col_type in ("STRING", "BOOL", "BOOLEAN", "INT64", "FLOAT64", "TIMESTAMP", "DATE"):
                expr = f"SAFE_CAST({base_expr} AS {col_type}) AS {col}"
            else:
                # Default to string if datatype unrecognized
                expr = f"SAFE_CAST({base_expr} AS STRING) AS {col}"

        select_exprs.append(expr)

    select_columns = ",\n    ".join(select_exprs)

    # -------------------------
    # Load and render SQL Jinja template
    # -------------------------
    sql_blob = storage_client.bucket(BUCKET_NAME).blob(SQL_TEMPLATE_PATH)
    sql_template_text = sql_blob.download_as_text()
    template = Template(sql_template_text)

    rendered_sql = template.render(
        project_id=PROJECT_ID,
        dataset=DATASET,
        select_columns=select_columns,
        source_columns=source_cols,
        raw_table=RAW_TABLE,
        target_table=TARGET_TABLE,
    )

    # -------------------------
    # Save generated SQL for debugging
    # -------------------------
    debug_blob = storage_client.bucket(BUCKET_NAME).blob("logs/test.sql")
    debug_blob.upload_from_string(rendered_sql)

    # -------------------------
    # Execute the MERGE
    # -------------------------
    client.query(rendered_sql).result()
    print("✅ MERGE completed successfully.")


# -------------------------------
# OPERATOR
# -------------------------------

build_insert_task = PythonOperator(
    task_id="build_and_execute_mapping_query",
    python_callable=generate_merge_sql,
    dag=dag,
)
