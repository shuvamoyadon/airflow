from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import storage
import json
from datetime import datetime

# -------------------------
# CONFIG
# -------------------------
PROJECT_ID = "playground-s-11-e0c0f33e"
DATASET = "test"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET}.source_customer_data"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET}.account_json_data"
FINAL_TABLE = f"{PROJECT_ID}.{DATASET}.final_data"
CONFIG_FILE = "config/schema.json"
BUCKET_NAME = "us-central1-myair-ee974251-bucket"

default_args = {"start_date": datetime(2025, 11, 9)}

dag = DAG(
    "bq_dynamic_mapping_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Map JSON target fields to source columns dynamically and insert final data",
)

# -------------------------
# TASK FUNCTION
# -------------------------
def generate_dynamic_sql(**kwargs):
    bq = BigQueryHook(bigquery_conn_id="bigquery_default", use_legacy_sql=False)
    client = bq.get_client()

    # Load config JSON from GCS
    storage_client = storage.Client()
    blob = storage_client.bucket(BUCKET_NAME).blob(CONFIG_FILE)
    config_data = json.loads(blob.download_as_text())

    # Build mapping dictionary: source_field -> target_field
    mapping_dict = {
        m["source_field"]: m.get("target_field", "") for m in config_data["field_mappings"]
    }

    # Fetch source table columns in order
    source_query = f"""
        SELECT column_name
        FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'source_customer_data'
        ORDER BY ordinal_position
    """
    source_columns = [row["column_name"] for row in client.query(source_query).result()]

    select_expressions = []

    for source_col in source_columns:
        target_field = mapping_dict.get(source_col, "")

        if not target_field or target_field.strip() == "":
            expr = f"NULL AS {source_col}"
        else:
            # Safe JSON extraction: handle both patterns (with and without .value)
            expr = (
                f"COALESCE("
                f"JSON_VALUE(json_string, '$.Account.{target_field}.value'), "
                f"JSON_VALUE(json_string, '$.Account.{target_field}')"
                f") AS {source_col}"
            )

        select_expressions.append(expr)

    # Combine into final SQL
    select_sql = ",\n  ".join(select_expressions)

    final_query = f"""
        INSERT INTO `{FINAL_TABLE}`
        SELECT
          {select_sql}
        FROM `{TARGET_TABLE}`;
    """

    # Log and execute
    print("Generated SQL:\n", final_query)
    client.query(final_query).result()

# -------------------------
# OPERATOR
# -------------------------
build_insert_task = PythonOperator(
    task_id="build_and_execute_mapping_query",
    python_callable=generate_dynamic_sql,
    dag=dag,
)
