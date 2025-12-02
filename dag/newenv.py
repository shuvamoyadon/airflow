"""
{
  "bucket_name": "cma-plss-onboarding-lan-ent-dev",
  "sql_dir": "sql",
  "project_name": "my-project",
  "dataset_name": "customer_raw",
  "env": "dev"
}  
to pass as config. 

for specific file : 
{
  "bucket_name": "cma-plss-onboarding-lan-ent-dev",
  "sql_dir": "sql",
  "sql_file": "create_customer.sql",
  "project_name": "my-project",
  "dataset_name": "customer_raw"
}

"""
from datetime import datetime
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from google.cloud import bigquery
from jinja2 import Template


# ============================================================
# CONFIG
# ============================================================
GCP_CONN_ID = "bigquery_plss"
LOCATION = "US"

default_args = {
    "start_date": datetime(2025, 11, 9),
}


# ============================================================
# PYTHON CALLABLE
# ============================================================
def run_ddl_sql_files(**context):

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=LOCATION)
    client: bigquery.Client = bq_hook.get_client()

    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run else {}

    # ------------------------------------------------------
    # Required Inputs
    # ------------------------------------------------------
    bucket_name = dag_conf.get("bucket_name")
    project_name = dag_conf.get("project_name")
    dataset_name = dag_conf.get("dataset_name")

    if not bucket_name:
        raise ValueError("Missing required dag_run.conf param: bucket_name")
    if not project_name:
        raise ValueError("Missing required dag_run.conf param: project_name")
    if not dataset_name:
        raise ValueError("Missing required dag_run.conf param: dataset_name")

    # Optional
    sql_dir = dag_conf.get("sql_dir")      # Example: "schemas/"
    sql_file_param = dag_conf.get("sql_file")  # Example: "schemas/create_customer.sql"
    env = dag_conf.get("env", "dev")

    execution_date = context.get("ds")
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    sql_files_to_run: List[str] = []

    # ------------------------------------------------------
    # Case 1: Run ONLY ONE SQL file
    # ------------------------------------------------------
    if sql_file_param:
        # full path or local path under sql_dir
        if "/" in sql_file_param:
            sql_files_to_run = [sql_file_param]  # full GCS path
        else:
            if not sql_dir:
                raise ValueError("sql_dir is required when passing sql_file without path")
            sql_files_to_run = [f"{sql_dir.rstrip('/')}/{sql_file_param}"]

    # ------------------------------------------------------
    # Case 2: Auto-discover all SQL files in directory
    # ------------------------------------------------------
    else:
        if not sql_dir:
            raise ValueError("sql_dir is required when no sql_file is passed")

        prefix = sql_dir.rstrip("/") + "/"

        all_objects = gcs_hook.list(bucket_name=bucket_name, prefix=prefix)
        if not all_objects:
            raise ValueError(f"No files found under gs://{bucket_name}/{prefix}")

        sql_files_to_run = [obj for obj in all_objects if obj.endswith(".sql")]

        if not sql_files_to_run:
            raise ValueError(f"No .sql files found under gs://{bucket_name}/{prefix}")

    # ------------------------------------------------------
    # Execute SQL Files
    # ------------------------------------------------------
    for sql_path in sql_files_to_run:

        content = gcs_hook.download(bucket_name=bucket_name, object_name=sql_path)
        raw_sql = content.decode("utf-8")

        # Render the SQL template
        rendered_sql = Template(raw_sql).render(
            env=env,
            ds=execution_date,
            execution_date=execution_date,
            project_name=project_name,
            dataset=dataset_name
        )

        # Execute the SQL in BigQuery
        job = client.query(rendered_sql, location=LOCATION)
        job.result()

        print(f"[SUCCESS] Executed SQL => gs://{bucket_name}/{sql_path}")


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="plss_onboarding_run_ddl_from_gcs",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    run_ddl = PythonOperator(
        task_id="run_ddl_sql_files",
        python_callable=run_ddl_sql_files,
        provide_context=True,
    )
