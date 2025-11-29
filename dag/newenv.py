from datetime import datetime
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from google.cloud import bigquery
from jinja2 import Template


# ============================================================
# CONFIG / CONSTANTS
# ============================================================
GCP_CONN_ID = "bigquery_plss"
LOCATION = "US"

BUCKET_NAME = "cma-plss-onboarding-lan-ent-dev"
SQL_PREFIX = "sql/"       # GCS "folder" where SQL files are stored

default_args = {
    "start_date": datetime(2025, 11, 9),
}


# ============================================================
# PYTHON CALLABLE
# ============================================================
def run_ddl_sql_files(sql_prefix: str, bucket_name: str, **context):

    # BigQuery client
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=LOCATION)
    client: bigquery.Client = bq_hook.get_client()

    # GCS hook
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    execution_date = context.get("ds")

    # ------------------------------------------------------
    # Read dag_run.conf params
    # ------------------------------------------------------
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run else {}

    # env is required for building: edp-<env>-carema
    env = dag_conf.get("env", "dev")   # default to dev

    # Optional: run only one SQL file
    sql_file_param = dag_conf.get("sql_file")

    sql_files_to_run: List[str] = []

    if sql_file_param:
        if "/" in sql_file_param:
            sql_files_to_run = [sql_file_param]   # Full GCS path
        else:
            sql_files_to_run = [f"{sql_prefix}{sql_file_param}"]
    else:
        # Discover all .sql files
        all_objects = gcs_hook.list(bucket_name=bucket_name, prefix=sql_prefix)
        if not all_objects:
            raise ValueError(
                f"No objects found in bucket '{bucket_name}' with prefix '{sql_prefix}'"
            )

        sql_files_to_run = [obj for obj in all_objects if obj.endswith(".sql")]

        if not sql_files_to_run:
            raise ValueError(
                f"No .sql files ending with .sql were found under prefix '{sql_prefix}'"
            )

    # ------------------------------------------------------
    # Execute each SQL file
    # ------------------------------------------------------
    for sql_object_path in sql_files_to_run:

        # Download the SQL from GCS
        content = gcs_hook.download(bucket_name=bucket_name, object_name=sql_object_path)
        raw_sql = content.decode("utf-8") if isinstance(content, bytes) else str(content)

        # Render SQL using Jinja2
        template = Template(raw_sql)
        rendered_sql = template.render(
            env=env,                 # edp-{{env}}-carema
            ds=execution_date,
            execution_date=execution_date,
        )

        # Execute in BigQuery
        job = client.query(rendered_sql, location=LOCATION)
        job.result()

        print(f"[BigQuery DDL] Executed SQL from: gs://{bucket_name}/{sql_object_path}")


# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id="plss_onboarding_run_ddl_from_prefix",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    run_ddl = PythonOperator(
        task_id="run_ddl_sql_files",
        python_callable=run_ddl_sql_files,
        op_kwargs={
            "sql_prefix": SQL_PREFIX,
            "bucket_name": BUCKET_NAME,
        },
        provide_context=True,
    )
