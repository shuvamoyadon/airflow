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
PROJECT_ID = "edp-dev-storage"
PROJECT_NAME = "edp-dev-carema"
DATASET = "edp_ent_cma_plss_onboarding_src"
GCP_CONN_ID = "bigquery_plss"
LOCATION = "US"

BUCKET_NAME = "cma-plss-onboarding-lan-ent-dev"

# GCS "folder" (prefix) where all your SQL files live
SQL_PREFIX = "sql/" 

default_args = {
    "start_date": datetime(2025, 11, 9),
}

# ============================================================
# PYTHON CALLABLE
# ============================================================
def run_ddl_sql_files(sql_prefix: str, bucket_name: str, **context):

    # Hooks
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=LOCATION)
    client: bigquery.Client = bq_hook.get_client(project_id=PROJECT_NAME)

    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    execution_date = context.get("ds")  # 'YYYY-MM-DD'

    # ------------------------------------
    # 1) Figure out if user requested a specific file
    # ------------------------------------
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run else {}
    sql_file_param = dag_conf.get("sql_file")

    sql_files_to_run: List[str] = []

    if sql_file_param:
        # User wants a specific file
        if "/" in sql_file_param:
            # Treat as full GCS object path
            sql_files_to_run = [sql_file_param]
        else:
            # Treat as file name under the prefix
            sql_files_to_run = [f"{sql_prefix}{sql_file_param}"]
    else:
        # ------------------------------------
        # 2) Discover all .sql files under the prefix
        # ------------------------------------
        all_objects = gcs_hook.list(bucket_name=bucket_name, prefix=sql_prefix)

        if not all_objects:
            raise ValueError(
                f"No objects found in bucket '{bucket_name}' with prefix '{sql_prefix}'"
            )

        sql_files_to_run = [obj for obj in all_objects if obj.endswith(".sql")]

        if not sql_files_to_run:
            raise ValueError(
                f"No .sql files found in bucket '{bucket_name}' with prefix '{sql_prefix}'"
            )

    # ------------------------------------
    # 3) Execute each SQL file in order
    # ------------------------------------
    for sql_object_path in sql_files_to_run:
        # Read the SQL from GCS
        content = gcs_hook.download(
            bucket_name=bucket_name,
            object_name=sql_object_path,
        )

        # Ensure we have a proper text string for Jinja
        if isinstance(content, bytes):
            raw_sql = content.decode("utf-8")
        else:
            raw_sql = str(content)

        # Render SQL with Jinja
        # In your .sql files you can use:
        #   {{ project_id }}, {{ project_name }}, {{ dataset }}, {{ ds }}, {{ execution_date }}
        template = Template(raw_sql)
        rendered_sql = template.render(
            project_id=PROJECT_ID,
            project_name=PROJECT_NAME,
            dataset=DATASET,
            ds=execution_date,
            execution_date=execution_date,
        )

        # Execute the SQL (DDL) in BigQuery
        job = client.query(rendered_sql, location=LOCATION)
        job.result()  # wait for job completion

        print(f"[BigQuery DDL] Executed SQL from: gs://{bucket_name}/{sql_object_path}")


# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id="plss_onboarding_run_ddl_from_prefix",
    default_args=default_args,
    schedule_interval=None,   # or a cron string if you want it scheduled
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
        provide_context=True,  # OK for Composer, passes context as **kwargs
    )
