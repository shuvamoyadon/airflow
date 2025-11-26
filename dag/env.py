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
# Base pattern: edp-<env>-carema
BASE_PROJECT_PREFIX = "edp"
BASE_PROJECT_SUFFIX = "carema"

# default env if not passed via dag_run.conf
DEFAULT_ENV = "dev"

# This dataset is the same across env in this example.
# If you have per-env datasets, you can also template this.
BASE_DATASET = "edp_ent_cma_plss_onboarding_src"

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

    # ---------------------------
    # 0) Resolve env + project/dataset for this run
    # ---------------------------
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run else {}
    env = dag_conf.get("env", DEFAULT_ENV)  # e.g. "dev", "qa", "prod"

    project_name = f"{BASE_PROJECT_PREFIX}-{env}-{BASE_PROJECT_SUFFIX}"  # edp-dev-carema
    dataset = BASE_DATASET  # or f"{BASE_DATASET}_{env}" if you want env-specific datasets

    print(f"Using env='{env}', project_name='{project_name}', dataset='{dataset}'")

    # Hooks
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=LOCATION)
    client: bigquery.Client = bq_hook.get_client(project_id=project_name)

    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    execution_date = context.get("ds")  # 'YYYY-MM-DD'

    # ---------------------------
    # 1) Specific SQL file? (dag_run.conf["sql_file"])
    # ---------------------------
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
        # ---------------------------
        # 2) Discover all .sql files under the prefix
        # ---------------------------
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

    # ---------------------------
    # 3) Execute each SQL file
    # ---------------------------
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
        # In your .sql files you can now use:
        #   {{ project_name }}, {{ dataset }}, {{ env }}, {{ ds }}, {{ execution_date }}
        template = Template(raw_sql)
        rendered_sql = template.render(
            project_name=project_name,
            dataset=dataset,
            env=env,
            ds=execution_date,
            execution_date=execution_date,
        )

        # Execute the SQL (DDL) in BigQuery
        job = client.query(rendered_sql, location=LOCATION)
        job.result()  # wait for job completion

        print(
            f"[BigQuery DDL] Executed SQL from: gs://{bucket_name}/{sql_object_path} "
            f"on project '{project_name}', dataset '{dataset}'"
        )


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
