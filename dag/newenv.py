"""
json file will be like this for specific file execution
{
  "bucket_name": "cma-plss-onboarding-lan-ent-dev",
  "projects": [
    {
      "project_name": "give your project name for bigquery ",
      "sql_dir": "sql/customer_raw",
      "sql_files": [
        "create_customer.sql",
        "create_customer_address.sql"
      ]
    }
  ]
}

or all sql file execution 

{
  "bucket_name": "cma-plss-onboarding-lan-ent-dev",
  "projects": [
    {
      "project_name": give your project name for bigquery",
      "sql_dir": "conformance/ddl"
    }
  ]
}

"""

from datetime import datetime
from typing import List
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from google.cloud import bigquery
from jinja2 import Template


# ============================================================
# CONFIG
# ============================================================

GCP_CONN_ID = "bigquery_plss"
LOCATION = "US"

# -------------------------------------------------------------------
# env from Airflow Variable
# In Airflow UI → Admin → Variables:
#   Key:   ENV
#   Value: dev   (or test / prod)
# -------------------------------------------------------------------
ENV = Variable.get("ENV", default_var="dev")
GCP_PROJECT_NAME = f"edp-{ENV}-carema"
CONFIG_BUCKET_NAME=f"cma-plss-onboarding-lan-ent-{ENV}"
# Config object path inside CONFIG_BUCKET_NAME
CONFIG_OBJECT_NAME = "config/oldschema.json"



default_args = {
    "start_date": datetime(2025, 11, 9),
}


def _load_config() -> dict:
    """
    Load JSON config from GCS:
      gs://CONFIG_BUCKET_NAME/CONFIG_OBJECT_NAME

    Example oldschema.json:

    {
      "bucket_name": "cma-plss-onboarding-lan-ent-dev",
      "projects": [
        {
          "project_name": "my-project",
          "sql_dir": "sql/customer_raw",
          "sql_files": [
            "create_customer.sql",
            "create_customer_address.sql"
          ]
        }
      ]
    }
    """
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    content = gcs_hook.download(
        bucket_name=CONFIG_BUCKET_NAME,
        object_name=CONFIG_OBJECT_NAME,
    )
    return json.loads(content.decode("utf-8"))


# ============================================================
# PYTHON CALLABLE
# ============================================================
def run_ddl_sql_files(**context):

    # BigQuery client
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=LOCATION)
    client: bigquery.Client = bq_hook.get_client(project_id=GCP_PROJECT_NAME)

    # Load JSON config (static)
    config = _load_config()

    # Top-level config params
    # bucket_name = where the SQL files live (data bucket)
    # You can keep it in config OR derive from ENV if you prefer.
    bucket_name = config.get("bucket_name")
    env = ENV

    projects = config.get("projects", [])
    if not projects:
        raise ValueError("Config must contain a non-empty 'projects' list.")

    execution_date = context.get("ds")
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    # ------------------------------------------------------
    # Loop through all projects in config
    # ------------------------------------------------------
    for project_block in projects:
        project_name = project_block.get("project_name")
        sql_dir = project_block.get("sql_dir")  # e.g. "sql/customer_raw"
        config_sql_files: List[str] = project_block.get("sql_files", []) or []

        if not project_name:
            raise ValueError("Project block missing 'project_name'")
        if not sql_dir:
            raise ValueError("Project block missing 'sql_dir'")

        # ------------------------------------------------------
        # Determine SQL files to run for this project
        # ------------------------------------------------------
        sql_files_to_run: List[str] = []

        # Case 1: Use sql_files list in config for this project (run specific files)
        if config_sql_files:
            sql_files_to_run = [
                f"{sql_dir.rstrip('/')}/{fname}" for fname in config_sql_files
            ]

        # Case 2: Auto-discover all SQL files under sql_dir in GCS
        else:
            prefix = sql_dir.rstrip("/") + "/"
            all_objects = gcs_hook.list(bucket_name=bucket_name, prefix=prefix)
            if not all_objects:
                raise ValueError(
                    f"No files found under gs://{bucket_name}/{prefix} "
                    f"for project {project_name}"
                )

            sql_files_to_run = [obj for obj in all_objects if obj.endswith(".sql")]

            if not sql_files_to_run:
                raise ValueError(
                    f"No .sql files found under gs://{bucket_name}/{prefix} "
                    f"for project {project_name}"
                )

        # ------------------------------------------------------
        # Execute SQL Files for this project
        # ------------------------------------------------------
        for sql_path in sql_files_to_run:
            content = gcs_hook.download(bucket_name=bucket_name, object_name=sql_path)
            raw_sql = content.decode("utf-8")

            # Render the SQL template
            # NOTE: Only project_name (plus env and dates) is passed;
            # dataset and table names are hard-coded in the SQL files
            rendered_sql = Template(raw_sql).render(
                env=env,
                ds=execution_date,
                execution_date=execution_date,
                project_name=project_name,
            )

            # Execute the SQL in BigQuery
            job = client.query(rendered_sql, location=LOCATION)
            job.result()

            print(
                f"[SUCCESS] Executed SQL => gs://{bucket_name}/{sql_path} "
                f"for project {project_name} (env={env})"
            )


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
