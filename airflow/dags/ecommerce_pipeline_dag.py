import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# NOTE: GlueJobOperator is available if reverting to AWS Cloud Glue when billing is restored.
# from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="ecommerce_multi_channel_pipeline",
    default_args=default_args,
    description="E-Commerce Multi-Channel Revenue & Inventory Pipeline",
    schedule=None,  # Set to None for manual execution, or "0 * * * *" for hourly
    start_date=datetime(2026, 8, 19),
    catchup=False,
    max_active_runs=1,
)
def ecommerce_multi_channel_pipeline():

    # 1. Start Task
    @task.bash(task_id="start_pipeline")
    def start_pipeline() -> str:
        return "echo 'Starting E-Commerce Data Pipeline execution...'"

    # 2. Extract MySQL Data (Operational DB)
    @task.bash(task_id="extract_mysql_data")
    def extract_mysql_data() -> str:
        return "python /opt/airflow/src/ingestion/mysql_ingest.py --date {{ ds }}"

    # 3. Extract Marketplace API Data
    @task.bash(task_id="extract_marketplace_data")
    def extract_marketplace_data() -> str:
        return "python /opt/airflow/src/ingestion/marketplace_ingest.py --date {{ ds }}"

    # 4. Local PySpark ETL Transformation (V2 — mirrors AWS Glue exactly)
    @task.bash(task_id="local_pyspark_etl_transform")
    def local_pyspark_etl_transform() -> str:
        return "python /opt/airflow/scripts/local_pyspark_transform.py --date {{ ds }}"

    # 5. Load Snowflake Star Schema
    @task.bash(task_id="load_snowflake_star_schema")
    def load_snowflake_star_schema() -> str:
        return "python /opt/airflow/scripts/load_snowflake.py --date {{ ds }}"

    # 6. End Task
    @task.bash(task_id="end_pipeline")
    def end_pipeline() -> str:
        return "echo 'E-Commerce Data Pipeline execution finished successfully!'"

    # Instantiate tasks
    t_start = start_pipeline()
    t_mysql = extract_mysql_data()
    t_mkt = extract_marketplace_data()
    t_spark = local_pyspark_etl_transform()
    t_snowflake = load_snowflake_star_schema()
    t_end = end_pipeline()

    # Define linear pipeline dependency flow
    t_start >> [t_mysql, t_mkt] >> t_spark >> t_snowflake >> t_end


# Instantiate the DAG
ecommerce_multi_channel_pipeline()
