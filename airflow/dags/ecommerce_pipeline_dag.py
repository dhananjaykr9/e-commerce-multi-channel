import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ecommerce_multi_channel_pipeline",
    default_args=default_args,
    description="E-Commerce Multi-Channel Revenue & Inventory Pipeline",
    schedule_interval=None,  # Set to None for manual execution, can be configured as "0 * * * *" (hourly)
    start_date=datetime(2026, 8, 19),
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Start Task
    start = BashOperator(
        task_id="start_pipeline",
        bash_command="echo 'Starting E-Commerce Data Pipeline execution...'",
    )

    # 2. Extract MySQL Data (Operational DB)
    extract_mysql = BashOperator(
        task_id="extract_mysql_data",
        bash_command="python /opt/airflow/src/ingestion/mysql_ingest.py --date {{ ds }}",
    )

    # 3. Extract Marketplace API Data
    extract_marketplace = BashOperator(
        task_id="extract_marketplace_data",
        bash_command="python /opt/airflow/src/ingestion/marketplace_ingest.py --date {{ ds }}",
    )

    # 4. AWS Glue ETL Transformation
    aws_glue_transform = GlueJobOperator(
        task_id="aws_glue_etl_transform",
        job_name="ecommerce_glue_transform",
        script_location=f"s3://{os.getenv('S3_BUCKET', 'e-commerce-multi-channel-dhananjay99')}/glue/ecommerce_glue_transform.py",
        s3_bucket=os.getenv('S3_BUCKET', 'e-commerce-multi-channel-dhananjay99'),
        iam_role_name="AWSGlueServiceRole-Ecommerce",
        create_job_kwargs={
            "GlueVersion": "4.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
            "DefaultArguments": {
                "--S3_BUCKET": os.getenv("S3_BUCKET", "e-commerce-multi-channel-dhananjay99"),
                "--RAW_PREFIX": os.getenv("S3_RAW_PREFIX", "raw-landing"),
                "--CURATED_PREFIX": os.getenv("S3_CURATED_PREFIX", "curated"),
                "--QUARANTINE_PREFIX": os.getenv("S3_QUARANTINE_PREFIX", "bad-records"),
            }
        },
        aws_conn_id="aws_default",
    )

    # 5. Load Snowflake Star Schema
    load_snowflake = BashOperator(
        task_id="load_snowflake_star_schema",
        bash_command="python /opt/airflow/scripts/load_snowflake.py --date {{ ds }}",
    )

    # 6. End Task
    end = BashOperator(
        task_id="end_pipeline",
        bash_command="echo 'E-Commerce Data Pipeline execution finished successfully!'",
    )

    # Set dependencies in a direct linear pipeline
    start >> [extract_mysql, extract_marketplace] >> aws_glue_transform >> load_snowflake >> end
