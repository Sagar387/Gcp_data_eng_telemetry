from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'sagar',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
}

# The dag run 
with DAG(
    'pulse_stream_silver_layer_dag',
    default_args=default_args,
    description='A DAG to trigger pyspark job inside the spark container to process data from bronze to silver layer',
    schedule="@hourly",
    start_date = datetime(2026, 2, 20),
    catchup=False,
    tags=['pyspark', 'silver-layer', 'data_processing'], ) as dag:
    
    # "docker exec" runs the script inside  SPARK container
    run_silver_pyspark_job = BashOperator(
        task_id='trigger_spark_processing',
        bash_command='docker exec ironman-spark spark-submit /home/jovyan/src/processing/process_data.py',    )
    
    run_silver_pyspark_job