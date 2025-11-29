from pathlib import Path
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
from airflow.decorators import task

sys.path.append('/opt/airflow')

default_args = {
    'description' : "A DAG to orchestrate data",
    'start_date': datetime(2025, 10, 20),
    'catchup' :False,
}


def example():
    print("this is example task")

dag = DAG(
    dag_id="ETL_ESG_Data",
    default_args= default_args,
    schedule= None
)
with dag:
    task1 = BashOperator(
        task_id = "run_spark_job",
        bash_command = "docker exec spark-master /opt/spark/bin/spark-submit "\
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "\
            "/opt/spark/spark-apps/bronze_to_silver.py  "
    )
