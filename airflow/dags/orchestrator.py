from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
import sys

sys.path.append('/opt/airflow')
from scripts.reading_pdf_file_demo import read_pdf

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
    schedule=timedelta(minutes=5)
)

with dag:
    task1 = PythonOperator(
        task_id= 'read_pdf',
        python_callable = read_pdf
    )