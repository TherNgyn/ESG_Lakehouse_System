from pathlib import Path
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
import sys
from airflow.decorators import task

sys.path.append('/opt/airflow')
from scripts.bronze.api import load_files_to_Bronze_Layer, load_extracted_data_to_Bronze_Layer
from scripts.crawling_pdf_files import crawl_pdfs
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
    # task_0 = PythonOperator(
    #     task_id = "Craw_pdf_files",
    #     python_callable= crawl_pdfs
    # )
    task_1 =PythonOperator(
        task_id ="Load_pdf_files_to_Bronze_Layer",
        python_callable= load_files_to_Bronze_Layer
    )
    task_2 = PythonOperator(
        task_id = 'load_extracted_data_to_Bronze_Layer',
        python_callable= load_extracted_data_to_Bronze_Layer
    )
task_2
