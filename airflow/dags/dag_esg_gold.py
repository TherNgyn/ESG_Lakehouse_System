from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'esg_analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'esg_gold_layer_pipeline',
    default_args=default_args,
    description='ESG Analytics Gold Layer Pipeline',
    schedule_interval='@daily',
    catchup=False,
)

create_staging_metrics = SparkSubmitOperator(
    task_id='create_staging_metrics',
    application='/opt/airflow/spark-apps/create_staging_metrics.py',
    conn_id='spark_default',
    dag=dag,
)

dbt_dimensions = BashOperator(
    task_id='dbt_run_dimensions',
    bash_command='cd /workspace/dbt && dbt run --models dim_company dim_metric dim_unit dim_date',
    dag=dag,
)

dbt_fact_metrics = BashOperator(
    task_id='dbt_run_fact_metrics',
    bash_command='cd /workspace/dbt && dbt run --models fact_esg_metric',
    dag=dag,
)

dbt_fact_scores = BashOperator(
    task_id='dbt_run_fact_scores',
    bash_command='cd /workspace/dbt && dbt run --models fact_esg_score',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /workspace/dbt && dbt test',
    dag=dag,
)

create_staging_metrics >> dbt_dimensions >> dbt_fact_metrics >> dbt_fact_scores >> dbt_test