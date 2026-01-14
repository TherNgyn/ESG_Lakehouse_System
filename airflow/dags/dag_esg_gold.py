from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'esg_analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'esg_gold_layer_only',
    default_args=default_args,
    description='ESG Gold Layer - dbt dimensional models only',
    schedule_interval='@daily',
    catchup=False,
)

# Dimension tables
dbt_dim_company = BashOperator(
    task_id='dbt_dim_company',
    bash_command='docker exec dbt dbt run --models dim_company --full-refresh --project-dir /usr/app',
    dag=dag,
)

dbt_dim_metric = BashOperator(
    task_id='dbt_dim_metric',
    bash_command='docker exec dbt dbt run --models dim_metric --full-refresh --project-dir /usr/app',
    dag=dag,
)

dbt_dim_unit = BashOperator(
    task_id='dbt_dim_unit',
    bash_command='docker exec dbt dbt run --models dim_unit --full-refresh --project-dir /usr/app',
    dag=dag,
)

dbt_dim_date = BashOperator(
    task_id='dbt_dim_date',
    bash_command='docker exec dbt dbt run --models dim_date --full-refresh --project-dir /usr/app',
    dag=dag,
)

# Fact tables
dbt_fact_esg_metric = BashOperator(
    task_id='dbt_fact_esg_metric',
    bash_command='docker exec dbt dbt run --models fact_esg_metric --full-refresh --project-dir /usr/app',
    dag=dag,
)

dbt_fact_esg_score = BashOperator(
    task_id='dbt_fact_esg_score',
    bash_command='docker exec dbt dbt run --models fact_esg_score_risk --full-refresh --project-dir /usr/app',
    dag=dag,
)

dbt_fact_esg_rank = BashOperator(
    task_id='dbt_fact_esg_rank',
    bash_command='docker exec dbt dbt run --models fact_esg_rank --full-refresh --project-dir /usr/app',
    dag=dag,
)

dbt_fact_esg_metric_cal = BashOperator(
    task_id='dbt_fact_esg_metric_cal',
    bash_command='docker exec dbt dbt run --models fact_esg_metric_cal --full-refresh --project-dir /usr/app',
    dag=dag,
)

# Test task
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='docker exec dbt dbt test --project-dir /usr/app',
    dag=dag,
)

# Verify task
def verify_gold_layer(**context):
    from trino.dbapi import connect
    
    conn = connect(
        host='trino',
        port=8080,
        user='user',
        catalog='delta',
        schema='default_marts'
    )
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM dim_company")
    companies = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dim_metric")
    metrics = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM fact_esg_metric")
    facts = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    print(f"Gold Layer Summary:")
    print(f"  - Companies: {companies:,}")
    print(f"  - Metrics: {metrics:,}")
    print(f"  - Facts: {facts:,}")
    
    if companies == 0 or metrics == 0 or facts == 0:
        raise ValueError("Gold layer has empty tables!")

verify_gold = PythonOperator(
    task_id='verify_gold_layer',
    python_callable=verify_gold_layer,
    dag=dag,
)

# Task dependencies
[dbt_dim_company, dbt_dim_metric, dbt_dim_unit, dbt_dim_date] >> dbt_fact_esg_metric
[dbt_dim_company, dbt_dim_metric, dbt_dim_unit, dbt_dim_date] >> dbt_fact_esg_score
[dbt_dim_company, dbt_dim_metric, dbt_dim_unit, dbt_dim_date] >> dbt_fact_esg_rank
[dbt_fact_esg_metric] >> dbt_fact_esg_metric_cal
[dbt_fact_esg_metric, dbt_fact_esg_score, dbt_fact_esg_rank, dbt_fact_esg_metric_cal] >> dbt_test >> verify_gold