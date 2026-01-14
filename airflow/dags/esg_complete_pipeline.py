from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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
    'esg_complete_pipeline',
    default_args=default_args,
    description='Complete ESG Analytics Pipeline: Bronze → Silver → Gold',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
)

clean_esg_score = BashOperator(
    task_id='clean_esg_score',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/clean_esg_score.py',
    dag=dag,
)

clean_industrials_esg = BashOperator(
    task_id='clean_industrials_esg',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/clean_industrials_esg_score.py',
    dag=dag,
)

clean_rank = BashOperator(
    task_id='clean_rank',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/clean_rank.py',
    dag=dag,
)

clean_rank_risk = BashOperator(
    task_id='clean_rank_risk',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/clean_rank_risk.py',
    dag=dag,
)

extract_kpi_pdf = BashOperator(
    task_id='extract_kpi_pdf',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/extract_kpi_pdf.py',
    dag=dag,
)

extract_kpi_xlsx = BashOperator(
    task_id='extract_kpi_xlsx',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/extract_kpi_xlsx.py',
    dag=dag,
)

clean_kpi_csv = BashOperator(
    task_id='clean_kpi_csv',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/clean_kpi_csv.py',
    dag=dag,
)

clean_kpi_excel = BashOperator(
    task_id='clean_kpi_excel',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/clean_kpi_excel.py',
    dag=dag,
)

clean_kpi_pdf = BashOperator(
    task_id='clean_kpi_pdf',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/clean_kpi_pdf.py',
    dag=dag,
)

merge_kpi = BashOperator(
    task_id='merge_kpi_data',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/merge_kpi_data.py',
    dag=dag,
)

classify_metrics = BashOperator(
    task_id='classify_metrics',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/classified_kpi.py',
    dag=dag,
)

semantic_classification = BashOperator(
    task_id='semantic_classification',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/sematic.py',
    dag=dag,
)

normalize_kpi = BashOperator(
    task_id='normalize_kpi',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/normalize_kpi.py',
    dag=dag,
)

normalize_final = BashOperator(
    task_id='normalize_final',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/normalize_final.py',
    dag=dag,
)

staging_companies = BashOperator(
    task_id='staging_companies_mapping',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/staging_companies_mapping.py',
    dag=dag,
)

staging_metrics = BashOperator(
    task_id='staging_metric_mapping',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/staging_metric_mapping.py',
    dag=dag,
)

staging_units = BashOperator(
    task_id='staging_unit_mapping',
    bash_command='docker exec spark-submit /opt/spark/bin/spark-submit /opt/spark-apps/silver/staging_unit_mapping.py',
    dag=dag,
)

drop_gold_tables = BashOperator(
    task_id='drop_gold_tables',
    bash_command='''docker exec trino trino --execute "
        DROP TABLE IF EXISTS delta.gold_marts.dim_company;
        DROP TABLE IF EXISTS delta.gold_marts.dim_metric;
        DROP TABLE IF EXISTS delta.gold_marts.dim_unit;
        DROP TABLE IF EXISTS delta.gold_marts.dim_date;
        DROP TABLE IF EXISTS delta.gold_marts.fact_esg_metric;
        DROP TABLE IF EXISTS delta.gold_marts.fact_esg_score_risk;
    "''',
    dag=dag,
)

dbt_dim_company = BashOperator(
    task_id='dbt_dim_company',
    bash_command='docker exec dbt dbt run --models dim_company --project-dir /usr/app',
    dag=dag,
)

dbt_dim_metric = BashOperator(
    task_id='dbt_dim_metric',
    bash_command='docker exec dbt dbt run --models dim_metric --project-dir /usr/app',
    dag=dag,
)

dbt_dim_unit = BashOperator(
    task_id='dbt_dim_unit',
    bash_command='docker exec dbt dbt run --models dim_unit --project-dir /usr/app',
    dag=dag,
)

dbt_dim_date = BashOperator(
    task_id='dbt_dim_date',
    bash_command='docker exec dbt dbt run --models dim_date --project-dir /usr/app',
    dag=dag,
)

dbt_fact_esg_metric = BashOperator(
    task_id='dbt_fact_esg_metric',
    bash_command='docker exec dbt dbt run --models fact_esg_metric --project-dir /usr/app',
    dag=dag,
)

dbt_fact_esg_score_risk = BashOperator(
    task_id='dbt_fact_esg_score_risk',
    bash_command='docker exec dbt dbt run --models fact_esg_score_risk --project-dir /usr/app',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test_all',
    bash_command='docker exec dbt dbt test --project-dir /usr/app',
    dag=dag,
)

def check_data_quality(**context):
    from trino.dbapi import connect
    
    conn = connect(
        host='trino',
        port=8080,
        user='user',
        catalog='delta',
        schema='gold_marts'
    )
    cursor = conn.cursor()
    
    checks = []
    
    cursor.execute("SELECT COUNT(*) FROM dim_company")
    company_count = cursor.fetchone()[0]
    checks.append(('dim_company', company_count, company_count > 0))
    
    cursor.execute("SELECT COUNT(*) FROM dim_metric")
    metric_count = cursor.fetchone()[0]
    checks.append(('dim_metric', metric_count, metric_count > 0))
    
    cursor.execute("SELECT COUNT(*) FROM fact_esg_metric")
    fact_metric_count = cursor.fetchone()[0]
    checks.append(('fact_esg_metric', fact_metric_count, fact_metric_count > 0))
    
    cursor.execute("""
        SELECT company_id, metric_id, year, COUNT(*) as cnt
        FROM fact_esg_metric
        GROUP BY company_id, metric_id, year
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    checks.append(('fact_esg_metric_duplicates', len(duplicates), len(duplicates) == 0))
    
    cursor.close()
    conn.close()
    
    failed_checks = [c for c in checks if not c[2]]
    
    if failed_checks:
        raise ValueError(f"Data quality checks failed: {failed_checks}")
    
    print(f"All data quality checks passed: {checks}")

data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality,
    dag=dag,
)

refresh_streamlit = BashOperator(
    task_id='refresh_streamlit_cache',
    bash_command='docker exec streamlit-app python -c "import streamlit.web.cli as stcli"',
    dag=dag,
)

# Bronze to Silver - Parallel Cleaning
[clean_esg_score, clean_industrials_esg, clean_rank, clean_rank_risk] >> staging_companies

# KPI Pipeline
extract_kpi_pdf >> clean_kpi_pdf
extract_kpi_xlsx >> clean_kpi_excel
clean_kpi_csv >> merge_kpi
clean_kpi_excel >> merge_kpi
clean_kpi_pdf >> merge_kpi

# KPI Processing
merge_kpi >> classify_metrics >> semantic_classification >> normalize_kpi >> normalize_final

# Staging Tables
normalize_final >> staging_metrics
normalize_final >> staging_units

# Drop tables before creating dimensions
[staging_companies, staging_metrics, staging_units] >> drop_gold_tables

# Dimensions (parallel, after drop)
drop_gold_tables >> dbt_dim_company
drop_gold_tables >> dbt_dim_metric
drop_gold_tables >> dbt_dim_unit
drop_gold_tables >> dbt_dim_date

# Facts (after all dimensions ready)
[dbt_dim_company, dbt_dim_metric, dbt_dim_unit, dbt_dim_date] >> dbt_fact_esg_metric
[dbt_dim_company, dbt_dim_metric, dbt_dim_unit, dbt_dim_date] >> dbt_fact_esg_score_risk

# Testing & Quality
[dbt_fact_esg_metric, dbt_fact_esg_score_risk] >> dbt_test >> data_quality_check

# Final refresh
data_quality_check >> refresh_streamlit