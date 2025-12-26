from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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

# ============================================================================
# BRONZE TO SILVER: Data Cleaning & Transformation
# ============================================================================

clean_esg_score = SparkSubmitOperator(
    task_id='clean_esg_score',
    application='/opt/spark-apps/clean_esg_score.py',
    conn_id='spark_default',
    conf={
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'admin',
        'spark.hadoop.fs.s3a.secret.key': 'admin123456'
    },
    dag=dag,
)

clean_industrials_esg = SparkSubmitOperator(
    task_id='clean_industrials_esg',
    application='/opt/spark-apps/clean_industrials_esg_score.py',
    conn_id='spark_default',
    dag=dag,
)

clean_rank = SparkSubmitOperator(
    task_id='clean_rank',
    application='/opt/spark-apps/clean_rank.py',
    conn_id='spark_default',
    dag=dag,
)

clean_rank_risk = SparkSubmitOperator(
    task_id='clean_rank_risk',
    application='/opt/spark-apps/clean_rank_risk.py',
    conn_id='spark_default',
    dag=dag,
)

# KPI Extraction & Cleaning
extract_kpi_pdf = SparkSubmitOperator(
    task_id='extract_kpi_pdf',
    application='/opt/spark-apps/extract_kpi_pdf.py',
    conn_id='spark_default',
    dag=dag,
)

extract_kpi_xlsx = SparkSubmitOperator(
    task_id='extract_kpi_xlsx',
    application='/opt/spark-apps/extract_kpi_xlsx.py',
    conn_id='spark_default',
    dag=dag,
)

clean_kpi_csv = SparkSubmitOperator(
    task_id='clean_kpi_csv',
    application='/opt/spark-apps/clean_kpi_csv.py',
    conn_id='spark_default',
    dag=dag,
)

clean_kpi_excel = SparkSubmitOperator(
    task_id='clean_kpi_excel',
    application='/opt/spark-apps/clean_kpi_excel.py',
    conn_id='spark_default',
    dag=dag,
)

clean_kpi_pdf = SparkSubmitOperator(
    task_id='clean_kpi_pdf',
    application='/opt/spark-apps/clean_kpi_pdf.py',
    conn_id='spark_default',
    dag=dag,
)

# Merge all KPI sources
merge_kpi = SparkSubmitOperator(
    task_id='merge_kpi_data',
    application='/opt/spark-apps/merge_kpi_data.py',
    conn_id='spark_default',
    dag=dag,
)

# ============================================================================
# SILVER: Semantic Classification & Normalization
# ============================================================================

classify_metrics = SparkSubmitOperator(
    task_id='classify_metrics',
    application='/opt/spark-apps/classified_kpi.py',
    conn_id='spark_default',
    dag=dag,
)

semantic_classification = SparkSubmitOperator(
    task_id='semantic_classification',
    application='/opt/spark-apps/sematic.py',
    conn_id='spark_default',
    dag=dag,
)

normalize_kpi = SparkSubmitOperator(
    task_id='normalize_kpi',
    application='/opt/spark-apps/normalize_kpi.py',
    conn_id='spark_default',
    dag=dag,
)

normalize_final = SparkSubmitOperator(
    task_id='normalize_final',
    application='/opt/spark-apps/normalize_final.py',
    conn_id='spark_default',
    dag=dag,
)

# ============================================================================
# SILVER: Staging Tables (Dimensions Preparation)
# ============================================================================

staging_companies = SparkSubmitOperator(
    task_id='staging_companies_mapping',
    application='/opt/spark-apps/staging_companies_mapping.py',
    conn_id='spark_default',
    dag=dag,
)

staging_metrics = SparkSubmitOperator(
    task_id='staging_metric_mapping',
    application='/opt/spark-apps/staging_metric_mapping.py',
    conn_id='spark_default',
    dag=dag,
)

staging_units = SparkSubmitOperator(
    task_id='staging_unit_mapping',
    application='/opt/spark-apps/staging_unit_mapping.py',
    conn_id='spark_default',
    dag=dag,
)

# ============================================================================
# GOLD: dbt Dimensional Models
# ============================================================================

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

dbt_fact_esg_metric = BashOperator(
    task_id='dbt_fact_esg_metric',
    bash_command='docker exec dbt dbt run --models fact_esg_metric --full-refresh --project-dir /usr/app',
    dag=dag,
)

dbt_fact_esg_score_risk = BashOperator(
    task_id='dbt_fact_esg_score_risk',
    bash_command='docker exec dbt dbt run --models fact_esg_score_risk --full-refresh --project-dir /usr/app',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test_all',
    bash_command='docker exec dbt dbt test --project-dir /usr/app',
    dag=dag,
)

# ============================================================================
# DATA QUALITY CHECKS
# ============================================================================

def check_data_quality(**context):
    from trino.dbapi import connect
    
    conn = connect(
        host='trino',
        port=8080,
        user='user',
        catalog='delta',
        schema='default_marts'
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

# ============================================================================
# REFRESH ANALYTICS DASHBOARDS
# ============================================================================

refresh_streamlit = BashOperator(
    task_id='refresh_streamlit_cache',
    bash_command='docker exec streamlit-app python -c "import streamlit.web.cli as stcli"',
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

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

# Dimensions (parallel)
staging_companies >> dbt_dim_company
staging_metrics >> dbt_dim_metric
staging_units >> dbt_dim_unit
staging_companies >> dbt_dim_date

# Facts (after all dimensions ready)
[dbt_dim_company, dbt_dim_metric, dbt_dim_unit, dbt_dim_date] >> dbt_fact_esg_metric
[dbt_dim_company, dbt_dim_metric, dbt_dim_unit, dbt_dim_date] >> dbt_fact_esg_score_risk

# Testing & Quality
[dbt_fact_esg_metric, dbt_fact_esg_score_risk] >> dbt_test >> data_quality_check

# Final refresh
data_quality_check >> refresh_streamlit