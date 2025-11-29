from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
from io import BytesIO
import json

default_args = {
    'owner': 'esg_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'esg_lakehouse_pipeline',
    default_args=default_args,
    description='ESG Data Pipeline: Bronze -> Silver -> Gold',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['esg', 'lakehouse', 'etl'],
)

# MinIO Configuration
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'admin123456'

def get_s3_client():
    """Initialize MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url=f'http://{MINIO_ENDPOINT}',
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

# ========== BRONZE LAYER ==========
def ingest_corporate_kpi_data(**context):
    """Ingest corporate ESG KPI data to Bronze layer"""
    s3_client = get_s3_client()
    
    # Example: Simulating data ingestion
    sample_data = {
        'company_name': ['Nestlé', 'American Airlines'],
        'kpi_name': ['GHG Emissions Reduction', 'Renewable Energy'],
        'year': [2023, 2023],
        'value': [20.38, 75.5],
        'unit': ['%', '%'],
        'baseline_year': [2018, 2018],
        'ingestion_timestamp': [datetime.now(), datetime.now()]
    }
    
    df = pd.DataFrame(sample_data)
    
    # Save to Bronze as Parquet
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    date_str = datetime.now().strftime('%Y%m%d')
    s3_client.put_object(
        Bucket='bronze',
        Key=f'corporate_esg/kpi_metrics/date={date_str}/data.parquet',
        Body=buffer.getvalue()
    )
    
    print(f"✅ Ingested {len(df)} records to Bronze layer")
    return len(df)

def ingest_esg_risk_scores(**context):
    """Ingest ESG risk scores to Bronze layer"""
    s3_client = get_s3_client()
    
    sample_risk_data = {
        'company_ticker': ['AAL', 'NESN'],
        'year': [2021, 2023],
        'overall_score': [59.03, 75.5],
        'environmental_score': [64.29, 78.4],
        'social_score': [56.40, 68.4],
        'governance_score': [54.39, 72.1],
        'ingestion_timestamp': [datetime.now(), datetime.now()]
    }
    
    df = pd.DataFrame(sample_risk_data)
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    date_str = datetime.now().strftime('%Y%m%d')
    s3_client.put_object(
        Bucket='bronze',
        Key=f'corporate_esg/esg_risk_scores/date={date_str}/data.parquet',
        Body=buffer.getvalue()
    )
    
    print(f"✅ Ingested {len(df)} risk score records to Bronze")
    return len(df)

def ingest_country_esg_data(**context):
    """Ingest country-level ESG indicators to Bronze layer"""
    s3_client = get_s3_client()
    
    sample_country_data = {
        'country_code': ['VNM', 'USA', 'CHN'],
        'country_name': ['Vietnam', 'United States', 'China'],
        'year': [2023, 2023, 2023],
        'pillar': ['E', 'S', 'G'],
        'indicator_name': ['CO2 Emissions per Capita', 'HDI Index', 'Corruption Index'],
        'value': [3.5, 0.921, 42],
        'global_rank': [78, 21, 87],
        'ingestion_timestamp': [datetime.now(), datetime.now(), datetime.now()]
    }
    
    df = pd.DataFrame(sample_country_data)
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    date_str = datetime.now().strftime('%Y%m%d')
    s3_client.put_object(
        Bucket='bronze',
        Key=f'country_esg/all_indicators/date={date_str}/data.parquet',
        Body=buffer.getvalue()
    )
    
    print(f"✅ Ingested {len(df)} country ESG records to Bronze")
    return len(df)

# ========== SILVER LAYER ==========
def clean_corporate_kpi_data(**context):
    """Clean and standardize corporate KPI data"""
    s3_client = get_s3_client()
    
    # Read from Bronze
    date_str = datetime.now().strftime('%Y%m%d')
    response = s3_client.get_object(
        Bucket='bronze',
        Key=f'corporate_esg/kpi_metrics/date={date_str}/data.parquet'
    )
    
    df = pd.read_parquet(BytesIO(response['Body'].read()))
    
    # Data Cleaning
    df['company_name'] = df['company_name'].str.strip().str.upper()
    df['kpi_name'] = df['kpi_name'].str.strip()
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value'])
    
    # Add data quality score
    df['data_quality_score'] = 1.0  # Simplified
    df['processing_timestamp'] = datetime.now()
    
    # Save to Silver
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket='silver',
        Key=f'corporate_esg_cleaned/kpi_standardized/date={date_str}/data.parquet',
        Body=buffer.getvalue()
    )
    
    print(f"✅ Cleaned {len(df)} KPI records to Silver layer")
    return len(df)

def normalize_esg_risk_scores(**context):
    """Normalize ESG risk scores (0-100 scale)"""
    s3_client = get_s3_client()
    
    date_str = datetime.now().strftime('%Y%m%d')
    response = s3_client.get_object(
        Bucket='bronze',
        Key=f'corporate_esg/esg_risk_scores/date={date_str}/data.parquet'
    )
    
    df = pd.read_parquet(BytesIO(response['Body'].read()))
    
    # Normalize scores to 0-100
    score_columns = ['overall_score', 'environmental_score', 'social_score', 'governance_score']
    for col in score_columns:
        df[f'{col}_normalized'] = (df[col] / df[col].max()) * 100
    
    # Classify risk levels
    def classify_risk(score):
        if score >= 75:
            return 'Low Risk'
        elif score >= 50:
            return 'Medium Risk'
        elif score >= 25:
            return 'High Risk'
        else:
            return 'Critical Risk'
    
    df['risk_level'] = df['overall_score'].apply(classify_risk)
    df['processing_timestamp'] = datetime.now()
    
    # Save to Silver
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket='silver',
        Key=f'corporate_esg_cleaned/risk_scores_validated/date={date_str}/data.parquet',
        Body=buffer.getvalue()
    )
    
    print(f"✅ Normalized {len(df)} risk scores to Silver layer")
    return len(df)

def validate_country_esg_data(**context):
    """Validate and enrich country ESG data"""
    s3_client = get_s3_client()
    
    date_str = datetime.now().strftime('%Y%m%d')
    response = s3_client.get_object(
        Bucket='bronze',
        Key=f'country_esg/all_indicators/date={date_str}/data.parquet'
    )
    
    df = pd.read_parquet(BytesIO(response['Body'].read()))
    
    # Validation
    df = df[df['value'].notna()]
    df['country_code'] = df['country_code'].str.upper()
    
    # Add regional classification
    region_mapping = {
        'VNM': 'Southeast Asia',
        'USA': 'North America',
        'CHN': 'East Asia'
    }
    df['region'] = df['country_code'].map(region_mapping)
    df['processing_timestamp'] = datetime.now()
    
    # Save to Silver
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket='silver',
        Key=f'country_esg_cleaned/all_normalized/date={date_str}/data.parquet',
        Body=buffer.getvalue()
    )
    
    print(f"✅ Validated {len(df)} country ESG records to Silver layer")
    return len(df)

# ========== GOLD LAYER ==========
def load_to_gold_fact_tables(**context):
    """Load data to Gold layer fact tables in PostgreSQL"""
    from sqlalchemy import create_engine
    import psycopg2
    
    # PostgreSQL connection
    engine = create_engine('postgresql://esg_user:esg_password@postgres:5432/esg_lakehouse')
    
    s3_client = get_s3_client()
    date_str = datetime.now().strftime('%Y%m%d')
    
    # Load KPI metrics to fact table
    response = s3_client.get_object(
        Bucket='silver',
        Key=f'corporate_esg_cleaned/kpi_standardized/date={date_str}/data.parquet'
    )
    df_kpi = pd.read_parquet(BytesIO(response['Body'].read()))
    
    # Transform for fact table
    df_kpi['metric_id'] = range(1, len(df_kpi) + 1)
    df_kpi['company_id'] = 1  # Simplified - should lookup from dim_company
    df_kpi['date_id'] = int(datetime.now().strftime('%Y%m%d'))
    df_kpi['kpi_id'] = 1  # Simplified
    
    # Load to PostgreSQL
    df_kpi.to_sql('fact_corporate_esg_metrics', engine, if_exists='append', index=False)
    
    print(f"✅ Loaded {len(df_kpi)} records to Gold fact_corporate_esg_metrics")
    
    # Load risk scores
    response = s3_client.get_object(
        Bucket='silver',
        Key=f'corporate_esg_cleaned/risk_scores_validated/date={date_str}/data.parquet'
    )
    df_risk = pd.read_parquet(BytesIO(response['Body'].read()))
    
    df_risk['risk_id'] = range(1, len(df_risk) + 1)
    df_risk['company_id'] = 1
    df_risk['date_id'] = int(datetime.now().strftime('%Y%m%d'))
    
    df_risk.to_sql('fact_esg_risk_scores', engine, if_exists='append', index=False)
    
    print(f"✅ Loaded {len(df_risk)} records to Gold fact_esg_risk_scores")
    
    return {'kpi_records': len(df_kpi), 'risk_records': len(df_risk)}

def update_gold_dimensions(**context):
    """Update dimension tables in Gold layer"""
    from sqlalchemy import create_engine
    
    engine = create_engine('postgresql://esg_user:esg_password@postgres:5432/esg_lakehouse')
    
    # Update dim_date
    date_data = {
        'date_id': [int(datetime.now().strftime('%Y%m%d'))],
        'full_date': [datetime.now().date()],
        'year': [datetime.now().year],
        'quarter': [(datetime.now().month - 1) // 3 + 1],
        'month': [datetime.now().month],
        'fiscal_year': [datetime.now().year],
        'is_reporting_period': [True]
    }
    
    df_date = pd.DataFrame(date_data)
    df_date.to_sql('dim_date', engine, if_exists='append', index=False)
    
    print("✅ Updated dimension tables in Gold layer")
    return True

# ========== QUALITY CHECKS ==========
def data_quality_check(**context):
    """Run data quality checks"""
    s3_client = get_s3_client()
    date_str = datetime.now().strftime('%Y%m%d')
    
    quality_report = {
        'timestamp': datetime.now().isoformat(),
        'checks': []
    }
    
    # Check Silver layer data
    try:
        response = s3_client.get_object(
            Bucket='silver',
            Key=f'corporate_esg_cleaned/kpi_standardized/date={date_str}/data.parquet'
        )
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        
        quality_report['checks'].append({
            'layer': 'silver',
            'dataset': 'kpi_standardized',
            'record_count': len(df),
            'null_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
            'status': 'PASS'
        })
    except Exception as e:
        quality_report['checks'].append({
            'layer': 'silver',
            'dataset': 'kpi_standardized',
            'status': 'FAIL',
            'error': str(e)
        })
    
    # Save quality report
    report_json = json.dumps(quality_report, indent=2)
    s3_client.put_object(
        Bucket='metadata',
        Key=f'quality_reports/date={date_str}/report.json',
        Body=report_json.encode('utf-8')
    )
    
    print(f"✅ Data quality check completed")
    return quality_report

# ========== DAG TASK DEFINITIONS ==========

# Bronze Layer Tasks
ingest_kpi_task = PythonOperator(
    task_id='ingest_corporate_kpi_data',
    python_callable=ingest_corporate_kpi_data,
    dag=dag,
)

ingest_risk_task = PythonOperator(
    task_id='ingest_esg_risk_scores',
    python_callable=ingest_esg_risk_scores,
    dag=dag,
)

ingest_country_task = PythonOperator(
    task_id='ingest_country_esg_data',
    python_callable=ingest_country_esg_data,
    dag=dag,
)

# Silver Layer Tasks
clean_kpi_task = PythonOperator(
    task_id='clean_corporate_kpi_data',
    python_callable=clean_corporate_kpi_data,
    dag=dag,
)

normalize_risk_task = PythonOperator(
    task_id='normalize_esg_risk_scores',
    python_callable=normalize_esg_risk_scores,
    dag=dag,
)

validate_country_task = PythonOperator(
    task_id='validate_country_esg_data',
    python_callable=validate_country_esg_data,
    dag=dag,
)

# Gold Layer Tasks
load_gold_task = PythonOperator(
    task_id='load_to_gold_fact_tables',
    python_callable=load_to_gold_fact_tables,
    dag=dag,
)

update_dims_task = PythonOperator(
    task_id='update_gold_dimensions',
    python_callable=update_gold_dimensions,
    dag=dag,
)

# Quality Check Task
quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag,
)

# ========== DAG WORKFLOW ==========
# Bronze -> Silver -> Gold pipeline
ingest_kpi_task >> clean_kpi_task
ingest_risk_task >> normalize_risk_task
ingest_country_task >> validate_country_task

[clean_kpi_task, normalize_risk_task, validate_country_task] >> load_gold_task
load_gold_task >> update_dims_task >> quality_check_task