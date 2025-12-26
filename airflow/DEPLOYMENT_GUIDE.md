# AIRFLOW DEPLOYMENT GUIDE

## ARCHITECTURE

```
DAG 1: esg_complete_pipeline
├── Bronze → Silver (Data Cleaning)
├── Silver → Silver (Semantic Processing)  
├── Silver → Gold (dbt Dimensional Models)
└── Quality Checks + Dashboard Refresh

DAG 2: esg_gold_layer_only
├── Silver → Gold (dbt only)
└── Quality Checks
```

## DEPLOYMENT

### 1. Copy DAG files

```bash
# Complete pipeline
cp airflow/esg_complete_pipeline.py /path/to/airflow/dags/

# Gold layer only
cp airflow/esg_gold_layer_only.py /path/to/airflow/dags/
```

### 2. Install Airflow dependencies

```bash
pip install apache-airflow-providers-apache-spark
pip install trino
```

### 3. Configure Spark connection

In Airflow UI:
- **Admin** → **Connections** → **Add**
- Connection Id: `spark_default`
- Connection Type: `Spark`
- Host: `spark://spark-master:7077`
- Extra: `{"queue": "default"}`

### 4. Configure Trino connection (for quality checks)

Connection already configured in Python code:
```python
conn = connect(
    host='trino',
    port=8080,
    user='user',
    catalog='delta',
    schema='default_marts'
)
```

## USAGE

### Option 1: Complete Pipeline (Bronze → Gold)

**When to use:**
- First time setup
- After adding new raw data
- Weekly/Monthly full refresh

**Trigger:**
```bash
airflow dags trigger esg_complete_pipeline
```

**Duration:** ~2-4 hours (depends on data volume)

**Steps:**
1. Clean raw ESG scores/ranks (Bronze → Silver)
2. Extract & clean KPI from CSV/Excel/PDF
3. Merge all KPI sources
4. Classify metrics by topic/group
5. Apply semantic analysis
6. Normalize metric names
7. Create staging tables
8. Build dbt dimensions
9. Build dbt facts
10. Run tests
11. Quality checks
12. Refresh Streamlit

### Option 2: Gold Layer Only (Silver → Gold)

**When to use:**
- Daily incremental updates
- dbt model changes
- Quick refresh

**Trigger:**
```bash
airflow dags trigger esg_gold_layer_only
```

**Duration:** ~15-30 minutes

**Steps:**
1. Run dbt dimensions (parallel)
2. Run dbt facts
3. Run dbt tests
4. Quality checks

## SCHEDULE

### Recommended Schedules

**Complete Pipeline:**
```python
schedule_interval='@weekly'  # Every Sunday at midnight
```

**Gold Layer Only:**
```python
schedule_interval='@daily'  # Every day at midnight
```

### Manual Triggers

```bash
# Trigger with specific date
airflow dags trigger esg_complete_pipeline --conf '{"execution_date": "2024-12-26"}'

# Backfill historical dates
airflow dags backfill esg_gold_layer_only \
  --start-date 2024-01-01 \
  --end-date 2024-12-26
```

## MONITORING

### View DAG Status

```bash
# List all DAGs
airflow dags list

# Show DAG tree
airflow dags show esg_complete_pipeline

# List task instances
airflow tasks list esg_complete_pipeline --tree
```

### Check Logs

```bash
# View task logs
airflow tasks logs esg_complete_pipeline dbt_fact_esg_metric 2024-12-26

# View latest run
airflow dags list-runs -d esg_complete_pipeline
```

### Data Quality Checks

The `data_quality_check` task verifies:

1. **Table Counts:**
   - dim_company > 0
   - dim_metric > 0
   - fact_esg_metric > 0

2. **Duplicates:**
   - No duplicate (company, metric, year) in fact_esg_metric

3. **Referential Integrity:**
   - All company_id in facts exist in dim_company
   - All metric_id in facts exist in dim_metric

If ANY check fails → Pipeline stops

## TASK DEPENDENCIES

### Complete Pipeline Flow

```
┌─────────────────┐
│  Bronze → Silver│
│  (Parallel)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   KPI Merge     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Classification  │
│   & Semantic    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Normalization   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Staging Tables  │
│   (Parallel)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ dbt Dimensions  │
│   (Parallel)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  dbt Facts      │
│   (Parallel)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  dbt Tests      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Quality Checks  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│Refresh Dashboards│
└─────────────────┘
```

### Gold Layer Only Flow

```
┌─────────────────┐
│ dbt Dimensions  │
│   (Parallel)    │
│ - company       │
│ - metric        │
│ - unit          │
│ - date          │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  dbt Facts      │
│   (Parallel)    │
│ - esg_metric    │
│ - esg_score     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  dbt Tests      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Quality Checks  │
└─────────────────┘
```

## TROUBLESHOOTING

### Issue: Spark tasks fail

```bash
# Check Spark connection
docker logs spark-master

# Test Spark connection
airflow tasks test esg_complete_pipeline clean_esg_score 2024-12-26
```

### Issue: dbt tasks fail

```bash
# Run dbt manually
docker exec dbt dbt run --models dim_company --project-dir /usr/app

# Check dbt logs
docker exec dbt dbt debug --project-dir /usr/app
```

### Issue: Quality checks fail

```bash
# Run quality check manually
docker exec -it airflow-webserver python << EOF
from trino.dbapi import connect
conn = connect(host='trino', port=8080, user='user', catalog='delta', schema='default_marts')
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM dim_company")
print(cursor.fetchone())
EOF
```

### Issue: Tasks stuck in "queued"

```bash
# Increase Airflow parallelism
# Edit airflow.cfg:
parallelism = 32
dag_concurrency = 16
max_active_runs_per_dag = 3

# Restart Airflow
docker-compose restart airflow-webserver airflow-scheduler
```

## OPTIMIZATION

### Speed up Complete Pipeline

1. **Use partitioning:**
```python
# In Spark tasks, add partition hint
.repartition(8, "year")
```

2. **Cache intermediate results:**
```python
df = df.cache()
```

3. **Run dbt incrementally:**
```bash
dbt run --models fact_esg_metric+ --select state:modified+
```

### Speed up Gold Layer

1. **Use --full-refresh only when needed:**
```bash
# Normal run (incremental)
docker exec dbt dbt run --models fact_esg_metric --project-dir /usr/app

# Full refresh (slower, complete rebuild)
docker exec dbt dbt run --models fact_esg_metric --full-refresh --project-dir /usr/app
```

2. **Parallel dbt runs:**
```bash
docker exec dbt dbt run --models dim_company dim_metric dim_unit dim_date --threads 4 --project-dir /usr/app
```

## BEST PRACTICES

1. **Always test locally first:**
```bash
airflow dags test esg_gold_layer_only 2024-12-26
```

2. **Use sensors for dependencies:**
```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_bronze = ExternalTaskSensor(
    task_id='wait_for_bronze_ready',
    external_dag_id='bronze_ingestion_dag',
    external_task_id='bronze_ready',
)
```

3. **Add SLA monitoring:**
```python
default_args = {
    'sla': timedelta(hours=4),
    'email_on_failure': True,
    'email': ['data-team@company.com'],
}
```

4. **Use XCom for metrics:**
```python
def push_metrics(**context):
    metrics = {'companies': 150, 'metrics': 500}
    context['task_instance'].xcom_push(key='gold_metrics', value=metrics)

push_task = PythonOperator(
    task_id='push_gold_metrics',
    python_callable=push_metrics,
)
```

## FILES

- `esg_complete_pipeline.py` - Full pipeline Bronze → Silver → Gold
- `esg_gold_layer_only.py` - Quick refresh Silver → Gold only
- This guide - Deployment & usage documentation
