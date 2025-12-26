ESG Analytics - Complete Directory Structure
=============================================

project-root/
├── docker-compose.yml
├── .env
│
├── spark-apps/                          # PySpark applications
│   ├── Dockerfile
│   ├── bronze/                          # Bronze layer scripts
│   │   ├── load_raw_data.py
│   │   └── crawl_esg_data.py
│   │
│   ├── silver/                          # Silver layer scripts
│   │   ├── normalize_metrics.py
│   │   ├── staging_companies.py
│   │   ├── staging_units.py
│   │   ├── create_staging_metrics.py   # ← File mới
│   │   └── classify_metrics.py
│   │
│   └── utils/
│       └── helpers.py
│
├── dbt-project/                         # dbt project
│   ├── dbt_project.yml                  # ← File upload
│   ├── profiles.yml                     # ← File upload
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   └── sources.yml              # ← File upload (đổi tên từ source.yaml)
│   │   │
│   │   └── marts/                       # Gold layer models
│   │       ├── dim_company.sql          # ← File upload
│   │       ├── dim_metric.sql           # ← File upload
│   │       ├── dim_unit.sql             # ← File upload
│   │       ├── dim_date.sql             # ← File mới
│   │       ├── fact_esg_metric.sql      # ← File mới
│   │       ├── fact_esg_score.sql       # ← File mới
│   │       ├── schema.yml               # ← File mới (update)
│   │       └── semantic_models.yml      # ← File mới (merged semantic files)
│   │
│   ├── macros/
│   │   └── custom_macros.sql
│   │
│   └── tests/
│       └── data_quality_tests.sql
│
├── airflow/
│   ├── dags/
│   │   ├── dag_bronze_ingestion.py
│   │   ├── dag_silver_processing.py
│   │   └── dag_esg_gold_pipeline.py     # ← File mới
│   │
│   └── logs/
│
├── trino/
│   ├── catalog/
│   │   ├── delta.properties
│   │   └── hive.properties
│   │
│   └── config/
│       └── config.properties
│
├── metricflow/
│   ├── Dockerfile                       # ← File upload
│   └── requirements.txt
│
├── scripts/
│   ├── setup_buckets.sh
│   ├── deploy_gold_layer.sh            # ← File mới
│   └── run_pipeline.sh
│
├── datasets/                            # Input data
│   ├── esg_scores.csv
│   ├── sustainability_reports.pdf
│   └── metrics_data.xlsx
│
├── notebooks/                           # Jupyter notebooks
│   ├── exploration.ipynb
│   └── analytics.ipynb
│
├── docs/
│   ├── DBT_PROJECT_GUIDE.txt           # ← File mới
│   ├── METRICFLOW_QUERIES.txt          # ← File mới
│   └── TRINO_QUERIES.sql               # ← File mới
│
└── configs/
    └── spark-defaults.conf


FILE PLACEMENT INSTRUCTIONS
===========================

1. EXISTING FILES (từ upload):
   - Dockerfile → metricflow/Dockerfile
   - dbt_project.yml → dbt-project/dbt_project.yml
   - profiles.yml → dbt-project/profiles.yml
   - dim_company.sql → dbt-project/models/marts/dim_company.sql
   - dim_metric.sql → dbt-project/models/marts/dim_metric.sql
   - dim_unit.sql → dbt-project/models/marts/dim_unit.sql
   - source.yaml → dbt-project/models/staging/sources.yml (đổi tên)
   - _user.yml → dbt-project/_user.yml
   - semantic_companies.yml → DELETE (merge vào semantic_models.yml)
   - semantic_metrics.yml → DELETE (merge vào semantic_models.yml)
   - time_spine.sql → DELETE (thay bằng dim_date.sql)
   - schema.yml → DELETE (thay bằng version mới)
   - fact_metric.sql → DELETE (thay bằng fact_esg_metric.sql)

2. NEW FILES (từ output):
   - create_staging_metrics.py → spark-apps/silver/create_staging_metrics.py
   - dim_date.sql → dbt-project/models/marts/dim_date.sql
   - fact_esg_metric.sql → dbt-project/models/marts/fact_esg_metric.sql
   - fact_esg_score.sql → dbt-project/models/marts/fact_esg_score.sql
   - schema.yml → dbt-project/models/marts/schema.yml
   - semantic_models.yml → dbt-project/models/marts/semantic_models.yml
   - deploy_gold_layer.sh → scripts/deploy_gold_layer.sh
   - dag_esg_gold_pipeline.py → airflow/dags/dag_esg_gold_pipeline.py
   - DBT_PROJECT_GUIDE.txt → docs/DBT_PROJECT_GUIDE.txt
   - METRICFLOW_QUERIES.txt → docs/METRICFLOW_QUERIES.txt
   - TRINO_QUERIES.sql → docs/TRINO_QUERIES.sql


UPDATED sources.yml CONTENT
===========================
(dbt-project/models/staging/sources.yml)

version: 2

sources:
  - name: silver
    database: delta
    schema: default
    tables:
      - name: staging_companies_mapping
      - name: staging_units
      - name: staging_metrics              # ← Thêm mới
      - name: classified_metrics


DOCKER-COMPOSE VOLUMES
=====================
Cần mount thêm:

services:
  dbt:
    volumes:
      - ./dbt-project:/usr/app
      - ~/.dbt:/root/.dbt

  airflow:
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./spark-apps:/opt/airflow/spark-apps
      - ./airflow/logs:/opt/airflow/logs

  spark-submit:
    volumes:
      - ./spark-apps:/opt/spark-apps
      - ./scripts:/opt/scripts


PERMISSIONS
===========
chmod +x scripts/deploy_gold_layer.sh
chmod +x scripts/run_pipeline.sh


DEPLOYMENT ORDER
================

1. Setup Silver tables:
   docker exec spark-submit spark-submit /opt/spark-apps/silver/staging_companies.py
   docker exec spark-submit spark-submit /opt/spark-apps/silver/staging_units.py
   docker exec spark-submit spark-submit /opt/spark-apps/silver/create_staging_metrics.py

2. Run dbt models:
   docker exec dbt dbt deps
   docker exec dbt dbt run --models dim_company dim_metric dim_unit dim_date
   docker exec dbt dbt run --models fact_esg_metric
   docker exec dbt dbt run --models fact_esg_score

3. Test:
   docker exec dbt dbt test

4. Verify MetricFlow:
   docker exec metricflow mf list metrics
   docker exec metricflow mf query --metrics avg_esg_score --group-by year

5. Or use automated script:
   ./scripts/deploy_gold_layer.sh