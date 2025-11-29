┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                              │
│  • CSV Files  • APIs  • Databases  • External Providers     │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                BRONZE LAYER (MinIO)                          │
│  • Raw data storage (Parquet)                               │
│  • Partitioned by date, source                              │
│  • No transformations                                        │
│  Buckets: bronze/corporate_esg/, bronze/country_esg/        │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼ (Spark ETL)
┌─────────────────────────────────────────────────────────────┐
│                SILVER LAYER (MinIO)                          │
│  • Cleaned & validated data                                 │
│  • Standardized schemas                                      │
│  • Data quality checks applied                              │
│  Buckets: silver/corporate_esg_cleaned/                     │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼ (Spark + dbt)
┌─────────────────────────────────────────────────────────────┐
│            GOLD LAYER (PostgreSQL + MinIO)                   │
│  • Star schema (fact + dimensions)                          │
│  • Business-ready aggregations                              │
│  • Optimized for analytics                                   │
│  Tables: fact_esg_metrics, dim_company, dim_kpi             │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              VISUALIZATION LAYER                             │
│  • Power BI Dashboards                                      │
│  • Metabase Reports                                         │
│  • Superset Analytics                                        │
└─────────────────────────────────────────────────────────────┘