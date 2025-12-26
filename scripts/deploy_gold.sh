#!/bin/bash
set -e

echo "Step 1: Initializing Metadata and Gold Schemas in Trino"

# 1.1 Khởi tạo Schema cho Silver (Metadata) và Gold (Marts)
docker exec trino trino --execute "
CREATE SCHEMA IF NOT EXISTS delta.default WITH (location = 's3a://silver/');
CREATE SCHEMA IF NOT EXISTS delta.gold_marts WITH (location = 's3a://gold/marts/');
"
# 1.2 Đăng ký các bảng Delta từ Silver vào Metastore
echo "Registering Silver tables..."
TABLES=(
    "staging_metrics:s3a://silver/staging_metrics/"
    "staging_companies_mapping:s3a://silver/staging_companies_mapping/"
    "staging_units:s3a://silver/staging_units/"
    "normalized_metrics:s3a://silver/metric_norm_final/"
    "clean_esg_level_score:s3a://silver/clean_esg_level_score/"
    "clean_esg_score:s3a://silver/clean_esg_score/"
    "clean_sp500_esg_risk:s3a://silver/clean_sp500_esg_risk/"
    "clean_sustainability_rank:s3a://silver/clean_sustainability_rank/"
    "clean_industrials_esg_score:s3a://silver/clean_industrials_esg_score/"
)

for entry in "${TABLES[@]}"; do
    IFS=":" read -r name loc <<< "$entry"
    echo "Registering $name..."
    # Xóa bảng cũ nếu tồn tại để tránh lỗi 'Table already exists' và cập nhật metadata mới nhất
    docker exec trino trino --execute "DROP TABLE IF EXISTS delta.default.$name; CALL delta.system.register_table(schema_name => 'default', table_name => '$name', table_location => '$loc');"
done

echo "Step 2: Run dbt models - Dimensions (Target: Gold)"
docker exec dbt dbt run --models dim_company dim_metric dim_unit dim_date --project-dir /usr/app

echo "Step 3: Run dbt models - Facts (Target: Gold)"
docker exec dbt dbt run --models fact_esg_metric --project-dir /usr/app

echo "Step 4: Run dbt models - ESG Scores (Target: Gold)"
docker exec dbt dbt run --models fact_esg_score_risk --project-dir /usr/app

echo "Step 5: Run dbt tests"
docker exec dbt dbt test --project-dir /usr/app

