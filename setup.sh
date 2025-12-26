#!/bin/bash

# echo "ESG Data Warehouse Setup"
# echo "========================"

# echo "1. Starting Docker containers..."
# docker-compose up -d

# echo "2. Waiting for services to be ready..."
# sleep 30

# echo "3. Running Spark staging jobs..."
# docker exec spark-master spark-submit \
#   --packages io.delta:delta-spark_2.12:3.2.1 \
#   /opt/spark-apps/staging_companies_mapping.py

# docker exec spark-master spark-submit \
#   --packages io.delta:delta-spark_2.12:3.2.1 \
#   /opt/spark-apps/staging_unit_mapping.py

# docker exec spark-master spark-submit \
#   --packages io.delta:delta-spark_2.12:3.2.1 \
#   /opt/spark-apps/normalizer_optimized_local.py

# docker exec spark-master spark-submit \
#   --packages io.delta:delta-spark_2.12:3.2.1 \
#   /opt/spark-apps/staging_normalized_metrics.py

echo "4. Verifying Delta tables accessible in Trino..."
docker exec trino trino --execute "SHOW SCHEMAS FROM delta"
docker exec trino trino --execute "SHOW TABLES FROM delta.default"

echo "5. Installing dbt dependencies..."
docker exec dbt bash -c "cd /usr/app && dbt deps"

echo "6. Running dbt models..."
docker exec dbt bash -c "cd /usr/app && dbt run"

echo "7. Running dbt tests..."
docker exec dbt bash -c "cd /usr/app && dbt test"

echo ""
echo "Setup complete!"
echo ""
echo "Access points:"
echo "- Trino UI: http://localhost:8080"
echo "- Spark UI: http://localhost:8081"
echo "- MinIO Console: http://localhost:9001"
echo ""
echo "Query Delta tables:"
echo "docker exec trino trino --execute 'SELECT * FROM delta.default.staging_companies_mapping LIMIT 10'"
echo "docker exec trino trino --execute 'SELECT * FROM delta.default.normalized_metrics WHERE year=2023 LIMIT 10'"
echo ""
echo "Query dbt marts:"
echo "docker exec trino trino --execute 'SELECT * FROM delta.marts.dim_company LIMIT 10'"
echo "docker exec trino trino --execute 'SELECT COUNT(*) FROM delta.marts.fact_metric'"