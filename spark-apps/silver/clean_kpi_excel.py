from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col, when, trim, regexp_replace, length, split, 
    lit, initcap, lower, coalesce, concat, regexp_extract, size, expr
)

from datetime import datetime

spark = SparkSession.builder \
    .appName("ESG-KPI-Excel-Clean") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

INPUT_PATH = "s3a://silver/kpi_extract_excel"
OUTPUT_PATH = "s3a://silver/clean_kpi_excel"

print("Loading data from silver/kpi_extract_excel...")
df = spark.read.parquet(INPUT_PATH)
print(f"Loaded {df.count():,} rows")

text_cols = ['topic', 'metric_category', 'name', 'metric_name', 'units', 'additional_notes']
for col_name in text_cols:
    if col_name in df.columns:
        df = df.withColumn(col_name, trim(col(col_name)))
        df = df.withColumn(col_name, regexp_replace(col(col_name), r'\s+', ' '))

df = df.withColumn("name", initcap(col("name")))

df = df.filter(~lower(col("metric_category")).contains("indicator"))
df = df.filter(~lower(col("metric_name")).contains("indicator"))

companies = df.select("name").distinct().rdd.flatMap(lambda x: x).collect()
for company in companies:
    if company:
        df = df.withColumn("metric_name", 
            regexp_replace(col("metric_name"), f"(?i){company}", ""))

text_clean_cols = ['metric_category', 'metric_name', 'category_group']
for col_name in text_clean_cols:
    if col_name in df.columns:
        df = df.withColumn(col_name, regexp_replace(col(col_name), r'([a-zA-Z])\d+', r'$1'))
        df = df.withColumn(col_name, regexp_replace(col(col_name), r'\d+\.\d+', ''))
        df = df.withColumn(col_name, trim(regexp_replace(col(col_name), r'\s+', ' ')))

if 'category_group' not in df.columns:
    df = df.withColumn("category_group", split(col("metric_category"), "_", 2)[1])

df = df.withColumn("category_group", 
    coalesce(col("category_group"), col("metric_category"))
)

df = df.withColumn("category_group", regexp_replace(col("category_group"), r'([a-zA-Z])\d+', r'$1'))
df = df.withColumn("category_group", trim(regexp_replace(col("category_group"), r'\d+\.\d+', '')))

print("\nProcessing Bradesco data...")
bradesco_mask = lower(col("name")).contains("bradesco")
bradesco_with_detail = bradesco_mask & col("category_group").isNotNull()

df = df.withColumn(
    "metric_name",
    regexp_replace(col("metric_name"), r'\b(\d+\s+a\s+\d+)\s+\1\b', r'$1')
)

df = df.withColumn(
    "metric_name",
    regexp_replace(col("metric_name"), r'\b(by\s+\w+\s+group[_\s]\w+)\s+\1\b', r'$1')
)

df = df.withColumn("metric_name", trim(col("metric_name")))

print("\nChecking for remaining duplicates...")
duplicate_check = df.filter(
    col("metric_name").rlike(r'(\w+\s+){2,}\1')
).select("metric_name").distinct()

duplicate_count = duplicate_check.count()
if duplicate_count > 0:
    print(f"WARNING: Still found {duplicate_count} potential duplicates:")
    duplicate_check.show(20, truncate=False)
else:
    print("No duplicates found!")

final_cols = ['topic', 'metric_category', 'category_group', 
              'name', 'year', 'metric_name', 'value', 
              'units', 'additional_notes']

df_final = df.select([c for c in final_cols if c in df.columns])

df_final = df_final.distinct()
df_final = df_final.orderBy(col("name"), col("year").desc(), col("metric_name"))

print(f"\nFinal cleaned data: {df_final.count():,} rows")
print(f"Unique companies: {df_final.select('name').distinct().count()}")
print(f"Unique metrics: {df_final.select('metric_name').distinct().count()}")

print("\nSample data (first 20 rows):")
df_final.show(20, truncate=False)

print("\nSchema:")
df_final.printSchema()

extract_date = datetime.now().strftime("%Y-%m-%d")
df_final = df_final.withColumn("extract_date", lit(extract_date))

df_final.write.format("delta").mode("overwrite").partitionBy("year").save(OUTPUT_PATH)

print(f"\nData saved to {OUTPUT_PATH}")

spark.stop()