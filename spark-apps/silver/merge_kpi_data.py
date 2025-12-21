from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, lower, regexp_replace, concat_ws, trim, current_timestamp
from delta.tables import DeltaTable
import os

spark = SparkSession.builder \
    .appName("ESG-Merge-Silver") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

OUTPUT_PATH = "s3a://silver/clean_kpi"

standard_schema = ['topic', 'metric_category', 'category_group',
                   'name', 'year', 'metric_name', 'value', 'units', 'additional_notes', 'source']

df_csv = spark.read.parquet("s3a://silver/clean_kpi_companies_csv")
df_csv = df_csv.withColumnRenamed("value_float", "value")
df_csv = df_csv.withColumn("source", lit("csv"))
df_csv = df_csv.withColumn("additional_notes", lit(None).cast("string"))

df_excel = spark.read.parquet("s3a://silver/clean_kpi_excel")
df_excel = df_excel.withColumnRenamed("company_name", "name")
df_excel = df_excel.withColumn("source", lit("excel"))

df_pdf = spark.read.parquet("s3a://silver/clean_kpi_pdf")
df_pdf = df_pdf.withColumnRenamed("company_name", "name")
df_pdf = df_pdf.withColumn("source", lit("pdf"))
df_pdf = df_pdf.withColumn("category_group", lit(None).cast("string"))

def align_schema(df, schema):
    for col_name in schema:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast("string"))
    return df.select(schema)

df_merged = align_schema(df_csv, standard_schema) \
    .union(align_schema(df_excel, standard_schema)) \
    .union(align_schema(df_pdf, standard_schema))

from pyspark.sql.functions import expr
df_merged = df_merged.filter(col("value").isNotNull())
df_merged = df_merged.filter(col("year").isNotNull())

special_companies = ["conocophillips", "lukoil", "cheniere", "the cigna group", "bradesco"]

df_merged = df_merged.withColumn(
    "metric_name",
    when(
        lower(col("name")).isin(special_companies),
        when(expr("instr(metric_name, category_group) = 0"), 
             concat_ws(" ", col("category_group"), col("metric_name")))
        .otherwise(col("metric_name"))
    )
)

df_merged = df_merged.withColumn("metric_name", regexp_replace(col("metric_name"), r"^\s*\d+\.\s*", ""))
df_merged = df_merged.withColumn("metric_name", regexp_replace(col("metric_name"), r"(.+?)\s+\1", r"$1"))
df_merged = df_merged.withColumn("metric_name", regexp_replace(col("metric_name"), r"0800", ""))
df_merged = df_merged.withColumn("metric_name", trim(col("metric_name")))

key_cols = ['name', 'metric_category', 'category_group', 'year', 'metric_name', 'units']
df_final = df_merged.dropDuplicates(key_cols)
df_final = df_final.withColumn("updated_at", current_timestamp())

print(f"Total rows: {df_final.count():,}")
print(f"Unique companies: {df_final.select('name').distinct().count()}")
print(f"Unique metrics: {df_final.select('metric_name').distinct().count()}")

print("\nSource distribution:")
df_final.groupBy("source").count().orderBy(col("count").desc()).show()

print("\nTopic distribution:")
df_final.groupBy("topic").count().orderBy(col("count").desc()).show()

df_final.write.format("delta").mode("overwrite").partitionBy("year").save(OUTPUT_PATH)

# Export to CSV (for testing)
df_final.coalesce(1).write.mode("overwrite").option("header", True).csv("s3a://silver/clean_kpi_csv_test")

print(f"\nDữ liệu đã được hợp nhất tại {OUTPUT_PATH}")
print("Dữ liệu CSV đã được xuất ra s3a://silver/clean_kpi_csv_test")
spark.stop()