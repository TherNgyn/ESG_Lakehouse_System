from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder \
    .appName("Silver: Staging Metric Mapping") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("s3a://silver/normalized_metrics")

df_units = spark.read.format("delta").load("s3a://silver/staging_units")

df_cleaned = df.withColumn("metric_norm",
    when(
        (col("topic") == "Social") & 
        (col("metric_norm").isin("PMx", "NOx", "SOx", "CO")), 
        col("metric_name")
    ).otherwise(col("metric_norm"))
)

raw_mapping = df_cleaned.select(
    col("metric_name").alias("metric_name_raw"),
    "metric_norm",
    "metric_group",
    "topic",
    col("units").alias("raw_unit_to_match")
)

stg_metric_mapping = raw_mapping.join(
    df_units, 
    raw_mapping.raw_unit_to_match == df_units.original_unit, 
    "left"
).select(
    "metric_name_raw",
    "metric_norm",
    "metric_group",
    "topic",
    coalesce(col("standard_unit"), col("raw_unit_to_match")).alias("unit_standard")
)

stg_metric_mapping = stg_metric_mapping.dropDuplicates(["metric_name_raw"])


stg_metric_mapping = stg_metric_mapping.withColumn("staging_metric_id", monotonically_increasing_id())

#
stg_metric_mapping = stg_metric_mapping.withColumn("last_updated", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))



total_mappings = stg_metric_mapping.count()
print(f"Total distinct metric mappings created: {total_mappings}")

print("\nSample metric mappings (Check PMx fix and Units):")
stg_metric_mapping.select(
    "metric_name_raw",
    "metric_norm",
    "topic",
    "unit_standard"
).filter(
    (col("metric_name_raw").contains("human capital")) | 
    (col("unit_standard").isNotNull())
).show(20, truncate=False)

print("\nSaving staging metric mapping table to S3...")

OUTPUT_STG_PATH = "s3a://silver/staging_metric_mapping"
stg_metric_mapping.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(OUTPUT_STG_PATH)

print(f"Successfully saved staging metric mapping to {OUTPUT_STG_PATH}")

spark.stop()