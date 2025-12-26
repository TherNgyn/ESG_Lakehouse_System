from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Staging-Normalized-Metrics") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df_norm = spark.read.format("delta").load("s3a://silver/metric_norm_final")

unique_metrics = df_norm.select(
    col("metric_norm").alias("metric_name"),
    col("metric_group"),
    col("topic")
).distinct()

window_metric = Window.orderBy("metric_name")

staging_metrics = unique_metrics.withColumn("metric_id", 
    concat(lit("MET-"), lpad(row_number().over(window_metric).cast("string"), 5, "0")))

staging_metrics.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://silver/staging_metrics")

spark.stop()