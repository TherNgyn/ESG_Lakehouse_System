from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Bronze-to-Silver: ESG Score S&P Crawled") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

BRONZE_PATH = "s3a://bronze/raw/esg_score/esg_data_score_s&p_crawled.csv"
SILVER_PATH = "s3a://silver/clean_esg_score"

df = spark.read.option("header", "true").option("encoding", "UTF-8").csv(BRONZE_PATH)

df = df.select(
    trim(col("company")).alias("company"),
    trim(col("industry")).alias("industry"),
    trim(col("search_term")).alias("search_term"),
    trim(col("score_under_review")).alias("score_under_review"),
    col("csa_score"),
    col("esg_score"),
    col("last_updated"),
    col("scraped_at")
)

df = df.select(
    trim(col("company")).alias("company"),
    trim(col("industry")).alias("industry"),
    trim(col("search_term")).alias("search_term"),
    to_date(trim(col("last_updated")), "MMMM d, yyyy").alias("last_updated"),
    col("csa_score").cast("double"),
    col("esg_score").cast("double"),
    to_timestamp(col("scraped_at")).alias("scraped_at"),
    upper(trim(col("score_under_review"))).alias("score_under_review")
)

df = df.dropna(subset=["csa_score", "esg_score"])
df = df.dropDuplicates(["company", "industry"])

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_PATH)

spark.stop()