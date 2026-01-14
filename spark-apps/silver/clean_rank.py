from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Bronze-to-Silver: Rank Tables") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df4 = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("dataAddress", "'Scores and ranks'!A1") \
    .option("maxRowsInMemory", 1000) \
    .load("s3a://bronze/raw/esg_rank/rank_sustainability_2022_2024_company.csv")

processed_rank_df = df4.select(
    F.trim(F.col("Company Name")).alias("company_name"),
    F.trim(F.col("WBA ID")).alias("wba_id"),
    F.trim(F.col("ISIN")).alias("isin"),
    F.col("Total Score \n(out of 100)").cast("double").alias("total_score"),
    F.col("MA1: Governance and strategy measurement area score (out of 100)").cast("double").alias("governance_strategy_score"),
    F.col("MA2: Ecosystems and biodiversity measurement area score (out of 100)").cast("double").alias("ecosystems_biodiversity_score"),
    F.col("MA3: Social inclusion and community impact measurement area score (out of 100)").cast("double").alias("social_community_score"),
    F.col("Total Rank").cast("int").alias("total_rank"),
    F.col("MA1 (Rank)").cast("int").alias("governance_strategy_rank"),
    F.col("MA2 (Rank)").cast("int").alias("ecosystems_biodiversity_rank"),
    F.col("MA3 (Rank)").cast("int").alias("social_community_rank"),
    F.trim(F.col("HQRegion")).alias("hq_region"),
    F.trim(F.col("HQCountry")).alias("hq_country"),
    F.trim(F.col("Industry_Grouped")).alias("sector"),
    F.trim(F.col("Industry_Disaggregated")).alias("industry"),
    F.col("Year Benchmarked").cast("int").alias("year_benchmarked")
).filter("company_name IS NOT NULL AND company_name != ''")
numeric_cols = [c for c, t in processed_rank_df.dtypes if t in ['double', 'int']]
string_cols = [c for c, t in processed_rank_df.dtypes if t == 'string']

median_map = {}
for col_name in numeric_cols:
    median_val = processed_rank_df.select(F.percentile_approx(F.col(col_name), 0.5)).collect()[0][0]
    median_map[col_name] = median_val if median_val is not None else -1

processed_rank_df = processed_rank_df.fillna(median_map).fillna("Unknown", subset=string_cols)

processed_rank_df = processed_rank_df.select(
    "company_name", "wba_id", "isin",
    F.when((F.col("total_score") < 0) | (F.col("total_score") > 100), -1).otherwise(F.col("total_score")).alias("total_score"),
    F.when((F.col("governance_strategy_score") < 0) | (F.col("governance_strategy_score") > 100), -1).otherwise(F.col("governance_strategy_score")).alias("governance_strategy_score"),
    F.when((F.col("ecosystems_biodiversity_score") < 0) | (F.col("ecosystems_biodiversity_score") > 100), -1).otherwise(F.col("ecosystems_biodiversity_score")).alias("ecosystems_biodiversity_score"),
    F.when((F.col("social_community_score") < 0) | (F.col("social_community_score") > 100), -1).otherwise(F.col("social_community_score")).alias("social_community_score"),
    F.when(F.col("total_rank") < 0, -1).otherwise(F.col("total_rank")).alias("total_rank"),
    F.when(F.col("governance_strategy_rank") < 0, -1).otherwise(F.col("governance_strategy_rank")).alias("governance_strategy_rank"),
    F.when(F.col("ecosystems_biodiversity_rank") < 0, -1).otherwise(F.col("ecosystems_biodiversity_rank")).alias("ecosystems_biodiversity_rank"),
    F.when(F.col("social_community_rank") < 0, -1).otherwise(F.col("social_community_rank")).alias("social_community_rank"),
    "hq_region", "hq_country", "sector", "industry",
    F.when((F.col("year_benchmarked") < 2020) | (F.col("year_benchmarked") > 2024), -1).otherwise(F.col("year_benchmarked")).alias("year_benchmarked")
).distinct().dropDuplicates(["company_name", "year_benchmarked"])

processed_rank_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save("s3a://silver/clean_sustainability_rank")