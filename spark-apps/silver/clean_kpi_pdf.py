from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, regexp_replace, regexp_extract,
    lit, initcap, lower, udf
)
from pyspark.sql.types import StringType
from datetime import datetime

spark = SparkSession.builder \
    .appName("ESG-KPI-PDF-Clean") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

INPUT_PATH = "s3a://silver/kpi_pdf_extracted"
OUTPUT_PATH = "s3a://silver/clean_kpi_pdf"

print("Loading data from silver/kpi_pdf_extracted...")
df = spark.read.parquet(INPUT_PATH)
print(f"Loaded {df.count():,} rows")

text_cols = ['metric_category', 'company_name', 'metric_name', 'units', 'additional_notes']
for col_name in text_cols:
    if col_name in df.columns:
        df = df.withColumn(col_name, trim(col(col_name)))
        df = df.withColumn(col_name, regexp_replace(col(col_name), r'\s+', ' '))

df = df.withColumn("company_name", regexp_replace(col("company_name"), r"\bBursa\b", ""))
df = df.withColumn("company_name", trim(regexp_replace(col("company_name"), r"\s+", " ")))
df = df.withColumn("company_name", initcap(col("company_name")))

text_cols_ghg = ['metric_category', 'metric_name', 'units', 'additional_notes']
for col_name in text_cols_ghg:
    if col_name in df.columns:
        df = df.withColumn(col_name, regexp_replace(col(col_name), r"(?i)\bGHG\b", "Greenhouse Gas"))

@udf(StringType())
def extract_parenthesis_udf(text):
    if not text:
        return text
    import re
    match = re.search(r'\(([^)]+)\)', text)
    return match.group(1).strip() if match else text

df = df.withColumn("metric_category", extract_parenthesis_udf(col("metric_category")))

keywords_dict = {
    "environment": "Environmental", "water": "Environmental", "climate": "Environmental",
    "carbon": "Environmental", "energy": "Environmental", "waste": "Environmental",
    "scope 3": "Environmental", "greenhouse": "Environmental", "raw material": "Environmental",
    "sustainable": "Environmental", "chemical": "Environmental", "emission": "Environmental",
    "biodiversity": "Environmental", "pollution": "Environmental", "packaging": "Environmental",
    "net zero": "Environmental", "co2": "Environmental",
    "social": "Social", "community": "Social", "diversity": "Social",
    "women": "Social", "minorities": "Social", "management": "Social",
    "healthy workforce": "Social", "community impact": "Social",
    "customer relationship management": "Social", "responsible supply chain": "Social",
    "health and safety": "Social", "healthy society": "Social",
    "employee": "Social", "human": "Social", "labor": "Social",
    "society": "Social", "safety": "Social", "people": "Social",
    "governance": "Governance", "board": "Governance", "ethics": "Governance",
    "compliance": "Governance", "healthy company": "Governance", "revenue": "Governance",
    "leadership": "Governance", "property": "Governance", "anti-corruption": "Governance",
    "innovation": "Governance", "data privacy": "Governance"
}

keywords_broadcast = spark.sparkContext.broadcast(keywords_dict)

@udf(StringType())
def assign_topic(metric_category, metric_name):
    if not metric_category and not metric_name:
        return "Other"
    text = f"{metric_category or ''} {metric_name or ''}".lower()
    for keyword, topic in keywords_broadcast.value.items():
        if keyword in text:
            return topic
    return "Other"

df = df.withColumn("topic", assign_topic(col("metric_category"), col("metric_name")))

print(f"Before filtering: {df.count()} rows")
df = df.filter(col("topic") != "Other")
print(f"After filtering out 'Other' topic: {df.count()} rows")

final_cols = ['topic', 'metric_category', 'company_name', 'year', 'metric_name',
              'value', 'units', 'additional_notes']

df_final = df.select([c for c in final_cols if c in df.columns])

df_final = df_final.distinct()
df_final = df_final.orderBy(col("company_name"), col("year").desc(), col("metric_name"))

print(f"\nFinal cleaned data: {df_final.count():,} rows")
print(f"Unique companies: {df_final.select('company_name').distinct().count()}")
print(f"Unique metrics: {df_final.select('metric_name').distinct().count()}")

print("\nTopic distribution:")
df_final.groupBy("topic").count().orderBy(col("count").desc()).show()

print("\nSample data (first 20 rows):")
df_final.show(20, truncate=False)

print("\nSchema:")
df_final.printSchema()

extract_date = datetime.now().strftime("%Y-%m-%d")
df_final = df_final.withColumn("extract_date", lit(extract_date))

df_final.write.format("delta").mode("append").partitionBy("year").save(OUTPUT_PATH)

print(f"\nData saved to {OUTPUT_PATH}")

spark.stop()