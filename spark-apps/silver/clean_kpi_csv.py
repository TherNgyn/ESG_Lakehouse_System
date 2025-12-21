from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lower, trim, regexp_extract, regexp_replace, length, split, 
    size, udf, lit, concat, initcap, array
)
from pyspark.sql.types import StringType, FloatType
from datetime import datetime
import operator
from functools import reduce

spark = SparkSession.builder \
    .appName("ESG-KPI-Companies-Clean") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.codegen.wholeStage", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

BRONZE_METADATA = "s3a://bronze/raw/_metadata"
OUTPUT_PATH = "s3a://silver/clean_kpi_companies_csv"

df_meta = spark.read.parquet(BRONZE_METADATA)
csv_file = df_meta.filter(
    (col("file_name") == "companies_KPI.csv") &
    (col("status") == "success")
).select("s3_path").collect()

if not csv_file:
    print("companies_KPI.csv not found in bronze")
    spark.stop()
    exit()

s3_path = csv_file[0].s3_path
s3_path = s3_path.replace("s3://", "s3a://")
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiLine", "true") \
    .option("encoding", "UTF-8") \
    .load(s3_path)

print(f"Loaded {df.count():,} rows from bronze")

encoding_fixes = {
    r"COâ‚‚|CO₂|CO2": "CO2",
    r"co₂|coâ‚‚": "co2",
    r"â‚€": "0", r"â‚": "1", r"â‚‚": "2", r"â‚ƒ": "3", r"â‚„": "4",
    r"â‚…": "5", r"â‚†": "6", r"â‚‡": "7", r"â‚ˆ": "8", r"â‚‰": "9",
    r"Ã§": "c", r"Ã£": "a", r"Ã©": "e", r"Ã­": "i", r"Ã³": "o",
    r"Ãº": "u", r"Ã±": "n", r"â€": "-", r"â€™": "'",
    r"â€œ|â€": '"', r"Â°": "°", r"Â´": "'", r"Â±": "±",
    r"Ã—": "x", r"Ã·": "/"
}

text_columns = ["metric_name", "metric_category", "units"]
for col_name in text_columns:
    if col_name in df.columns:
        combined_expression = col(col_name)
        for pattern, replacement in encoding_fixes.items():
            combined_expression = regexp_replace(combined_expression, pattern, replacement)
        df = df.withColumn(col_name, combined_expression)

df = (df
    .na.drop("all")
    .filter(~col("metric_name").cast("string").rlike("Â¡|Â¯|â€|Ã|�"))
    .filter(~col("name").isin(["name"]))
)

df = df.withColumn(
    "metric_name",
    when(
        (col("name").rlike("-")) | (col("name").rlike("U\\.S\\.")) | 
        (col("name").rlike("Amazon's")) | (col("name").rlike("Supplier Audits by Type")),
        concat(col("name"), lit(" - "), col("metric_name"))
    ).otherwise(col("metric_name"))
).withColumn(
    "name",
    when(
        (col("name").rlike("-")) | (col("name").rlike("U\\.S\\.")) | 
        (col("name").rlike("Amazon's")) | (col("name").rlike("Supplier Audits by Type")),
        "Amazon"
    ).otherwise(col("name"))
)

df = df.withColumn(
    "name_parenthesis",
    regexp_extract(col("name"), r"Brookfield Corporate \((.*?)\)", 1)
).withColumn(
    "name",
    when(col("name_parenthesis") != "", "Brookfield Corporate").otherwise(col("name"))
).withColumn(
    "metric_name",
    when(col("name_parenthesis") != "",
         concat(col("name_parenthesis"), lit(" - "), col("metric_name"))
    ).otherwise(col("metric_name"))
).drop("name_parenthesis")

df = df.withColumn("name", initcap(col("name")))
df = df.withColumn("name", regexp_replace(col("name"), r"\bBursa\b", ""))
df = df.withColumn("name", trim(regexp_replace(col("name"), r"\s+", " ")))

df = df.withColumn("metric_name", regexp_replace(col("metric_name"), r"\bGHG\b", "Greenhouse Gas"))
df = df.withColumn("metric_category", regexp_replace(col("metric_category"), r"\bGHG\b", "Greenhouse Gas"))
df = df.withColumn("units", regexp_replace(col("units"), r"\bGHG\b", "Greenhouse Gas"))

df = df.withColumn("metric_name", regexp_replace(col("metric_name"), r"CO2|co2", "CO2"))
df = df.withColumn("metric_category", regexp_replace(col("metric_category"), r"CO2|co2", "CO2"))
df = df.withColumn("units", regexp_replace(col("units"), r"CO2e?", "CO2"))

df = df.filter(
    (length(col("metric_name")) < 150) &
    (length(col("metric_name")) >= 5) &
    (~col("metric_name").rlike("CEO MESSAGE|COMPANY PROFILE|CHAPTER|APPENDIX|Sustainability Report|Highlights")) &
    (~col("metric_name").rlike(r"^\s")) &
    (~col("metric_name").rlike(r"^\d{4}$")) &
    (~col("metric_name").rlike(r"^[\d\-–\+\s]*$")) &
    (~col("metric_name").rlike(r"[–—;ọc]{2,}")) &
    (col("metric_name").rlike(r".*[a-zA-Z]{3,}.*"))
)

exclude_keywords = ["r&d", "research", "supply chain", "logistics", "supply chain management"]
exclude_condition = reduce(
    operator.or_,
    [lower(col("metric_category")).contains(k) for k in exclude_keywords]
)
df = df.filter(~exclude_condition)

df = df.withColumn("category_group", col("metric_category"))

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
    "workforce": "Social", "injury": "Social", "illness": "Social",
    "lost-time": "Social", "ehs": "Social", "customer satisfaction": "Social",
    "training": "Social", "fatality": "Social", "accident": "Social", "recordable": "Social",
    "governance": "Governance", "board": "Governance", "ethics": "Governance",
    "compliance": "Governance", "healthy company": "Governance", "revenue": "Governance",
    "leadership": "Governance", "property": "Governance", "anti-corruption": "Governance",
    "innovation": "Governance", "data privacy": "Governance", "data security": "Governance",
    "security": "Governance"
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

na_values = ["n/a", "na", "not available", "not applicable", "n.a.", "not applicable.", 
             "not available.", "__", "-", "--", " "]
df = df.filter(
    col("value").isNotNull() & 
    (trim(col("value")) != "") &
    (~lower(col("value")).isin(na_values)) &
    (~col("value").rlike("[a-zA-Z]"))
)

special_pattern = r"[^0-9\.\:\,\-\s]"
df = df.withColumn("value", regexp_replace(col("value"), special_pattern, ""))

@udf(FloatType())
def extract_first_number(value):
    try:
        if value and ':' in value:
            return float(value.split(':')[0])
    except:
        return None
    return None

df = df.filter(
    (col("units") != "ratio") | (size(split(col("value"), ":")) <= 2)
)

df = df.withColumn(
    "value_float",
    when(
        (col("units") == "ratio") & (col("value").contains(":")),
        extract_first_number(col("value"))
    ).otherwise(col("value").cast("float"))
).withColumn(
    "units",
    when(
        (col("units") == "ratio") & (col("value").contains(":")),
        "%"
    ).otherwise(col("units"))
)

cols_to_drop = ["metric_lower", "additional_notes", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "_c13"]
for col_name in cols_to_drop:
    if col_name in df.columns:
        df = df.drop(col_name)

df = df.withColumn(
    "year_clean",
    when(col("year").rlike(r"^\d{4}$"), col("year").cast("int"))
    .when(col("year").rlike(r"^FY\s*(\d{4})$"), regexp_extract(col("year"), r"(\d{4})", 1).cast("int"))
    .when(col("year").rlike(r"^(\d{4})\s*FY$"), regexp_extract(col("year"), r"(\d{4})", 1).cast("int"))
    .when(col("year").rlike(r"^\d{4}/\d{2}$"), regexp_extract(col("year"), r"(\d{4})", 1).cast("int"))
    .otherwise(None)
)

df = df.filter(col("year_clean").isNotNull() & (col("year_clean") >= 2000) & (col("year_clean") <= 2030))

final_cols = ["topic", "metric_category", "category_group", 
              "name", "year_clean", "metric_name", "value_float", "units"]
df_final = df.select([c for c in final_cols if c in df.columns])
df_final = df_final.withColumnRenamed("year_clean", "year")

df_final = df_final.distinct()
df_final = df_final.orderBy(col("name"), col("year").desc(), col("metric_name"))

print(f"\nFinal cleaned data: {df_final.count():,} rows")
print(f"Unique companies: {df_final.select('name').distinct().count()}")
print(f"Unique metrics: {df_final.select('metric_name').distinct().count()}")

print("\nTopic distribution:")
df_final.groupBy("topic").count().orderBy(col("count").desc()).show()

print("\nSample data (first 20 rows):")
df_final.show(20, truncate=False)

print("\nSchema:")
df_final.printSchema()

extract_date = datetime.now().strftime("%Y-%m-%d")
df_final = df_final.withColumn("extract_date", lit(extract_date))

df_final.write.format("delta").mode("overwrite").partitionBy("year").parquet(OUTPUT_PATH)

print(f"\nData saved to {OUTPUT_PATH}")

spark.stop()