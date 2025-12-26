from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, regexp_replace, regexp_extract,
    lit, initcap, lower, udf, length, split, size
)
from pyspark.sql.types import StringType, FloatType
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

print(f"Before filtering topic: {df.count()} rows")
df = df.filter(col("topic") != "Other")
print(f"After filtering out 'Other' topic: {df.count()} rows")

print("\n=== FILTERING NULL AND INVALID VALUES ===")


df = df.filter(
    col("company_name").isNotNull() & 
    (trim(col("company_name")) != "") &
    col("metric_name").isNotNull() & 
    (trim(col("metric_name")) != "") &
    col("year").isNotNull() &
    col("value").isNotNull()
)
print(f"After filtering null critical columns: {df.count()} rows")

na_values = ["n/a", "na", "not available", "not applicable", "n.a.", 
             "not applicable.", "not available.", "__", "-", "--", " ", ""]
             
df = df.filter(
    ~lower(trim(col("value"))).isin(na_values) &
    ~lower(trim(col("metric_name"))).isin(na_values)
)
print(f"After filtering N/A values: {df.count()} rows")

df = df.filter(
    (length(col("metric_name")) >= 5) &
    (length(col("metric_name")) < 200) &
    (~col("metric_name").rlike("^\\s")) &
    (~col("metric_name").rlike("^\\d{4}$")) &
    (~col("metric_name").rlike("^[\\d\\-\\+\\s]*$")) &
    (col("metric_name").rlike(".*[a-zA-Z]{3,}.*"))
)
print(f"After filtering invalid metric names: {df.count()} rows")

print("\n=== CLEANING AND CONVERTING VALUE COLUMN ===")

special_pattern = r"[^0-9\.\:\,\-\s]"
df = df.withColumn("value", regexp_replace(col("value"), special_pattern, ""))


df = df.withColumn("value", regexp_replace(col("value"), ",", ""))


df = df.filter(~col("value").rlike("[a-zA-Z]"))
print(f"After filtering values with letters: {df.count()} rows")

@udf(FloatType())
def extract_first_number(value):
    try:
        if value and ':' in value:
            parts = value.split(':')
            if len(parts) <= 2: 
                return float(parts[0].strip())
    except:
        return None
    try:
        return float(value)
    except:
        return None

df = df.filter(
    ~col("value").contains(":") | 
    (size(split(col("value"), ":")) <= 2)
)

df = df.withColumn("value_float", extract_first_number(col("value")))

df = df.withColumn(
    "units",
    when(
        (col("units") == "ratio") & (col("value").contains(":")),
        "%"
    ).otherwise(col("units"))
)

df = df.filter(col("value_float").isNotNull())
print(f"After converting to float and filtering nulls: {df.count()} rows")

print("\n=== CLEANING AND CONVERTING YEAR COLUMN ===")

df = df.withColumn(
    "year_clean",
    when(col("year").rlike(r"^\d{4}$"), col("year").cast("int"))
    .when(col("year").rlike(r"^FY\s*(\d{4})$"), 
          regexp_extract(col("year"), r"(\d{4})", 1).cast("int"))
    .when(col("year").rlike(r"^(\d{4})\s*FY$"), 
          regexp_extract(col("year"), r"(\d{4})", 1).cast("int"))
    .when(col("year").rlike(r"^\d{4}/\d{2}$"), 
          regexp_extract(col("year"), r"(\d{4})", 1).cast("int"))
    .when(col("year").rlike(r"^\d{4}-\d{4}$"), 
          regexp_extract(col("year"), r"(\d{4})", 1).cast("int"))
    .otherwise(None)
)


df = df.filter(
    col("year_clean").isNotNull() & 
    (col("year_clean") >= 2000) & 
    (col("year_clean") <= 2030)
)
print(f"After year validation (2000-2030): {df.count()} rows")


final_cols = ['topic', 'metric_category', 'company_name', 'year_clean', 'metric_name',
              'value_float', 'units', 'additional_notes']

df_final = df.select([c for c in final_cols if c in df.columns])

df_final = df_final.withColumnRenamed("year_clean", "year")
df_final = df_final.withColumnRenamed("value_float", "value")


df_final = df_final.distinct()
df_final = df_final.orderBy(col("company_name"), col("year").desc(), col("metric_name"))

print(f"Total rows: {df_final.count():,}")
print(f"Unique companies: {df_final.select('company_name').distinct().count()}")
print(f"Unique metrics: {df_final.select('metric_name').distinct().count()}")
print(f"Year range: {df_final.agg({'year': 'min'}).collect()[0][0]} - {df_final.agg({'year': 'max'}).collect()[0][0]}")

print("\nTopic distribution:")
df_final.groupBy("topic").count().orderBy(col("count").desc()).show()

print("\nYear distribution:")
df_final.groupBy("year").count().orderBy("year").show()

print("\nCompany distribution:")
df_final.groupBy("company_name").count().orderBy(col("count").desc()).show()

print("\nData types:")
df_final.printSchema()

print("\nSample data (first 20 rows):")
df_final.show(20, truncate=False)

print("\nNull value check:")
df_final.select([col(c).isNull().cast("int").alias(c) for c in df_final.columns]) \
    .agg({c: "sum" for c in df_final.columns}).show()


extract_date = datetime.now().strftime("%Y-%m-%d")
df_final = df_final.withColumn("extract_date", lit(extract_date))

df_final.write.format("delta").mode("overwrite").partitionBy("year").save(OUTPUT_PATH)

print(f" Data saved to {OUTPUT_PATH}")

spark.stop()