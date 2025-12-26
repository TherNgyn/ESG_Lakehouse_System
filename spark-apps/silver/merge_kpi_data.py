from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, lower, regexp_replace, concat_ws, trim, 
    current_timestamp, length, size, split, udf, regexp_extract
)
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
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
                   'name', 'year', 'metric_name', 'value', 'units', 
                   'additional_notes', 'source', 'updated_at']

print("Loading data from silver layer...")

df_csv = spark.read.parquet("s3a://silver/clean_kpi_companies_csv")
df_csv = df_csv.withColumnRenamed("value_float", "value")
df_csv = df_csv.withColumn("source", lit("csv"))
df_csv = df_csv.withColumn("additional_notes", lit(None).cast("string"))
df_csv = df_csv.withColumn("updated_at", current_timestamp())

df_excel = spark.read.parquet("s3a://silver/clean_kpi_excel")
df_excel = df_excel.withColumnRenamed("company_name", "name")
df_excel = df_excel.withColumn("source", lit("excel"))
df_excel = df_excel.withColumn("updated_at", current_timestamp())

df_pdf = spark.read.parquet("s3a://silver/clean_kpi_pdf")
df_pdf = df_pdf.withColumnRenamed("company_name", "name")
df_pdf = df_pdf.withColumnRenamed("value_float", "value")
df_pdf = df_pdf.withColumn("source", lit("pdf"))
df_pdf = df_pdf.withColumn("category_group", lit(None).cast("string"))
df_pdf = df_pdf.withColumn("updated_at", current_timestamp())

print(f"CSV: {df_csv.count():,} | Excel: {df_excel.count():,} | PDF: {df_pdf.count():,}")

def align_schema(df, schema):
    for col_name in schema:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast("string"))
    return df.select(schema)

df_merged = align_schema(df_csv, standard_schema) \
    .unionByName(align_schema(df_excel, standard_schema)) \
    .unionByName(align_schema(df_pdf, standard_schema))

print(f"After union: {df_merged.count():,}")

initial_count = df_merged.count()

df_merged = df_merged.filter(
    col("name").isNotNull() & 
    (trim(col("name")) != "") &
    col("metric_name").isNotNull() & 
    (trim(col("metric_name")) != "") &
    col("year").isNotNull() &
    col("value").isNotNull()
)

na_values = ["n/a", "na", "not available", "not applicable", "n.a.", 
             "not applicable.", "not available.", "__", "-", "--", " ", ""]

df_merged = df_merged.filter(
    ~lower(trim(col("value").cast("string"))).isin(na_values)
)

df_merged = df_merged.filter(
    (length(col("metric_name")) >= 5) &
    (length(col("metric_name")) < 200) &
    (~col("metric_name").rlike("^\\s")) &
    (~col("metric_name").rlike("^\\d{4}$")) &
    (~col("metric_name").rlike("^[\\d\\-\\+\\s]*$")) &
    (col("metric_name").rlike(".*[a-zA-Z]{3,}.*"))
)

print(f"After filtering: {df_merged.count():,} (removed {initial_count - df_merged.count():,})")

df_merged = df_merged.withColumn(
    "value_clean",
    regexp_replace(col("value").cast("string"), "[^0-9\\.\\-\\+\\:]", "")
)

@udf(FloatType())
def parse_value(value_str):
    if not value_str:
        return None
    try:
        value_str = value_str.strip()
        if ':' in value_str:
            parts = value_str.split(':')
            if len(parts) <= 2:
                return float(parts[0].strip())
        return float(value_str)
    except:
        return None

df_merged = df_merged.withColumn("value_float", parse_value(col("value_clean")))

df_merged = df_merged.withColumn(
    "units",
    when(
        (col("value").cast("string").contains(":")) & 
        (col("units") == "ratio"),
        "%"
    ).otherwise(col("units"))
)

before_value_filter = df_merged.count()
df_merged = df_merged.filter(col("value_float").isNotNull())
df_merged = df_merged.drop("value", "value_clean")
df_merged = df_merged.withColumnRenamed("value_float", "value")

df_merged = df_merged.withColumn(
    "year_int",
    when(col("year").rlike(r"^\d{4}$"), col("year").cast("int"))
    .when(col("year").rlike(r"^FY\s*(\d{4})$"), 
          regexp_replace(col("year"), r"^FY\s*(\d{4})$", r"$1").cast("int"))
    .when(col("year").rlike(r"^(\d{4})\s*FY$"), 
          regexp_replace(col("year"), r"^(\d{4})\s*FY$", r"$1").cast("int"))
    .when(col("year").rlike(r"^\d{4}/\d{2}$"), 
          regexp_replace(col("year"), r"^(\d{4})/\d{2}$", r"$1").cast("int"))
    .when(col("year").rlike(r"^\d{4}-\d{4}$"), 
          regexp_replace(col("year"), r"^(\d{4})-\d{4}$", r"$1").cast("int"))
    .otherwise(col("year").cast("int"))
)

before_year_filter = df_merged.count()
df_merged = df_merged.filter(
    col("year_int").isNotNull() & 
    (col("year_int") >= 2000) & 
    (col("year_int") <= 2030)
)
df_merged = df_merged.drop("year")
df_merged = df_merged.withColumnRenamed("year_int", "year")

print(f"After data type conversion: {df_merged.count():,} (removed {before_value_filter - df_merged.count():,})")

text_cols = ['topic', 'metric_category', 'category_group', 'name', 
             'metric_name', 'units', 'additional_notes']

for col_name in text_cols:
    if col_name in df_merged.columns:
        df_merged = df_merged.withColumn(
            col_name, 
            when(col(col_name).isNotNull(), trim(col(col_name)))
            .otherwise(None)
        )

df_merged = df_merged.withColumn(
    "boeing_prefix",
    when(
        (lower(col("name")) == "the boeing company") & 
        col("metric_name").contains("-"),
        regexp_extract(col("metric_name"), r"^([^-]+)", 1)
    ).otherwise(None)
)

df_merged = df_merged.withColumn(
    "category_group",
    when(
        (lower(col("name")) == "the boeing company") & 
        col("boeing_prefix").isNotNull() &
        (trim(col("boeing_prefix")) != ""),
        trim(col("boeing_prefix"))
    ).otherwise(col("category_group"))
)

df_merged = df_merged.drop("boeing_prefix")

print("Processed Boeing")

special_companies = ["conocophillips", "lukoil", "cheniere", "the cigna group"]

df_merged = df_merged.withColumn(
    "metric_name",
    when(
        lower(col("name")).isin(special_companies) & col("category_group").isNotNull(),
        when(
            ~col("metric_name").startswith(col("category_group")),
            concat_ws(" ", col("category_group"), col("metric_name"))
        ).otherwise(col("metric_name"))
    ).otherwise(col("metric_name"))
)

df_merged = df_merged.withColumn("metric_category", regexp_replace(col("metric_category"), "_", " "))

df_merged = df_merged.withColumn(
    "metric_name",
    when(
        (lower(col("name")) == "bradesco") & 
        col("category_group").isNotNull() & 
        (trim(col("category_group")) != ""),
        when(
            ~col("metric_name").startswith(col("category_group")),
            concat_ws(" ", col("category_group"), col("metric_name"))
        ).otherwise(col("metric_name"))
    ).otherwise(col("metric_name"))
)

print("Processed special companies")

df_merged = df_merged.withColumn(
    "metric_name", 
    regexp_replace(col("metric_name"), r"(?i)(Environment|Social|Governance)\s*/\s*", "")
)

df_merged = df_merged.withColumn(
    "metric_name", 
    regexp_replace(col("metric_name"), r"^\s*\d+\.\s*", "")
)

df_merged = df_merged.withColumn(
    "metric_name", 
    regexp_replace(col("metric_name"), r"(.+?)\s+\1", r"$1")
)

df_merged = df_merged.withColumn(
    "metric_name", 
    regexp_replace(col("metric_name"), r"0800", "")
)

df_merged = df_merged.withColumn("metric_name", trim(col("metric_name")))

key_cols = ['name', 'metric_category', 'category_group', 'year', 'metric_name', 'units']

df_merged = df_merged.withColumn(
    "source_priority",
    when(col("source") == "csv", 1)
    .when(col("source") == "pdf", 2)
    .otherwise(3)
)

window = Window.partitionBy(key_cols).orderBy(
    col("source_priority").asc(),
    col("updated_at").desc()
)

before_dedup = df_merged.count()
df_final = df_merged.withColumn("row_num", row_number().over(window)) \
    .filter(col("row_num") == 1) \
    .drop("row_num", "source_priority")

print(f"After deduplication: {df_final.count():,} (removed {before_dedup - df_final.count():,})")

print(f"\nFinal: {df_final.count():,} rows | {df_final.select('name').distinct().count()} companies | {df_final.select('metric_name').distinct().count()} metrics")
print(f"Year range: {df_final.agg({'year': 'min'}).collect()[0][0]} - {df_final.agg({'year': 'max'}).collect()[0][0]}")

df_final.groupBy("source").count().orderBy(col("count").desc()).show(truncate=False)
df_final.groupBy("topic").count().orderBy(col("count").desc()).show(truncate=False)

df_final.write.format("delta").mode("overwrite").partitionBy("year").save(OUTPUT_PATH)
df_final.coalesce(1).write.mode("overwrite").option("header", True).csv("s3a://silver/clean_kpi_csv_test")

print(f"\nSaved to {OUTPUT_PATH}")

spark.stop()