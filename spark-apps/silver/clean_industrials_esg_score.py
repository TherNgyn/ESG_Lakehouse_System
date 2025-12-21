from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder \
    .appName("Bronze-to-Silver: Industrials Sector ESG & Score") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

BRONZE_PATH = "s3a://bronze/raw/esg_score/Industrials_sector_ESG_and_score_data.csv"
SILVER_PATH = "s3a://silver/clean_industrials_esg_score"

df = spark.read.option("header", "true").csv(BRONZE_PATH)

df = df.dropDuplicates()

numeric_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, (DoubleType, FloatType, IntegerType, LongType))]
categorical_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]

for col_name in numeric_cols:
    null_ratio = df.filter(col(col_name).isNull()).count() / df.count()
    median_val = df.approxQuantile(col_name, [0.5], 0.05)[0] if null_ratio <= 0.5 else None
    if null_ratio > 0.5 and col_name != "SNP":
        df = df.drop(col_name)
    elif median_val is not None:
        df = df.withColumn(col_name, when(col(col_name).isNull(), lit(median_val)).otherwise(col(col_name)))

for col_name in categorical_cols:
    null_ratio = df.filter(col(col_name).isNull()).count() / df.count()
    if null_ratio > 0.5:
        df = df.drop(col_name)
    else:
        df = df.withColumn(col_name, when(col(col_name).isNull(), lit("Unknown")).otherwise(col(col_name)))

date_cols = [c for c in df.columns if "date" in c.lower()]
for c in date_cols:
    df = df.withColumn(c, to_date(col(c)))

for c in ["Volume", "Market Cap"]:
    if c in df.columns:
        df = df.withColumn(c, regexp_replace(col(c), ",", "").cast("double"))

text_cols = [c for c in df.columns if dict(df.dtypes)[c] == "string"]
for c in text_cols:
    df = df.withColumn(c, trim(regexp_replace(col(c), r"\s+", " ")))

for c in numeric_cols:
    if c in df.columns:
        quantiles = df.approxQuantile(c, [0.25, 0.75], 0.05)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        df = df.withColumn(c, when(col(c) < lower, lower).when(col(c) > upper, upper).otherwise(col(c)))

if "Address" in df.columns:
    addr_split = split(col("Address"), ",")
    df = df.withColumn("country",
        when(trim(addr_split.getItem(size(addr_split)-1)).rlike("^[0-9\\-\\s]+$"),
             trim(addr_split.getItem(size(addr_split)-2))
        ).otherwise(trim(addr_split.getItem(size(addr_split)-1)))
    )
    df = df.withColumn("city",
        when(size(addr_split) >= 3, trim(addr_split.getItem(size(addr_split)-3))).otherwise(lit(None))
    )
df = df.select([col(c).alias(re.sub(r"[ ,;{}()\n\t=]", "_", c)) for c in df.columns])
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_PATH)

spark.stop()