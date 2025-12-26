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

country_list = "United States|USA|United Kingdom|UK|Canada|China|Japan|Hong Kong|Germany|France|Netherlands|NLD|Switzerland|Sweden|Ireland|Israel|Spain|Bermuda|Monaco|Greece|Italy|Singapore|Thailand|Brazil|Colombia|Panama|Luxembourg|Austria|Norway|Chile|Taiwan|Finland|Bahamas|Russia|Mexico|VNM|Vietnam|Denmark|Cyprus"

is_country_invalid = True
if "Country" in df.columns:
    valid_countries = df.filter(col("Country").rlike(country_list)).count()
    if valid_countries > (df.count() * 0.8):
        is_country_invalid = False

if "Address" in df.columns and is_country_invalid:
    df = df.withColumn("Addr_Clean", regexp_replace(col("Address"), r"[\"\r\t]+", " "))
    df = df.withColumn("Addr_Clean", regexp_replace(col("Addr_Clean"), r"\n", " "))
    df = df.withColumn("Addr_Clean", regexp_replace(col("Addr_Clean"), r"\s+", " "))
    df = df.withColumn("Addr_Clean", regexp_replace(col("Addr_Clean"), r"\s*,\s*", ", "))
    df = df.withColumn("Addr_Clean", regexp_replace(col("Addr_Clean"), r",\s*,+", ","))
    df = df.withColumn("Addr_Clean", regexp_replace(col("Addr_Clean"), r"^\s*,+\s*", ""))
    df = df.withColumn("Addr_Clean", regexp_replace(col("Addr_Clean"), r"\s*,+\s*$", ""))
    df = df.withColumn("Addr_Clean", regexp_replace(col("Addr_Clean"), r"(\d{4})([A-Z]{2})\b", "$1 $2"))
    df = df.withColumn("Addr_Clean", trim(col("Addr_Clean")))
    
    addr_arr = split(col("Addr_Clean"), ",")
    
    df = df.withColumn("last_part", trim(element_at(addr_arr, -1)))
    df = df.withColumn("second_last", when(size(addr_arr) >= 2, trim(element_at(addr_arr, -2))).otherwise(lit("")))
    df = df.withColumn("third_last", when(size(addr_arr) >= 3, trim(element_at(addr_arr, -3))).otherwise(lit("")))
    df = df.withColumn("fourth_last", when(size(addr_arr) >= 4, trim(element_at(addr_arr, -4))).otherwise(lit("")))
    df = df.withColumn("fifth_last", when(size(addr_arr) >= 5, trim(element_at(addr_arr, -5))).otherwise(lit("")))
    
    df = df.withColumn("is_last_pure_number", col("last_part").rlike(r"^[\d\s-]+$"))
    df = df.withColumn("is_last_uk_postal", col("last_part").rlike(r"^[A-Z]{1,2}\d[A-Z\d]?\s\d[A-Z]{2}$"))
    df = df.withColumn("is_last_canada_postal", col("last_part").rlike(r"^[A-Z]\d[A-Z]\s\d[A-Z]\d$"))
    df = df.withColumn("is_last_nld_postal", col("last_part").rlike(r"^\d{4}\s[A-Z]{2}$"))
    df = df.withColumn("is_last_bermuda_postal", col("last_part").rlike(r"^[A-Z]{2}\s\d{2}$"))
    
    df = df.withColumn("is_second_pure_number", col("second_last").rlike(r"^[\d\s-]+$"))
    df = df.withColumn("is_second_nld_postal", col("second_last").rlike(r"^\d{4}\s[A-Z]{2}$"))
    
    df = df.withColumn("is_last_country", col("last_part").rlike(country_list))
    df = df.withColumn("is_second_country", col("second_last").rlike(country_list))
    df = df.withColumn("is_third_country", col("third_last").rlike(country_list))
    
    df = df.withColumn("country",
        when(col("is_last_country"), col("last_part"))
        .when(col("is_second_country"), col("second_last"))
        .when(col("is_third_country"), col("third_last"))
        .otherwise(lit("Unknown"))
    )
    
    df = df.withColumn("has_postal_at_end",
        col("is_last_pure_number") | col("is_last_uk_postal") | 
        col("is_last_canada_postal") | col("is_last_nld_postal") | col("is_last_bermuda_postal"))
    
    df = df.withColumn("postal_code",
        when(col("has_postal_at_end"), col("last_part"))
        .when(col("is_second_pure_number") | col("is_second_nld_postal"),
             col("second_last"))
        .otherwise(lit(None))
    )
    
    canadian_provinces = "AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT"
    us_states = "AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC"
    
    df = df.withColumn("state_code",
        when(col("country").rlike("(?i)(Canada)"),
             when(col("second_last").rlike(f"^({canadian_provinces})$"), col("second_last"))
             .when(col("third_last").rlike(f"^({canadian_provinces})$"), col("third_last"))
             .when(col("fourth_last").rlike(f"^({canadian_provinces})$"), col("fourth_last"))
             .otherwise(lit(None)))
        .when(col("country").rlike("(?i)(United States|USA)"),
             when(col("second_last").rlike(f"^({us_states})$"), col("second_last"))
             .when(col("third_last").rlike(f"^({us_states})$"), col("third_last"))
             .when(col("fourth_last").rlike(f"^({us_states})$"), col("fourth_last"))
             .when(regexp_extract(col("Addr_Clean"), r",\s*([A-Z]{2})(?:\s*,|\s+[\d-])", 1) != "",
                  regexp_extract(col("Addr_Clean"), r",\s*([A-Z]{2})(?:\s*,|\s+[\d-])", 1))
             .otherwise(lit(None)))
        .otherwise(lit(None))
    )
    
    df = df.withColumn("has_state_code", col("state_code").isNotNull())
    
    df = df.withColumn("city_idx",
        when(col("has_postal_at_end") & col("has_state_code") & col("is_second_country"), -4)
        .when(col("has_postal_at_end") & col("has_state_code") & col("is_third_country"), -3)
        .when(col("has_postal_at_end") & ~col("has_state_code") & col("is_second_country"), -3)
        .when(col("has_postal_at_end") & ~col("has_state_code") & col("is_third_country"), -2)
        .when(~col("has_postal_at_end") & col("has_state_code") & col("is_last_country"), -3)
        .when(~col("has_postal_at_end") & col("has_state_code") & col("is_second_country"), -2)
        .when(col("is_last_country"), -2)
        .when(col("is_second_country"), -2)
        .otherwise(-2)
    )
    
    df = df.withColumn("city_from_idx",
        when(col("city_idx") == -2, col("second_last"))
        .when(col("city_idx") == -3, col("third_last"))
        .when(col("city_idx") == -4, col("fourth_last"))
        .otherwise(col("second_last"))
    )
    
    df = df.withColumn("city_pattern_regex",
        regexp_extract(col("Addr_Clean"), 
                      r",\s*([^,]+?)\s*,\s*(?:[A-Z]{2,4}\s*,\s*)?(?:" + country_list + r")", 1))
    
    df = df.withColumn("city",
        when((length(trim(col("city_pattern_regex"))) > 0) &
             (~col("city_pattern_regex").rlike(r"^\d+")) &
             (~col("city_pattern_regex").rlike(f"^({us_states}|{canadian_provinces})$")) &
             (~col("city_pattern_regex").rlike(r"^\d{4,5}$")),
             trim(col("city_pattern_regex")))
        .when((length(trim(col("city_from_idx"))) > 0) &
              (~col("city_from_idx").rlike(r"^[\d\s-]+$")) &
              (~col("city_from_idx").rlike(f"^({us_states}|{canadian_provinces})$")),
             trim(col("city_from_idx")))
        .otherwise(lit("Unknown"))
    )
    
    df = df.withColumn("city", regexp_replace(col("city"), r"^[\d\s]+", ""))
    df = df.withColumn("city", regexp_replace(col("city"), r"^\d+.*?\s+", ""))
    df = df.withColumn("city", regexp_replace(col("city"), f"\\s*\\b({us_states}|{canadian_provinces})\\b\\s*", ""))
    df = df.withColumn("city", regexp_replace(col("city"), r"\s*[\d-]{5,}.*$", ""))
    df = df.withColumn("city", regexp_replace(col("city"), r"\s*(?i:PO\s+Box|Suite).*$", ""))
    df = df.withColumn("city", regexp_replace(col("city"), r"[A-Z]\d[A-Z]\s\d[A-Z]\d", ""))
    df = df.withColumn("city", regexp_replace(col("city"), r"[A-Z]{1,2}\d[A-Z\d]?\s\d[A-Z]{2}", ""))
    df = df.withColumn("city", regexp_replace(col("city"), r"\d{4}\s[A-Z]{2}", ""))
    df = df.withColumn("city", regexp_replace(col("city"), r"[A-Z]{2}\s\d{2}$", ""))
    df = df.withColumn("city", trim(col("city")))
    df = df.withColumn("city", 
                      when((col("city") == "") | (col("city").isNull()), "Unknown")
                      .otherwise(col("city")))
    
    df = df.drop("Addr_Clean", "last_part", "second_last", "third_last", "fourth_last", "fifth_last",
                "is_last_pure_number", "is_last_uk_postal", "is_last_canada_postal", 
                "is_last_nld_postal", "is_last_bermuda_postal",
                "is_second_pure_number", "is_second_nld_postal",
                "is_last_country", "is_second_country", "is_third_country",
                "has_postal_at_end", "has_state_code",
                "city_idx", "city_from_idx", "city_pattern_regex")

df = df.select([col(c).alias(re.sub(r"[ ,;{}()\n\t=]", "_", c)) for c in df.columns])

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_PATH)

spark.stop()