from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Bronze-to-Silver: All ESG Reference Tables") \
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

df1 = spark.read.csv("s3a://bronze/raw/esg_rank/company_rank_esg.csv", header=True, inferSchema=True)
esg_rank_score_df = df1.select(
    upper(col("ticker")).alias("ticker"),
    trim(col("name")).alias("company_name"),
    trim(col("industry")).alias("industry"),
    col("environment_grade"), col("social_grade"), col("governance_grade"),
    lower(col("environment_level")).alias("environment_level"),
    lower(col("social_level")).alias("social_level"),
    lower(col("governance_level")).alias("governance_level"),
    (col("environment_score")/10).alias("environment_score"),
    (col("social_score")/10).alias("social_score"),
    (col("governance_score")/10).alias("governance_score"),
    lit(2022).alias("year"),
    col("total_grade"),
    lower(trim(col("total_level"))).alias("total_level")
).filter(
    col("environment_score").between(10, 100) &
    col("social_score").between(10, 100) &
    col("governance_score").between(10, 100)
).dropDuplicates(["ticker"])

esg_rank_score_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save("s3a://silver/clean_esg_level_score")

del df1
del esg_rank_score_df
# df2 = spark.read.format("com.crealytics.spark.excel") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .option("timestampFormat", "yyyy-MM-dd") \
#     .load("s3a://bronze/raw/esg_score/esg_and_score_dataset.xlsx")

# processed_esg_score_df = df2.select(
#     col("Identifier (RIC)").alias("ticker"),
#     trim(col("Company Name")).alias("company_name"),
#     year(
#         expr("date_add(to_date('1899-12-30'), cast(`Date` as int))")
#     ).alias("year"),
#     col("ESG_score"), col("Env_score"), col("Social_score"), col("Gov_score"),
#     lower(trim(col("Industry"))).alias("industry"),
#     when(col("Scope_1").isNull() | (col("Scope_1") < 0), -1).otherwise(col("Scope_1")).alias("scope_1"),
#     when(col("Scope_2").isNull() | (col("Scope_2") < 0), -1).otherwise(col("Scope_2")).alias("scope_2"),
#     when(col("CO2_emissions").isNull() | (col("CO2_emissions") < 0), -1).otherwise(col("CO2_emissions")).alias("co2_emissions"),
#     when(col("Energy_use").isNull() | (col("Energy_use") < 0), -1).otherwise(col("Energy_use")).alias("energy_use"),
#     when(col("Water_use").isNull() | (col("Water_use") < 0), -1).otherwise(col("Water_use")).alias("water_use"),
#     when(col("Water_recycle").isNull() | (col("Water_recycle") < 0), -1).otherwise(col("Water_recycle")).alias("water_recycle"),
#     when(col("Injury_rate").isNull() | (col("Injury_rate") > 100), -1).otherwise(col("Injury_rate")).alias("injury_rate"),
#     when(col("Women_Employees").isNull() | (col("Women_Employees") > 100), -1).otherwise(col("Women_Employees")).alias("women_employees_rate"),
#     when(col("Human_Rights").isin(0,1), col("Human_Rights")).otherwise(-1).alias("human_right"),
#     when(col("Turnover_empl").isNull() | (col("Turnover_empl") > 100), -1).otherwise(col("Turnover_empl")).alias("turnover_rate"),
#     when(col("Board_Size").isNull() | (col("Board_Size") < 0), -1).otherwise(col("Board_Size")).alias("board_size"),
#     when(col("Bribery").isNull() | (col("Bribery") < 0), -1).otherwise(col("Bribery")).alias("bribery"),
#     when(col("Strikes").isNull() | (col("Strikes") < 0), -1).otherwise(col("Strikes")).alias("strike"),
#     when(col("Recycling_Initiatives").isNull() | (col("Recycling_Initiatives") < 0), -1).otherwise(col("Recycling_Initiatives")).alias("recycling_initiatives")
# ).filter(
#     col("ESG_score").between(0,100) &
#     col("Env_score").between(0,100) &
#     col("Gov_score").between(0,100) &
#     col("Social_score").between(0,100)
# ).dropDuplicates(["ticker","year"])

# processed_esg_score_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save("s3a://silver/clean_esg_score_with_metrics")
from pyspark.sql import functions as F
# 1. Đọc dữ liệu từ S3 (Sử dụng inferSchema=false để an toàn bộ nhớ)
df3 = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .load("s3a://bronze/raw/esg_risk/SP_500_ESG_Risk_Ratings.xlsx")

# 2. Bước Select, Cast và lọc Null ban đầu
processed_risk_df = df3.select(
    F.upper(F.trim(F.col("Symbol"))).alias("ticker"),
    F.trim(F.col("Name")).alias("company_name"),
    F.trim(F.col("Address")).alias("address"),
    F.trim(F.col("Sector")).alias("sector"),
    F.trim(F.col("Industry")).alias("industry"),
    F.col("Full Time Employees").cast("integer").alias("full_time_employees"),
    F.trim(F.col("Description")).alias("description"),
    F.col("Total ESG Risk score").cast("double").alias("total_esg_risk_score"),
    F.col("Environment Risk Score").cast("double").alias("environment_risk_score"),
    F.col("Governance Risk Score").cast("double").alias("governance_risk_score"),
    F.col("Social Risk Score").cast("double").alias("social_risk_score"),
    F.col("Controversy Score").cast("double").alias("controversy_score"),
    F.lower(F.trim(F.col("Controversy Level"))).alias("controversy_level"),
    F.lower(F.trim(F.col("ESG Risk Percentile"))).alias("esg_risk_percentile"),
    F.lower(F.trim(F.col("ESG Risk Level"))).alias("esg_risk_level")
).filter("ticker IS NOT NULL AND ticker != '' AND company_name IS NOT NULL")

# 3. Trích xuất thông tin địa chỉ bằng Regex
processed_risk_df = processed_risk_df \
    .withColumn("state", F.regexp_extract(F.col("address"), r",\s*([A-Z]{2})\s+\d{5}", 1)) \
    .withColumn("city", F.regexp_extract(F.col("address"), r",\s*([A-Za-z\s]+?),\s*[A-Z]{2}", 1)) \
    .withColumn("country", F.regexp_extract(F.col("address"), r"\n([A-Za-z\s]+)$", 1))

# 4. Tính toán Median cho các cột số (thay thế logic Pandas)
numeric_cols = [c for c, t in processed_risk_df.dtypes if t in ['double', 'int']]
string_cols = [c for c, t in processed_risk_df.dtypes if t == 'string']

# Tính median phân tán bằng percentile_approx
median_map = {}
for col_name in numeric_cols:
    # Lấy giá trị median (50th percentile)
    median_val = processed_risk_df.select(F.percentile_approx(F.col(col_name), 0.5)).collect()[0][0]
    median_map[col_name] = median_val if median_val is not None else -1

# 5. Điền giá trị trống (Fillna) và xử lý giá trị âm
processed_risk_df = processed_risk_df.fillna(median_map).fillna("Unknown", subset=string_cols)

# Áp dụng logic: Nếu < 0 thì gán -1 (theo yêu cầu của bạn)
for col_name in numeric_cols:
    processed_risk_df = processed_risk_df.withColumn(
        col_name, 
        F.when(F.col(col_name) < 0, -1).otherwise(F.col(col_name))
    )

# 6. Ghi dữ liệu xuống Silver (Delta Lake)
processed_risk_df.distinct().write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://silver/clean_sp500_esg_risk")

# Giải phóng bộ nhớ Driver
del df3
del processed_risk_df


spark.stop()