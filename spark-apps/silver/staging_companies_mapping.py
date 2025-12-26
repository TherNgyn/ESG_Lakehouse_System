from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder \
    .appName("Silver: Staging Company Mapping") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("s3a://silver/checkpoints")

print("Loading data from all sources...")

df_snp_pulse = spark.read.format("delta").load("s3a://silver/clean_industrials_esg_score") \
    .select(
        col("Company_name").alias("company_name"),
        col("Symbol").alias("symbol"),
        col("ISIN").alias("isin"),
        col("gicSector").alias("sector"),
        col("gicSubindustry").alias("sub_industry"),
        col("country"),
        col("city"),
        lit(None).cast(StringType()).alias("industry"),
        lit(None).cast(StringType()).alias("region"),
        lit("snp_pulse").alias("source")
    )

df_esg_crawled = spark.read.format("delta").load("s3a://silver/clean_esg_score") \
    .select(
        col("company").alias("company_name"),
        col("industry"),
        lit(None).cast(StringType()).alias("symbol"),
        lit(None).cast(StringType()).alias("isin"),
        lit(None).cast(StringType()).alias("sector"),
        lit(None).cast(StringType()).alias("sub_industry"),
        lit(None).cast(StringType()).alias("city"),
        lit(None).cast(StringType()).alias("country"),
        lit(None).cast(StringType()).alias("region"),
        lit("esg_crawled").alias("source")
    )

df_esg_rank = spark.read.format("delta").load("s3a://silver/clean_esg_level_score") \
    .select(
        col("company_name"),
        col("ticker").alias("symbol"),
        col("industry"),
        lit(None).cast(StringType()).alias("isin"),
        lit(None).cast(StringType()).alias("sector"),
        lit(None).cast(StringType()).alias("sub_industry"),
        lit(None).cast(StringType()).alias("city"),
        lit(None).cast(StringType()).alias("country"),
        lit(None).cast(StringType()).alias("region"),
        lit("esg_rank").alias("source")
    )

# df_esg_score = spark.read.format("delta").load("s3a://silver/clean_esg_score_with_metrics") \
#     .select(
#         col("company_name"),
#         col("ticker").alias("symbol"),
#         col("industry"),
#         lit(None).cast(StringType()).alias("isin"),
#         lit(None).cast(StringType()).alias("sector"),
#         lit(None).cast(StringType()).alias("sub_industry"),
#         lit(None).cast(StringType()).alias("city"),
#         lit(None).cast(StringType()).alias("country"),
#         lit(None).cast(StringType()).alias("region"),
#         lit("esg_score").alias("source")
#     )

df_risk = spark.read.format("delta").load("s3a://silver/clean_sp500_esg_risk") \
    .select(
        col("company_name"),
        col("ticker").alias("symbol"),
        col("sector"),
        col("industry"),
        col("city"),
        col("country"),
        lit(None).cast(StringType()).alias("isin"),
        lit(None).cast(StringType()).alias("sub_industry"),
        lit(None).cast(StringType()).alias("region"),
        lit("risk").alias("source")
    )

df_rank = spark.read.format("delta").load("s3a://silver/clean_sustainability_rank") \
    .select(
        col("company_name"),
        col("isin"),
        col("hq_region").alias("region"),
        col("hq_country").alias("country"),
        col("sector"),
        col("industry"),
        lit(None).cast(StringType()).alias("symbol"),
        lit(None).cast(StringType()).alias("sub_industry"),
        lit(None).cast(StringType()).alias("city"),
        lit("rank").alias("source")
    )

df_metric = spark.read.format("delta").load("s3a://silver/classified_metrics") \
    .select(col("name").alias("company_name")) \
    .distinct() \
    .select(
        col("company_name"),
        lit(None).cast(StringType()).alias("symbol"),
        lit(None).cast(StringType()).alias("isin"),
        lit(None).cast(StringType()).alias("sector"),
        lit(None).cast(StringType()).alias("industry"),
        lit(None).cast(StringType()).alias("sub_industry"),
        lit(None).cast(StringType()).alias("city"),
        lit(None).cast(StringType()).alias("country"),
        lit(None).cast(StringType()).alias("region"),
        lit("metric").alias("source")
    )

df_all = df_snp_pulse \
    .unionByName(df_esg_crawled) \
    .unionByName(df_esg_rank) \
    .unionByName(df_risk) \
    .unionByName(df_rank) \
    .unionByName(df_metric)

total_records = df_all.count()
print(f"Total records: {total_records}")

@udf(returnType=StringType())
def normalize_name(name):
    if not name:
        return ""
    
    name = name.lower().strip()
    
    suffixes = [
        ' inc', ' corp', ' corporation', ' ltd', ' limited', ' company', 
        ' llc', ' plc', ' sa', ' nv', ' ag', ' se', ' gmbh', ' co', ' com'
    ]
    
    for suffix in suffixes:
        if name.endswith(suffix):
            remaining = name[:-len(suffix)].strip()
            if len(remaining) > 0 and remaining[-1] not in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']:
                name = remaining
                break
            else:
                words = name.split()
                if len(words) > 1:
                    last_word = words[-1]
                    if last_word == suffix.strip():
                        name = ' '.join(words[:-1])
                        break
    
    name = name.replace(',', '').replace('.', '').replace('&', 'and').replace("'", '')
    return ' '.join(name.split())

@udf(returnType=StringType())
def normalize_field(field):
    if not field:
        return None
    return field.lower().strip()

@udf(returnType=BooleanType())
def is_valid_isin(isin):
    if not isin:
        return False
    isin = str(isin).strip()
    if isin in ['Unknown', 'N/A', 'nan', 'None', '']:
        return False
    if len(isin) < 10:
        return False
    return True

df_all = df_all \
    .withColumn("name_norm", normalize_name(col("company_name"))) \
    .withColumn("sector_norm", normalize_field(col("sector"))) \
    .withColumn("industry_norm", normalize_field(col("industry"))) \
    .withColumn("country_norm", normalize_field(col("country"))) \
    .withColumn("isin_valid", is_valid_isin(col("isin"))) \
    .withColumn("name_block", substring(col("name_norm"), 1, 3))

# THỐNG NHẤT ID GỐC: REC-000001
windowAll = Window.orderBy("company_name", "source")
df_all = df_all.withColumn("staging_id", 
    concat(lit("REC-"), lpad(row_number().over(windowAll).cast("string"), 6, "0")))

print("\nData Statistics:")
records_with_isin = df_all.filter(col('isin_valid') == True).count()
print(f"Records with valid ISIN: {records_with_isin}")
print(f"Records without ISIN: {total_records - records_with_isin}")

print("\nRecords by source:")
df_all.groupBy("source").count().orderBy("source").show()

print("\nISIN availability by source:")
df_all.groupBy("source").agg(sum(when(col("isin_valid"), 1).otherwise(0)).alias("valid_isin_count")).show()

@udf(returnType=IntegerType())
def jaccard_similarity(s1, s2):
    if not s1 or not s2:
        return 0
    
    tokens1 = s1.lower().split()
    tokens2 = s2.lower().split()
    
    if len(tokens1) == 0 or len(tokens2) == 0:
        return 0
    
    set1 = {}
    for token in tokens1:
        set1[token] = 1
    
    set2 = {}
    for token in tokens2:
        set2[token] = 1
    
    intersection_count = 0
    for token in set1:
        if token in set2:
            intersection_count += 1
    
    union_count = len(set1)
    for token in set2:
        if token not in set1:
            union_count += 1
    
    if union_count == 0:
        return 0
    
    similarity = (intersection_count * 100.0) / union_count
    return int(similarity)

print("\n" + "="*60)
print("PHASE 1: Initializing with 'rank' source")
print("="*60)

df_rank_records = df_all.filter(col("source") == "rank")
rank_count = df_rank_records.count()
print(f"\nRecords from 'rank' source: {rank_count}")

# THỐNG NHẤT ID CÔNG TY: CMP-00001
windowPhase1 = Window.orderBy("staging_id")
df_rank_with_id = df_rank_records \
    .withColumn("matched_company_id", 
                concat(lit("CMP-"), lpad(row_number().over(windowPhase1).cast("string"), 5, "0")))

df_other_records = df_all.filter(col("source") != "rank")

df_staging = df_rank_with_id.unionByName(
    df_other_records.withColumn("matched_company_id", lit(None).cast(StringType()))
)

print("Checkpointing after Phase 1...")
df_staging = df_staging.localCheckpoint(eager=True)

last_id_num = rank_count

print(f"Initial companies created: {rank_count}")
print(f"Next available company number: {last_id_num + 1}")

print("\n" + "="*60)
print("PHASE 2: Processing other sources sequentially")
print("="*60)

other_sources = ['snp_pulse', 'esg_crawled', 'esg_rank', 'esg_score', 'risk', 'metric']

merge_count = 0
new_count = 0

for source_name in other_sources:
    print(f"\n{'='*50}")
    print(f"Processing source: {source_name}")
    print(f"{'='*50}")
    
    df_source = df_staging.filter(
        (col("source") == source_name) & 
        col("matched_company_id").isNull()
    )
    
    source_count = df_source.count()
    print(f"Records to process: {source_count}")
    
    if source_count == 0:
        print("No records to process, skipping...")
        continue
    
    df_existing = df_staging.filter(col("matched_company_id").isNotNull()) \
        .groupBy("matched_company_id") \
        .agg(
            first("name_norm").alias("name_norm"),
            first("name_block").alias("name_block"),
            first("sector_norm").alias("sector_norm"),
            first("industry_norm").alias("industry_norm"),
            first("country_norm").alias("country_norm")
        )
    
    existing_count = df_existing.count()
    print(f"Existing companies to match against: {existing_count}")
    
    df_matches = df_source.alias("new").join(
        broadcast(df_existing.alias("existing")),
        on=(col("new.name_block") == col("existing.name_block")),
        how="inner"
    )
    
    df_matches = df_matches \
        .withColumn("name_sim", jaccard_similarity(col("new.name_norm"), col("existing.name_norm"))) \
        .filter(col("name_sim") >= 85)
    
    df_matches = df_matches.withColumn(
        "match_score",
        col("name_sim") +
        when(
            col("new.country_norm").isNotNull() & 
            col("existing.country_norm").isNotNull() & 
            (col("new.country_norm") == col("existing.country_norm")), 
            15
        ).when(
            col("new.country_norm").isNotNull() & 
            col("existing.country_norm").isNotNull() & 
            (col("new.country_norm") != col("existing.country_norm")), 
            -20
        ).otherwise(0) +
        when(
            col("new.sector_norm").isNotNull() & 
            col("existing.sector_norm").isNotNull() & 
            (col("new.sector_norm") == col("existing.sector_norm")), 
            10
        ).otherwise(0) +
        when(
            col("new.industry_norm").isNotNull() & 
            col("existing.industry_norm").isNotNull() & 
            (col("new.industry_norm") == col("existing.industry_norm")), 
            10
        ).otherwise(0)
    ).filter(col("match_score") >= 100)
    
    window_best = Window.partitionBy("new.staging_id").orderBy(desc("match_score"), desc("name_sim"))
    df_best_matches = df_matches \
        .withColumn("rank", row_number().over(window_best)) \
        .filter(col("rank") == 1) \
        .select(
            col("new.staging_id").alias("source_staging_id"),
            col("existing.matched_company_id").alias("best_match_id"),
            col("match_score"),
            col("name_sim")
        )
    
    matched_this_round = df_best_matches.count()
    
    if matched_this_round > 0:
        print(f"Matched records: {matched_this_round}")
        merge_count += matched_this_round
        
        df_staging = df_staging.join(
            df_best_matches.select("source_staging_id", "best_match_id"),
            df_staging.staging_id == df_best_matches.source_staging_id,
            "left"
        ).withColumn(
            "matched_company_id",
            coalesce(col("best_match_id"), col("matched_company_id"))
        ).drop("source_staging_id", "best_match_id")
    
    df_still_unmatched = df_staging.filter(
        (col("source") == source_name) & 
        col("matched_company_id").isNull()
    )
    
    unmatched_count = df_still_unmatched.count()
    
    if unmatched_count > 0:
        print(f"New companies created: {unmatched_count}")
        
        windowNew = Window.orderBy("staging_id")
        df_new_ids = df_still_unmatched.select("staging_id") \
            .withColumn("new_company_id", 
                       concat(lit("CMP-"), lpad((row_number().over(windowNew) + last_id_num).cast("string"), 5, "0")))
        
        df_staging = df_staging.join(
            df_new_ids,
            "staging_id",
            "left"
        ).withColumn(
            "matched_company_id",
            coalesce(col("new_company_id"), col("matched_company_id"))
        ).drop("new_company_id")
        
        last_id_num += unmatched_count
        new_count += unmatched_count
    
    df_staging = df_staging.localCheckpoint(eager=True)

print("\n" + "="*60)
print("PHASE 3: Consolidating Company Attributes (Golden Records)")
print("="*60)

# 1. Tạo bảng tham chiếu Golden Record bằng cách lấy thông tin đầy đủ nhất từ các nguồn
# Sử dụng first(col, ignorenulls=True) để lấy giá trị có dữ liệu đầu tiên tìm thấy
df_golden_records = df_staging.groupBy("matched_company_id").agg(
    first("company_name", ignorenulls=True).alias("company_name_final"),
    first("symbol", ignorenulls=True).alias("symbol_final"),
    first("isin", ignorenulls=True).alias("isin_final"),
    first("sector", ignorenulls=True).alias("sector_final"),
    first("industry", ignorenulls=True).alias("industry_final"),
    first("sub_industry", ignorenulls=True).alias("sub_industry_final"),
    first("city", ignorenulls=True).alias("city_final"),
    first("country", ignorenulls=True).alias("country_final"),
    first("region", ignorenulls=True).alias("region_final"),
    first("sector_norm", ignorenulls=True).alias("sector_norm_final"),
    first("industry_norm", ignorenulls=True).alias("industry_norm_final"),
    first("country_norm", ignorenulls=True).alias("country_norm_final")
)

# 2. Join ngược lại bảng staging để điền dữ liệu thiếu vào các bản ghi ban đầu
df_consolidated = df_staging.join(
    df_golden_records,
    "matched_company_id",
    "left"
)

# 3. Sử dụng coalesce để ưu tiên dữ liệu gốc, nếu null thì lấy dữ liệu từ Golden Record (nguồn khác)
staging_final = df_consolidated.select(
    col("staging_id"),
    col("matched_company_id"),
    coalesce(col("company_name"), col("company_name_final")).alias("company_name"),
    coalesce(col("symbol"), col("symbol_final")).alias("symbol"),
    coalesce(col("isin"), col("isin_final")).alias("isin"),
    coalesce(col("sector"), col("sector_final")).alias("sector"),
    coalesce(col("industry"), col("industry_final")).alias("industry"),
    coalesce(col("sub_industry"), col("sub_industry_final")).alias("sub_industry"),
    coalesce(col("city"), col("city_final")).alias("city"),
    coalesce(col("country"), col("country_final")).alias("country"),
    coalesce(col("region"), col("region_final")).alias("region"),
    col("source"),
    col("name_norm"),
    coalesce(col("sector_norm"), col("sector_norm_final")).alias("sector_norm"),
    coalesce(col("industry_norm"), col("industry_norm_final")).alias("industry_norm"),
    coalesce(col("country_norm"), col("country_norm_final")).alias("country_norm"),
    col("isin_valid")
)

print("\n" + "="*60)
print("FINAL STATISTICS")
print("="*60)

total_companies = staging_final.select("matched_company_id").distinct().count()
print(f"\nTotal unique companies: {total_companies}")

# Lưu dữ liệu
staging_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("sector_norm") \
    .save("s3a://silver/staging_companies_mapping")

print("Successfully saved consolidated staging table")
spark.stop()