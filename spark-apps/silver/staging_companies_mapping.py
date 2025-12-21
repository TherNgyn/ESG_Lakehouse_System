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

df_esg_score = spark.read.format("delta").load("s3a://silver/clean_esg_score_with_metrics") \
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
        lit("esg_score").alias("source")
    )

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

df_metric = spark.read.format("delta").load("s3a://silver/normalized_metrics") \
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
    .unionByName(df_esg_score) \
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
    .withColumn("name_block", substring(col("name_norm"), 1, 3)) \
    .withColumn("staging_id", monotonically_increasing_id())

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

df_rank_with_id = df_rank_records \
    .withColumn("matched_company_id", 
                row_number().over(Window.partitionBy(lit(1)).orderBy("staging_id")))

df_other_records = df_all.filter(col("source") != "rank")

df_staging = df_rank_with_id.unionByName(
    df_other_records.withColumn("matched_company_id", lit(None).cast(IntegerType()))
)

print("Checkpointing after Phase 1...")
df_staging = df_staging.localCheckpoint(eager=True)

max_company_id = df_rank_with_id.agg(max("matched_company_id")).collect()[0][0]
next_company_id = max_company_id + 1 if max_company_id else 1

print(f"Initial companies created: {rank_count}")
print(f"Next available company_id: {next_company_id}")

print("\n" + "="*60)
print("PHASE 2: Processing other sources sequentially")
print("="*60)

other_sources = ['snp_pulse', 'esg_crawled', 'esg_rank', 'esg_score', 'risk', 'metric']

merge_count = 0
new_count = 0
current_next_id = next_company_id

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
        
        print("\nSample matches:")
        sample_matches = df_source.alias("s").join(
            df_best_matches.alias("m"),
            col("s.staging_id") == col("m.source_staging_id"),
            "inner"
        ).join(
            df_staging.filter(col("matched_company_id").isNotNull()).alias("e"),
            col("m.best_match_id") == col("e.matched_company_id"),
            "inner"
        ).select(
            col("s.company_name").alias("new_company"),
            col("s.source").alias("new_source"),
            col("e.company_name").alias("existing_company"),
            col("m.match_score"),
            col("m.name_sim")
        ).limit(10)
        
        sample_matches.show(truncate=False)
        
        df_staging = df_staging.join(
            df_best_matches.select("source_staging_id", "best_match_id"),
            df_staging.staging_id == df_best_matches.source_staging_id,
            "left"
        ).withColumn(
            "matched_company_id",
            coalesce(col("best_match_id"), col("matched_company_id"))
        ).drop("source_staging_id", "best_match_id")
    else:
        print("No matches found")
    
    df_still_unmatched = df_staging.filter(
        (col("source") == source_name) & 
        col("matched_company_id").isNull()
    )
    
    unmatched_count = df_still_unmatched.count()
    
    if unmatched_count > 0:
        print(f"New companies created: {unmatched_count}")
        new_count += unmatched_count
        
        df_new_ids = df_still_unmatched.select("staging_id") \
            .withColumn("new_company_id", 
                       row_number().over(Window.partitionBy(lit(1)).orderBy("staging_id")) + current_next_id - 1)
        
        df_staging = df_staging.join(
            df_new_ids,
            "staging_id",
            "left"
        ).withColumn(
            "matched_company_id",
            coalesce(col("new_company_id"), col("matched_company_id"))
        ).drop("new_company_id")
        
        current_next_id += unmatched_count
    else:
        print("All records matched, no new companies")
    
    print(f"Checkpointing after processing {source_name}...")
    df_staging = df_staging.localCheckpoint(eager=True)
    
    current_companies = df_staging.select("matched_company_id").distinct().count()
    print(f"\nCurrent total unique companies: {current_companies}")

print("\n" + "="*60)
print("FINAL STATISTICS")
print("="*60)

total_companies = df_staging.select("matched_company_id").distinct().count()
print(f"\nTotal unique companies: {total_companies}")
print(f"Total records merged (Phase 2): {merge_count}")
print(f"Total new companies created (Phase 2): {new_count}")

print("\nValidation - Records without matched_company_id:")
unassigned = df_staging.filter(col("matched_company_id").isNull()).count()
print(f"Unassigned records: {unassigned}")

if unassigned > 0:
    print("WARNING: Some records were not assigned a matched_company_id!")
    df_staging.filter(col("matched_company_id").isNull()).show(10)

print("\nCompany size distribution (top 30):")
df_staging.groupBy("matched_company_id") \
    .count() \
    .orderBy(desc("count")) \
    .show(30)

print("\nSample of matched companies:")
df_staging.filter(col("matched_company_id").isNotNull()) \
    .select(
        "matched_company_id",
        "company_name",
        "source",
        "country",
        "sector",
        "name_norm"
    ) \
    .orderBy("matched_company_id", "company_name") \
    .show(50, truncate=False)

print("\n" + "="*60)
print("SAVING STAGING TABLE")
print("="*60)

staging_final = df_staging.select(
    "staging_id",
    "matched_company_id",
    "company_name",
    "symbol",
    "isin",
    "sector",
    "industry",
    "sub_industry",
    "city",
    "country",
    "region",
    "source",
    "name_norm",
    "sector_norm",
    "industry_norm",
    "country_norm",
    "isin_valid"
)

print("\nWriting to s3a://silver/staging_companies_mapping...")

staging_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://silver/staging_companies_mapping")

print("Successfully saved staging table")

print("\nStaging table schema:")
staging_final.printSchema()

print("\nRow count verification:")
saved_count = spark.read.format("delta").load("s3a://silver/staging_companies_mapping").count()
print(f"Records saved: {saved_count}")
print(f"Original records: {total_records}")

if saved_count == total_records:
    print("All records saved successfully!")
else:
    print("Warning: Record count mismatch!")

spark.stop()