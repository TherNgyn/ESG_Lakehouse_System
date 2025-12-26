from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re

try:
    from esg_norm_config import STANDARD_NAMES_BY_GROUP, GLOBAL_STANDARD_NAMES
except ImportError:
    STANDARD_NAMES_BY_GROUP = {}
    GLOBAL_STANDARD_NAMES = []

try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    EMBEDDING_AVAILABLE = True
except ImportError:
    EMBEDDING_AVAILABLE = False

INPUT_PATH = "s3a://silver/semantic_classified"
OUTPUT_PATH = "s3a://silver/normalized_metrics_csv"
CONFIDENCE_THRESHOLD = 0.75
MODEL_NAME = "intfloat/e5-large-v2"

spark = SparkSession.builder \
    .appName("Optimized-Normalizer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.default.parallelism", "8") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryoserializer.buffer.max", "512m") \
    .getOrCreate()

normalized_names = []
seen = set()
for name in GLOBAL_STANDARD_NAMES:
    if not name:
        continue
    cleaned = name.strip().replace("–", "-").replace("—", "-").replace("―", "-")
    cleaned = re.sub(r'\s+', ' ', cleaned)
    if cleaned.lower() not in seen:
        seen.add(cleaned.lower())
        normalized_names.append(cleaned)
GLOBAL_STANDARD_NAMES = sorted(normalized_names)

ENTITY_ACTIVITY_BOOST = {
    ('organization', 'consumption'): ['renewable energy', 'water consumption'],
    ('organization', 'withdrawal'): ['water consumption'],
    ('organization', 'discharge'): ['water discharge', 'water recycled'],
    ('organization', 'emission'): ['scope 1 emissions', 'scope 2 emissions', 'scope 3 emissions'],
    ('organization', 'disposal'): ['hazardous waste', 'total waste'],
    ('organization', 'recycling'): ['waste recycled'],
    ('employees', 'composition'): ['gender diversity', 'minority representation'],
    ('employees', 'turnover'): ['employee turnover'],
    ('employees', 'training'): ['employee training'],
    ('employees', 'injury'): ['lost time injury', 'recordable injury'],
}

if EMBEDDING_AVAILABLE:
    model = SentenceTransformer(MODEL_NAME)
    
    GROUP_EMBEDDINGS = {}
    for group_name, std_list in STANDARD_NAMES_BY_GROUP.items():
        if std_list:
            emb = model.encode([f"passage: {s}" for s in std_list], 
                             normalize_embeddings=True, batch_size=64, show_progress_bar=False)
            GROUP_EMBEDDINGS[group_name] = (std_list, emb)
    
    GLOBAL_EMBEDDINGS = model.encode([f"passage: {s}" for s in GLOBAL_STANDARD_NAMES],
                                    normalize_embeddings=True, batch_size=64, show_progress_bar=False)

def batch_embed_local(df):
    if not EMBEDDING_AVAILABLE:
        return df
    
    unique_metrics = df.select("metric_name", "metric_group", "entity", "activity").distinct().collect()
    
    if not unique_metrics:
        return df
    
    batch_names = [row.metric_name for row in unique_metrics]
    batch_emb = model.encode([f"query: {n}" for n in batch_names],
                            normalize_embeddings=True, batch_size=64, show_progress_bar=False)
    
    mapping = []
    
    for i, row in enumerate(unique_metrics):
        group_key = row.metric_group.lower() if row.metric_group else None
        
        if group_key and group_key in GROUP_EMBEDDINGS:
            standards, std_emb = GROUP_EMBEDDINGS[group_key]
            filter_type = "group"
        elif row.entity and row.activity:
            e_list = [e.strip() for e in str(row.entity).split(',')]
            a_list = [a.strip() for a in str(row.activity).split(',')]
            boost_groups = []
            for e in e_list:
                for a in a_list:
                    boost_groups.extend(ENTITY_ACTIVITY_BOOST.get((e, a), []))
            if boost_groups:
                combined = []
                for g in set(boost_groups):
                    if g in GROUP_EMBEDDINGS:
                        combined.extend(GROUP_EMBEDDINGS[g][0])
                if combined:
                    standards = list(set(combined))
                    std_emb = model.encode([f"passage: {s}" for s in standards],
                                          normalize_embeddings=True, batch_size=64, show_progress_bar=False)
                    filter_type = "semantic"
                else:
                    standards = GLOBAL_STANDARD_NAMES
                    std_emb = GLOBAL_EMBEDDINGS
                    filter_type = "global"
            else:
                standards = GLOBAL_STANDARD_NAMES
                std_emb = GLOBAL_EMBEDDINGS
                filter_type = "global"
        else:
            standards = GLOBAL_STANDARD_NAMES
            std_emb = GLOBAL_EMBEDDINGS
            filter_type = "global"
        
        sim = cosine_similarity([batch_emb[i]], std_emb)[0]
        best_idx = np.argmax(sim)
        confidence = float(sim[best_idx])
        
        if confidence >= CONFIDENCE_THRESHOLD:
            mapping.append((row.metric_name, standards[best_idx], confidence, filter_type))
    
    if mapping:
        schema = StructType([
            StructField("metric_name", StringType(), False),
            StructField("metric_norm", StringType(), False),
            StructField("norm_confidence", DoubleType(), False),
            StructField("norm_filter", StringType(), False)
        ])
        map_df = spark.createDataFrame(mapping, schema)
        df = df.join(broadcast(map_df), "metric_name", "left")
    else:
        df = df.withColumn("metric_norm", lit(None).cast(StringType()))
        df = df.withColumn("norm_confidence", lit(None).cast("double"))
        df = df.withColumn("norm_filter", lit(None).cast(StringType()))
    
    return df

def apply_native_rules(df):
    df = df.withColumn("metric_name_clean",
        regexp_replace(
            regexp_replace(lower(col("metric_name")), r"healthy workforce.*\s+pmx$", ""),
            r"(percentage|training|investment).*\s+pmx$", ""
        )
    )
    df = df.withColumn("metric_name", col("metric_name_clean")).drop("metric_name_clean")
    
    df = df.withColumn("rule_norm",
        when(col("metric_name").contains("healthy workforce") & 
             col("metric_name").contains("(employees by type) full-time"), "Employees - Full-Time")
        .when(col("metric_name").contains("healthy workforce") & 
              col("metric_name").contains("(employees by age) under 30"), "Employees - Age Under 30")
        .when(col("metric_name").contains("healthy workforce") & 
              col("metric_name").contains("(employees by gender) women"), "Employees - Women")
        .when(col("metric_name").rlike(r"\bscope 1\b") & 
              ~col("metric_name").contains("scope 2"),
              when(col("metric_name").contains("intensity"), "Scope 1 Emissions Intensity")
              .otherwise("Scope 1 GHG Emissions - Total"))
        .when(col("metric_name").rlike(r"\bscope 2\b") & 
              ~col("metric_name").contains("scope 1"),
              when(col("metric_name").contains("market"), "Scope 2 Emissions - Market-Based")
              .otherwise("Scope 2 Emissions - Total"))
        .when(col("metric_name").rlike(r"\bscope 3\b"), "Scope 3 Emissions - Total")
        .when(col("metric_name").contains("nitrogen oxides") | 
              (col("metric_name").contains("nox") & col("metric_name").contains("emission")), "NOx")
        .when(col("metric_name").contains("sulfur oxides") | 
              (col("metric_name").contains("sox") & col("metric_name").contains("emission")), "SOx")
        .when(col("metric_name").contains("carbon monoxide") & 
              col("metric_name").contains("emission"), "CO")
        .when(col("metric_name").contains("particulate matter") | 
              (col("metric_name").contains("pm") & col("metric_name").contains("emission")), "PMx")
        .when(col("metric_name").contains("volatile organic") | 
              (col("metric_name").contains("voc") & col("metric_name").contains("emission")), "VOC")
        .when((col("metric_group") == "employee turnover") & 
              col("metric_name").contains("retention") & col("metric_name").contains("women"), 
              "Retention Rate - Women")
        .when((col("metric_group") == "employee turnover") & 
              col("metric_name").contains("retention"), "Retention Rate")
        .when((col("metric_group") == "employee turnover") & 
              col("metric_name").contains("attrition") & col("metric_name").contains("voluntary"), 
              "Voluntary Attrition Rate")
        .when((col("metric_group") == "employee turnover") & 
              col("metric_name").contains("attrition"), "Attrition Rate")
        .when((col("metric_group") == "employee turnover") & 
              col("metric_name").contains("voluntary") & col("metric_name").contains("turnover"), 
              "Voluntary Turnover Rate - Total")
        .when((col("metric_group") == "employee turnover") & 
              col("metric_name").contains("turnover"), "Employee Turnover Rate - Total")
        .when(col("metric_name").rlike(r"audit committee$"), "Audit Committee")
        .when(col("metric_name").contains("data leakage") & 
              col("metric_name").contains("total number"), "Data Breach - Incident Count")
        .otherwise(None)
    )
    
    df = df.withColumn("final_norm",
        coalesce(col("rule_norm"), col("metric_norm"), col("metric_name"))
    )
    
    return df.withColumn("metric_norm", col("final_norm")).drop("rule_norm", "final_norm")

def main():
    df = spark.read.format("delta").load(INPUT_PATH)
    
    df = df.withColumn("metric_name", lower(col("metric_name")))
    
    df = df.repartition(8, "metric_group")
    df = df.cache()
    
    df = batch_embed_local(df)
    
    df = apply_native_rules(df)
    
    for c in ["name", "topic", "metric_category", "metric_group", "units"]:
        if c in df.columns:
            df = df.withColumn(c, initcap(col(c)))
    
    df = df.withColumn("extract_date", lit(datetime.now().strftime("%Y-%m-%d")))
    df = df.filter(col("year").isNotNull())
    
    # Write to CSV with single partition for full output
    df.coalesce(1).write.format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .save(OUTPUT_PATH)
    
    print(f"CSV file written to: {OUTPUT_PATH}")
    print(f"Total records: {df.count()}")
    
    df.unpersist()
    spark.stop()

if __name__ == "__main__":
    main()