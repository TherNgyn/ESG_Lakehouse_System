from pyspark.sql import SparkSession

#  Cấu hình Spark kết nối với MinIO

spark = SparkSession.builder.appName("Transform_data_from_Bronze_layers")\
                            .config("spark.hadoop.fs.s3a.endpoint", 'http://minio:9000')\
                            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")\
                            .config("spark.hadoop.fs.s3a.secret.key", 'minioadmin')\
                            .config("spark.hadoop.fs.s3a.path.style.access", 'true')\
                            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                            .getOrCreate()



# 3 loại df tương tự như: ESG_score, ESG_risk, ESG_rank
ESG_rank_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .csv("s3a://bronze/raw/ESG_rank/**/*.csv")
ESG_risk_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .csv("s3a://bronze/raw/ESG_risk/**/*.csv")
ESG_score_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .csv("s3a://bronze/raw/ESG_score/**/*.csv")

def processing_esg_score():
    pass
def processing_esg_rank():
    pass
def processing_esg_risk():
    pass

