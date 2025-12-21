import os
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)
from minio import Minio

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123456")
MINIO_BUCKET = "bronze" 
INPUT_DIR = "/opt/spark-data/input"
PDF_INPUT_DIR = "/opt/spark-data/input/pdf"
BRONZE_BASE = "s3a://bronze/raw"

spark = SparkSession.builder \
    .appName("ESG-Bronze-Raw-Ingest") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


SUPPORTED_EXT = {".csv", ".xlsx", ".xls", ".json", ".parquet", ".pdf"}

company_map = {}

minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

def normalize_company(name):
    clean = re.sub(r'[^a-zA-Z0-9]', '', name).lower()
    return company_map.get(clean) or company_map.get(name.lower()) or name.title()

def extract_company(filename):
    name = os.path.splitext(filename)[0]
    lower = filename.lower()
    if any(k in lower for k in ['score', 'rank', 'risk']) and 'kpi' not in lower:
        return "Multiple"
    match = re.match(r'^(.+?)_KPI', name, re.IGNORECASE)
    if match:
        return normalize_company(match.group(1))
    parts = re.split(r'[_-]', name)
    return normalize_company(parts[0]) if parts else "Unknown"

def get_table_type(filename):
    lower = filename.lower()
    if 'kpi' in lower:           return "kpi"
    if 'score' in lower:         return "esg_score"
    if 'rank' in lower:          return "esg_rank"
    if 'risk' in lower:          return "esg_risk"
    if filename.endswith('.pdf'): return "pdf"
    return "other"


def build_s3_path(filename, table_type, company, ingest_date):
    if table_type == "kpi":
        return f"raw/kpi/company={company}/{filename}"
    elif table_type == "pdf":
        return f"raw/pdf/{filename}"
    else:
        return f"raw/{table_type}/{filename}"

def collect_files(directories):
    files = []
    for directory in directories:
        if not os.path.exists(directory):
            continue
        files.extend([
            os.path.join(r, f) for r, _, fs in os.walk(directory)
            for f in fs if os.path.splitext(f)[1].lower() in SUPPORTED_EXT
        ])
    return files

def main():
    files = collect_files([INPUT_DIR, PDF_INPUT_DIR])

    if not files:
        print("No files found to ingest")
        return

    ingest_date = datetime.now().strftime("%Y-%m-%d")
    metadata = []
    success_count = 0

    print(f"Starting Bronze ingestion: {len(files)} files")

    for file_path in files:
        filename = os.path.basename(file_path)
        table_type = get_table_type(filename)
        company = extract_company(filename)
        size_bytes = os.path.getsize(file_path)
        s3_path = build_s3_path(filename, table_type, company, ingest_date)

        try:
            minio_client.fput_object(MINIO_BUCKET, s3_path, file_path)
            status = "success"

            success_key = os.path.dirname(s3_path) + "/_SUCCESS"
            minio_client.fput_object(MINIO_BUCKET, success_key, "/dev/null")

            success_count += 1
            print(f"Uploaded: {filename} → {company}/{table_type}")
        except Exception as e:
            status = "failed"
            print(f"Failed: {filename} | {str(e)[:80]}")

        metadata.append((
            filename,
            company,
            table_type,
            ingest_date,
            os.path.splitext(filename)[1][1:],
            size_bytes,
            f"s3://{MINIO_BUCKET}/{s3_path}",
            status,
            datetime.now(),
            str(e) if status == "failed" else None
        ))

    schema = StructType([
        StructField("file_name", StringType()),
        StructField("company", StringType()),
        StructField("table_type", StringType()),
        StructField("ingest_date", StringType()),
        StructField("file_format", StringType()),
        StructField("file_size_bytes", IntegerType()),
        StructField("s3_path", StringType()),
        StructField("status", StringType()),
        StructField("ingestion_timestamp", TimestampType()),
        StructField("error_message", StringType())
    ])

    df_meta = spark.createDataFrame(metadata, schema)
    meta_path = f"{BRONZE_BASE}/_metadata/ingest_date={ingest_date}"
    df_meta.write.mode("overwrite").parquet(meta_path)

    print(f"\nIngestion completed: {success_count}/{len(files)} successful")
    print(f"Metadata: {meta_path}")

    spark.stop()

if __name__ == "__main__":
    main()