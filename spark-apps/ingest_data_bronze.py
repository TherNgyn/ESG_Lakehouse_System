import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import re


spark = SparkSession.builder \
    .appName("ESG-Data-Bronze-Raw-Only") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

MINIO_HOST = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123456"
MINIO_BUCKET = "bronze"

INPUT_DIR = "/opt/spark-data/input"
BRONZE_RAW_BASE = "s3a://bronze/raw"

SUPPORTED_FORMATS = {
    '.csv': 'csv',
    '.xlsx': 'excel',
    '.xls': 'excel',
    '.json': 'json',
    '.parquet': 'parquet'
}

metadata_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("company", StringType(), True),
    StructField("table_type", StringType(), True),
    StructField("ingest_date", StringType(), True),
    StructField("file_format", StringType(), True),
    StructField("file_size_bytes", IntegerType(), True),
    StructField("s3_path", StringType(), True),
    StructField("status", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("error_message", StringType(), True)
])

def extract_company_from_filename(filename):
    """Extract company name from filename"""
    try:
        name_without_ext = os.path.splitext(filename)[0]
        filename_lower = filename.lower()
        
        # ESG score/rank/risk files -> Multiple companies
        if any(keyword in filename_lower for keyword in ['esg_score', 'esg_rank', 'esg_risk', 
                                                          'score', 'rank', 'risk']) and \
           'kpi' not in filename_lower:
            return "Multiple"
        
        # KPI pattern: CompanyName_KPI_report
        pattern_kpi = r'^(.+?)_KPI[_-]?report'
        match = re.search(pattern_kpi, name_without_ext, re.IGNORECASE)
        
        if match:
            company = match.group(1).replace('_', ' ').replace('-', ' ').title()
            return company
        
        # Default: take first part before underscore
        parts = re.split(r'[_-]', name_without_ext)
        if parts:
            return parts[0].replace('_', ' ').title()
        
        return "Unknown"
    except Exception as e:
        print(f"  [WARNING] Error extracting company name: {e}")
        return "Unknown"

def determine_table_type(filename):
    """Determine table type from filename"""
    filename_lower = filename.lower()
    
    if 'kpi' not in filename_lower:
        if 'score' in filename_lower:
            return 'ESG_score'
        elif 'rank' in filename_lower:
            return 'ESG_rank'
        elif 'risk' in filename_lower:
            return 'ESG_risk'
    
    return 'KPI'

def upload_to_minio(client, local_file, bucket_name, object_name):
    """Upload file to MinIO"""
    try:
        client.fput_object(bucket_name, object_name, local_file)
        print(f"    ✓ Uploaded: s3://{bucket_name}/{object_name}")
        return True
    except Exception as e:
        print(f"    ✗ Upload failed: {e}")
        return False

def get_file_size(file_path):
    """Get file size in bytes"""
    try:
        return os.path.getsize(file_path)
    except:
        return 0

def process_files(input_dir):
    """Process all files and upload to Bronze raw layer"""
    
    if not os.path.exists(input_dir):
        print(f"[ERROR] Input directory not found: {input_dir}")
        return

    # Find all supported files
    all_files = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext in SUPPORTED_FORMATS:
                all_files.append(os.path.join(root, file))
    
    if not all_files:
        print(f"[WARNING] No supported files found in {input_dir}")
        return
    
    print(f"\n{'='*80}")
    print(f"Found {len(all_files)} files to ingest")
    print(f"{'='*80}\n")
    
    # Initialize MinIO client
    minio_client = Minio(
        MINIO_HOST, 
        access_key=MINIO_ACCESS_KEY, 
        secret_key=MINIO_SECRET_KEY, 
        secure=False
    )
    
    # Ensure bucket exists
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            print(f"[+] Created bucket: {MINIO_BUCKET}")
    except Exception as e:
        print(f"[ERROR] Bucket error: {e}")
        return
    
    metadata_records = []
    ingest_date = datetime.now().strftime("%Y-%m-%d")
    
    # Process each file
    for file_path in all_files:
        file_name = os.path.basename(file_path)
        file_ext = os.path.splitext(file_name)[1].lower()
        file_format = SUPPORTED_FORMATS[file_ext]
        
        print(f"\n{'-'*80}")
        print(f"Processing: {file_name}")
        print(f"{'-'*80}")
        
        try:
            # Extract metadata from filename
            table_type = determine_table_type(file_name)
            company = extract_company_from_filename(file_name)
            file_size = get_file_size(file_path)
            
            print(f"  Company: {company}")
            print(f"  Type: {table_type}")
            print(f"  Format: {file_format.upper()}")
            print(f"  Size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
            
            # Determine S3 path structure
            if table_type == 'KPI':
                # KPI files: organized by company
                s3_path = f"raw/KPI/company={company}/ingest_date={ingest_date}/{file_name}"
            else:
                # ESG score/rank/risk: organized by type
                s3_path = f"raw/{table_type}/ingest_date={ingest_date}/{file_name}"
            
            print(f"  Target: s3://{MINIO_BUCKET}/{s3_path}")
            
            # Upload to MinIO
            upload_success = upload_to_minio(minio_client, file_path, MINIO_BUCKET, s3_path)
            
            if upload_success:
                status = "success"
                error_message = None
                print(f"  [✓ SUCCESS]")
            else:
                status = "failed"
                error_message = "Upload failed"
                print(f"  [✗ FAILED]")
            
            # Record metadata
            metadata_record = {
                "file_name": file_name,
                "company": company,
                "table_type": table_type,
                "ingest_date": ingest_date,
                "file_format": file_format,
                "file_size_bytes": file_size,
                "s3_path": f"s3://{MINIO_BUCKET}/{s3_path}",
                "status": status,
                "ingestion_timestamp": datetime.now(),
                "error_message": error_message
            }
            metadata_records.append(metadata_record)
            
        except Exception as e:
            print(f"  [✗ ERROR] {str(e)[:120]}")
            
            # Record failed metadata
            metadata_record = {
                "file_name": file_name,
                "company": extract_company_from_filename(file_name),
                "table_type": determine_table_type(file_name),
                "ingest_date": ingest_date,
                "file_format": file_format,
                "file_size_bytes": get_file_size(file_path),
                "s3_path": "N/A",
                "status": "failed",
                "ingestion_timestamp": datetime.now(),
                "error_message": str(e)[:200]
            }
            metadata_records.append(metadata_record)
    
    # Save metadata
    if metadata_records:
        print(f"\n{'='*80}")
        print(f"Saving metadata for {len(metadata_records)} records...")
        print(f"{'='*80}")
        
        try:
            metadata_df = spark.createDataFrame(metadata_records, schema=metadata_schema)
            
            # Save metadata as Parquet
            metadata_path = f"{BRONZE_RAW_BASE}/_metadata/ingest_date={ingest_date}"
            metadata_df.write \
                .mode("overwrite") \
                .parquet(metadata_path)
            
            print(f"[✓] Metadata saved to: {metadata_path}")
            
            # Print summary statistics
            print(f"\n{'='*40}")
            print(f"INGESTION SUMMARY")
            print(f"{'='*40}")
            
            print(f"\nStatus:")
            metadata_df.groupBy("status").count().show(truncate=False)
            
            print(f"\nBy Table Type:")
            metadata_df.groupBy("table_type").count().show(truncate=False)
            
            print(f"\nBy Company:")
            metadata_df.groupBy("company").count() \
                .orderBy("count", ascending=False) \
                .show(20, truncate=False)
            
            print(f"\nBy Format:")
            metadata_df.groupBy("file_format").count().show(truncate=False)
            
            # Success rate
            total = metadata_df.count()
            success = metadata_df.filter("status = 'success'").count()
            success_rate = (success / total * 100) if total > 0 else 0
            
            print(f"\n{'='*40}")
            print(f"Success Rate: {success}/{total} ({success_rate:.1f}%)")
            print(f"{'='*40}")
            
        except Exception as e:
            print(f"[✗ ERROR] Failed to save metadata: {str(e)[:120]}")

def main():
    """Main execution function"""
    
    print(f"\n{'='*80}")
    print(f"ESG Data Bronze Layer - RAW FILES ONLY")
    print(f"{'='*80}")
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Input Directory: {INPUT_DIR}")
    print(f"Storage Location: {BRONZE_RAW_BASE}")
    print(f"Bucket: {MINIO_BUCKET}")
    print(f"{'='*80}\n")
    
    process_files(INPUT_DIR)
    
    print(f"\n{'='*80}")
    print(f"Ingestion Complete!")
    print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    spark.stop()

if __name__ == "__main__":
    main()