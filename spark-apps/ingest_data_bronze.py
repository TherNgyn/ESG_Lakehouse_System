import os
import sys
from pyspark.sql import SparkSession
from datetime import datetime
from minio import Minio
from minio.error import S3Error

# Initialize Spark Session - CLUSTER MODE
spark = SparkSession.builder \
    .appName("ESG-Data-Bronze-Ingest") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# MinIO Configuration
MINIO_HOST = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123456"
MINIO_BUCKET = "bronze"

# Local paths
CSV_DIR = "/opt/spark-data/input"
PARQUET_DIR = "/opt/spark-data/output/parquet"

# Create directories if not exist
os.makedirs(PARQUET_DIR, exist_ok=True)

def upload_to_minio(local_file, bucket_name, object_name):
    """Upload file to MinIO"""
    try:
        client = Minio(MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        client.fput_object(bucket_name, object_name, local_file)
        print(f"✓ Uploaded: s3://{bucket_name}/{object_name}")
        return True
    except S3Error as e:
        print(f"✗ Upload failed: {e}")
        return False

def process_csv_files(csv_dir):
    """Process all CSV files in directory"""
    
    if not os.path.exists(csv_dir):
        print(f"❌ CSV directory not found: {csv_dir}")
        return
    
    # Find all CSV files recursively
    csv_files = []
    for root, dirs, files in os.walk(csv_dir):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    
    if not csv_files:
        print(f"❌ No CSV files found in {csv_dir}")
        print(f"Directory contents: {os.listdir(csv_dir)}")
        return
    
    print(f"\n{'='*60}")
    print(f"Processing {len(csv_files)} CSV files...")
    print(f"{'='*60}\n")
    
    for csv_path in csv_files:
        csv_file = os.path.basename(csv_path)
        file_name = csv_file.replace('.csv', '')
        
        try:
            print(f"Processing: {csv_file}")
            
            # Read CSV
            df = spark.read.option("encoding", "UTF-8").csv(csv_path, header=True, inferSchema=True)
            row_count = df.count()
            col_count = len(df.columns)
            print(f"  ✓ Rows: {row_count} | Columns: {col_count}")
            
            # Save to Parquet locally
            parquet_file = os.path.join(PARQUET_DIR, f"{file_name}.parquet")
            df.write.mode("overwrite").parquet(parquet_file)
            print(f"  ✓ Saved Parquet: {parquet_file}")
            
            # Upload Raw CSV to MinIO (bronze/raw/)
            csv_s3_path = f"raw/{csv_file}"
            if upload_to_minio(csv_path, MINIO_BUCKET, csv_s3_path):
                print(f"  ✓ Uploaded CSV to MinIO")
            
            # Upload Parquet to MinIO (bronze/parquet/)
            parquet_s3_path = f"parquet/{file_name}.parquet"
            if upload_to_minio(parquet_file, MINIO_BUCKET, parquet_s3_path):
                print(f"  ✓ Uploaded Parquet to MinIO")
            
            print(f"  ✓ Complete: {csv_file}\n")
            
        except Exception as e:
            print(f"  ✗ Error processing {csv_file}: {str(e)}\n")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    print(f"\n{'='*60}")
    print(f"ESG Data Bronze Layer Ingestion")
    print(f"{'='*60}")
    print(f"Start Time: {datetime.now()}")
    print(f"CSV Directory: {CSV_DIR}")
    print(f"Parquet Output: {PARQUET_DIR}")
    print(f"MinIO Bucket: s3://{MINIO_BUCKET}")
    print(f"{'='*60}\n")
    
    # Process CSV files
    process_csv_files(CSV_DIR)
    
    print(f"\n{'='*60}")
    print(f"✓ Ingestion Complete!")
    print(f"End Time: {datetime.now()}")
    print(f"{'='*60}\n")
    
    spark.stop()

if __name__ == "__main__":
    main()