import os
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("ESG-Silver-Layer-Processing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

BRONZE_PARQUET_BASE = "s3a://bronze/parquet"

def read_esg_risk():
    """
    Simple: Read ESG Risk from Bronze, show schema & data
    """
    print("\n" + "="*80)
    print("Reading ESG Risk from Bronze Layer")
    print("="*80 + "\n")
    
    try:
        # Read from Bronze
        path = f"{BRONZE_PARQUET_BASE}/ESG_risk/*/*"
        print(f"[+] Reading: {path}\n")
        
        df = spark.read.parquet(path)
        
        row_count = df.count()
        col_count = len(df.columns)
        
        print(f"[+] Rows: {row_count:,}")
        print(f"[+] Columns: {col_count}\n")
        
        # Show schema
        print("="*80)
        print("SCHEMA:")
        print("="*80)
        df.printSchema()
        
        # Show column names
        print("\n" + "="*80)
        print("COLUMN NAMES:")
        print("="*80)
        for i, col in enumerate(df.columns, 1):
            print(f"{i:2d}. {col}")
        
        # Show sample data
        print("\n" + "="*80)
        print("SAMPLE DATA (first 10 rows):")
        print("="*80 + "\n")
        df.show(10, truncate=False)
        
        # Show data types
        print("\n" + "="*80)
        print("DATA TYPES:")
        print("="*80)
        for field in df.schema.fields:
            print(f"  {field.name}: {field.dataType}")
        
        return df
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def main():
    print("\n" + "="*80)
    print("ESG Silver Layer - Read & Inspect Bronze Data")
    print("="*80)
    print(f"Start: {datetime.now()}\n")
    
    df = read_esg_risk()
    
    print("\n" + "="*80)
    print(f"Complete! | End: {datetime.now()}")
    print("="*80 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()