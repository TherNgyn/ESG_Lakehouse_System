import os
import yaml
import time
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit
import google.generativeai as genai
import boto3

spark = SparkSession.builder \
    .appName("ESG-PDF-Extract") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyD1Lb4ZLCks_wvAPRGq8GnNwvXxam-WT4w")
genai.configure(api_key=GEMINI_API_KEY)

CONFIG_FILE = "/opt/spark-apps/configs/pdf_config.yaml"
OUTPUT_PATH = "s3a://silver/kpi_pdf_extracted"

s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='admin123456'
)

def extract_esg_databook_continuous(pdf_path, company_name, section):
    model = genai.GenerativeModel('gemini-2.5-flash')
    
    pdf_file = genai.upload_file(pdf_path)
    time.sleep(5)
    
    all_data = []
    batch_count = 1

    while True:
        prompt = f"""
Extract exactly 150 next data rows from "{section}" section.

Requirements:
1. Find table data right after the above point. Don't repeat old data.
2. Format: metric_category,name,year,metric_name,value,units,additional_notes
3. Company name (name) is always "{company_name}".
4. If a metric has multiple years, split into 1 row per year.
5. Output pure CSV data only, NO ```csv or explanations.
6. If reached the last page of section (no more tables), write "---END_OF_DATA---".
"""

        try:
            response = model.generate_content([pdf_file, prompt])
            raw_text = response.text.strip()
            clean_text = raw_text.replace("```csv", "").replace("```", "").strip()
            
            is_finished = "---END_OF_DATA---" in clean_text
            clean_text = clean_text.replace("---END_OF_DATA---", "").strip()
            
            lines = [l.strip() for l in clean_text.split('\n') if len(l.split(',')) >= 4]
            lines = [l for l in lines if not l.startswith('metric_category')]

            if lines:
                all_data.extend(lines)
                batch_count += 1
            
            if is_finished or not lines:
                break

            time.sleep(8)

        except Exception as e:
            print(f"Error: {e}")
            break
    
    return all_data

def main():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    all_rows = []
    
    for company_key, company_config in config['pdf_files'].items():
        pdf_filename = company_config['pdf_path'].split('/')[-1]
        
        df_bronze = spark.read.parquet("s3a://bronze/raw/_metadata")
        pdf_rows = df_bronze.filter(
            (df_bronze.table_type == "pdf") &
            (df_bronze.status == "success") &
            (df_bronze.s3_path.contains(pdf_filename))
        ).collect()
        
        if not pdf_rows:
            print(f"PDF not found in bronze: {pdf_filename}")
            continue
        
        s3_path = pdf_rows[0].s3_path
        local_temp_path = f"/tmp/{pdf_filename}"
        
        bucket, key = s3_path.replace('s3://', '').split('/', 1)
        s3_client.download_file(bucket, key, local_temp_path)
        
        if not Path(local_temp_path).exists():
            print(f"Failed to download: {pdf_filename}")
            continue
        
        company_name = company_config['company_name']
        section = company_config.get('section', 'ESG Data')
        
        print(f"Processing {company_name}...")
        
        company_data = extract_esg_databook_continuous(local_temp_path, company_name, section)
        
        for line in company_data:
            parts = line.split(',')
            if len(parts) >= 7:
                all_rows.append({
                    'metric_category': parts[0],
                    'company_name': parts[1],
                    'year': parts[2],
                    'metric_name': parts[3],
                    'value': parts[4],
                    'units': parts[5],
                    'additional_notes': ','.join(parts[6:])
                })
        
        os.remove(local_temp_path)
    
    if not all_rows:
        print("No data extracted")
        return
    
    schema = StructType([
        StructField("metric_category", StringType()),
        StructField("company_name", StringType()),
        StructField("year", StringType()),
        StructField("metric_name", StringType()),
        StructField("value", StringType()),
        StructField("units", StringType()),
        StructField("additional_notes", StringType())
    ])
    
    df = spark.createDataFrame(all_rows, schema)
    
    extract_date = datetime.now().strftime("%Y-%m-%d")
    df = df.withColumn("extract_date", lit(extract_date))
    
    print(f"\nSample extracted data (first 20 rows):")
    df.show(20, truncate=False)
    
    print(f"\nSchema:")
    df.printSchema()
    
    print(f"\nTotal rows: {df.count()}")
    
    df.write.format("delta").mode("append").save(OUTPUT_PATH)
    
    print(f"\nExtracted {len(all_rows)} rows to {OUTPUT_PATH}")
    
    spark.stop()

if __name__ == "__main__":
    main()