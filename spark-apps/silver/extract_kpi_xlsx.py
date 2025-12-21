import os
import yaml
from pathlib import Path
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import boto3
from pyspark.sql.functions import lit

spark = SparkSession.builder \
    .appName("ESG-Excel-Extract") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

CONFIG_FILE = "/opt/spark-apps/configs/xlsx_companies.yaml"
OUTPUT_PATH = "s3a://silver/kpi_extract_excel"

def extract_excel_data(file_path, config):
    company_name = config['company_name']
    sheet_mapping = config['sheet_mapping']
    years = config['years']
    header_indicators = config['header_indicators']
    col_map = config['column_mapping']
    
    all_data = []
    excel_file = pd.ExcelFile(file_path)
    
    for sheet_name in excel_file.sheet_names:
        sheet_key = sheet_name.strip()
        if sheet_key not in sheet_mapping:
            continue
            
        topic = sheet_mapping[sheet_key]
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        
        header_row_idx = -1
        for i, row in df.iterrows():
            row_values = [str(val).lower() for val in row.values]
            if all(ind in ' '.join(row_values) for ind in header_indicators):
                header_row_idx = i
                break
        
        if header_row_idx == -1:
            continue
        
        header = df.iloc[header_row_idx].tolist()
        cols = {str(year): -1 for year in years}
        cols.update({'uom': -1, 'metric': -1})
        
        for idx, val in enumerate(header):
            v = str(val).strip()
            for year in years:
                if str(year) in v:
                    cols[str(year)] = idx
            if col_map['unit'].lower() in v.lower():
                cols['uom'] = idx
            if col_map['metric'].lower() in v.lower():
                cols['metric'] = idx
        
        current_subcategory = ''
        
        for i in range(header_row_idx + 1, len(df)):
            row = df.iloc[i]
            metric_name = str(row[cols['metric']]).strip()
            
            if metric_name.lower() in ['nan', 'notes.', 'indicator', '']:
                continue
            
            first_col_val = str(row[0]).strip()
            if first_col_val and first_col_val.lower() != 'nan' and cols[str(years[0])] > 0:
                year_col_val = str(row[cols[str(years[0])]]).strip()
                if not year_col_val or year_col_val.lower() == 'nan':
                    current_subcategory = first_col_val
                    continue
            
            for year in years:
                col_idx = cols[str(year)]
                if col_idx != -1:
                    val = str(row[col_idx]).strip()
                    if val and val.lower() != 'nan':
                        clean_val = val.replace('\xa0', '').replace(' ', '').replace('*', '').replace(',', '')
                        
                        all_data.append({
                            'topic': topic,
                            'metric_category': current_subcategory.replace(',', ' '),
                            'name': company_name,
                            'year': str(year),
                            'metric_name': metric_name.replace(',', ' '),
                            'value': clean_val,
                            'units': str(row[cols['uom']]).strip().replace(',', ' '),
                            'additional_notes': f"Sheet: {sheet_name}"
                        })
    
    return all_data

def extract_bradesco_data(file_path, config):
    company_name = config['company_name']
    sheet_mapping = config['sheet_mapping']
    years = config['years']
    
    all_data = []
    excel_file = pd.ExcelFile(file_path)
    
    for sheet_name in excel_file.sheet_names:
        sheet_key = sheet_name.strip()
        if sheet_key not in sheet_mapping:
            continue
            
        topic = sheet_mapping[sheet_key]
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        
        theme_header_row = None
        theme_col_idx = None 
        
        for i in range(len(df)):
            row_vals = [str(val).lower().strip() for val in df.iloc[i]]
            if 'theme' in row_vals:
                theme_header_row = i
                theme_col_idx = row_vals.index('theme')
                break
        
        if theme_header_row is None:
            continue

        prev_theme_col_idx = theme_col_idx - 1
        header_row = df.iloc[theme_header_row]
        
        year_cols = {}
        indicator_col_idx = None
        unit_col_idx = None
        
        for col_idx, col_val in enumerate(header_row):
            col_str = str(col_val).lower().strip()
            for year in years:
                if str(year) == col_str:
                    year_cols[str(year)] = col_idx
            if 'indicator' in col_str:
                indicator_col_idx = col_idx
            if 'unit' in col_str:
                unit_col_idx = col_idx

        data_start = theme_header_row + 1
        df_data = df.iloc[data_start:].copy()
        
        df_data[prev_theme_col_idx] = df_data[prev_theme_col_idx].ffill()
        df_data[theme_col_idx] = df_data[theme_col_idx].ffill()
      
        for _, row in df_data.iterrows():
            indicator = str(row[indicator_col_idx]).strip() if indicator_col_idx is not None else ''
            
            if indicator.lower() in ['nan', '', 'indicator', 'theme']:
                continue
          
            val_prev = str(row[prev_theme_col_idx]).strip()
            val_theme = str(row[theme_col_idx]).strip()
            
            clean_prev = "" if val_prev.lower() == 'nan' else val_prev
            clean_theme = "" if val_theme.lower() == 'nan' else val_theme
            
            if clean_prev and clean_theme and clean_prev != clean_theme:
                metric_category = f"{clean_prev}_{clean_theme}"
            else:
                metric_category = clean_prev or clean_theme
            
            unit = str(row[unit_col_idx]).strip() if unit_col_idx is not None else ''

            for year, col_idx in year_cols.items():
                if col_idx < len(row):
                    val_raw = row[col_idx]
                    if pd.notna(val_raw):
                        value = str(val_raw).replace(',', '').strip()
                        if value and value.lower() != 'nan':
                            all_data.append({
                                'topic': topic,
                                'metric_category': metric_category.replace(',', ' ').replace('\n', ' '),
                                'name': company_name,
                                'year': year,
                                'metric_name': indicator.replace(',', ' ').replace('\n', ' '),
                                'value': value,
                                'units': unit.replace(',', ' '),
                                'additional_notes': f"Sheet: {sheet_name}"
                            })
    
    return all_data

def main():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='admin123456'
    )
    all_rows = []
    
    for file_key, file_config in config['files'].items():
        excel_filename = file_config['file_path'].split('/')[-1]
        company_name = file_config['company_name']
        
        df_bronze = spark.read.parquet("s3a://bronze/raw/_metadata")
        excel_rows = df_bronze.filter(
            (df_bronze.file_name == excel_filename) & 
            (df_bronze.table_type == "kpi") &
            (df_bronze.status == "success")
        ).collect()
        
        if not excel_rows:
            print(f"Excel not found in bronze: {excel_filename}")
            continue
        
        s3_path = excel_rows[0].s3_path
        local_temp_path = f"/tmp/{excel_filename}"
        
        bucket, key = s3_path.replace('s3://', '').split('/', 1)
        s3_client.download_file(bucket, key, local_temp_path)
        
        if not Path(local_temp_path).exists():
            print(f"Failed to download: {excel_filename}")
            continue
        
        print(f"Processing {company_name}...")
        
        if file_key == 'bradesco':
            company_data = extract_bradesco_data(local_temp_path, file_config)
        elif file_config['file_type'] == 'excel':
            company_data = extract_excel_data(local_temp_path, file_config)
        else:
            continue
        
        all_rows.extend(company_data)
        os.remove(local_temp_path)
    
    if not all_rows:
        print("No data extracted")
        return
    
    schema = StructType([
        StructField("topic", StringType()),
        StructField("metric_category", StringType()),
        StructField("name", StringType()),
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
    
    df.write.format("delta").mode("overwrite").partitionBy("extract_date").save(OUTPUT_PATH)
    
    print(f"\nExtracted {len(all_rows)} rows to {OUTPUT_PATH}")
    
    spark.stop()

if __name__ == "__main__":
    main()