import sys
import os
from pyspark.sql.window import Window
from pyspark.sql.functions import last
import re 
sys.path.insert(0, '/opt/utils')

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, explode, array, struct,
    when, trim, regexp_extract, coalesce
)

from pattern_detector import ExcelStructureDetector
from config_loader import ConfigLoader

class BronzeToSilverPipeline:
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.detector = ExcelStructureDetector()
    
    def process_company(self, s3_pattern: str, company_config: dict):
        company_id = company_config['id']
        company_name = company_config['name']
        config = company_config['excel']
        
        print(f"\n>>> Looking for files: {s3_pattern}")
        
        from minio import Minio
        client = Minio(
            "minio:9000",
            access_key="admin",
            secret_key="admin123456",
            secure=False
        )
        
        bucket = "bronze"
        prefix = f"raw/KPI/company={company_name}/"
        
        files = []
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        for obj in objects:
            if obj.object_name.endswith(('.xlsx', '.xls')):
                files.append(f"s3a://{bucket}/{obj.object_name}")
        
        if not files:
            print(f">>> [ERROR] No files found for {company_name}")
            return None
        
        print(f">>> Found {len(files)} files: {files}")
        
        all_dataframes = []
        
        for file_path in files:
            print(f"\n>>> Processing file: {file_path}")
            for sheet_cfg in company_config.get('sheets', []):
                print(f"\n  >>> Sheet: {sheet_cfg['sheet']}")
                data_address = sheet_cfg['data_address']
                print(f"  >>> Data address: {data_address}")
                
                try:
                    df = self.spark.read \
                        .format("com.crealytics.spark.excel") \
                        .option("header","false") \
                        .option("inferSchema", "false") \
                        .option("treatEmptyValuesAsNulls", "true") \
                        .option("usePlainNumberFormat", "true") \
                        .option("dataAddress", data_address) \
                        .load(file_path)
                    # Lấy dòng đầu làm header thủ công
                    header = df.first()
                    df = df.filter(col("_c0") != header[0])  # Bỏ dòng header
                    # CLEAN COLUMN NAMES - Bỏ .0 khỏi year columns
                    cleaned_cols = []
                    for h in header:
                        if h:
                            col_str = str(h).strip()
                            # Nếu là year với .0 → bỏ .0
                            if re.match(r'^(19|20)\d{2}\.0+$', col_str):
                                col_str = col_str.split('.')[0]  # "2021.0" -> "2021"
                            cleaned_cols.append(col_str)
                        else:
                            cleaned_cols.append(f"_c{len(cleaned_cols)}")
                    
                    df = df.toDF(*cleaned_cols)
                    
                    print(f"  >>> Read {df.count()} rows")
                    print(f"  >>> Cleaned columns: {df.columns}")
                    df.show(3, truncate=False)

                    
                except Exception as e:
                    print(f"  >>> [ERROR] Failed to read Excel: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
                
                try:
                    df_transformed = self.transform_single_file(
                        df, company_id, company_name, config, company_config
                    )
                    
                    if df_transformed:
                        transformed_count = df_transformed.count()
                        print(f"  >>> After transform: {transformed_count} rows")
                        print(f"  >>> Transformed sample:")
                        df_transformed.show(3, truncate=False)
                        all_dataframes.append(df_transformed)
                    else:
                        print(f"  >>> [WARNING] df_transformed is None")
                        
                except Exception as e:
                    print(f"  >>> [ERROR] in transform_single_file: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
        
        print(f"\n>>> Total dataframes collected: {len(all_dataframes)}")
        
        # Union all files
        if all_dataframes:
            df_final = all_dataframes[0]
            for df in all_dataframes[1:]:
                df_final = df_final.union(df)
            print(f">>> Final union count: {df_final.count()}")
            return df_final
        
        print(f">>> [ERROR] No valid dataframes to union")
        return None
    
    def fill_metric_topic(self, df, indicator_col, unit_col):
        """Forward-fill metric_category từ dòng topic"""
        print(f"    >> fill_metric_topic: indicator_col={indicator_col}, unit_col={unit_col}")
        
        # Detect dòng topic: Unit null, Indicator có giá trị
        df = df.withColumn(
            'temp_topic',
            when(
                col(unit_col).isNull() & col(indicator_col).isNotNull(),
                col(indicator_col)
            )
        )
        
        print(f"    >> Before forward fill: {df.count()} rows")
        df.filter(col('temp_topic').isNotNull()).show(3, truncate=False)
        
        # Forward fill
        window = Window.orderBy().rowsBetween(Window.unboundedPreceding, 0)
        df = df.withColumn(
            'metric_category_filled',
            last('temp_topic', ignorenulls=True).over(window)
        )
        
        # Loại bỏ dòng topic (giữ lại chỉ dòng dữ liệu thực)
        df_filtered = df.filter(col(unit_col).isNotNull())
        print(f"    >> After filter (Unit not null): {df_filtered.count()} rows")
        
        return df_filtered.drop('temp_topic')

    def transform_single_file(self, df, company_id, company_name, excel_config, full_config):
        """Transform 1 Excel file"""
        print(f"    >> transform_single_file start")
        print(f"    >> Original columns: {df.columns}")
        print(f"    >> Original rows: {df.count()}")
        
        indicator_col = excel_config['indicator_column']
        unit_col = excel_config['unit_column']
        
        print(f"    >> indicator_col={indicator_col}, unit_col={unit_col}")
        
        # ===== Extract baseline info =====
        baseline_col = excel_config.get('baseline_column')
        baseline_year = None
        baseline_patterns = excel_config.get('baseline_patterns', [])
        
        if baseline_col and baseline_col in df.columns:
            # Extract year từ baseline column name
            for pattern in baseline_patterns:
                match = re.search(pattern, baseline_col)
                if match:
                    baseline_year = int(match.group(1))
                    print(f"    >> Detected baseline_year: {baseline_year} from column '{baseline_col}'")
                    break
        
        # Fill metric topic nếu cấu hình
        if excel_config.get('detect_topic_rows'):
            print(f"    >> detect_topic_rows=True, calling fill_metric_topic...")
            df = self.fill_metric_topic(df, indicator_col, unit_col)
        
        # Detect year columns
        year_columns = excel_config.get('year_columns') or self.detector.detect_year_columns(df)
        print(f"    >> year_columns detected: {year_columns}")
        # VALIDATE year columns
        if not year_columns:
            print(f"    >> [ERROR] No year columns found!")
            print(f"    >> Available columns: {df.columns}")
            return None
        
        category_col = excel_config.get('category_column')
        
        # 2. Handle merged cells
        if category_col and category_col in df.columns:
            print(f"    >> Filling merged cells in: {category_col}")
            df = self.detector.fill_merged_cells(df, category_col)
        
        # 3. Filter valid rows
        df_valid = df.filter(
            col(indicator_col).isNotNull() &
            (trim(col(indicator_col)) != "")
        )
        
        valid_count = df_valid.count()
        print(f"    >> Valid rows after filter: {valid_count}")
        if valid_count == 0:
            print(f"    >> [WARNING] No valid rows found!")
            return None
        
        print(f"    >> Valid data sample:")
        df_valid.show(3, truncate=False)
        
        # 4. Unpivot years
        # Unpivot years - Escape columns có dấu chấm
        year_structs = [
            struct(
                lit(int(float(str(year_col)))).alias('year'),
                col(f"`{year_col}`").cast('double').alias('value')  # <-- THÊM backticks
            )
            for year_col in year_columns
        ]
    
        print(f"    >> Created {len(year_structs)} year_structs")
        
        # Include category in unpivot if exists
        select_cols = [
            col(indicator_col).alias('metric_name'),
            col(unit_col).alias('unit')
        ]
        
        # Thêm metric_category_filled nếu có
        if 'metric_category_filled' in df_valid.columns:
            print(f"    >> Using metric_category_filled")
            select_cols.append(col('metric_category_filled').alias('category_raw'))
        elif category_col and category_col in df_valid.columns:
            print(f"    >> Using category_col: {category_col}")
            select_cols.append(col(category_col).alias('category_raw'))
        else:
            print(f"    >> No category found")
            select_cols.append(lit(None).alias('category_raw'))
        
        # Add baseline value if exists
        if baseline_col and baseline_col in df_valid.columns:
            print(f"    >> Using baseline_col: {baseline_col}")
            select_cols.append(col(baseline_col).cast('double').alias('baseline_value'))
        else:
            select_cols.append(lit(None).cast('double').alias('baseline_value'))
        
        # Add notes if exists
        notes_col = excel_config.get('notes_column')
        if notes_col and notes_col in df_valid.columns:
            select_cols.append(col(notes_col).alias('notes'))
        else:
            select_cols.append(lit(None).alias('notes'))
        
        select_cols.append(explode(array(*year_structs)).alias('year_data'))
        
        df_unpivot = df_valid.select(*select_cols).select(
            'metric_name', 'unit', 'notes',
            col('category_raw'),
            col('baseline_value'),
            col('year_data.year').alias('year'),
            col('year_data.value').alias('value')
        )
        
        unpivot_count = df_unpivot.count()
        print(f"    >> After unpivot: {unpivot_count} rows")
        
        # 5. Enrich metadata
        df_enriched = self.enrich_metadata(df_unpivot, full_config)
        
        # 6. Map to target schema
        result = df_enriched.select(
            lit(company_id).alias('company_id'),
            lit(company_name).alias('company_name'),
            col('topic'),
            coalesce(col('metric_category'), col('category_raw')).alias('metric_category'),
            col('metric_name'),
            col('value'),
            col('unit'),
            lit(baseline_year).cast('int').alias('baseline_year'),  # <-- BASELINE YEAR
            col('baseline_value').cast('double').alias('baseline_value'),  # <-- BASELINE VALUE
            col('notes').alias('boundary_scope'),
            col('year'),
            col('notes'),
            current_timestamp().alias('ingestion_date')
        ).filter(col('value').isNotNull())
        
        final_count = result.count()
        print(f"    >> Final result: {final_count} rows")
        print(f"    >> Baseline year: {baseline_year}")
        
        return result

    def enrich_metadata(self, df, config):
        """Add topic & category"""
        print(f"    >> enrich_metadata start")
        
        topic_rules = config.get('topics', {})
        topic_expr = None
        
        for topic, rules in topic_rules.items():
            pattern = '|'.join(rules['keywords'])
            condition = (
                col('metric_name').rlike(f'(?i){pattern}') |
                (col('category_raw').rlike(f'(?i){pattern}') if 'category_raw' in df.columns else lit(False))
            )
            
            if topic_expr is None:
                topic_expr = when(condition, lit(topic))
            else:
                topic_expr = topic_expr.when(condition, lit(topic))
        
        df = df.withColumn('topic', topic_expr.otherwise(lit('Unknown')))
        
        # Category extraction
        if 'category_raw' in df.columns:
            df = df.withColumn(
                'metric_category',
                when(col('category_raw').isNotNull(), col('category_raw'))
                .otherwise(lit(None))
            )
        else:
            category_patterns = config.get('categories', [])
            category_expr = None
            
            for cp in category_patterns:
                condition = col('metric_name').rlike(cp['pattern'])
                
                if category_expr is None:
                    category_expr = when(condition, lit(cp['name']))
                else:
                    category_expr = category_expr.when(condition, lit(cp['name']))
            
            df = df.withColumn('metric_category', category_expr.otherwise(lit(None)))
        
        print(f"    >> enrich_metadata end")
        return df
    
    def run(self, company_patterns_path: str, output_path: str):
        """Main execution"""
        
        print(f"\n{'='*70}")
        print(f"START PIPELINE")
        print(f"{'='*70}")
        
        loader = ConfigLoader(company_patterns_path)
        companies = loader.load_all_companies()
        
        print(f"\nLoaded {len(companies)} companies")
        for c in companies:
            print(f"  - {c['name']} (sheets: {len(c.get('sheets', []))})")
        
        all_results = []
        
        for company_cfg in companies:
            print(f"\n{'='*70}")
            print(f"Processing: {company_cfg['name']}")
            print(f"{'='*70}")
            
            try:
                df_result = self.process_company(
                    company_cfg['s3_pattern'],
                    company_cfg
                )
                
                if df_result:
                    all_results.append(df_result)
                    print(f">>> Added {df_result.count()} rows to results")
                else:
                    print(f">>> [WARNING] No result for {company_cfg['name']}")
            except Exception as e:
                print(f">>> [ERROR] Failed to process {company_cfg['name']}: {str(e)}")
                import traceback
                traceback.print_exc()
        
        print(f"\n>>> Total results: {len(all_results)}")
        
        # Union all companies
        if all_results:
            df_final = all_results[0]
            for df in all_results[1:]:
                df_final = df_final.union(df)
            
            final_count = df_final.count()
            print(f"\n>>> Writing to Silver: {output_path}")
            print(f">>> Total records: {final_count}")
            
            df_final.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            print(f">>> ✓ Write completed")
            
            print(f"\n>>> Summary by Company & Year:")
            df_final.groupBy("company_name", "year") \
                .count() \
                .orderBy("company_name", "year") \
                .show(50, truncate=False)
        else:
            print(f">>> [ERROR] No results to write!")

# Main
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Bronze-to-Silver-KPI") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars", "/opt/spark/jars/spark-excel_2.12-3.3.1_0.18.5.jar") \
        .getOrCreate()
    
    pipeline = BronzeToSilverPipeline(spark)
    pipeline.run(
        company_patterns_path="/opt/spark-apps/configs/company_patterns.yaml",
        output_path="s3a://silver/kpi_metrics/"
    )
    
    spark.stop()