import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct,
    sum as spark_sum, avg,
    min as spark_min, max as spark_max,
    when
)

def test_silver_layer(spark, silver_path):
    """
    Read and validate Silver layer KPI metrics
    """
    
    print("\n" + "="*80)
    print("SILVER LAYER DATA VALIDATION")
    print("="*80)
    
    try:
        # 1. Read parquet
        print(f"\n>>> Reading from: {silver_path}")
        df = spark.read.parquet(silver_path)
        
        # 2. Schema
        print("\n>>> SCHEMA:")
        df.printSchema()
        
        # 3. Row count
        total_rows = df.count()
        print(f"\n>>> Total rows: {total_rows:,}")
        
        # 4. Column list
        print(f"\n>>> Columns ({len(df.columns)}):")
        for i, col_name in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col_name}")
        
        # 5. Sample data
        print("\n>>> Sample data (5 rows):")
        df.show(5, truncate=False)
        # print tail 
        print("\n>>> Sample data (last 5 rows):")
        df.show(df.count() - 5, truncate=False)
        
        # 6. Data quality checks
        print("\n>>> DATA QUALITY CHECKS:")
        print("-" * 80)
        
        # Null counts per column
        print("\n  Null counts per column:")
        null_counts = df.select([
            count(when(col(c).isNull(), 1)).alias(c) 
            for c in df.columns
        ]).collect()[0].asDict()
        
        for col_name, null_count in null_counts.items():
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            print(f"    {col_name:30s}: {null_count:6d} nulls ({null_pct:5.2f}%)")
        
        # 7. Summary statistics
        print("\n>>> SUMMARY STATISTICS:")
        print("-" * 80)
        
        summary_df = df.select(
            countDistinct('company_name').alias('num_companies'),
            countDistinct('year').alias('num_years'),
            countDistinct('metric_name').alias('num_metrics'),
            countDistinct('topic').alias('num_topics'),
            count('*').alias('total_records')
        )
        
        summary = summary_df.collect()[0]
        print(f"\n  Companies:     {summary['num_companies']}")
        print(f"  Years:         {summary['num_years']}")
        print(f"  Metrics:       {summary['num_metrics']}")
        print(f"  Topics:        {summary['num_topics']}")
        print(f"  Total records: {summary['total_records']:,}")
        
        # 8. Records by company and year
        print("\n>>> RECORDS BY COMPANY & YEAR:")
        df.groupBy('company_name', 'year') \
            .count() \
            .orderBy('company_name', 'year') \
            .show(50, truncate=False)
        
        # 9. Records by topic
        print("\n>>> RECORDS BY TOPIC:")
        df.groupBy('topic') \
            .count() \
            .orderBy(col('count').desc()) \
            .show(truncate=False)
        
        # 10. Top 10 metrics by record count
        print("\n>>> TOP 10 METRICS (by record count):")
        df.groupBy('metric_name', 'unit') \
            .count() \
            .orderBy(col('count').desc()) \
            .show(10, truncate=False)
        
        # 11. Baseline analysis
        print("\n>>> BASELINE YEAR ANALYSIS:")
        df.groupBy('baseline_year') \
            .agg(
                count('*').alias('num_records'),
                countDistinct('metric_name').alias('num_metrics')
            ) \
            .orderBy('baseline_year') \
            .show(truncate=False)
        
        # 12. Value statistics
        print("\n>>> VALUE STATISTICS:")
        df.select(
            spark_min('value').alias('min_value'),
            spark_max('value').alias('max_value'),
            avg('value').alias('avg_value'),
            count(when(col('value') == 0, 1)).alias('zero_values'),
            count(when(col('value') < 0, 1)).alias('negative_values')
        ).show(truncate=False)
        
        # 13. Sample records with baseline
        print("\n>>> SAMPLE RECORDS WITH BASELINE VALUES:")
        df.filter(col('baseline_value').isNotNull()) \
            .select(
                'company_name', 'metric_name', 'year', 
                'value', 'baseline_year', 'baseline_value', 'unit'
            ) \
            .show(5, truncate=False)
        
        # 14. Check for duplicates
        print("\n>>> DUPLICATE CHECK:")
        duplicate_check = df.groupBy(
            'company_name', 'metric_name', 'year'
        ).count().filter(col('count') > 1)
        
        dup_count = duplicate_check.count()
        if dup_count > 0:
            print(f"  ⚠️  Found {dup_count} duplicate records!")
            duplicate_check.show(10, truncate=False)
        else:
            print("  ✅ No duplicates found")
        
        # 15. Year range check
        print("\n>>> YEAR RANGE:")
        df.select(
            spark_min('year').alias('min_year'),
            spark_max('year').alias('max_year')
        ).show()
        
        print("\n" + "="*80)
        print("✅ VALIDATION COMPLETE")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


def export_sample_data(spark, silver_path, output_path, num_rows=100):
    """
    Export sample data to CSV for inspection
    """
    print(f"\n>>> Exporting FULL dataset to CSV...")

    try:
        df = spark.read.parquet(silver_path)

        # Export full dataset
        df.coalesce(1) \
            .write \
            .mode('overwrite') \
            .option('header', 'true') \
            .csv(output_path)

        print(f"  ✅ Exported full dataset to: {output_path}")

    except Exception as e:
        print(f"  ❌ Export failed: {str(e)}")


if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Test-Silver-Layer") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    # Paths
    silver_path = "s3a://silver/kpi_metrics/"
    sample_export_path = "s3a://silver/kpi_metrics_sample/"
    
    # Run tests
    success = test_silver_layer(spark, silver_path)
    
    if success:
        # Export sample data
        export_sample_data(spark, silver_path, sample_export_path, num_rows=100)
    
    spark.stop()
    
    print("\n" + "="*80)
    print("TEST COMPLETED")
    print("="*80)