import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, avg, sum, count, min, max,
    round, when, percentile_approx, row_number, dense_rank,
    year, month, quarter, lag, lead
)
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder \
    .appName("ESG-Gold-Layer-Analytics") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

SILVER_BASE = "s3a://silver"
GOLD_BASE = "s3a://gold"

def validate_and_show_schema(df, table_name):
    """
    Validate schema and show sample data
    """
    print(f"\n[DEBUG] Schema for {table_name}:")
    df.printSchema()
    print(f"\n[DEBUG] Sample data:")
    df.select("Sector", "Symbol", "Name", "Total_ESG_Risk_score").show(5, truncate=False)

# ============================================================================
# 1. ESG RISK SUMMARY BY SECTOR
# ============================================================================
def create_esg_risk_by_sector():
    """
    Aggregate ESG risk metrics by Sector
    Output: Sector-level ESG performance summary
    """
    print("\n" + "="*80)
    print("Creating Gold Table: ESG Risk Summary by Sector")
    print("="*80 + "\n")
    
    try:
        # Read from Silver
        df_silver = spark.read.parquet(f"{SILVER_BASE}/ESG_risk")
        
        print(f"[+] Loaded {df_silver.count():,} rows from Silver")
        
        # Validate schema
        validate_and_show_schema(df_silver, "ESG_risk Silver")
        
        # Check for null Sector values
        null_sectors = df_silver.filter(col("Sector").isNull()).count()
        print(f"\n[INFO] Rows with NULL Sector: {null_sectors:,}")
        
        if null_sectors > 0:
            print("[WARNING] Filtering out NULL sectors")
            df_silver = df_silver.filter(col("Sector").isNotNull())
        
        # Aggregate by Sector - ONLY STRING COLUMNS FOR GROUPBY
        df_gold = df_silver.groupBy(
            col("Sector").cast("string"),
            col("ingest_date").cast("string")
        ).agg(
            count(col("Symbol")).alias("total_companies"),
            round(avg(col("Total_ESG_Risk_score").cast("double")), 2).alias("avg_esg_risk_score"),
            round(avg(col("Environment_Risk_Score").cast("double")), 2).alias("avg_environment_score"),
            round(avg(col("Governance_Risk_Score").cast("double")), 2).alias("avg_governance_score"),
            round(avg(col("Social_Risk_Score").cast("double")), 2).alias("avg_social_score"),
            round(min(col("Total_ESG_Risk_score").cast("double")), 2).alias("min_esg_risk"),
            round(max(col("Total_ESG_Risk_score").cast("double")), 2).alias("max_esg_risk"),
            sum(col("Full_Time_Employees").cast("long")).alias("total_employees")
        ).orderBy("avg_esg_risk_score", ascending=False)
        
        # Add metadata
        df_gold = df_gold \
            .withColumn("created_timestamp", current_timestamp()) \
            .withColumn("table_name", lit("esg_risk_by_sector"))
        
        # Write to Gold
        gold_path = f"{GOLD_BASE}/esg_risk_by_sector"
        df_gold.write \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .parquet(gold_path)
        
        print(f"\n[+] Gold table saved: {gold_path}")
        print(f"\nSample Data:")
        df_gold.show(15, truncate=False)
        
        return df_gold
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# ============================================================================
# 2. TOP/BOTTOM ESG PERFORMERS
# ============================================================================
def create_esg_top_bottom_performers():
    """
    Identify top 10 best and worst ESG performers
    """
    print("\n" + "="*80)
    print("Creating Gold Table: Top & Bottom ESG Performers")
    print("="*80 + "\n")
    
    try:
        df_silver = spark.read.parquet(f"{SILVER_BASE}/ESG_risk")
        
        # Filter NULL values
        df_silver = df_silver.filter(
            col("Symbol").isNotNull() &
            col("Total_ESG_Risk_score").isNotNull()
        )
        
        # Window for ranking
        window_spec = Window.partitionBy("ingest_date").orderBy(col("Total_ESG_Risk_score"))
        
        df_ranked = df_silver.withColumn("rank", row_number().over(window_spec))
        
        # Top 10 Best (lowest risk = best)
        df_top10 = df_ranked.filter(col("rank") <= 10) \
            .withColumn("performance_tier", lit("Top 10 Best"))
        
        # Top 10 Worst (highest risk = worst)
        window_spec_desc = Window.partitionBy("ingest_date").orderBy(col("Total_ESG_Risk_score").desc())
        df_worst10 = df_silver.withColumn("rank", row_number().over(window_spec_desc)) \
            .filter(col("rank") <= 10) \
            .withColumn("performance_tier", lit("Top 10 Worst"))
        
        # Combine
        df_gold = df_top10.union(df_worst10)
        
        # Select relevant columns
        df_gold = df_gold.select(
            "Symbol", "Name", "Sector", "Industry",
            col("Total_ESG_Risk_score").cast("double"),
            col("Environment_Risk_Score").cast("double"),
            col("Governance_Risk_Score").cast("double"),
            col("Social_Risk_Score").cast("double"),
            "Risk_Level", "performance_tier", "rank", "ingest_date"
        ).orderBy("performance_tier", "rank")
        
        gold_path = f"{GOLD_BASE}/esg_top_bottom_performers"
        df_gold.write \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .parquet(gold_path)
        
        print(f"[+] Gold table saved: {gold_path}")
        df_gold.show(20, truncate=False)
        
        return df_gold
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# ============================================================================
# 3. ESG RISK DISTRIBUTION BY RISK LEVEL
# ============================================================================
def create_esg_risk_distribution():
    """
    Count of companies by Risk Level (Low, Medium, High, Severe)
    """
    print("\n" + "="*80)
    print("Creating Gold Table: ESG Risk Distribution")
    print("="*80 + "\n")
    
    try:
        df_silver = spark.read.parquet(f"{SILVER_BASE}/ESG_risk")
        
        df_silver = df_silver.filter(col("Risk_Level").isNotNull())
        
        # Count by Risk Level
        df_gold = df_silver.groupBy("Risk_Level", "ingest_date").agg(
            count("Symbol").alias("company_count"),
            round(avg(col("Total_ESG_Risk_score").cast("double")), 2).alias("avg_risk_score"),
            round(avg(col("ESG_Risk_Percentile").cast("double")), 2).alias("avg_percentile")
        ).orderBy("avg_risk_score")
        
        # Add percentage
        total_companies = df_silver.count()
        df_gold = df_gold.withColumn(
            "percentage", 
            round((col("company_count") / total_companies) * 100, 2)
        )
        
        gold_path = f"{GOLD_BASE}/esg_risk_distribution"
        df_gold.write \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .parquet(gold_path)
        
        print(f"[+] Gold table saved: {gold_path}")
        df_gold.show(truncate=False)
        
        return df_gold
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# ============================================================================
# 4. COMPANY ESG SCORECARD (Comprehensive View)
# ============================================================================
def create_company_esg_scorecard():
    """
    Comprehensive ESG scorecard for each company
    """
    print("\n" + "="*80)
    print("Creating Gold Table: Company ESG Scorecard")
    print("="*80 + "\n")
    
    try:
        df_silver = spark.read.parquet(f"{SILVER_BASE}/ESG_risk")
        
        df_silver = df_silver.filter(
            col("Symbol").isNotNull() &
            col("Total_ESG_Risk_score").isNotNull()
        )
        
        # Calculate percentile ranks
        window_spec = Window.partitionBy("ingest_date").orderBy(col("Total_ESG_Risk_score"))
        
        df_gold = df_silver.withColumn(
            "esg_percentile_rank",
            round((row_number().over(window_spec) / count("*").over(Window.partitionBy("ingest_date"))) * 100, 2)
        )
        
        # Add ESG Grade based on score
        df_gold = df_gold.withColumn(
            "esg_grade",
            when(col("Total_ESG_Risk_score").cast("double") < 10, "A")
            .when(col("Total_ESG_Risk_score").cast("double") < 20, "B")
            .when(col("Total_ESG_Risk_score").cast("double") < 30, "C")
            .when(col("Total_ESG_Risk_score").cast("double") < 40, "D")
            .otherwise("F")
        )
        
        # Select key columns with type casting
        df_gold = df_gold.select(
            "Symbol", "Name", "Sector", "Industry",
            col("Total_ESG_Risk_score").cast("double"),
            "esg_grade", "esg_percentile_rank",
            col("Environment_Risk_Score").cast("double"),
            col("Governance_Risk_Score").cast("double"),
            col("Social_Risk_Score").cast("double"),
            col("Controversy_Score").cast("double"),
            "Controversy_Level", "Risk_Level",
            col("ESG_Risk_Percentile").cast("integer"),
            col("Full_Time_Employees").cast("long"),
            "ingest_date"
        ).orderBy("Total_ESG_Risk_score")
        
        gold_path = f"{GOLD_BASE}/company_esg_scorecard"
        df_gold.write \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .parquet(gold_path)
        
        print(f"[+] Gold table saved: {gold_path}")
        print(f"\nSample Scorecards:")
        df_gold.show(10, truncate=False)
        
        return df_gold
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# ============================================================================
# 5. SECTOR COMPARISON MATRIX
# ============================================================================
def create_sector_comparison_matrix():
    """
    Side-by-side comparison of all sectors across ESG dimensions
    """
    print("\n" + "="*80)
    print("Creating Gold Table: Sector Comparison Matrix")
    print("="*80 + "\n")
    
    try:
        df_silver = spark.read.parquet(f"{SILVER_BASE}/ESG_risk")
        
        # Filter NULL sectors
        df_silver = df_silver.filter(col("Sector").isNotNull())
        
        # Calculate sector rankings
        df_gold = df_silver.groupBy("Sector", "ingest_date").agg(
            count("Symbol").alias("company_count"),
            round(avg(col("Total_ESG_Risk_score").cast("double")), 2).alias("avg_total_risk"),
            round(avg(col("Environment_Risk_Score").cast("double")), 2).alias("avg_env_risk"),
            round(avg(col("Governance_Risk_Score").cast("double")), 2).alias("avg_gov_risk"),
            round(avg(col("Social_Risk_Score").cast("double")), 2).alias("avg_social_risk"),
            round(avg(col("Controversy_Score").cast("double")), 2).alias("avg_controversy")
        )
        
        # Add sector rank by total risk
        window_spec = Window.partitionBy("ingest_date").orderBy("avg_total_risk")
        df_gold = df_gold.withColumn("sector_rank", dense_rank().over(window_spec))
        
        # Add best/worst indicators
        max_rank = df_gold.agg(max("sector_rank")).collect()[0][0]
        
        df_gold = df_gold.withColumn(
            "performance_category",
            when(col("sector_rank") <= 3, "Top Performer")
            .when(col("sector_rank") >= max_rank - 2, "Bottom Performer")
            .otherwise("Average")
        )
        
        df_gold = df_gold.orderBy("sector_rank")
        
        gold_path = f"{GOLD_BASE}/sector_comparison_matrix"
        df_gold.write \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .parquet(gold_path)
        
        print(f"[+] Gold table saved: {gold_path}")
        df_gold.show(20, truncate=False)
        
        return df_gold
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# ============================================================================
# 6. INDUSTRY BENCHMARKS
# ============================================================================
def create_industry_benchmarks():
    """
    Industry-level benchmarks for ESG comparison
    """
    print("\n" + "="*80)
    print("Creating Gold Table: Industry Benchmarks")
    print("="*80 + "\n")
    
    try:
        df_silver = spark.read.parquet(f"{SILVER_BASE}/ESG_risk")
        
        df_silver = df_silver.filter(
            col("Industry").isNotNull() &
            col("Sector").isNotNull()
        )
        
        df_gold = df_silver.groupBy("Industry", "Sector", "ingest_date").agg(
            count("Symbol").alias("company_count"),
            round(avg(col("Total_ESG_Risk_score").cast("double")), 2).alias("industry_avg_esg"),
            round(min(col("Total_ESG_Risk_score").cast("double")), 2).alias("industry_best_esg"),
            round(max(col("Total_ESG_Risk_score").cast("double")), 2).alias("industry_worst_esg"),
            round(percentile_approx(col("Total_ESG_Risk_score").cast("double"), 0.5), 2).alias("industry_median_esg"),
            round(avg(col("Environment_Risk_Score").cast("double")), 2).alias("avg_env"),
            round(avg(col("Governance_Risk_Score").cast("double")), 2).alias("avg_gov"),
            round(avg(col("Social_Risk_Score").cast("double")), 2).alias("avg_social")
        ).filter(col("company_count") >= 2)
        
        df_gold = df_gold.orderBy("industry_avg_esg")
        
        gold_path = f"{GOLD_BASE}/industry_benchmarks"
        df_gold.write \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .parquet(gold_path)
        
        print(f"[+] Gold table saved: {gold_path}")
        df_gold.show(15, truncate=False)
        
        return df_gold
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    print("\n" + "="*80)
    print("ESG Gold Layer Analytics - Business Metrics Generation")
    print("="*80)
    print(f"Start: {datetime.now()}\n")
    
    results = {}
    
    # Create all Gold tables
    print("\n[INFO] Creating Gold Layer Analytics Tables...\n")
    
    results['sector_summary'] = create_esg_risk_by_sector()
    results['top_bottom'] = create_esg_top_bottom_performers()
    results['risk_distribution'] = create_esg_risk_distribution()
    results['scorecard'] = create_company_esg_scorecard()
    results['sector_comparison'] = create_sector_comparison_matrix()
    results['industry_benchmarks'] = create_industry_benchmarks()
    
    # Summary
    print("\n" + "="*80)
    print("Gold Layer Summary:")
    print("="*80)
    
    success_count = sum(1 for v in results.values() if v is not None)
    total_count = len(results)
    
    print(f"\nTables Created: {success_count}/{total_count}")
    for table_name, df in results.items():
        status = "[OK]" if df is not None else "[FAILED]"
        print(f"  {status} {table_name}")
    
    print(f"\nEnd: {datetime.now()}")
    print("="*80 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()