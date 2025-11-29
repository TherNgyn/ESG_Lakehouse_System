from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, year, month, quarter,
    avg, sum, count, lag, lead, rank, dense_rank,
    percent_rank, ntile, row_number, stddev, variance
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
import sys

# Initialize Spark Session with MinIO configuration
spark = SparkSession.builder \
    .appName("ESG Lakehouse Processing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# BRONZE TO SILVER TRANSFORMATIONS
# =====================================================

class ESGDataProcessor:
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.bronze_path = "s3a://bronze"
        self.silver_path = "s3a://silver"
        self.gold_path = "s3a://gold"
    
    def process_corporate_kpi_data(self, date_partition):
        """
        Process corporate ESG KPI data from Bronze to Silver
        - Data cleaning
        - Standardization
        - Validation
        - Enrichment
        """
        print("🔄 Processing Corporate KPI Data...")
        
        # Read from Bronze
        bronze_df = self.spark.read.parquet(
            f"{self.bronze_path}/corporate_esg/kpi_metrics/date={date_partition}/"
        )
        
        # Data Cleaning
        cleaned_df = bronze_df \
            .dropna(subset=["company_name", "kpi_name", "value"]) \
            .withColumn("company_name_clean", col("company_name").cast("string")) \
            .withColumn("value_numeric", col("value").cast("double")) \
            .filter(col("value_numeric").isNotNull())
        
        # Standardize KPI names (GRI/SASB alignment)
        kpi_mapping = {
            "GHG Emissions Reduction": "305-5_GHG_REDUCTION",
            "Renewable Energy": "302-1_RENEWABLE_ENERGY",
            "Water Consumption": "303-5_WATER_CONSUMPTION",
            "Waste Recycling Rate": "306-3_WASTE_RECYCLING"
        }
        
        # Apply standardization
        for old_name, new_code in kpi_mapping.items():
            cleaned_df = cleaned_df.withColumn(
                "kpi_code",
                when(col("kpi_name") == old_name, new_code)
                .otherwise(col("kpi_code"))
            )
        
        # Calculate year-over-year changes
        window_spec = Window.partitionBy("company_name", "kpi_name").orderBy("year")
        
        enriched_df = cleaned_df \
            .withColumn("previous_year_value", lag("value_numeric").over(window_spec)) \
            .withColumn(
                "yoy_change_pct",
                ((col("value_numeric") - col("previous_year_value")) / col("previous_year_value")) * 100
            ) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("data_quality_score", lit(1.0))
        
        # Write to Silver
        enriched_df.write \
            .mode("overwrite") \
            .partitionBy("year") \
            .parquet(f"{self.silver_path}/corporate_esg_cleaned/kpi_standardized/")
        
        print(f"✅ Processed {enriched_df.count()} KPI records to Silver")
        return enriched_df
    
    def process_esg_risk_scores(self, date_partition):
        """
        Process ESG risk scores with advanced analytics
        - Normalization
        - Risk classification
        - Industry benchmarking
        - Trend analysis
        """
        print("🔄 Processing ESG Risk Scores...")
        
        # Read from Bronze
        risk_df = self.spark.read.parquet(
            f"{self.bronze_path}/corporate_esg/esg_risk_scores/date={date_partition}/"
        )
        
        # Normalize scores (0-100 scale)
        normalized_df = risk_df \
            .withColumn("overall_score_norm", col("overall_score")) \
            .withColumn("environmental_score_norm", col("environmental_score")) \
            .withColumn("social_score_norm", col("social_score")) \
            .withColumn("governance_score_norm", col("governance_score"))
        
        # Risk Level Classification
        normalized_df = normalized_df.withColumn(
            "risk_level",
            when(col("overall_score_norm") >= 75, "Low Risk")
            .when(col("overall_score_norm") >= 50, "Medium Risk")
            .when(col("overall_score_norm") >= 25, "High Risk")
            .otherwise("Critical Risk")
        )
        
        # Calculate ESG composite score with weighted average
        normalized_df = normalized_df.withColumn(
            "esg_composite_score",
            (col("environmental_score_norm") * 0.4 +
             col("social_score_norm") * 0.3 +
             col("governance_score_norm") * 0.3)
        )
        
        # Industry Percentile Ranking
        window_industry = Window.partitionBy("year").orderBy(col("overall_score_norm").desc())
        
        ranked_df = normalized_df \
            .withColumn("global_rank", rank().over(window_industry)) \
            .withColumn("percentile_rank", percent_rank().over(window_industry) * 100)
        
        # Trend Analysis (YoY improvement)
        window_company = Window.partitionBy("company_ticker").orderBy("year")
        
        trend_df = ranked_df \
            .withColumn("prev_year_score", lag("overall_score_norm").over(window_company)) \
            .withColumn(
                "score_improvement",
                col("overall_score_norm") - col("prev_year_score")
            ) \
            .withColumn(
                "improvement_trend",
                when(col("score_improvement") > 5, "Significant Improvement")
                .when(col("score_improvement") > 0, "Modest Improvement")
                .when(col("score_improvement") == 0, "No Change")
                .when(col("score_improvement") > -5, "Modest Decline")
                .otherwise("Significant Decline")
            )
        
        # Add controversy flags (simplified)
        trend_df = trend_df.withColumn(
            "has_controversy",
            when(col("governance_score_norm") < 30, True).otherwise(False)
        )
        
        # Processing metadata
        final_df = trend_df \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("data_version", lit("v1.0"))
        
        # Write to Silver
        final_df.write \
            .mode("overwrite") \
            .partitionBy("year") \
            .parquet(f"{self.silver_path}/corporate_esg_cleaned/risk_scores_validated/")
        
        print(f"✅ Processed {final_df.count()} risk score records to Silver")
        return final_df
    
    def process_country_esg_indicators(self, date_partition):
        """
        Process country-level ESG indicators
        - Data validation
        - Regional aggregation
        - Global ranking
        - SDG alignment
        """
        print("🔄 Processing Country ESG Indicators...")
        
        # Read from Bronze
        country_df = self.spark.read.parquet(
            f"{self.bronze_path}/country_esg/all_indicators/date={date_partition}/"
        )
        
        # Data Validation
        validated_df = country_df \
            .dropna(subset=["country_code", "value"]) \
            .withColumn("value_validated", col("value").cast("double")) \
            .filter(col("value_validated").isNotNull())
        
        # Regional Classification
        region_mapping = {
            "VNM": "Southeast Asia",
            "USA": "North America",
            "CHN": "East Asia",
            "DEU": "Europe",
            "BRA": "South America"
        }
        
        # Apply region mapping using when-otherwise chain
        region_expr = None
        for country, region in region_mapping.items():
            if region_expr is None:
                region_expr = when(col("country_code") == country, region)
            else:
                region_expr = region_expr.when(col("country_code") == country, region)
        
        validated_df = validated_df.withColumn("region", region_expr.otherwise("Other"))
        
        # Global Rankings by Indicator
        window_global = Window.partitionBy("indicator_name", "year").orderBy(col("value_validated").desc())
        
        ranked_df = validated_df \
            .withColumn("global_rank_calc", rank().over(window_global)) \
            .withColumn("global_percentile", percent_rank().over(window_global) * 100)
        
        # Regional Rankings
        window_regional = Window.partitionBy("region", "indicator_name", "year").orderBy(col("value_validated").desc())
        
        ranked_df = ranked_df \
            .withColumn("regional_rank", rank().over(window_regional))
        
        # SDG Alignment Mapping
        sdg_mapping = {
            "CO2 Emissions per Capita": "SDG-13: Climate Action",
            "Renewable Energy Share": "SDG-7: Affordable Clean Energy",
            "HDI Index": "SDG-1: No Poverty",
            "Gender Equality Index": "SDG-5: Gender Equality",
            "Corruption Index": "SDG-16: Peace & Justice"
        }
        
        # Apply SDG mapping
        sdg_expr = None
        for indicator, sdg in sdg_mapping.items():
            if sdg_expr is None:
                sdg_expr = when(col("indicator_name") == indicator, sdg)
            else:
                sdg_expr = sdg_expr.when(col("indicator_name") == indicator, sdg)
        
        final_df = ranked_df \
            .withColumn("sdg_alignment", sdg_expr.otherwise("Not Mapped")) \
            .withColumn("processing_timestamp", current_timestamp())
        
        # Write to Silver
        final_df.write \
            .mode("overwrite") \
            .partitionBy("year", "pillar") \
            .parquet(f"{self.silver_path}/country_esg_cleaned/all_normalized/")
        
        print(f"✅ Processed {final_df.count()} country ESG records to Silver")
        return final_df
    
    # =====================================================
    # SILVER TO GOLD TRANSFORMATIONS
    # =====================================================
    
    def create_gold_fact_esg_metrics(self):
        """
        Create Gold layer fact table for ESG metrics
        Aggregated, business-ready format
        """
        print("🔄 Creating Gold Fact Table: ESG Metrics...")
        
        # Read from Silver
        kpi_df = self.spark.read.parquet(f"{self.silver_path}/corporate_esg_cleaned/kpi_standardized/")
        risk_df = self.spark.read.parquet(f"{self.silver_path}/corporate_esg_cleaned/risk_scores_validated/")
        
        # Create comprehensive fact table
        fact_df = kpi_df.join(
            risk_df,
            (kpi_df.company_name == risk_df.company_ticker) & (kpi_df.year == risk_df.year),
            "left"
        )
        
        # Select and rename columns for fact table
        gold_fact = fact_df.select(
            col("company_name").alias("company"),
            col("year").alias("fiscal_year"),
            col("kpi_name"),
            col("kpi_code"),
            col("value_numeric").alias("metric_value"),
            col("unit"),
            col("yoy_change_pct"),
            col("overall_score_norm").alias("esg_overall_score"),
            col("environmental_score_norm").alias("env_score"),
            col("social_score_norm").alias("soc_score"),
            col("governance_score_norm").alias("gov_score"),
            col("risk_level"),
            col("percentile_rank"),
            col("improvement_trend"),
            current_timestamp().alias("last_updated")
        )
        
        # Write to Gold as Delta table for ACID transactions
        gold_fact.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("fiscal_year") \
            .save(f"{self.gold_path}/fact_esg_metrics_delta/")
        
        print(f"✅ Created Gold fact table with {gold_fact.count()} records")
        return gold_fact
    
    def create_gold_aggregated_insights(self):
        """
        Create aggregated insights for dashboards
        - Industry averages
        - Trend analysis
        - Top performers
        """
        print("🔄 Creating Gold Aggregated Insights...")
        
        # Read fact data
        fact_df = self.spark.read.format("delta").load(f"{self.gold_path}/fact_esg_metrics_delta/")
        
        # Industry-level aggregations
        industry_insights = fact_df.groupBy("fiscal_year") \
            .agg(
                avg("esg_overall_score").alias("avg_esg_score"),
                avg("env_score").alias("avg_env_score"),
                avg("soc_score").alias("avg_soc_score"),
                avg("gov_score").alias("avg_gov_score"),
                count("company").alias("company_count"),
                stddev("esg_overall_score").alias("score_std_dev")
            )
        
        # Write aggregated insights
        industry_insights.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{self.gold_path}/industry_insights_delta/")
        
        print(f"✅ Created aggregated insights")
        return industry_insights

# =====================================================
# MAIN EXECUTION
# =====================================================

if __name__ == "__main__":
    
    print("=" * 60)
    print("ESG LAKEHOUSE DATA PROCESSING")
    print("=" * 60)
    
    # Initialize processor
    processor = ESGDataProcessor(spark)
    
    # Get date partition from arguments or use default
    date_partition = sys.argv[1] if len(sys.argv) > 1 else "20240101"
    
    try:
        # Bronze to Silver transformations
        print("\n📊 BRONZE → SILVER LAYER PROCESSING")
        print("-" * 60)
        
        kpi_result = processor.process_corporate_kpi_data(date_partition)
        risk_result = processor.process_esg_risk_scores(date_partition)
        country_result = processor.process_country_esg_indicators(date_partition)
        
        # Silver to Gold transformations
        print("\n💎 SILVER → GOLD LAYER PROCESSING")
        print("-" * 60)
        
        fact_result = processor.create_gold_fact_esg_metrics()
        insights_result = processor.create_gold_aggregated_insights()
        
        print("\n" + "=" * 60)
        print("✅ ESG DATA PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()