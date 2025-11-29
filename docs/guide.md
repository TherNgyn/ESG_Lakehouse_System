daily_ingestion_dag:
  Schedule: "0 2 * * *" (Daily at 2 AM)
  Tasks:
    1. ingest_corporate_kpi_data
    2. ingest_esg_risk_scores
    3. ingest_country_esg_data
    4. clean_and_standardize
    5. load_to_gold
    6. run_quality_checks

weekly_aggregation_dag:
  Schedule: "0 3 * * 0" (Weekly Sunday 3 AM)
  Tasks:
    1. calculate_industry_benchmarks
    2. update_trend_analysis
    3. generate_reports