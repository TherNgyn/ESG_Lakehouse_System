insert into "delta"."default_marts"."fact_esg_metric" ("metric_key", "company_id", "metric_id", "unit_id", "year", "original_value", "normalized_value", "standard_unit", "created_at", "updated_at")
    (
        select "metric_key", "company_id", "metric_id", "unit_id", "year", "original_value", "normalized_value", "standard_unit", "created_at", "updated_at"
        from "delta"."default_marts"."fact_esg_metric__dbt_tmp"
    )

