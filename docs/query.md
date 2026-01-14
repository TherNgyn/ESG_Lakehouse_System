# DBT Project Structure for ESG Analytics

## Directory Structure
```
dbt-project/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── marts/
│   │   ├── dim_company.sql
│   │   ├── dim_metric.sql
│   │   ├── dim_unit.sql
│   │   ├── dim_date.sql
│   │   ├── fact_esg_metric.sql
│   │   └── fact_esg_score.sql
│   ├── schema.yml
│   ├── semantic_models.yml
│   └── sources.yml
```

## Model Descriptions

### Dimensions

1. **dim_company** (CMP-XXXXX)
   - Company master data with unique IDs
   - Fields: company_id, company_name, isin, sector, industry, country

2. **dim_metric** (MET-XXXXX)
   - Metric definitions and classifications
   - Fields: metric_id, metric_name, metric_group, topic, category

3. **dim_unit** (UNT-XXXXX)
   - Unit conversions and standardization
   - Fields: unit_id, original_unit, standard_unit, conversion_factor

4. **dim_date**
   - Date dimension for time-based analysis
   - Fields: date_id, year, quarter, month, week

### Facts

1. **fact_esg_metric**
   - Granular ESG metric values
   - Keys: metric_key (composite), company_id, metric_id, unit_id
   - Measures: original_value, normalized_value

2. **fact_esg_score**
   - Aggregated ESG scores by company/year
   - Keys: score_key (composite), company_id
   - Measures: environmental_score, social_score, governance_score, overall_esg_score

## Data Flow

Silver Layer → dbt → Gold Layer (Marts)

1. staging_companies_mapping → dim_company
2. staging_metrics → dim_metric
3. staging_units → dim_unit
4. date generation → dim_date
5. classified_metrics + dimensions → fact_esg_metric
6. fact_esg_metric aggregations → fact_esg_score

## MetricFlow Semantic Layer

Semantic models define business metrics:
- total_emissions (tCO2e)
- total_energy_consumption (GJ)
- total_water_consumption (m3)
- avg_employee_turnover_rate (%)
- avg_esg_score (0-100)

## Setup Commands

```bash
cd /workspace/dbt

dbt deps

dbt run --models dim_company dim_metric dim_unit dim_date

dbt run --models fact_esg_metric

dbt run --models fact_esg_score

dbt test

mf query --metrics total_emissions --group-by company__company_name,year

mf query --metrics avg_esg_score --group-by year

mf list metrics
```

## Trino Catalog Configuration

Ensure `/trino/catalog/delta.properties`:
```
connector.name=delta_lake
hive.metastore.uri=thrift://hive-metastore:9083
delta.s3.endpoint=http://minio:9000
delta.s3.path-style-access=true
delta.s3.aws-access-key=admin
delta.s3.aws-secret-key=admin123456
```

## Incremental Strategy

fact_esg_metric and fact_esg_score use incremental materialization:
- On first run: full load
- On subsequent runs: only last 2 years updated
- Partition by year for performance