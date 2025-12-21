CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metadata;

SET search_path TO gold, public;

-- DimCountry
CREATE TABLE IF NOT EXISTS gold.DimCountry (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(100)
);
CREATE INDEX idx_country_region ON gold.DimCountry(region);
COMMENT ON TABLE gold.DimCountry IS 'Country dimension for country-level ESG analysis';
--      
-- DimIndusty
CREATE TABLE IF NOT EXISTS gold.DimIndustry (
    industry_id SERIAL PRIMARY KEY,
    industry_name VARCHAR(100) NOT NULL,
    sector VARCHAR(100)
);

-- DimCompany
CREATE TABLE IF NOT EXISTS gold.DimCompany (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(200) NOT NULL,
    industry_id VARCHAR(100) REFERENCES gold.DimIndustry(industry_id),
    sector VARCHAR(100), -- Industry Sector
    country_id VARCHAR(100) REFERENCES gold.DimCountry(country_id),
    region VARCHAR(100)
);
CREATE INDEX idx_company_industry ON gold.DimCompany(industry);
CREATE INDEX idx_company_sector ON gold.DimCompany(sector);
CREATE INDEX idx_company_country ON gold.DimCompany(country);

COMMENT ON TABLE gold.DimCompany IS 'Company dimension containing master data for all tracked organizations';

-- DimKPI: KPI metadata following GRI, SASB, TCFD standards
CREATE TABLE IF NOT EXISTS gold.DimKPI (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(200) NOT NULL,
    metric_category VARCHAR(100),
    unit VARCHAR(50),
    boundary_scope VARCHAR(100),
    topic VARCHAR(20) -- E, S, or G
    description TEXT
);

CREATE INDEX idx_kpi_pillar ON gold.DimKPI(topic);
CREATE INDEX idx_kpi_category ON gold.DimKPI(category);


COMMENT ON TABLE gold.DimKPI IS 'KPI definitions aligned with GRI, SASB, and TCFD reporting standards';

-- DimDate: Date dimension for time-series analysis
CREATE TABLE IF NOT EXISTS gold.DimDate (
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    week_of_year INTEGER,
    day_of_year INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    is_reporting_period BOOLEAN DEFAULT FALSE,
    is_weekend BOOLEAN DEFAULT FALSE,
    is_holiday BOOLEAN DEFAULT FALSE,
    CONSTRAINT unique_date UNIQUE (full_date)
);

CREATE INDEX idx_date_year ON gold.DimDate(year);
CREATE INDEX idx_date_fiscal_year ON gold.DimDate(fiscal_year);
CREATE INDEX idx_date_reporting ON gold.DimDate(is_reporting_period);

COMMENT ON TABLE gold.DimDate IS 'Date dimension for temporal analysis and reporting periods';

-- fact_corporate_esg_metrics: Corporate ESG KPI measurements
CREATE TABLE IF NOT EXISTS gold.fact_corporate_esg_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES gold.DimCompany(company_id),
    date_id INTEGER REFERENCES gold.DimDate(date_id),
    kpi_id INTEGER REFERENCES gold.DimKPI(id),
    metric_value DECIMAL(18,4),
    unit_of_measure VARCHAR(50),
    baseline_year INTEGER,
    baseline_value DECIMAL(18,4),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_metrics_company ON gold.fact_corporate_esg_metrics(company_id);
CREATE INDEX idx_fact_metrics_date ON gold.fact_corporate_esg_metrics(date_id);
CREATE INDEX idx_fact_metrics_kpi ON gold.fact_corporate_esg_metrics(kpi_id);
CREATE INDEX idx_fact_metrics_composite ON gold.fact_corporate_esg_metrics(company_id, date_id, kpi_id);

COMMENT ON TABLE gold.fact_corporate_esg_metrics IS 'Fact table containing all corporate ESG KPI measurements';

-- fact_esg_risk_scores: ESG risk assessment scores
CREATE TABLE IF NOT EXISTS gold.fact_esg_risk_scores (
    risk_id BIGSERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES gold.DimCompany(company_id),
    date_id INTEGER REFERENCES gold.DimDate(date_id),
    overall_esg_score DECIMAL(10,4),
    environmental_score DECIMAL(10,4),
    social_score DECIMAL(10,4),
    governance_score DECIMAL(10,4),
    risk_level VARCHAR(20) CHECK (risk_level IN ('Low Risk', 'Medium Risk', 'High Risk', 'Critical Risk')),
    industry_percentile DECIMAL(5,2),
    global_percentile DECIMAL(5,2),
    year_over_year_improvement DECIMAL(10,4),
    controversy_score DECIMAL(10,4),
    compliance_score DECIMAL(10,4),
    rating_agency VARCHAR(100),
    assessment_methodology VARCHAR(100),
    confidence_level DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_risk_company ON gold.fact_esg_risk_scores(company_id);
CREATE INDEX idx_fact_risk_date ON gold.fact_esg_risk_scores(date_id);
CREATE INDEX idx_fact_risk_level ON gold.fact_esg_risk_scores(risk_level);
CREATE INDEX idx_fact_risk_composite ON gold.fact_esg_risk_scores(company_id, date_id);

COMMENT ON TABLE gold.fact_esg_risk_scores IS 'ESG risk scores and ratings from third-party assessment agencies';

-- fact_country_esg_indicators: Country-level ESG indicators
CREATE TABLE IF NOT EXISTS gold.fact_country_esg_indicators (
    indicator_id BIGSERIAL PRIMARY KEY,
    country_id INTEGER REFERENCES gold.DimCountry(country_id),
    date_id INTEGER REFERENCES gold.DimDate(date_id),
    pillar CHAR(1) CHECK (pillar IN ('E', 'S', 'G')),
    indicator_name VARCHAR(200) NOT NULL,
    indicator_value DECIMAL(18,4),
    unit_of_measure VARCHAR(50),
    global_rank INTEGER,
    regional_rank INTEGER,
    year_over_year_change DECIMAL(10,4),
    sdg_alignment VARCHAR(50),
    data_source VARCHAR(100),
    confidence_interval_lower DECIMAL(18,4),
    confidence_interval_upper DECIMAL(18,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_country_country ON gold.fact_country_esg_indicators(country_id);
CREATE INDEX idx_fact_country_date ON gold.fact_country_esg_indicators(date_id);
CREATE INDEX idx_fact_country_pillar ON gold.fact_country_esg_indicators(pillar);
CREATE INDEX idx_fact_country_composite ON gold.fact_country_esg_indicators(country_id, date_id, pillar);

COMMENT ON TABLE gold.fact_country_esg_indicators IS 'Country-level ESG indicators for sovereign risk assessment';

-- fact_sustainability_rankings: Third-party sustainability rankings
CREATE TABLE IF NOT EXISTS gold.fact_sustainability_rankings (
    ranking_id BIGSERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES gold.dim_company(company_id),
    date_id INTEGER REFERENCES gold.dim_date(date_id),
    ranking_body VARCHAR(100) NOT NULL,
    overall_score DECIMAL(10,4),
    environmental_score DECIMAL(10,4),
    social_score DECIMAL(10,4),
    governance_score DECIMAL(10,4),
    global_rank INTEGER,
    industry_rank INTEGER,
    region_rank INTEGER,
    total_participants INTEGER,
    percentile DECIMAL(5,2),
    award_level VARCHAR(50),
    certification_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_ranking_company ON gold.fact_sustainability_rankings(company_id);
CREATE INDEX idx_fact_ranking_date ON gold.fact_sustainability_rankings(date_id);
CREATE INDEX idx_fact_ranking_body ON gold.fact_sustainability_rankings(ranking_body);

COMMENT ON TABLE gold.fact_sustainability_rankings IS 'External sustainability rankings from bodies like CDP, DJSI, GRESB';


-- agg_company_esg_summary: Pre-aggregated company ESG summary # Qua các năm 
CREATE TABLE IF NOT EXISTS gold.agg_company_esg_summary (
    summary_id BIGSERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES gold.DimCompany(company_id),
    year INTEGER,
    avg_esg_score DECIMAL(10,4),
    avg_environmental_score DECIMAL(10,4),
    avg_social_score DECIMAL(10,4),
    avg_governance_score DECIMAL(10,4),
    total_kpis_tracked INTEGER,
    kpis_on_target INTEGER,
    kpis_at_risk INTEGER,
    overall_achievement_rate DECIMAL(5,2),
    industry_rank INTEGER,
    yoy_improvement DECIMAL(10,4),
    maturity_level VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_company_year UNIQUE (company_id, year)
);

CREATE INDEX idx_agg_summary_company ON gold.agg_company_esg_summary(company_id);
CREATE INDEX idx_agg_summary_year ON gold.agg_company_esg_summary(year);

COMMENT ON TABLE gold.agg_company_esg_summary IS 'Pre-aggregated ESG summary for faster dashboard queries';

-- agg_industry_benchmarks: Industry-level benchmarks
CREATE TABLE IF NOT EXISTS gold.agg_industry_benchmarks (
    benchmark_id BIGSERIAL PRIMARY KEY,
    industry VARCHAR(100),
    year INTEGER,
    avg_esg_score DECIMAL(10,4),
    median_esg_score DECIMAL(10,4),
    top_quartile_score DECIMAL(10,4),
    bottom_quartile_score DECIMAL(10,4),
    std_deviation DECIMAL(10,4),
    company_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_industry_year UNIQUE (industry, year)
);

CREATE INDEX idx_agg_benchmark_industry ON gold.agg_industry_benchmarks(industry);
CREATE INDEX idx_agg_benchmark_year ON gold.agg_industry_benchmarks(year);

COMMENT ON TABLE gold.agg_industry_benchmarks IS 'Industry-level ESG benchmarks for comparative analysis';

-- =====================================================
-- METADATA TABLES
-- =====================================================

CREATE TABLE IF NOT EXISTS metadata.data_lineage (
    lineage_id BIGSERIAL PRIMARY KEY,
    source_table VARCHAR(100),
    target_table VARCHAR(100),
    transformation_type VARCHAR(50),
    pipeline_name VARCHAR(100),
    execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    execution_status VARCHAR(20),
    error_message TEXT
);

CREATE INDEX idx_lineage_pipeline ON metadata.data_lineage(pipeline_name);
CREATE INDEX idx_lineage_timestamp ON metadata.data_lineage(execution_timestamp);

CREATE TABLE IF NOT EXISTS metadata.data_quality_checks (
    check_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    check_type VARCHAR(50),
    check_description TEXT,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    passed BOOLEAN,
    metric_value DECIMAL(10,2),
    threshold_value DECIMAL(10,2),
    severity VARCHAR(20) CHECK (severity IN ('Critical', 'High', 'Medium', 'Low'))
);

CREATE INDEX idx_quality_table ON metadata.data_quality_checks(table_name);
CREATE INDEX idx_quality_timestamp ON metadata.data_quality_checks(check_timestamp);


-- Comprehensive ESG dashboard view
CREATE OR REPLACE VIEW gold.vw_esg_dashboard AS
SELECT 
    c.company_name,
    c.ticker_symbol,
    c.industry,
    c.sector,
    c.country,
    d.year,
    d.quarter,
    r.overall_esg_score,
    r.environmental_score,
    r.social_score,
    r.governance_score,
    r.risk_level,
    r.industry_percentile,
    r.global_percentile,
    COUNT(m.metric_id) as kpis_reported,
    AVG(m.achievement_rate) as avg_achievement_rate
FROM gold.fact_esg_risk_scores r
JOIN gold.dim_company c ON r.company_id = c.company_id
JOIN gold.dim_date d ON r.date_id = d.date_id
LEFT JOIN gold.fact_corporate_esg_metrics m ON r.company_id = m.company_id AND r.date_id = m.date_id
GROUP BY c.company_name, c.ticker_symbol, c.industry, c.sector, c.country,
         d.year, d.quarter, r.overall_esg_score, r.environmental_score,
         r.social_score, r.governance_score, r.risk_level, 
         r.industry_percentile, r.global_percentile;

-- KPI performance view
CREATE OR REPLACE VIEW gold.vw_kpi_performance AS
SELECT 
    c.company_name,
    k.kpi_name,
    k.kpi_code,
    k.esg_pillar,
    k.category,
    d.year,
    m.metric_value,
    m.unit_of_measure,
    m.target_value,
    m.achievement_rate,
    m.year_over_year_change,
    m.data_quality_score
FROM gold.fact_corporate_esg_metrics m
JOIN gold.dim_company c ON m.company_id = c.company_id
JOIN gold.dim_kpi k ON m.kpi_id = k.kpi_id
JOIN gold.dim_date d ON m.date_id = d.date_id;

COMMENT ON VIEW gold.vw_esg_dashboard IS 'Comprehensive view for Power BI ESG dashboard';
COMMENT ON VIEW gold.vw_kpi_performance IS 'KPI performance tracking view for detailed analysis';

GRANT USAGE ON SCHEMA gold TO esg_user;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO esg_user;
GRANT SELECT ON ALL VIEWS IN SCHEMA gold TO esg_user;
GRANT USAGE ON SCHEMA metadata TO esg_user;
GRANT SELECT ON ALL TABLES IN SCHEMA metadata TO esg_user;

SELECT 'ESG Lakehouse Gold Layer Schema Created Successfully' AS status;