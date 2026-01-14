-- Trino SQL Queries for ESG Gold Layer

-- ============================================
-- DIMENSION QUERIES
-- ============================================

-- View all companies
SELECT company_id, company_name, sector, industry, country
FROM marts.dim_company
LIMIT 10;

-- Companies by sector
SELECT sector, COUNT(*) as company_count
FROM marts.dim_company
WHERE sector IS NOT NULL
GROUP BY sector
ORDER BY company_count DESC;

-- View all metrics
SELECT metric_id, metric_name, metric_group, topic
FROM marts.dim_metric
ORDER BY metric_group, metric_name
LIMIT 20;

-- Metrics by topic
SELECT topic, COUNT(*) as metric_count
FROM marts.dim_metric
GROUP BY topic;

-- View unit conversions
SELECT original_unit, standard_unit, conversion_factor, unit_category
FROM marts.dim_unit
WHERE unit_category = 'emissions'
ORDER BY conversion_factor DESC;

-- ============================================
-- FACT TABLE QUERIES
-- ============================================

-- Latest ESG metrics for a company
SELECT 
    c.company_name,
    m.metric_name,
    f.year,
    f.normalized_value,
    u.standard_unit
FROM marts.fact_esg_metric f
JOIN marts.dim_company c ON f.company_id = c.company_id
JOIN marts.dim_metric m ON f.metric_id = m.metric_id
JOIN marts.dim_unit u ON f.unit_id = u.unit_id
WHERE c.company_name LIKE '%Tesla%'
    AND f.year = 2024
ORDER BY m.metric_name;

-- Total emissions by company (2024)
SELECT 
    c.company_name,
    c.sector,
    SUM(f.normalized_value) as total_emissions_tco2e
FROM marts.fact_esg_metric f
JOIN marts.dim_company c ON f.company_id = c.company_id
JOIN marts.dim_metric m ON f.metric_id = m.metric_id
WHERE m.metric_group IN ('scope 1 emissions', 'scope 2 emissions', 'scope 3 emissions')
    AND f.year = 2024
GROUP BY c.company_id, c.company_name, c.sector
ORDER BY total_emissions_tco2e DESC
LIMIT 20;

-- Energy consumption trends by year
SELECT 
    f.year,
    SUM(f.normalized_value) as total_energy_gj,
    COUNT(DISTINCT f.company_id) as company_count
FROM marts.fact_esg_metric f
JOIN marts.dim_metric m ON f.metric_id = m.metric_id
WHERE m.metric_group = 'renewable energy'
GROUP BY f.year
ORDER BY f.year;

-- ============================================
-- ESG SCORE QUERIES
-- ============================================

-- Top ESG performers (2024)
SELECT 
    c.company_name,
    c.sector,
    s.overall_esg_score,
    s.environmental_score,
    s.social_score,
    s.governance_score
FROM marts.fact_esg_score s
JOIN marts.dim_company c ON s.company_id = c.company_id
WHERE s.year = 2024
ORDER BY s.overall_esg_score DESC
LIMIT 20;

-- ESG score trends by sector
SELECT 
    c.sector,
    s.year,
    AVG(s.overall_esg_score) as avg_esg_score,
    AVG(s.environmental_score) as avg_env_score,
    AVG(s.social_score) as avg_social_score
FROM marts.fact_esg_score s
JOIN marts.dim_company c ON s.company_id = c.company_id
WHERE c.sector IS NOT NULL
GROUP BY c.sector, s.year
ORDER BY c.sector, s.year;

-- Companies with declining ESG scores
WITH yearly_scores AS (
    SELECT 
        company_id,
        year,
        overall_esg_score,
        LAG(overall_esg_score) OVER (PARTITION BY company_id ORDER BY year) as prev_score
    FROM marts.fact_esg_score
)
SELECT 
    c.company_name,
    y.year,
    y.overall_esg_score,
    y.prev_score,
    (y.overall_esg_score - y.prev_score) as score_change
FROM yearly_scores y
JOIN marts.dim_company c ON y.company_id = c.company_id
WHERE y.prev_score IS NOT NULL
    AND (y.overall_esg_score - y.prev_score) < -5
ORDER BY score_change;

-- ============================================
-- ADVANCED ANALYTICS
-- ============================================

-- Emissions intensity by sector
SELECT 
    c.sector,
    f.year,
    SUM(CASE WHEN m.metric_group LIKE '%emissions%' THEN f.normalized_value ELSE 0 END) as total_emissions,
    COUNT(DISTINCT f.company_id) as company_count,
    SUM(CASE WHEN m.metric_group LIKE '%emissions%' THEN f.normalized_value ELSE 0 END) / 
        NULLIF(COUNT(DISTINCT f.company_id), 0) as avg_emissions_per_company
FROM marts.fact_esg_metric f
JOIN marts.dim_company c ON f.company_id = c.company_id
JOIN marts.dim_metric m ON f.metric_id = m.metric_id
WHERE c.sector IS NOT NULL
GROUP BY c.sector, f.year
ORDER BY c.sector, f.year;

-- Top metrics by data availability
SELECT 
    m.metric_name,
    m.metric_group,
    COUNT(DISTINCT f.company_id) as company_count,
    COUNT(*) as measurement_count,
    MIN(f.year) as first_year,
    MAX(f.year) as last_year
FROM marts.fact_esg_metric f
JOIN marts.dim_metric m ON f.metric_id = m.metric_id
GROUP BY m.metric_id, m.metric_name, m.metric_group
HAVING COUNT(DISTINCT f.company_id) >= 10
ORDER BY company_count DESC
LIMIT 30;

-- Correlation: Emissions vs ESG Score
SELECT 
    c.company_name,
    s.year,
    s.environmental_score,
    SUM(f.normalized_value) as total_emissions
FROM marts.fact_esg_score s
JOIN marts.dim_company c ON s.company_id = c.company_id
LEFT JOIN marts.fact_esg_metric f ON s.company_id = f.company_id AND s.year = f.year
LEFT JOIN marts.dim_metric m ON f.metric_id = m.metric_id
WHERE m.metric_group LIKE '%emissions%'
GROUP BY c.company_id, c.company_name, s.year, s.environmental_score
HAVING SUM(f.normalized_value) > 0
ORDER BY s.year, s.environmental_score DESC;

-- Year-over-year growth rates
WITH yearly_totals AS (
    SELECT 
        c.company_id,
        c.company_name,
        f.year,
        SUM(f.normalized_value) as total_emissions
    FROM marts.fact_esg_metric f
    JOIN marts.dim_company c ON f.company_id = c.company_id
    JOIN marts.dim_metric m ON f.metric_id = m.metric_id
    WHERE m.metric_group LIKE '%emissions%'
    GROUP BY c.company_id, c.company_name, f.year
)
SELECT 
    company_name,
    year,
    total_emissions,
    LAG(total_emissions) OVER (PARTITION BY company_id ORDER BY year) as prev_year_emissions,
    CASE 
        WHEN LAG(total_emissions) OVER (PARTITION BY company_id ORDER BY year) > 0 
        THEN ROUND(((total_emissions - LAG(total_emissions) OVER (PARTITION BY company_id ORDER BY year)) / 
              LAG(total_emissions) OVER (PARTITION BY company_id ORDER BY year) * 100), 2)
        ELSE NULL 
    END as yoy_growth_pct
FROM yearly_totals
WHERE year >= 2020
ORDER BY company_name, year;

-- ============================================
-- DATA QUALITY CHECKS
-- ============================================

-- Check for missing values
SELECT 
    'dim_company' as table_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN company_name IS NULL THEN 1 ELSE 0 END) as null_company_name,
    SUM(CASE WHEN sector IS NULL THEN 1 ELSE 0 END) as null_sector
FROM marts.dim_company
UNION ALL
SELECT 
    'fact_esg_metric',
    COUNT(*),
    SUM(CASE WHEN company_id IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN metric_id IS NULL THEN 1 ELSE 0 END)
FROM marts.fact_esg_metric;

-- Verify referential integrity
SELECT 
    COUNT(*) as orphaned_records
FROM marts.fact_esg_metric f
LEFT JOIN marts.dim_company c ON f.company_id = c.company_id
WHERE c.company_id IS NULL;