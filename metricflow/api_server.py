from flask import Flask, request, jsonify
from flask_cors import CORS
import trino
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)

TRINO_HOST = os.getenv('TRINO_HOST', 'trino')
TRINO_PORT = int(os.getenv('TRINO_PORT', 8080))
TRINO_USER = os.getenv('TRINO_USER', 'user')
TRINO_CATALOG = os.getenv('TRINO_CATALOG', 'delta')
TRINO_SCHEMA = os.getenv('TRINO_SCHEMA', 'default_marts')

def get_trino_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA
    )

def execute_query(sql):
    conn = get_trino_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append(dict(zip(columns, row)))
        return result
    finally:
        cursor.close()
        conn.close()

@app.route('/health', methods=['GET'])
def health():
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "service": "MetricFlow API - Power BI Enhanced",
            "trino": f"{TRINO_HOST}:{TRINO_PORT}",
            "catalog": TRINO_CATALOG,
            "schema": TRINO_SCHEMA,
            "timestamp": datetime.now().isoformat()
        }), 200
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

# ============================================================================
# POWER BI OPTIMIZED ENDPOINTS
# ============================================================================

@app.route('/api/powerbi/companies', methods=['GET', 'POST'])
def powerbi_companies():
    """
    Complete company master table for Power BI
    Filters: sector, industry, country, has_valid_isin
    """
    try:
        filters = request.get_json() if request.method == 'POST' else {}
        
        where_clauses = []
        if filters.get('sector'):
            where_clauses.append(f"sector_normalized = '{filters['sector']}'")
        if filters.get('industry'):
            where_clauses.append(f"industry_normalized = '{filters['industry']}'")
        if filters.get('country'):
            where_clauses.append(f"country_normalized = '{filters['country']}'")
        if filters.get('has_valid_isin') is not None:
            where_clauses.append(f"has_valid_isin = {filters['has_valid_isin']}")
        
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        sql = f"""
        SELECT
            company_key as CompanyKey,
            company_name as CompanyName,
            symbol as StockSymbol,
            isin as ISIN,
            sector as Sector,
            industry as Industry,
            sub_industry as SubIndustry,
            city as City,
            country as Country,
            region as Region,
            sector_normalized as SectorNormalized,
            industry_normalized as IndustryNormalized,
            country_normalized as CountryNormalized,
            has_valid_isin as HasValidISIN
        FROM dim_company
        {where_clause}
        ORDER BY company_name
        """
        
        result = execute_query(sql)
        
        return jsonify({
            "data": result,
            "count": len(result),
            "table": "dim_company",
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e), "table": "dim_company"}), 500

@app.route('/api/powerbi/esg-scores', methods=['GET', 'POST'])
def powerbi_esg_scores():
    """
    ESG scores fact table - optimized for Power BI
    Join with dim_company for filtering
    """
    try:
        filters = request.get_json() if request.method == 'POST' else {}
        
        where_clauses = []
        if filters.get('year'):
            where_clauses.append(f"s.year = {filters['year']}")
        if filters.get('source'):
            where_clauses.append(f"s.source = '{filters['source']}'")
        if filters.get('min_score'):
            where_clauses.append(f"s.overall_score >= {filters['min_score']}")
        if filters.get('risk_level'):
            where_clauses.append(f"s.esg_risk_level = '{filters['risk_level']}'")
        
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        sql = f"""
        SELECT
            s.score_key as ScoreKey,
            s.company_id as CompanyKey,
            c.company_name as CompanyName,
            c.sector_normalized as Sector,
            c.industry_normalized as Industry,
            c.country_normalized as Country,
            s.year as Year,
            s.source as DataSource,
            s.overall_score as OverallScore,
            s.esg_pulse as ESGPulse,
            s.total_level as TotalLevel,
            s.total_grade as TotalGrade,
            s.total_esg_risk_score as TotalRiskScore,
            s.esg_risk_level as RiskLevel,
            s.esg_risk_percentile as RiskPercentile,
            s.controversy_score as ControversyScore,
            s.controversy_level as ControversyLevel
        FROM fact_esg_score_risk s
        JOIN dim_company c ON s.company_id = c.company_key
        {where_clause}
        ORDER BY c.company_name, s.year DESC
        """
        
        result = execute_query(sql)
        
        return jsonify({
            "data": result,
            "count": len(result),
            "table": "fact_esg_score_risk",
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e), "table": "fact_esg_score_risk"}), 500

@app.route('/api/powerbi/esg-metrics', methods=['GET', 'POST'])
def powerbi_esg_metrics():
    """
    ESG metrics fact table with full dimension joins
    Optimized for Power BI performance
    """
    try:
        filters = request.get_json() if request.method == 'POST' else {}
        
        where_clauses = []
        if filters.get('year'):
            where_clauses.append(f"f.year = {filters['year']}")
        if filters.get('topic'):
            where_clauses.append(f"m.topic = '{filters['topic']}'")
        if filters.get('metric_group'):
            where_clauses.append(f"m.metric_group = '{filters['metric_group']}'")
        if filters.get('company_id'):
            where_clauses.append(f"f.company_id = '{filters['company_id']}'")
        
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        limit = filters.get('limit', 10000)
        
        sql = f"""
        SELECT
            f.metric_key as MetricKey,
            f.company_id as CompanyKey,
            c.company_name as CompanyName,
            c.sector_normalized as Sector,
            c.industry_normalized as Industry,
            f.metric_id as MetricKey_Dim,
            m.metric_name as MetricName,
            m.metric_group as MetricGroup,
            m.topic as Topic,
            f.unit_id as UnitKey,
            u.original_unit as OriginalUnit,
            u.standard_unit as StandardUnit,
            f.year as Year,
            f.original_value as OriginalValue,
            f.normalized_value as NormalizedValue
        FROM fact_esg_metric f
        JOIN dim_company c ON f.company_id = c.company_key
        JOIN dim_metric m ON f.metric_id = m.metric_key
        LEFT JOIN dim_unit u ON f.unit_id = u.unit_key
        {where_clause}
        ORDER BY c.company_name, f.year DESC, m.metric_name
        LIMIT {limit}
        """
        
        result = execute_query(sql)
        
        return jsonify({
            "data": result,
            "count": len(result),
            "table": "fact_esg_metric",
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e), "table": "fact_esg_metric"}), 500

@app.route('/api/powerbi/aggregated/company-scores', methods=['GET'])
def powerbi_agg_company_scores():
    """
    Pre-aggregated company scores for dashboard KPIs
    """
    try:
        sql = """
        SELECT
            c.company_key as CompanyKey,
            c.company_name as CompanyName,
            c.sector_normalized as Sector,
            c.industry_normalized as Industry,
            c.country_normalized as Country,
            MAX(s.year) as LatestYear,
            AVG(CASE WHEN s.source = 'esg_level' THEN s.overall_score END) as AvgESGScore,
            AVG(CASE WHEN s.source = 'industrials' THEN s.esg_pulse END) as AvgESGPulse,
            AVG(CASE WHEN s.source = 'sp500_risk' THEN s.total_esg_risk_score END) as AvgRiskScore,
            MAX(CASE WHEN s.source = 'sp500_risk' THEN s.esg_risk_level END) as CurrentRiskLevel,
            MAX(CASE WHEN s.source = 'esg_level' THEN s.total_grade END) as CurrentGrade,
            COUNT(DISTINCT s.source) as DataSourceCount
        FROM dim_company c
        LEFT JOIN fact_esg_score_risk s ON c.company_key = s.company_id
        GROUP BY c.company_key, c.company_name, c.sector_normalized, c.industry_normalized, c.country_normalized
        ORDER BY c.company_name
        """
        
        result = execute_query(sql)
        
        return jsonify({
            "data": result,
            "count": len(result),
            "description": "Aggregated company ESG scores",
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/powerbi/aggregated/sector-benchmarks', methods=['GET'])
def powerbi_sector_benchmarks():
    """
    Sector-level benchmarks for comparison
    """
    try:
        sql = """
        SELECT
            c.sector_normalized as Sector,
            COUNT(DISTINCT c.company_key) as CompanyCount,
            AVG(s.overall_score) as AvgOverallScore,
            MIN(s.overall_score) as MinScore,
            MAX(s.overall_score) as MaxScore,
            APPROX_PERCENTILE(s.overall_score, 0.5) as MedianScore,
            AVG(s.total_esg_risk_score) as AvgRiskScore,
            COUNT(DISTINCT CASE WHEN s.esg_risk_level = 'high' THEN s.company_id END) as HighRiskCount,
            COUNT(DISTINCT CASE WHEN s.esg_risk_level = 'low' THEN s.company_id END) as LowRiskCount
        FROM dim_company c
        LEFT JOIN fact_esg_score_risk s ON c.company_key = s.company_id
        WHERE c.sector_normalized IS NOT NULL
        GROUP BY c.sector_normalized
        ORDER BY AvgOverallScore DESC
        """
        
        result = execute_query(sql)
        
        return jsonify({
            "data": result,
            "count": len(result),
            "description": "Sector-level ESG benchmarks",
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/powerbi/aggregated/metric-summary', methods=['GET', 'POST'])
def powerbi_metric_summary():
    """
    Metric summary by topic and group
    """
    try:
        filters = request.get_json() if request.method == 'POST' else {}
        
        where_clauses = []
        if filters.get('topic'):
            where_clauses.append(f"m.topic = '{filters['topic']}'")
        if filters.get('year'):
            where_clauses.append(f"f.year = {filters['year']}")
        
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        sql = f"""
        SELECT
            m.topic as Topic,
            m.metric_group as MetricGroup,
            COUNT(DISTINCT f.company_id) as CompanyCount,
            COUNT(DISTINCT f.metric_id) as UniqueMetrics,
            AVG(f.normalized_value) as AvgValue,
            MIN(f.normalized_value) as MinValue,
            MAX(f.normalized_value) as MaxValue,
            COUNT(*) as RecordCount
        FROM fact_esg_metric f
        JOIN dim_metric m ON f.metric_id = m.metric_key
        {where_clause}
        GROUP BY m.topic, m.metric_group
        ORDER BY m.topic, CompanyCount DESC
        """
        
        result = execute_query(sql)
        
        return jsonify({
            "data": result,
            "count": len(result),
            "description": "Metric coverage summary",
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/powerbi/time-series/esg-trend', methods=['POST'])
def powerbi_esg_trend():
    """
    ESG score time series for trend analysis
    """
    try:
        data = request.get_json()
        company_id = data.get('company_id')
        
        where_clause = f"WHERE s.company_id = '{company_id}'" if company_id else ""
        
        sql = f"""
        SELECT
            c.company_name as CompanyName,
            c.sector_normalized as Sector,
            s.year as Year,
            s.source as DataSource,
            s.overall_score as OverallScore,
            s.esg_pulse as ESGPulse,
            s.total_esg_risk_score as RiskScore,
            s.esg_risk_level as RiskLevel
        FROM fact_esg_score_risk s
        JOIN dim_company c ON s.company_id = c.company_key
        {where_clause}
        ORDER BY c.company_name, s.year
        """
        
        result = execute_query(sql)
        
        return jsonify({
            "data": result,
            "count": len(result),
            "description": "ESG score time series",
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ============================================================================
# UTILITY ENDPOINTS
# ============================================================================

@app.route('/api/powerbi/filters/sectors', methods=['GET'])
def get_sectors():
    """Get unique sectors for filter"""
    try:
        sql = """
        SELECT DISTINCT sector_normalized as Sector
        FROM dim_company
        WHERE sector_normalized IS NOT NULL
        ORDER BY sector_normalized
        """
        result = execute_query(sql)
        return jsonify({"data": result, "count": len(result)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/powerbi/filters/industries', methods=['GET'])
def get_industries():
    """Get unique industries for filter"""
    try:
        sql = """
        SELECT DISTINCT industry_normalized as Industry
        FROM dim_company
        WHERE industry_normalized IS NOT NULL
        ORDER BY industry_normalized
        """
        result = execute_query(sql)
        return jsonify({"data": result, "count": len(result)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/powerbi/filters/countries', methods=['GET'])
def get_countries():
    """Get unique countries for filter"""
    try:
        sql = """
        SELECT DISTINCT country_normalized as Country
        FROM dim_company
        WHERE country_normalized IS NOT NULL
        ORDER BY country_normalized
        """
        result = execute_query(sql)
        return jsonify({"data": result, "count": len(result)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/powerbi/filters/years', methods=['GET'])
def get_years():
    """Get available years"""
    try:
        sql = """
        SELECT DISTINCT year as Year
        FROM fact_esg_score_risk
        WHERE year IS NOT NULL
        ORDER BY year DESC
        """
        result = execute_query(sql)
        return jsonify({"data": result, "count": len(result)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/powerbi/filters/topics', methods=['GET'])
def get_topics():
    """Get ESG topics"""
    try:
        sql = """
        SELECT DISTINCT topic as Topic
        FROM dim_metric
        WHERE topic IS NOT NULL
        ORDER BY topic
        """
        result = execute_query(sql)
        return jsonify({"data": result, "count": len(result)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)