from flask import Flask, request, jsonify
from flask_cors import CORS
import trino
import os

app = Flask(__name__)
CORS(app)

# Trino connection config
TRINO_HOST = os.getenv('TRINO_HOST', 'trino')
TRINO_PORT = int(os.getenv('TRINO_PORT', 8080))
TRINO_USER = os.getenv('TRINO_USER', 'user')
TRINO_CATALOG = os.getenv('TRINO_CATALOG', 'delta')
TRINO_SCHEMA = os.getenv('TRINO_SCHEMA', 'default_marts')

def get_trino_connection():
    """Get Trino connection"""
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA
    )

def execute_query(sql):
    """Execute SQL query on Trino"""
    conn = get_trino_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Convert to list of dicts
        result = []
        for row in rows:
            result.append(dict(zip(columns, row)))
        
        return result
    finally:
        cursor.close()
        conn.close()

@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    try:
        # Test Trino connection
        conn = get_trino_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "service": "MetricFlow API",
            "trino": f"{TRINO_HOST}:{TRINO_PORT}",
            "catalog": TRINO_CATALOG,
            "schema": TRINO_SCHEMA
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500

@app.route('/api/v1/metrics', methods=['GET'])
def list_metrics():
    """
    List all available metrics
    Returns predefined metric definitions
    """
    metrics = [
        {
            "name": "company_overall_score",
            "description": "Company's overall ESG score",
            "type": "simple",
            "measure": "AVG(overall_score)",
            "table": "fact_esg_score_risk"
        },
        {
            "name": "company_esg_pulse",
            "description": "Company's ESG pulse (Industrials source)",
            "type": "simple",
            "measure": "AVG(esg_pulse)",
            "table": "fact_esg_score_risk",
            "filter": "source = 'industrials'"
        },
        {
            "name": "company_risk_score",
            "description": "Company's total ESG risk",
            "type": "simple",
            "measure": "AVG(total_esg_risk_score)",
            "table": "fact_esg_score_risk",
            "filter": "source = 'sp500_risk'"
        },
        {
            "name": "industry_avg_score",
            "description": "Industry average ESG score",
            "type": "simple",
            "measure": "AVG(overall_score)",
            "table": "fact_esg_score_risk"
        },
        {
            "name": "high_risk_companies",
            "description": "Count of high risk companies",
            "type": "simple",
            "measure": "COUNT(DISTINCT company_id)",
            "table": "fact_esg_score_risk",
            "filter": "esg_risk_level = 'high'"
        }
    ]
    
    return jsonify({
        "metrics": metrics,
        "count": len(metrics)
    }), 200

@app.route('/api/v1/dimensions', methods=['GET'])
def list_dimensions():
    """List available dimensions"""
    dimensions = [
        {
            "name": "company__company_name",
            "type": "categorical",
            "table": "dim_company",
            "column": "company_name"
        },
        {
            "name": "company__sector",
            "type": "categorical",
            "table": "dim_company",
            "column": "sector_normalized"
        },
        {
            "name": "company__industry",
            "type": "categorical",
            "table": "dim_company",
            "column": "industry_normalized"
        },
        {
            "name": "company__country",
            "type": "categorical",
            "table": "dim_company",
            "column": "country_normalized"
        },
        {
            "name": "esg_scores__year",
            "type": "time",
            "table": "fact_esg_score_risk",
            "column": "year"
        },
        {
            "name": "esg_scores__source",
            "type": "categorical",
            "table": "fact_esg_score_risk",
            "column": "source"
        },
        {
            "name": "esg_scores__total_grade",
            "type": "categorical",
            "table": "fact_esg_score_risk",
            "column": "total_grade"
        },
        {
            "name": "esg_scores__esg_risk_level",
            "type": "categorical",
            "table": "fact_esg_score_risk",
            "column": "esg_risk_level"
        }
    ]
    
    return jsonify({
        "dimensions": dimensions,
        "count": len(dimensions)
    }), 200

@app.route('/api/v1/query', methods=['POST'])
def query_metrics():
    """
    Execute metric query
    
    Request body:
    {
        "metrics": ["company_overall_score"],
        "group_by": ["company__company_name", "company__sector"],
        "where": ["company__sector = 'Technology'"],
        "order_by": ["-company_overall_score"],
        "limit": 100
    }
    """
    try:
        data = request.get_json()
        
        metrics = data.get('metrics', [])
        group_by = data.get('group_by', [])
        where = data.get('where', [])
        order_by = data.get('order_by', [])
        limit = data.get('limit', 100)
        
        if not metrics:
            return jsonify({"error": "No metrics specified"}), 400
        
        # Build SQL query
        sql = build_sql_query(metrics, group_by, where, order_by, limit)
        
        # Execute query
        result = execute_query(sql)
        
        return jsonify({
            "data": result,
            "count": len(result),
            "sql": sql,
            "query": {
                "metrics": metrics,
                "group_by": group_by,
                "where": where,
                "limit": limit
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            "error": str(e),
            "type": type(e).__name__
        }), 500

def build_sql_query(metrics, group_by, where, order_by, limit):
    """Build SQL query from metric request"""
    
    # Metric definitions
    metric_defs = {
        "company_overall_score": {
            "sql": "AVG(s.overall_score) as company_overall_score",
            "table": "fact_esg_score_risk",
            "filter": None
        },
        "company_esg_pulse": {
            "sql": "AVG(s.esg_pulse) as company_esg_pulse",
            "table": "fact_esg_score_risk",
            "filter": "s.source = 'industrials'"
        },
        "company_risk_score": {
            "sql": "AVG(s.total_esg_risk_score) as company_risk_score",
            "table": "fact_esg_score_risk",
            "filter": "s.source = 'sp500_risk'"
        },
        "industry_avg_score": {
            "sql": "AVG(s.overall_score) as industry_avg_score",
            "table": "fact_esg_score_risk",
            "filter": None
        },
        "high_risk_companies": {
            "sql": "COUNT(DISTINCT s.company_id) as high_risk_companies",
            "table": "fact_esg_score_risk",
            "filter": "s.esg_risk_level = 'high'"
        }
    }
    
    # Dimension mapping
    dim_mapping = {
        "company__company_name": "c.company_name",
        "company__sector": "c.sector_normalized",
        "company__industry": "c.industry_normalized",
        "company__country": "c.country_normalized",
        "esg_scores__year": "s.year",
        "esg_scores__source": "s.source",
        "esg_scores__total_grade": "s.total_grade",
        "esg_scores__esg_risk_level": "s.esg_risk_level"
    }
    
    # Build SELECT clause
    select_parts = []
    for metric in metrics:
        if metric in metric_defs:
            select_parts.append(metric_defs[metric]["sql"])
    
    for dim in group_by:
        if dim in dim_mapping:
            select_parts.append(dim_mapping[dim])
    
    select_clause = ",\n    ".join(select_parts)
    
    # Build FROM clause (always use fact_esg_score_risk + dim_company)
    from_clause = """fact_esg_score_risk s
    JOIN dim_company c ON s.company_id = c.company_key"""
    
    # Build WHERE clause
    where_parts = []
    
    # Add metric filters
    for metric in metrics:
        if metric in metric_defs and metric_defs[metric]["filter"]:
            where_parts.append(metric_defs[metric]["filter"])
    
    # Add user filters
    for w in where:
        # Replace dimension names with column names
        sql_filter = w
        for dim, col in dim_mapping.items():
            sql_filter = sql_filter.replace(dim, col)
        where_parts.append(sql_filter)
    
    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)
    
    # Build GROUP BY clause
    group_clause = ""
    if group_by:
        group_cols = [dim_mapping.get(dim, dim) for dim in group_by]
        group_clause = "GROUP BY " + ", ".join(group_cols)
    
    # Build ORDER BY clause
    order_clause = ""
    if order_by:
        order_parts = []
        for o in order_by:
            if o.startswith('-'):
                # Descending
                col = o[1:]
                if col in dim_mapping:
                    order_parts.append(f"{dim_mapping[col]} DESC")
                else:
                    order_parts.append(f"{col} DESC")
            else:
                # Ascending
                if o in dim_mapping:
                    order_parts.append(f"{dim_mapping[o]} ASC")
                else:
                    order_parts.append(f"{o} ASC")
        order_clause = "ORDER BY " + ", ".join(order_parts)
    
    # Build complete SQL
    sql = f"""
SELECT
    {select_clause}
FROM {from_clause}
{where_clause}
{group_clause}
{order_clause}
LIMIT {limit}
    """.strip()
    
    return sql

@app.route('/api/v1/explain', methods=['POST'])
def explain_query():
    """Show SQL that would be generated"""
    try:
        data = request.get_json()
        
        metrics = data.get('metrics', [])
        group_by = data.get('group_by', [])
        where = data.get('where', [])
        order_by = data.get('order_by', [])
        limit = data.get('limit', 100)
        
        sql = build_sql_query(metrics, group_by, where, order_by, limit)
        
        return jsonify({
            "sql": sql,
            "query": {
                "metrics": metrics,
                "group_by": group_by,
                "where": where,
                "limit": limit
            }
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)