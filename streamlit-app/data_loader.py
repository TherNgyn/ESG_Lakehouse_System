import pandas as pd
from trino.dbapi import connect
import os

class DataLoader:
    def __init__(self):
        self.trino_host = os.getenv('TRINO_HOST', 'trino')
        self.trino_port = int(os.getenv('TRINO_PORT', 8080))
        self.trino_user = os.getenv('TRINO_USER', 'user')
        self.catalog = 'delta'
        self.schema = 'default_marts'
        
    def _get_connection(self):
        return connect(
            host=self.trino_host,
            port=self.trino_port,
            user=self.trino_user,
            catalog=self.catalog,
            schema=self.schema
        )
    
    def _query(self, sql):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            print(f"Query error: {e}")
            print(f"SQL: {sql}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def get_sectors(self):
        sql = """
        SELECT DISTINCT sector_normalized as sector
        FROM dim_company
        WHERE sector_normalized IS NOT NULL
        ORDER BY sector_normalized
        """
        df = self._query(sql)
        return df['sector'].tolist() if not df.empty else []
    
    def get_industries(self, sector=None):
        if sector:
            sql = f"""
            SELECT DISTINCT industry_normalized as industry
            FROM dim_company
            WHERE sector_normalized = '{sector}'
              AND industry_normalized IS NOT NULL
            ORDER BY industry_normalized
            """
        else:
            sql = """
            SELECT DISTINCT industry_normalized as industry
            FROM dim_company
            WHERE industry_normalized IS NOT NULL
            ORDER BY industry_normalized
            """
        df = self._query(sql)
        return df['industry'].tolist() if not df.empty else []
    
    def get_countries(self):
        sql = """
        SELECT DISTINCT country_normalized as country
        FROM dim_company
        WHERE country_normalized IS NOT NULL
        ORDER BY country_normalized
        """
        df = self._query(sql)
        return df['country'].tolist() if not df.empty else []
    
    def get_companies(self, sector=None, industry=None, country=None):
        filters = []
        if sector:
            filters.append(f"c.sector_normalized = '{sector}'")
        if industry:
            filters.append(f"c.industry_normalized = '{industry}'")
        if country:
            filters.append(f"c.country_normalized = '{country}'")
        
        where_clause = " AND " + " AND ".join(filters) if filters else ""
        
        sql = f"""
        WITH latest_scores AS (
            SELECT 
                company_id,
                MAX(year) as latest_year
            FROM fact_esg_score_risk
            WHERE year IS NOT NULL
            GROUP BY company_id
        ),
        company_scores AS (
            SELECT 
                s.company_id,
                MAX(CASE WHEN s.source = 'industrials' THEN s.esg_pulse END) as esg_pulse,
                MAX(CASE WHEN s.source = 'sp500_risk' THEN s.total_esg_risk_score END) as risk_score,
                MAX(CASE WHEN s.source = 'sp500_risk' THEN s.esg_risk_level END) as esg_risk_level,
                AVG(s.overall_score) as overall_score,
                MAX(s.total_grade) as total_grade
            FROM fact_esg_score_risk s
            JOIN latest_scores ls ON s.company_id = ls.company_id 
                AND (s.year = ls.latest_year OR s.year IS NULL)
            GROUP BY s.company_id
        )
        SELECT 
            c.company_key,
            c.company_name,
            c.symbol,
            c.sector_normalized as sector,
            c.industry_normalized as industry,
            c.country_normalized as country,
            COALESCE(cs.overall_score, 0) as overall_score,
            cs.esg_pulse,
            cs.risk_score,
            cs.total_grade,
            cs.esg_risk_level
        FROM dim_company c
        LEFT JOIN company_scores cs ON c.company_key = cs.company_id
        WHERE 1=1 {where_clause}
        ORDER BY cs.overall_score DESC NULLS LAST
        LIMIT 100
        """
        return self._query(sql)
    
    def get_company_info(self, company_key):
        sql = f"""
        SELECT 
            company_key,
            company_name,
            symbol,
            sector_normalized as sector,
            industry_normalized as industry,
            country_normalized as country,
            city,
            region
        FROM dim_company
        WHERE company_key = '{company_key}'
        """
        return self._query(sql)
    
    def get_company_scores(self, company_key):
        sql = f"""
        SELECT 
            source,
            year,
            overall_score,
            esg_pulse,
            total_esg_risk_score,
            esg_risk_level,
            total_level,
            total_grade
        FROM fact_esg_score_risk
        WHERE company_id = '{company_key}'
        ORDER BY year DESC NULLS LAST
        """
        return self._query(sql)
    
    def get_industry_avg(self, industry):
        sql = f"""
        SELECT AVG(s.overall_score) as avg_score
        FROM fact_esg_score_risk s
        JOIN dim_company c ON s.company_id = c.company_key
        WHERE c.industry_normalized = '{industry}'
          AND s.overall_score IS NOT NULL
        """
        df = self._query(sql)
        return df['avg_score'].iloc[0] if not df.empty and df['avg_score'].iloc[0] is not None else None
    
    def get_score_trends(self, company_key):
        sql = f"""
        SELECT 
            year,
            AVG(overall_score) as overall_score,
            MAX(CASE WHEN source = 'industrials' THEN esg_pulse END) as esg_pulse,
            MAX(CASE WHEN source = 'sp500_risk' THEN total_esg_risk_score END) as risk_score
        FROM fact_esg_score_risk
        WHERE company_id = '{company_key}'
          AND year IS NOT NULL
        GROUP BY year
        ORDER BY year
        """
        return self._query(sql)
    
    def get_company_metrics(self, company_key, topic):
        """
        FIXED: Get latest year metrics for a company by topic
        """
        sql = f"""
        WITH latest_year AS (
            SELECT MAX(f.year) as max_year
            FROM fact_esg_metric f
            JOIN dim_metric m ON f.metric_id = m.metric_key
            WHERE f.company_id = '{company_key}'
              AND LOWER(m.topic) = LOWER('{topic}')
              AND f.year IS NOT NULL
        )
        SELECT DISTINCT
            m.metric_name,
            m.metric_group,
            f.year,
            f.normalized_value as value,
            u.standard_unit as unit
        FROM fact_esg_metric f
        JOIN dim_metric m ON f.metric_id = m.metric_key
        LEFT JOIN dim_unit u ON f.unit_id = u.unit_key
        CROSS JOIN latest_year ly
        WHERE f.company_id = '{company_key}'
          AND LOWER(m.topic) = LOWER('{topic}')
          AND f.year = ly.max_year
        ORDER BY m.metric_group, m.metric_name
        """
        return self._query(sql)
    
    def get_all_metrics_by_topic(self, topic):
        """
        Get ALL possible metrics for a topic (for showing full list with N/A)
        """
        sql = f"""
        SELECT DISTINCT
            metric_key,
            metric_name,
            metric_group
        FROM dim_metric
        WHERE LOWER(topic) = LOWER('{topic}')
        ORDER BY metric_group, metric_name
        """
        return self._query(sql)
    
    def get_metric_history(self, company_key, metric_name):
        """
        FIXED: Get metric history across all years
        """
        sql = f"""
        SELECT DISTINCT
            f.year,
            f.normalized_value as value,
            u.standard_unit as unit
        FROM fact_esg_metric f
        JOIN dim_metric m ON f.metric_id = m.metric_key
        LEFT JOIN dim_unit u ON f.unit_id = u.unit_key
        WHERE f.company_id = '{company_key}'
          AND m.metric_name = '{metric_name}'
          AND f.year IS NOT NULL
        ORDER BY f.year
        """
        return self._query(sql)