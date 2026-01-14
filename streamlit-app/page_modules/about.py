import streamlit as st

def show():
    st.markdown("# About ESG Lakehouse")
    
    st.markdown("""
    ## 🌱 Our Mission
    
    **Making sustainable investing transparent and accessible for everyone.**
    
    The ESG Lakehouse is a comprehensive data platform that aggregates, standardizes, and analyzes 
    Environmental, Social, and Governance (ESG) metrics from multiple authoritative sources to provide 
    investors, analysts, and stakeholders with actionable insights into corporate sustainability performance.
    
    ---
    
    ## 🎯 What We Do
    
    ### Data Integration
    We consolidate ESG data from leading providers including:
    - **S&P 500 ESG Risk Ratings** - Comprehensive risk assessment
    - **World Benchmarking Alliance (WBA)** - Global sustainability rankings
    - **Industrials Sector Data** - Sector-specific ESG pulse metrics
    - **Public ESG Disclosures** - Company-reported sustainability data
    
    ### Advanced Analytics
    Our platform provides:
    - **Derived Metrics** - YoY comparisons, industry benchmarks, decarbonization progress
    - **Multi-dimensional Scoring** - Separate E, S, and G component scores
    - **Trend Analysis** - Historical performance tracking
    - **Peer Benchmarking** - Industry and sector comparisons
    
    ### Data Architecture
    Built on modern data lakehouse principles:
    - **Bronze Layer** - Raw data ingestion from multiple sources
    - **Silver Layer** - Cleaned, normalized, and standardized data
    - **Gold Layer** - Business-ready fact and dimension tables
    - **Semantic Layer** - Metric definitions for consistent reporting
    
    ---
    
    ## 📊 Data Coverage
    
    - **400+ ESG Metrics** across Environmental, Social, and Governance categories
    - **100+ Companies** from S&P 500 and global indices
    - **10+ Years** of historical data
    - **Multiple Sources** validated and cross-referenced
    
    ---
    
    ## 🔬 Technology Stack
    
    - **Data Lake**: Delta Lake for ACID transactions
    - **Query Engine**: Trino for high-performance analytics
    - **Transformation**: dbt for data modeling and testing
    - **Processing**: Apache Spark for large-scale ETL
    - **Visualization**: Streamlit for interactive dashboards
    - **API**: RESTful endpoints for Power BI and other tools
    
    ---
    
    ## 👥 Team
    - Students:
        - Ms. Nguyen Thi Hong Tho (Nguyễn Thị Hồng Thơ) - Data Engineer Student at Ho Chi Minh City University of Technology and Engineering (HCM-UTE)
        - Mr. Doan Quang Lam (Đoàn Quang Lâm) - Data Engineer Student at Ho Chi Minh City University of Technology and Engineering (HCM-UTE)
    - Advisor: MSc. Tran Trong Binh (Trần Trọng Bình) - Lecturer at Ho Chi Minh City University of Technology and Engineering (HCM-UTE)
    - Support: Data4ESGenius Team from SCG Scholarship - Sharing the Dream 2024
    ---
    """)
    
    st.markdown("---")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        ### 🌍 Environmental
        - Carbon Emissions
        - Energy Consumption
        - Water Usage
        - Waste Management
        - Biodiversity Impact
        - Etc.
        """)
    
    with col2:
        st.markdown("""
        ### 👥 Social
        - Labor Practices
        - Employee Diversity
        - Health and Safety
        - Community Impact
        - Human Rights
        - Etc.
        """)
    
    with col3:
        st.markdown("""
        ### ⚖️ Governance
        - Board Structure
        - Executive Compensation
        - Business Ethics
        - Transparency
        - Shareholder Rights
        - Etc.
        """)