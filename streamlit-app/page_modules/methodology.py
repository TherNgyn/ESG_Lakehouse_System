import streamlit as st

def show():
    st.markdown("# Methodology")
    
    st.markdown("""
    ## 📚 ESG Scoring Framework
    
    Our comprehensive approach to ESG evaluation combines multiple authoritative sources 
    and applies rigorous normalization to ensure comparability across companies and industries.
    
    ---
    
    ## 🎯 Data Sources
    
    ### Primary Sources
    
    1. **S&P 500 ESG Risk Ratings**
       - Total ESG Risk Score (0-100)
       - Risk Level Classification (Low/Medium/High)
       - Controversy Scores
       - Source: Sustainalytics/S&P Global: https://www.spglobal.com/sustainable1/en/solutions/esg-scores-data
    
    2. **S&P 500 ESG Scores**
        - Overall ESG Score (0-100)
        - Environmental, Social, Governance Component Scores
        - Source: S&P Global ESG Scores: https://www.spglobal.com/sustainable1/en/solutions/esg-scores-data
    
    3. **World Benchmarking Alliance (WBA)**
       - Global Rankings
       - Industry and Sector Benchmarks
       - Governance, Environmental, and Social Rankings
       - Source: WBA Annual Benchmark Reports: https://www.worldbenchmarkingalliance.org/
    
    3. **Industrials Sector Database**
       - ESG Pulse Scores: -1 (negative) to +1 (positive)
       - It compares a company's ESG risk against its industry peers.
       - Update frequency: Quarterly
       - Source: https://www.kaggle.com/datasets/jenniferaduffy/industrial-sector-esg-ratings-and-stock-market-data 

    4. **Overall ESG Ratings**
        - The environmental, social, governance and total scores are numeric values, while the corresponding grades are letter ratings (like AAA, BB etc.) and levels are categorical (like High, Medium, Low).
        - Source: https://www.kaggle.com/datasets/alistairking/public-company-esg-ratings-dataset
                
    5. **Company ESG Disclosures**
       - Sustainability Reports (PDF) from company websites: Air Liquide, Dow, Goldman Sachs, etc.
       - Excel-based ESG Data Submissions from Bradesco, Cheniere, Lukoil, Nestle, etc.
       - Source: Direct company disclosures and sustainability portals.
    ---
    """)

    st.markdown("## 📈 Derived Metrics")
    
    st.markdown("""
    We calculate three key derived metrics that provide relative performance insights:
    """)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("#### 🌍 Decarbonization")
    with col2:
        st.markdown("#### 👥 Talent Stability")
    with col3:
        st.markdown("#### ⚖️ Board Independence")
    
    st.markdown("---")
    
    st.markdown("### 1️⃣ Decarbonization Gap (Environmental)")
    
    st.latex(r'''
    \text{Emissions YoY\%} = \frac{\text{Current Year} - \text{Previous Year}}{\text{Previous Year}} \times 100
    ''')
    
    st.latex(r'''
    \text{Decarbonization Gap} = \text{Company YoY\%} - \text{Industry Avg YoY\%}
    ''')
    
    col1, col2 = st.columns([3, 2])
    with col1:
        st.markdown("""
        **Example**:
        - Company emissions: -10% YoY (reduced by 10%)
        - Industry average: -5% YoY (reduced by 5%)
        - **Gap: -10% - (-5%) = -5%** → 🟢 Leading
        """)
    with col2:
        st.markdown("""
        **Interpretation**:
        - **Negative** = Faster reduction ✅
        - **Positive** = Lagging behind ⚠️
        """)
    
    st.markdown("---")
    
    st.markdown("### 2️⃣ Talent Stability Gap (Social)")
    
    st.latex(r'''
    \text{Talent Gap} = \text{Company Turnover\%} - \text{Industry Avg Turnover\%}
    ''')
    
    col1, col2 = st.columns([3, 2])
    with col1:
        st.markdown("""
        **Example**:
        - Company turnover: 8%
        - Industry average: 12%
        - **Gap: 8% - 12% = -4%** → 🟢 Better Retention
        """)
    with col2:
        st.markdown("""
        **Interpretation**:
        - **Negative** = Better retention ✅
        - **Positive** = Higher attrition ⚠️
        """)
    
    st.markdown("---")
    
    st.markdown("### 3️⃣ Board Independence YoY (Governance)")
    
    st.latex(r'''
    \text{Board YoY Change} = \text{Current \% Independent} - \text{Previous \% Independent}
    ''')
    
    st.info("⚠️ **Note**: Measures absolute percentage point change (not industry comparison due to limited data)")
    
    col1, col2 = st.columns([3, 2])
    with col1:
        st.markdown("""
        **Example**:
        - Previous year: 60% independent directors
        - Current year: 65% independent directors
        - **Change: 65% - 60% = +5%** → 🟢 Improving
        """)
    with col2:
        st.markdown("""
        **Interpretation**:
        - **Positive** = Improving ✅
        - **Negative** = Declining ⚠️
        """)
    
    st.markdown("""
    ---
    
    ## 🔄 Data Update Frequency
    
    - **ESG Scores**: Annual (with quarterly updates for key metrics)
    - **ESG Pulse**: Quarterly
    - **Risk Ratings**: Annual
    - **WBA Rankings**: Annual
    - **Derived Metrics**: Calculated on-demand from latest data
    
    ---
    
    ## ✅ Data Quality Assurance
    
    1. **Completeness Checks**: Minimum 70% metric coverage required
    2. **Consistency Validation**: Year-over-year change thresholds
    3. **Outlier Detection**: Statistical anomaly flagging
    4. **Cross-Source Verification**: Multi-source agreement required
    5. **Audit Trail**: All transformations logged and versioned
    
    ---
    
    ## 📖 Standards Alignment
    
    Our methodology aligns with:
    - **GRI** (Global Reporting Initiative)
    - **SASB** (Sustainability Accounting Standards Board)
    - **TCFD** (Task Force on Climate-related Financial Disclosures)
    - **UN SDGs** (Sustainable Development Goals)
    - **ISO 26000** (Social Responsibility)
    
    ---
    For questions about our methodology, please visit the [Contact](/contact) page.
    """)