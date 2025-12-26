import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
sys.path.append('.')
from data_loader import DataLoader
from visualization import create_gauge_chart, create_trend_chart, create_benchmark_chart

st.set_page_config(
    page_title="ESG Analytics Dashboard",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
.metric-card {
    background-color: #f0f2f6;
    padding: 20px;
    border-radius: 10px;
    margin: 10px 0;
}
.score-good { color: #28a745; }
.score-medium { color: #ffc107; }
.score-bad { color: #dc3545; }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_data_loader():
    return DataLoader()

def main():
    loader = get_data_loader()
    
    st.sidebar.title("🌱 ESG Analytics")
    
    # Auto switch to detail page if company selected
    if st.session_state.get('selected_company'):
        page = "🔍 Company Detail"
    else:
        page = st.sidebar.radio("Navigate", ["🏢 Market Explorer", "🔍 Company Detail"])
    
    if page == "🏢 Market Explorer":
        show_market_explorer(loader)
    else:
        show_company_detail(loader)

def show_market_explorer(loader):
    st.title("ESG Market Explorer")
    st.markdown("### Discover and compare ESG performance across companies")
    
    # Initialize session state for filters
    if 'filter_sector' not in st.session_state:
        st.session_state.filter_sector = "All"
    if 'filter_industry' not in st.session_state:
        st.session_state.filter_industry = "All"
    if 'filter_country' not in st.session_state:
        st.session_state.filter_country = "All"
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        sectors = loader.get_sectors()
        selected_sector = st.selectbox(
            "Sector", 
            ["All"] + sectors,
            index=(["All"] + sectors).index(st.session_state.filter_sector) 
                if st.session_state.filter_sector in ["All"] + sectors else 0,
            key="sector_select"
        )
        st.session_state.filter_sector = selected_sector
    
    with col2:
        industries = loader.get_industries(selected_sector if selected_sector != "All" else None)
        # Reset industry if sector changed
        if selected_sector != st.session_state.get('prev_sector'):
            st.session_state.filter_industry = "All"
            st.session_state.prev_sector = selected_sector
        
        selected_industry = st.selectbox(
            "Industry", 
            ["All"] + industries,
            index=(["All"] + industries).index(st.session_state.filter_industry) 
                if st.session_state.filter_industry in ["All"] + industries else 0,
            key="industry_select"
        )
        st.session_state.filter_industry = selected_industry
    
    with col3:
        countries = loader.get_countries()
        selected_country = st.selectbox(
            "Country", 
            ["All"] + countries,
            index=(["All"] + countries).index(st.session_state.filter_country) 
                if st.session_state.filter_country in ["All"] + countries else 0,
            key="country_select"
        )
        st.session_state.filter_country = selected_country
    
    st.markdown("---")
    
    companies_df = loader.get_companies(
        sector=selected_sector if selected_sector != "All" else None,
        industry=selected_industry if selected_industry != "All" else None,
        country=selected_country if selected_country != "All" else None
    )
    
    if companies_df.empty:
        st.warning("No companies found with selected filters")
        return
    
    metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
    
    with metrics_col1:
        st.metric("Total Companies", len(companies_df))
    
    with metrics_col2:
        avg_score = companies_df['overall_score'].mean()
        st.metric("Avg ESG Score", f"{avg_score:.1f}")
    
    with metrics_col3:
        avg_pulse = companies_df['esg_pulse'].mean()
        st.metric("Avg ESG Pulse", f"{avg_pulse:.1f}" if pd.notna(avg_pulse) else "N/A")
    
    with metrics_col4:
        high_risk = len(companies_df[companies_df['esg_risk_level'] == 'high'])
        st.metric("High Risk Companies", high_risk)
    
    st.markdown("### Company Rankings")
    st.markdown("*Click on a company to view details*")
    
    # Prepare display dataframe
    display_df = companies_df[[
        'company_name', 'sector', 'industry', 'country',
        'overall_score', 'esg_pulse', 'total_grade', 'esg_risk_level'
    ]].copy()
    
    # Fill None/NaN values
    display_df['esg_pulse'] = display_df['esg_pulse'].fillna(0)
    display_df['total_grade'] = display_df['total_grade'].fillna('N/A')
    display_df['esg_risk_level'] = display_df['esg_risk_level'].fillna('Unknown')
    
    display_df.columns = [
        'Company', 'Sector', 'Industry', 'Country',
        'ESG Score', 'ESG Pulse', 'Grade', 'Risk Level'
    ]
    
    # Create styled dataframe
    styled_df = display_df.style.format({
        'ESG Score': '{:.1f}',
        'ESG Pulse': '{:.1f}'
    }, na_rep='N/A')
    
    # Add background gradient
    styled_df = styled_df.background_gradient(
        subset=['ESG Score'], 
        cmap='RdYlGn',
        vmin=0,
        vmax=100
    )
    
    # Display table with selection
    event = st.dataframe(
        styled_df,
        use_container_width=True,
        height=400,
        on_select="rerun",
        selection_mode="single-row"
    )
    
    # Handle row selection
    if event.selection.rows:
        selected_idx = event.selection.rows[0]
        selected_company = companies_df.iloc[selected_idx]['company_key']
        st.session_state['selected_company'] = selected_company
        st.rerun()
    
    # Alternative: Search box
    st.markdown("---")
    st.markdown("### Quick Search")
    
    search_query = st.text_input(
        "Search company by name",
        placeholder="Type company name..."
    )
    
    if search_query:
        filtered = companies_df[
            companies_df['company_name'].str.contains(search_query, case=False, na=False)
        ]
        
        if not filtered.empty:
            st.markdown(f"**Found {len(filtered)} companies:**")
            
            for idx, row in filtered.head(10).iterrows():
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.write(f"**{row['company_name']}**")
                    st.caption(f"{row['sector']} | {row['industry']}")
                
                with col2:
                    score = row['overall_score']
                    color = "🟢" if score >= 70 else "🟡" if score >= 50 else "🔴"
                    st.write(f"{color} Score: {score:.1f}")
                
                with col3:
                    if st.button("View", key=f"view_{row['company_key']}"):
                        st.session_state['selected_company'] = row['company_key']
                        st.rerun()

def show_company_detail(loader):
    st.title("Company ESG Detail")
    
    company_key = st.session_state.get('selected_company')
    
    if not company_key:
        st.info("Please select a company from Market Explorer")
        
        # Back button
        if st.button("← Go to Market Explorer"):
            st.session_state.pop('selected_company', None)
            st.rerun()
        
        return
    
    company_info = loader.get_company_info(company_key)
    
    if company_info.empty:
        st.error("Company not found")
        st.session_state.pop('selected_company', None)
        
        if st.button("← Back to Market Explorer"):
            st.rerun()
        
        return
    
    info = company_info.iloc[0]
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header(f"{info['company_name']}")
        st.markdown(f"**{info['sector']}** | **{info['industry']}** | **{info['country']}**")
    
    with col2:
        if st.button("← Back to Market Explorer"):
            st.session_state.pop('selected_company', None)
            st.rerun()
    
    st.markdown("---")
    
    scores = loader.get_company_scores(company_key)
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Overview", 
        "🌍 Environmental", 
        "👥 Social", 
        "⚖️ Governance"
    ])
    
    with tab1:
        show_overview_tab(loader, company_key, scores, info)
    
    with tab2:
        show_topic_tab(loader, company_key, "Environmental")
    
    with tab3:
        show_topic_tab(loader, company_key, "Social")
    
    with tab4:
        show_topic_tab(loader, company_key, "Governance")

def show_overview_tab(loader, company_key, scores, info):
    col1, col2, col3 = st.columns(3)
    
    if not scores.empty:
        overall_score = scores['overall_score'].iloc[0] if 'overall_score' in scores.columns else None
        esg_pulse = scores['esg_pulse'].iloc[0] if 'esg_pulse' in scores.columns else None
        risk_score = scores['total_esg_risk_score'].iloc[0] if 'total_esg_risk_score' in scores.columns else None
    else:
        overall_score = esg_pulse = risk_score = None
    
    industry_avg = loader.get_industry_avg(info['industry'])
    
    with col1:
        st.markdown("### Overall ESG Score")
        if overall_score and pd.notna(overall_score):
            fig = create_gauge_chart(overall_score, industry_avg, "ESG Score")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No score available")
    
    with col2:
        st.markdown("### ESG Pulse")
        if esg_pulse and pd.notna(esg_pulse):
            fig = create_gauge_chart(esg_pulse, None, "ESG Pulse")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No pulse data")
    
    with col3:
        st.markdown("### Risk Level")
        if risk_score and pd.notna(risk_score):
            st.metric("ESG Risk Score", f"{risk_score:.1f}")
            risk_level = scores['esg_risk_level'].iloc[0] if 'esg_risk_level' in scores.columns else 'Unknown'
            st.markdown(f"**Level:** {risk_level}")
        else:
            st.info("No risk data")
    
    st.markdown("### Historical Trends")
    trends = loader.get_score_trends(company_key)
    
    if not trends.empty:
        fig = create_trend_chart(trends)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No historical data available")

def show_topic_tab(loader, company_key, topic):
    st.markdown(f"### {topic} Metrics")
    
    metrics_df = loader.get_company_metrics(company_key, topic)
    
    if metrics_df.empty:
        st.warning(f"No {topic} metrics available for this company")
        return
    
    all_metrics = loader.get_all_metrics_by_topic(topic)
    
    # FIX: Chỉ lấy các cột cần thiết từ metrics_df để tránh trùng lặp
    merged = all_metrics.merge(
        metrics_df[['metric_name', 'year', 'value', 'unit']],  # Bỏ metric_group khỏi metrics_df
        on='metric_name', 
        how='left'
    )
    
    merged['year'] = merged['year'].fillna('-')
    merged['value'] = merged['value'].fillna('N/A')
    merged['unit'] = merged['unit'].fillna('')
    
    # Bây giờ metric_group sẽ được giữ nguyên từ all_metrics
    display_cols = ['metric_name', 'metric_group', 'year', 'value', 'unit']
    
    # Safety check: Kiểm tra cột có tồn tại không
    available_cols = [col for col in display_cols if col in merged.columns]
    
    if len(available_cols) != len(display_cols):
        st.error(f"Missing columns: {set(display_cols) - set(available_cols)}")
        st.write("Available columns:", merged.columns.tolist())
        return
    
    display_df = merged[display_cols].copy()
    display_df.columns = ['Metric', 'Group', 'Year', 'Value', 'Unit']
    
    st.dataframe(display_df, use_container_width=True, height=500)
    
    available_metrics = merged[merged['value'] != 'N/A']
    
    if not available_metrics.empty:
        st.markdown("### Metric Trends")
        
        selected_metric = st.selectbox(
            "Select metric to visualize",
            available_metrics['metric_name'].unique()
        )
        
        metric_history = loader.get_metric_history(company_key, selected_metric)
        
        if not metric_history.empty:
            fig = px.line(
                metric_history,
                x='year',
                y='value',
                title=f"{selected_metric} over time",
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()