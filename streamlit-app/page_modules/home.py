import streamlit as st
import pandas as pd
from ui_components import render_filters, render_company_card, render_metric_card

def show(loader):
    st.markdown("# ESG Lakehouse Index")
    st.markdown("### Interactive leaderboard ranking companies by sustainability and investment potential")
    
    selected_sector, selected_industry, selected_country = render_filters(loader)
    
    companies_df = loader.get_companies(
        sector=selected_sector if selected_sector != "All" else None,
        industry=selected_industry if selected_industry != "All" else None,
        country=selected_country if selected_country != "All" else None
    )
    
    if companies_df.empty:
        st.warning("No companies found with selected filters")
        return
    
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        render_metric_card(
            "🏢",
            f"{len(companies_df)}",
            "Total Companies"
        )
    
    with col2:
        avg_score = companies_df['overall_score'].mean()
        render_metric_card(
            "📊",
            f"{avg_score:.1f}",
            "Avg ESG Score"
        )
    
    with col3:
        avg_pulse = companies_df['esg_pulse'].mean()
        pulse_display = f"{avg_pulse:.1f}" if pd.notna(avg_pulse) else "N/A"
        render_metric_card(
            "⚡",
            pulse_display,
            "Avg ESG Pulse"
        )
    
    with col4:
        high_risk = len(companies_df[companies_df['esg_risk_level'] == 'high'])
        render_metric_card(
            "⚠️",
            f"{high_risk}",
            "High Risk Companies"
        )
    
    st.markdown("---")
    st.markdown("### 📋 Company Rankings")
    st.caption(f"Showing {len(companies_df)} companies | Sorted by overall score (highest first)")
    
    search_query = st.text_input(
        "🔍 Search by company name",
        placeholder="Type to filter results...",
        key="company_search"
    )
    
    if search_query:
        companies_df = companies_df[
            companies_df['company_name'].str.contains(search_query, case=False, na=False)
        ]
        st.caption(f"Found {len(companies_df)} matching companies")
    
    for idx, company in companies_df.iterrows():
        render_company_card(company.to_dict(), loader)
    
    if len(companies_df) == 0:
        st.info("No companies match your search criteria")