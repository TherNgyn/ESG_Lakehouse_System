import streamlit as st
import pandas as pd
import plotly.express as px
from ui_components import render_score_circle, render_metric_card, render_company_map
from visualization import (
    create_trend_chart,
    create_ranking_trend_chart,
    create_emissions_trend_chart,
    create_multi_metric_comparison
)

def show(loader, company_key):
    if st.button("← Back to Index", key="back_to_index"):
        st.session_state.pop('selected_company', None)
        st.rerun()
    
    company_info = loader.get_company_info(company_key)
    
    if company_info.empty:
        st.error("Company not found")
        return
    
    info = company_info.iloc[0]
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        if 'logo_url' in info and pd.notna(info['logo_url']):
            st.image(info['logo_url'], width=120)
    
    with col2:
        st.markdown(f"# {info['company_name']}")
        st.markdown(f"**Symbol:** {info['symbol']}")
        st.markdown(f"**{info['sector']}** | **{info['industry']}**")
        st.markdown(f"📍 {info['country']} | {info['city']}")
    
    st.markdown("---")
    
    scores = loader.get_company_scores(company_key)
    wba = loader.get_wba_rankings(company_key)
    derived = loader.get_derived_metrics(company_key)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if not scores.empty:
            latest = scores.sort_values('year', ascending=False).iloc[0]
            score = latest['overall_score'] if pd.notna(latest['overall_score']) else 0
            year = latest['year'] if pd.notna(latest['year']) else 'N/A'
            render_metric_card("🎯", f"{score:.0f}", "ESG Score", f"Year {year}")
        else:
            render_metric_card("🎯", "N/A", "ESG Score")
    
    with col2:
        if not scores.empty:
            pulse_data = scores[scores['esg_pulse'].notna()]
            if not pulse_data.empty:
                pulse = pulse_data.iloc[0]['esg_pulse']
                render_metric_card("⚡", f"{pulse:.1f}", "ESG Pulse")
            else:
                render_metric_card("⚡", "N/A", "ESG Pulse")
        else:
            render_metric_card("⚡", "N/A", "ESG Pulse")
    
    with col3:
        if not scores.empty:
            risk_data = scores[scores['total_esg_risk_score'].notna()]
            if not risk_data.empty:
                risk = risk_data.iloc[0]['total_esg_risk_score']
                level = risk_data.iloc[0]['esg_risk_level'] if 'esg_risk_level' in risk_data.iloc[0] else 'Unknown'
                render_metric_card("⚠️", f"{risk:.1f}", "Risk Score", f"{level.capitalize()}")
            else:
                render_metric_card("⚠️", "N/A", "Risk Score")
        else:
            render_metric_card("⚠️", "N/A", "Risk Score")
    
    with col4:
        if not wba.empty:
            latest_wba = wba.sort_values('year_benchmarked', ascending=False).iloc[0]
            rank = latest_wba['global_rank']
            render_metric_card("🏆", f"#{int(rank)}", "Global Rank", f"WBA {latest_wba['year_benchmarked']}")
        else:
            render_metric_card("🏆", "N/A", "Global Rank")
    
    with col5:
        if not derived.empty:
            latest_derived = derived.sort_values('year', ascending=False).iloc[0]
            status = str(latest_derived['decarbonization_status']).split()[0] if pd.notna(latest_derived['decarbonization_status']) else "N/A"
            render_metric_card("🌍", status, "Decarb Status")
        else:
            render_metric_card("🌍", "N/A", "Decarb Status")
    
    st.markdown("---")
    
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Overview",
        "🌍 Environmental",
        "👥 Social",
        "⚖️ Governance",
        "🏆 WBA Benchmark",
        "📈 Derived Metrics"
    ])
    
    with tab1:
        show_overview_tab(loader, company_key, scores, info)
    
    with tab2:
        show_topic_tab(loader, company_key, "Environmental")
    
    with tab3:
        show_topic_tab(loader, company_key, "Social")
    
    with tab4:
        show_topic_tab(loader, company_key, "Governance")
    
    with tab5:
        show_wba_tab(loader, company_key, wba)
    
    with tab6:
        show_derived_tab(loader, company_key, derived)

def show_overview_tab(loader, company_key, scores, info):
    col1, col2, col3 = st.columns(3)
    
    if not scores.empty and 'year' in scores.columns:
        scores_sorted = scores.sort_values('year', ascending=False)
        latest = scores_sorted.iloc[0]
        
        e_score = latest.get('e_score', 0)
        s_score = latest.get('s_score', 0)
        g_score = latest.get('g_score', 0)
        overall = latest.get('overall_score', 0)
        
        with col1:
            render_score_circle(overall if pd.notna(overall) else 0, "Overall", "#93BD57")
        with col2:
            render_score_circle(e_score if pd.notna(e_score) else 0, "Environmental", "#7BA850")
        with col3:
            render_score_circle(s_score if pd.notna(s_score) else 0, "Social", "#5A9BD5")
    
    st.markdown("---")
    st.markdown("### 📈 Score Trends")
    
    trends = loader.get_score_trends(company_key)
    if not trends.empty:
        fig = create_trend_chart(trends)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No historical trend data available")
    
    st.markdown("---")
    st.markdown("### 📍 Company Location")
    render_company_map(info['country'], info['city'], info['company_name'])

def show_topic_tab(loader, company_key, topic):
    st.markdown(f"### {topic} Metrics")
    
    metrics_df = loader.get_company_metrics(company_key, topic)
    
    if metrics_df.empty:
        st.warning(f"No {topic} metrics available")
        return
    
    # Convert year column to integer to remove decimal places
    if 'year' in metrics_df.columns:
        metrics_df['year'] = metrics_df['year'].apply(lambda x: int(x) if pd.notna(x) else x)
    
    all_metrics = loader.get_all_metrics_by_topic(topic)
    
    merged = all_metrics.merge(
        metrics_df[['metric_name', 'year', 'value', 'unit']],
        on='metric_name',
        how='left'
    )

    merged['year'] = merged['year'].apply(lambda x: str(int(x)) if pd.notna(x) and x != 'N/A' else '-')
    merged['value'] = merged['value'].fillna('N/A')
    merged['unit'] = merged['unit'].fillna('')
    
    display_df = merged[['metric_name', 'metric_group', 'year', 'value', 'unit']].copy()
    display_df.columns = ['Metric', 'Group', 'Year', 'Value', 'Unit']
    
    st.dataframe(display_df, use_container_width=True, height=500)
    
    available_metrics = merged[merged['value'] != 'N/A']
    
    if not available_metrics.empty:
        st.markdown("### Metric Trend")
        
        selected_metric = st.selectbox(
            "Select metric to visualize",
            available_metrics['metric_name'].unique(),
            key=f"metric_select_{topic}"
        )
        
        metric_history = loader.get_metric_history(company_key, selected_metric)
        
        if not metric_history.empty:
            if 'year' in metric_history.columns:
                metric_history['year'] = metric_history['year'].apply(lambda x: int(x) if pd.notna(x) else x)
            
            fig = px.line(
                metric_history,
                x='year',
                y='value',
                title=f"{selected_metric} over time",
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)

def show_wba_tab(loader, company_key, wba):
    st.markdown("### 🏆 WBA Global Benchmark Rankings")
    
    if wba.empty:
        st.info("No WBA benchmark data available")
        return
    
    years = wba['year_benchmarked'].unique()
    selected_year = st.selectbox("Select Year", sorted(years, reverse=True), key="wba_year")
    
    year_data = wba[wba['year_benchmarked'] == selected_year].iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        rank = year_data['global_rank']
        render_metric_card("🌍", f"#{int(rank)}", "Global Rank")
    
    with col2:
        gov_rank = year_data['governance_strategy_rank']
        render_metric_card("⚖️", f"#{int(gov_rank)}" if pd.notna(gov_rank) else "N/A", "Governance")
    
    with col3:
        env_rank = year_data['ecosystems_biodiversity_rank']
        render_metric_card("🌱", f"#{int(env_rank)}" if pd.notna(env_rank) else "N/A", "Environment")
    
    with col4:
        soc_rank = year_data['social_community_rank']
        render_metric_card("👥", f"#{int(soc_rank)}" if pd.notna(soc_rank) else "N/A", "Social")
    

def show_derived_tab(loader, company_key, derived):
    st.markdown("### 📊 Derived ESG Metrics")
    
    if derived.empty:
        st.info("No derived metrics available")
        return
    
    years = [int(y) for y in derived['year'].unique() if pd.notna(y)]
    selected_year = st.selectbox("Select Year", sorted(years, reverse=True), key="derived_year")
    
    year_data = derived[derived['year'] == selected_year].iloc[0]
    
    st.markdown(f"#### 🌍 Environmental Metrics - Year {int(selected_year)}")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        emissions = year_data['scope_1_2_emissions']
        st.metric(
            label="Scope 1+2 Emissions",
            value=f"{emissions:.2f} tCO2e" if pd.notna(emissions) else "N/A"
        )
    
    with col2:
        yoy = year_data['emissions_yoy_pct']
        st.metric(
            label="YoY Change",
            value=f"{yoy:+.2f}%" if pd.notna(yoy) else "N/A",
            delta=f"{yoy:.2f}%" if pd.notna(yoy) else None,
            delta_color="inverse"
        )
    
    with col3:
        gap = year_data['decarbonization_gap']
        status_emoji = "✅" if pd.notna(gap) and gap < 0 else "⚠️" if pd.notna(gap) else ""
        st.metric(
            label=f"Decarb Gap {status_emoji}",
            value=f"{gap:+.2f}%" if pd.notna(gap) else "N/A"
        )
    
    st.markdown(f"#### 👥 Social Metrics - Year {int(selected_year)}")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        turnover = year_data['company_turnover_rate']
        st.metric(
            label="Company Turnover Rate",
            value=f"{turnover:.2f}%" if pd.notna(turnover) else "N/A"
        )
    
    with col2:
        ind_turnover = year_data['industry_avg_turnover']
        st.metric(
            label="Industry Avg Turnover",
            value=f"{ind_turnover:.2f}%" if pd.notna(ind_turnover) else "N/A"
        )
    
    with col3:
        talent_gap = year_data['talent_stability_gap']
        status_emoji = "✅" if pd.notna(talent_gap) and talent_gap < 0 else "⚠️" if pd.notna(talent_gap) else ""
        st.metric(
            label=f"Talent Gap {status_emoji}",
            value=f"{talent_gap:+.2f}%" if pd.notna(talent_gap) else "N/A"
        )
    
    st.markdown(f"#### ⚖️ Governance Metrics - Year {int(selected_year)}")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        board_ind = year_data['company_board_independence']
        st.metric(
            label="Board Independence",
            value=f"{board_ind:.2f}%" if pd.notna(board_ind) else "N/A"
        )
    
    with col2:
        board_yoy = year_data['board_independence_yoy']
        st.metric(
            label="YoY Change",
            value=f"{board_yoy:+.2f}%" if pd.notna(board_yoy) else "N/A",
            delta=f"{board_yoy:.2f}%" if pd.notna(board_yoy) else None
        )
    
    with col3:
        status = year_data['governance_status']
        st.metric(
            label="Governance Status",
            value=str(status) if pd.notna(status) else "N/A"
        )
    
    if len(derived) > 1:
        st.markdown("---")
        st.markdown("### 📈 Historical Trends")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = create_emissions_trend_chart(derived)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = create_multi_metric_comparison(derived)
            if fig:
                st.plotly_chart(fig, use_container_width=True)