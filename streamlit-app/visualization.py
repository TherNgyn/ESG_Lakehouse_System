import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

def create_gauge_chart(value, benchmark=None, title="Score"):
    if pd.isna(value):
        value = 0
    
    if value >= 70:
        color = "green"
    elif value >= 50:
        color = "orange"
    else:
        color = "red"
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title},
        delta={'reference': benchmark} if benchmark else None,
        gauge={
            'axis': {'range': [None, 100]},
            'bar': {'color': color},
            'steps': [
                {'range': [0, 50], 'color': "lightgray"},
                {'range': [50, 70], 'color': "gray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 90
            }
        }
    ))
    
    fig.update_layout(
        height=250,
        margin=dict(l=20, r=20, t=50, b=20)
    )
    
    return fig

def create_trend_chart(trends_df):
    fig = go.Figure()
    
    if 'overall_score' in trends_df.columns and not trends_df['overall_score'].isna().all():
        fig.add_trace(go.Scatter(
            x=trends_df['year'],
            y=trends_df['overall_score'],
            mode='lines+markers',
            name='Overall Score',
            line=dict(color='blue', width=2),
            marker=dict(size=8)
        ))
    
    if 'esg_pulse' in trends_df.columns and not trends_df['esg_pulse'].isna().all():
        fig.add_trace(go.Scatter(
            x=trends_df['year'],
            y=trends_df['esg_pulse'],
            mode='lines+markers',
            name='ESG Pulse',
            line=dict(color='green', width=2),
            marker=dict(size=8)
        ))
    
    if 'risk_score' in trends_df.columns and not trends_df['risk_score'].isna().all():
        fig.add_trace(go.Scatter(
            x=trends_df['year'],
            y=trends_df['risk_score'],
            mode='lines+markers',
            name='Risk Score',
            line=dict(color='red', width=2),
            marker=dict(size=8),
            yaxis='y2'
        ))
    
    fig.update_layout(
        title='ESG Scores Over Time',
        xaxis_title='Year',
        yaxis_title='Score (0-100)',
        yaxis2=dict(
            title='Risk Score',
            overlaying='y',
            side='right'
        ),
        hovermode='x unified',
        height=400
    )
    
    return fig

def create_benchmark_chart(company_value, industry_avg, sector_avg, metric_name):
    fig = go.Figure()
    
    categories = ['Company', 'Industry Avg', 'Sector Avg']
    values = [company_value, industry_avg, sector_avg]
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
    
    fig.add_trace(go.Bar(
        x=categories,
        y=values,
        marker_color=colors,
        text=[f'{v:.1f}' for v in values],
        textposition='auto'
    ))
    
    fig.update_layout(
        title=f'{metric_name} Benchmark',
        yaxis_title='Score',
        height=300,
        showlegend=False
    )
    
    return fig

def create_ranking_trend_chart(wba_data):
    """WBA Ranking trend over time"""
    fig = go.Figure()
    
    trend_data = wba_data[['year_benchmarked', 'global_rank', 'industry_rank', 'sector_rank']].sort_values('year_benchmarked')
    
    fig.add_trace(go.Scatter(
        x=trend_data['year_benchmarked'],
        y=trend_data['global_rank'],
        mode='lines+markers',
        name='Global Rank',
        line=dict(color='#1f77b4', width=2),
        marker=dict(size=8)
    ))
    
    fig.add_trace(go.Scatter(
        x=trend_data['year_benchmarked'],
        y=trend_data['industry_rank'],
        mode='lines+markers',
        name='Industry Rank',
        line=dict(color='#ff7f0e', width=2),
        marker=dict(size=8)
    ))
    
    fig.add_trace(go.Scatter(
        x=trend_data['year_benchmarked'],
        y=trend_data['sector_rank'],
        mode='lines+markers',
        name='Sector Rank',
        line=dict(color='#2ca02c', width=2),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        title="Ranking Progress (Lower is Better)",
        xaxis_title="Year",
        yaxis_title="Rank",
        hovermode='x unified',
        height=400,
        template='plotly_white'
    )
    
    return fig

def create_emissions_trend_chart(derived_data):
    """Emissions YoY trend"""
    trend_data = derived_data[['year', 'emissions_yoy_pct']].sort_values('year').dropna(subset=['emissions_yoy_pct'])
    
    if trend_data.empty:
        return None
    
    fig = go.Figure()
    
    colors = ['red' if x > 0 else 'green' for x in trend_data['emissions_yoy_pct']]
    
    fig.add_trace(go.Bar(
        x=trend_data['year'],
        y=trend_data['emissions_yoy_pct'],
        name='YoY Change %',
        marker_color=colors,
        text=trend_data['emissions_yoy_pct'].apply(lambda x: f'{x:+.2f}%'),
        textposition='auto'
    ))
    
    fig.update_layout(
        title="Emissions YoY Change (Negative = Better)",
        xaxis_title="Year",
        yaxis_title="Change %",
        height=350,
        template='plotly_white',
        showlegend=False
    )
    
    return fig

def create_multi_metric_comparison(derived_data):
    """Compare multiple metrics across years"""
    trend_data = derived_data[['year', 'company_turnover_rate', 'company_board_independence']].sort_values('year').dropna()
    
    if trend_data.empty:
        return None
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=trend_data['year'],
        y=trend_data['company_turnover_rate'],
        mode='lines+markers',
        name='Turnover Rate %',
        line=dict(color='#d62728', width=2),
        marker=dict(size=8)
    ))
    
    fig.add_trace(go.Scatter(
        x=trend_data['year'],
        y=trend_data['company_board_independence'],
        mode='lines+markers',
        name='Board Independence %',
        line=dict(color='#2ca02c', width=2),
        marker=dict(size=8),
        yaxis='y2'
    ))
    
    fig.update_layout(
        title="Social & Governance Metrics Trend",
        xaxis_title="Year",
        yaxis_title="Turnover Rate %",
        yaxis2=dict(
            title='Board Independence %',
            overlaying='y',
            side='right'
        ),
        hovermode='x unified',
        height=400,
        template='plotly_white'
    )
    
    return fig

def create_metric_distribution(metrics_df, metric_group):
    group_data = metrics_df[metrics_df['metric_group'] == metric_group]
    
    fig = px.bar(
        group_data,
        x='metric_name',
        y='value',
        title=f'{metric_group} Metrics',
        labels={'value': 'Normalized Value', 'metric_name': 'Metric'}
    )
    
    fig.update_layout(
        xaxis_tickangle=-45,
        height=400,
        template='plotly_white'
    )
    
    return fig

def create_risk_heatmap(risk_data):
    fig = px.density_heatmap(
        risk_data,
        x='year',
        y='esg_risk_level',
        z='count',
        title='Risk Level Distribution Over Time',
        color_continuous_scale='Reds'
    )
    
    fig.update_layout(height=300)
    
    return fig

def create_score_comparison_card(label, value, delta=None, delta_color='normal'):
    """Helper for creating metric cards"""
    return {
        'label': label,
        'value': value,
        'delta': delta,
        'delta_color': delta_color
    }