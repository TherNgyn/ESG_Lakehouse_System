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
            line=dict(color='blue', width=2)
        ))
    
    if 'esg_pulse' in trends_df.columns and not trends_df['esg_pulse'].isna().all():
        fig.add_trace(go.Scatter(
            x=trends_df['year'],
            y=trends_df['esg_pulse'],
            mode='lines+markers',
            name='ESG Pulse',
            line=dict(color='green', width=2)
        ))
    
    if 'risk_score' in trends_df.columns and not trends_df['risk_score'].isna().all():
        fig.add_trace(go.Scatter(
            x=trends_df['year'],
            y=trends_df['risk_score'],
            mode='lines+markers',
            name='Risk Score',
            line=dict(color='red', width=2),
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
        height=400
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