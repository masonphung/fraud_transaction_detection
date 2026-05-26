import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from config import PATTERN_COLORS

# ---------------------------------------------------------------------------
# Pattern donut
# ---------------------------------------------------------------------------

def pattern_donut(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()
    colors = [PATTERN_COLORS.get(p, '#6B7280') for p in df['fraud_pattern']]
    fig = go.Figure(go.Pie(
        labels=df['fraud_pattern'],
        values=df['count'],
        hole=0.55,
        marker_colors=colors,
        textinfo='label+percent',
        hovertemplate='%{label}<br>Count: %{value}<extra></extra>',
    ))
    fig.update_layout(
        showlegend=True,
        margin=dict(t=30, b=10, l=10, r=10),
        height=320,
    )
    return fig


# ---------------------------------------------------------------------------
# Pattern time series
# ---------------------------------------------------------------------------

def pattern_timeseries(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()
    fig = go.Figure()
    for pattern, grp in df.groupby('fraud_pattern'):
        fig.add_trace(go.Scatter(
            x=grp['hour'], y=grp['count'],
            mode='lines+markers',
            name=pattern,
            line=dict(color=PATTERN_COLORS.get(pattern, '#6B7280'), width=2),
        ))
    fig.update_layout(
        xaxis_title='Time', yaxis_title='Fraud Events',
        legend_title='Pattern',
        margin=dict(t=30, b=10, l=10, r=10),
        height=320,
        hovermode='x unified',
    )
    return fig


# ---------------------------------------------------------------------------
# Metrics trend (model health)
# ---------------------------------------------------------------------------

def metrics_trend(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()
    metric_cols = ['auc_pr', 'precision', 'recall', 'f1_score']
    colors      = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444']
    fig = go.Figure()
    for metric, color in zip(metric_cols, colors):
        if metric in df.columns:
            fig.add_trace(go.Scatter(
                x=df['started'], y=df[metric],
                mode='lines+markers',
                name=metric.replace('_', ' ').title(),
                line=dict(color=color, width=2),
            ))
    fig.update_layout(
        xaxis_title='Training Run', yaxis_title='Score',
        yaxis_range=[0, 1],
        legend_title='Metric',
        margin=dict(t=30, b=10, l=10, r=10),
        height=320,
        hovermode='x unified',
    )
    return fig


# ---------------------------------------------------------------------------
# Hourly fraud volume bar
# ---------------------------------------------------------------------------

def hourly_bar(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()
    fig = px.bar(
        df, x='hour', y='fraud_count',
        color_discrete_sequence=['#EF4444'],
        labels={'hour': 'Hour', 'fraud_count': 'Fraud Events'},
    )
    fig.update_layout(margin=dict(t=30, b=10, l=10, r=10), height=260)
    return fig


# ---------------------------------------------------------------------------
# Country bar chart
# ---------------------------------------------------------------------------

def country_bar(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()
    fig = px.bar(
        df.head(15), x='fraud_count', y='location',
        orientation='h',
        color='fraud_count',
        color_continuous_scale='Reds',
        labels={'fraud_count': 'Fraud Count', 'location': 'Country'},
    )
    fig.update_layout(
        yaxis=dict(autorange='reversed'),
        margin=dict(t=30, b=10, l=10, r=10),
        height=320,
        coloraxis_showscale=False,
    )
    return fig


# ---------------------------------------------------------------------------
# Amount ratio trend (user case view)
# ---------------------------------------------------------------------------

def amount_ratio_trend(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['detected_at'], y=df['amount_to_avg_ratio'],
        mode='lines+markers',
        name='Amount / 14-day Avg',
        line=dict(color='#F97316', width=2),
        fill='tozeroy',
        fillcolor='rgba(249,115,22,0.1)',
    ))
    fig.add_hline(y=1.0, line_dash='dash', line_color='#6B7280',
                  annotation_text='Baseline (1×)')
    fig.update_layout(
        xaxis_title='Date', yaxis_title='Ratio',
        margin=dict(t=30, b=10, l=10, r=10),
        height=260,
    )
    return fig
