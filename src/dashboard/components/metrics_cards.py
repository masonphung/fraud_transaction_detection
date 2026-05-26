import streamlit as st


def kpi_row(metrics: list[dict]) -> None:
    """Render a horizontal row of KPI metric tiles.

    Each dict in `metrics` accepts:
        label   (str)           – tile title
        value   (str|int|float) – main displayed value
        delta   (str, optional) – delta string shown below value
        help    (str, optional) – tooltip text
    """
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        col.metric(
            label=m['label'],
            value=m['value'],
            delta=m.get('delta'),
            help=m.get('help'),
        )


def status_badge(label: str, healthy: bool) -> None:
    color = 'green' if healthy else 'red'
    icon  = '🟢' if healthy else '🔴'
    st.markdown(f"{icon} &nbsp; **{label}**", unsafe_allow_html=True)


def system_health_row(services: list[dict]) -> None:
    """Render a row of service health badges.

    Each dict: {'name': str, 'healthy': bool}
    """
    cols = st.columns(len(services))
    for col, svc in zip(cols, services):
        with col:
            status_badge(svc['name'], svc['healthy'])
