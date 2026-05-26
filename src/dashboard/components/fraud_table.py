import pandas as pd
import streamlit as st

from config import PATTERN_COLORS

_DISPLAY_COLS = {
    'transaction_id': 'Transaction ID',
    'user_id':         'User',
    'amount':          'Amount ($)',
    'currency':        'Currency',
    'merchant':        'Merchant',
    'location':        'Location',
    'fraud_probability': 'Confidence',
    'fraud_pattern':   'Pattern',
    'detected_at':     'Detected At',
}


def _highlight_pattern(row: pd.Series) -> list[str]:
    color = PATTERN_COLORS.get(row.get('Pattern', ''), '#6B7280')
    return [f'border-left: 4px solid {color}'] + [''] * (len(row) - 1)


def fraud_table(df: pd.DataFrame, key: str = 'fraud_table') -> None:
    if df.empty:
        st.info('No fraud predictions yet.')
        return

    display = df[[c for c in _DISPLAY_COLS if c in df.columns]].copy()
    display = display.rename(columns={k: v for k, v in _DISPLAY_COLS.items() if k in display.columns})

    if 'Amount ($)' in display.columns:
        display['Amount ($)'] = display['Amount ($)'].map('${:,.2f}'.format)

    if 'Confidence' in display.columns:
        display['Confidence'] = display['Confidence'].map('{:.1%}'.format)

    if 'Detected At' in display.columns:
        display['Detected At'] = pd.to_datetime(display['Detected At']).dt.strftime('%Y-%m-%d %H:%M:%S')

    styled = display.style.apply(_highlight_pattern, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True, key=key)
