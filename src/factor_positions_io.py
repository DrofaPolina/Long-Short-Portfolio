"""
Shared helper to save factor positions to Excel (Date, Ticker per sheet).
Used by value, momentum, lowvol, quality, liquidity factor modules.
"""

from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Union


def _is_jan_or_jul_rebalance(dt: pd.Timestamp) -> bool:
    """True if date is in January or July (rebalance months)."""
    return pd.Timestamp(dt).month in (1, 7)


def _to_month_end_jan_jul(dt: pd.Timestamp) -> pd.Timestamp:
    """Return month-end date for Jan/Jul; otherwise same date."""
    if dt.month not in (1, 7):
        return dt
    try:
        return pd.Timestamp(dt.year, dt.month, 1) + pd.offsets.MonthEnd(0)
    except Exception:
        return dt


def build_positions_df(rows):
    """
    Build DataFrame with columns Date, Ticker.
    rows: list of (date, list of ticker strings). Dates are normalized to month-end.
    """
    if not rows:
        return pd.DataFrame(columns=["Date", "Ticker"])
    out = []
    for date, tickers in rows:
        d = pd.Timestamp(date)
        d = _to_month_end_jan_jul(d) if d.month in (1, 7) else d
        for t in (tickers or []):
            if t and str(t).strip():
                out.append({"Date": d, "Ticker": str(t).strip()})
    return pd.DataFrame(out)


def save_positions_excel(
    long_us: list,
    short_us: list,
    long_eu: list,
    short_eu: list,
    path: Union[str, Path],
) -> None:
    """
    Save four sheets (long_us, short_us, long_eu, short_eu) to one Excel file.
    Each argument is a list of (date, list of ticker strings). Only Jan/Jul dates are included.
    Columns per sheet: Date, Ticker (one row per stock per rebalance date).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Filter to Jan/Jul
    def filter_jan_jul(rows):
        return [(d, t) for d, t in rows if _is_jan_or_jul_rebalance(pd.Timestamp(d))]
    long_us_f = filter_jan_jul(long_us)
    short_us_f = filter_jan_jul(short_us)
    long_eu_f = filter_jan_jul(long_eu)
    short_eu_f = filter_jan_jul(short_eu)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        build_positions_df(long_us_f).to_excel(w, sheet_name="long_us", index=False)
        build_positions_df(short_us_f).to_excel(w, sheet_name="short_us", index=False)
        build_positions_df(long_eu_f).to_excel(w, sheet_name="long_eu", index=False)
        build_positions_df(short_eu_f).to_excel(w, sheet_name="short_eu", index=False)
