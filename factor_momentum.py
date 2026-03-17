"""
TSFM (Time-Series Factor Momentum) factor weights.

Reads factor return history from outputs/factors/*_regional.xlsx, computes
signals (formation return / vol) at 6-month rebalance dates, and saves
factor weights for analysis and config.get_portfolio_weights().

Usage:
    python factor_momentum.py

Output:
    outputs/hrp_weights/factor_momentum_weights.xlsx
      - current: Factor, Weight, Signal, Formation Return, Vol (latest rebalance)
      - history: Date, VAL, MOM, LVOL, QLT (all rebalance dates)

HRP (hrp_allocation.py) is unchanged and separate. This script is for
analytical factor weighting only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Project paths
import config
ROOT = config.PROJECT_ROOT
sys.path.insert(0, str(ROOT))

FACTORS_DIR = config.get_output_path("factors")
OUT_DIR = config.get_output_path("hrp_weights")
OUT_FILE = OUT_DIR / "factor_momentum_weights.xlsx"

# Return file stems per factor — mirrors _FACTOR_STEMS in tsfm_stock_weights.py
_FACTOR_RETURN_STEMS = {
    "VAL":   "value",
    "MOM":   "momentum",
    "LVOL":  "lowvol",
    "QLT":   "quality",
    "SIZE":  "size",
    "BETA0": "beta0",
    "BETA1": "beta1",
    "BETA2": "beta2",
}

def _get_active_factor_files() -> dict[str, str]:
    """
    Build {FACTOR: filename} for all active factors that have a return file stem.
    Driven by config.ACTIVE_FACTORS — adding a new factor there is all that's needed.
    """
    return {
        factor: f"{_FACTOR_RETURN_STEMS[factor]}_regional.xlsx"
        for factor in config.get_active_factors()
        if factor in _FACTOR_RETURN_STEMS
    }

# TSFM parameters from config (formation, vol, signal cap, rebalance months)
FORMATION_MONTHS = config.TSFM_CONFIG.get("formation_months", 12)
VOL_MONTHS = config.TSFM_CONFIG.get("vol_months", 36)
SIGNAL_CAP = config.TSFM_CONFIG.get("signal_cap", 2.0)
REBALANCE_MONTHS = config.TSFM_CONFIG.get("rebalance_months", (1, 7))


def load_factor_returns() -> pd.DataFrame:
    """Load active factor monthly returns from outputs/factors/*_regional.xlsx."""
    series = {}
    for factor, filename in _get_active_factor_files().items():
        path = FACTORS_DIR / filename
        if not path.exists():
            print(f"  Warning: {path} not found, skipping {factor}")
            continue
        df = pd.read_excel(path, index_col=0)
        df.index = pd.to_datetime(df.index, errors="coerce")
        # Prefer exact combined column (e.g. 'MOM') over regional columns
        # (e.g. 'MOM_US', 'MOM_EU') to avoid double-counting
        if factor in df.columns:
            series[factor] = df[factor].dropna()
        else:
            # Fall back to regional columns — average them
            regional = [c for c in df.columns if str(c).startswith(factor + "_")]
            if not regional:
                print(f"  Warning: no column for {factor} in {path}")
                continue
            series[factor] = df[regional].mean(axis=1, skipna=True).dropna()
    if not series:
        raise FileNotFoundError(f"No factor files found under {FACTORS_DIR}")
    # Align to common index (month-end)
    out = pd.DataFrame(series)
    try:
        out = out.resample("ME").last().dropna(how="all")
    except ValueError:
        out = out.resample("M").last().dropna(how="all")
    return out


def get_rebalance_dates(returns: pd.DataFrame) -> pd.DatetimeIndex:
    """Return dates where month is 1 or 7 (Jan, Jul)."""
    return returns.index[returns.index.month.isin(REBALANCE_MONTHS)]


def compute_tsfm_weights_at_date(
    returns: pd.DataFrame,
    rebal_date: pd.Timestamp,
) -> tuple[dict[str, float], dict[str, float], dict[str, float], dict[str, float]]:
    """
    At rebal_date, compute signal = formation_return / vol (capped), then weights.
    Returns (weights, signals, formation_returns, vols) as dicts factor -> value.
    """
    idx = returns.index.get_indexer([rebal_date], method="ffill")[0]
    if idx < 0 or idx < VOL_MONTHS:
        return {}, {}, {}, {}
    # 36 months before rebal_date (exclusive of rebal_date)
    window_36 = returns.iloc[idx - VOL_MONTHS : idx]
    # 12 months before rebal_date
    window_12 = returns.iloc[idx - FORMATION_MONTHS : idx]
    if len(window_12) < FORMATION_MONTHS or len(window_36) < VOL_MONTHS:
        return {}, {}, {}, {}
    weights = {}
    signals = {}
    formation_returns = {}
    vols = {}
    for col in returns.columns:
        r12 = window_12[col].replace([np.inf, -np.inf], np.nan).dropna()
        r36 = window_36[col].replace([np.inf, -np.inf], np.nan).dropna()
        if len(r12) < FORMATION_MONTHS // 2 or len(r36) < VOL_MONTHS // 2:
            continue
        formation_ret = float((1 + r12).prod() - 1)
        if np.isnan(formation_ret) or np.isinf(formation_ret):
            continue
        std_36 = r36.std()
        if pd.isna(std_36) or std_36 <= 0:
            vol_annual = 0.0
            sig = 0.0
        else:
            vol_annual = float(std_36 * np.sqrt(12))
            if vol_annual < 1e-9:
                sig = 0.0
            else:
                sig = formation_ret / vol_annual
                sig = max(-SIGNAL_CAP, min(SIGNAL_CAP, float(sig)))
        signals[col] = sig
        formation_returns[col] = formation_ret
        vols[col] = vol_annual
    total_abs = sum(abs(s) for s in signals.values())
    if total_abs < 1e-12:
        n = len(signals)
        weights = {f: 1.0 / n for f in signals}
    else:
        weights = {f: s / total_abs for f, s in signals.items()}
    return weights, signals, formation_returns, vols


def run_tsfm() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load factor returns, compute TSFM weights at each rebalance date.
    Returns (current_df, history_df).
    """
    print("Loading factor returns...")
    returns = load_factor_returns()
    print(f"  Loaded {list(returns.columns)} from {returns.index[0].date()} to {returns.index[-1].date()} ({len(returns)} months)")
    rebal_dates = get_rebalance_dates(returns)
    if len(rebal_dates) == 0:
        raise ValueError("No rebalance dates (Jan/Jul) in factor return index")
    print(f"  Rebalance dates: {len(rebal_dates)} (month in {REBALANCE_MONTHS})")
    history_rows = []
    current_weights = {}
    current_signals = {}
    current_formation = {}
    current_vol = {}
    for d in rebal_dates:
        weights, signals, formation_returns, vols = compute_tsfm_weights_at_date(returns, d)
        if not weights:
            continue
        history_rows.append({"Date": d, **weights})
        # Keep latest for "current"
        current_weights = weights
        current_signals = signals
        current_formation = formation_returns
        current_vol = vols
    history_df = pd.DataFrame(history_rows)
    if history_df.empty:
        raise ValueError("No valid TSFM weights computed (check factor history length)")
    # Current sheet: Factor, Weight, Signal, Formation Return, Vol
    factors = list(current_weights.keys())
    current_df = pd.DataFrame({
        "Factor": factors,
        "Weight": [current_weights[f] for f in factors],
        "Signal": [current_signals[f] for f in factors],
        "Formation Return": [current_formation[f] for f in factors],
        "Vol": [current_vol[f] for f in factors],
    })
    return current_df, history_df


def main():
    print("=" * 60)
    print("TSFM FACTOR WEIGHTS")
    print("=" * 60)
    current_df, history_df = run_tsfm()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as w:
        current_df.to_excel(w, sheet_name="current", index=False)
        history_df.to_excel(w, sheet_name="history", index=False)
    print(f"\n  ✓ Written {OUT_FILE}")
    print("  Sheets: current, history")
    print("\nCurrent weights (for config.get_portfolio_weights()):")
    print(current_df.to_string(index=False))
    return current_df, history_df


if __name__ == "__main__":
    main()
