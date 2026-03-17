"""
Performance from weight files and Performance workbook.

Reads:
  - Weights: Excel with sheets long_eu, short_eu, long_us, short_us (Ticker, Weight)
    Two inputs by default: hrp_weights.xlsx and tsfm_stock_weights.xlsx
  - Prices: data/Performance_SPRING_2026.xlsx (US PORTFOLIO, EU PORTFOLIO; optional EURUSD)

Writes (for each weight file):
  - outputs/portfolio_combined/<output_name>.xlsx
    - EU PORTFOLIO: long leg, long leg change, short leg, short leg change, total
    - US PORTFOLIO: same
    - Total: EU total, US total, US total in EUR, total sum in EUR

Usage:
  python performance_from_hrp.py              # run both HRP and TSFM
  python performance_from_hrp.py --weights hrp_weights.xlsx --output performance_from_hrp.xlsx  # one only
"""
from __future__ import annotations

import argparse
import pandas as pd
import numpy as np
from pathlib import Path

import config

# Paths
WEIGHTS_DIR = config.get_output_path("hrp_weights")
DATA_PATH = config.get_data_path("Performance_SPRING_2026.xlsx")
OUT_DIR = config.get_output_path("portfolio_combined")

# Two portfolios: (weights filename, output filename) — same format for both
PORTFOLIO_CONFIGS = [
    (WEIGHTS_DIR / "hrp_weights.xlsx", OUT_DIR / "performance_from_hrp.xlsx"),
    (WEIGHTS_DIR / "tsfm_stock_weights.xlsx", OUT_DIR / "performance_from_tsfm.xlsx"),
]

# US DATA / EU DATA layout: tickers C4, prices C5, dates B5 (0-based: row 3, row 4, col 1, col 2+)
DATA_TICKER_ROW = 3
DATA_DATE_START_ROW = 4
DATA_DATE_COL = 1
DATA_VALUE_FIRST_COL = 2

# US/EU PORTFOLIO sheet layout (formula uses this sheet): dates col A, prices B:U, row 11 = first data
# Row 10 (0-based 9) = tickers in B:U, row 11 (0-based 10) = first date and first price row
PORTFOLIO_TICKER_ROW = 9   # Excel row 10
PORTFOLIO_DATE_START_ROW = 10  # Excel row 11
PORTFOLIO_DATE_COL = 0    # Column A
def _portfolio_value_cols(ncols: int) -> list[int]:
    """B:U = 20 columns; if sheet has fewer, use 1..ncols-1 (skip col 0 = date)."""
    return list(range(1, min(21, ncols)))
# Scale so first long-leg cell matches sheet AR:11 = 2500 (weights*5000, growth = P_t/P_0)
LEG_SCALE = 2500


def load_weights(path: Path) -> dict[str, pd.DataFrame]:
    """Load long/short weights from hrp_weights.xlsx (sheets: long_eu, short_eu, long_us, short_us)."""
    out = {}
    for sheet in ["long_eu", "short_eu", "long_us", "short_us"]:
        df = pd.read_excel(path, sheet_name=sheet)
        if "Ticker" in df.columns and "Weight" in df.columns:
            out[sheet] = df[["Ticker", "Weight"]].copy()
        else:
            out[sheet] = pd.DataFrame(columns=["Ticker", "Weight"])
    return out


def load_prices_from_portfolio_sheet(path: Path, sheet: str) -> pd.DataFrame:
    """
    Load prices from the PORTFOLIO sheet where the formula lives (US PORTFOLIO / EU PORTFOLIO).
    Layout: dates in column A from row 11, prices in B:U (20 cols), tickers in row 10 B:U.
    Returns price matrix for leg_performance (growth = P_t / P_0) so values match the sheet.
    """
    raw = pd.read_excel(path, sheet_name=sheet, header=None)
    value_cols = _portfolio_value_cols(raw.shape[1])
    tickers = raw.iloc[PORTFOLIO_TICKER_ROW, value_cols].astype(str).str.strip().values
    dates = pd.to_datetime(raw.iloc[PORTFOLIO_DATE_START_ROW:, PORTFOLIO_DATE_COL], errors="coerce")
    vals = raw.iloc[PORTFOLIO_DATE_START_ROW:, value_cols].copy()
    valid = dates.notna()
    dates = dates.loc[valid]
    vals = vals.loc[valid]
    vals.columns = range(vals.shape[1])
    vals = vals.apply(pd.to_numeric, errors="coerce")
    vals.index = dates.values
    vals = vals.loc[~vals.index.duplicated(keep="first")]
    vals.columns = tickers[: vals.shape[1]]
    return vals


def load_prices_from_data_sheet(path: Path, sheet: str) -> pd.DataFrame:
    """
    Load prices from US DATA / EU DATA (tickers C4, prices C5, dates B5).
    Use for short leg because PORTFOLIO sheets only have long tickers in B:U.
    """
    raw = pd.read_excel(path, sheet_name=sheet, header=None)
    value_cols = list(range(DATA_VALUE_FIRST_COL, raw.shape[1]))
    tickers = raw.iloc[DATA_TICKER_ROW, value_cols].astype(str).str.strip().values
    dates = pd.to_datetime(raw.iloc[DATA_DATE_START_ROW:, DATA_DATE_COL], errors="coerce")
    vals = raw.iloc[DATA_DATE_START_ROW:, value_cols].copy()
    valid = dates.notna()
    dates = dates.loc[valid]
    vals = vals.loc[valid]
    vals.columns = range(vals.shape[1])
    vals = vals.apply(pd.to_numeric, errors="coerce")
    vals.index = dates.values
    vals = vals.loc[~vals.index.duplicated(keep="first")]
    vals.columns = tickers[: vals.shape[1]]
    return vals


def leg_performance(weights: pd.DataFrame, prices: pd.DataFrame) -> pd.Series:
    """
    Leg performance: SUMPRODUCT(weights*LEG_SCALE, growth) with growth = P_t / P_0 (sheet formula).
    Matches sheet: (B11:U11)/($B$11:$U$11) and weights*5000 -> first cell 2500.
    """
    tickers = weights["Ticker"].astype(str).str.strip().values
    w = weights["Weight"].values
    common = [c for c in tickers if c in prices.columns]
    if not common:
        return pd.Series(index=prices.index, dtype=float)
    P = prices[common].reindex(columns=common)
    w_aligned = np.array([w[list(tickers).index(c)] for c in common])
    p0 = P.iloc[0]
    growth = P / np.where(p0 == 0, np.nan, p0)
    growth = growth.fillna(1.0)
    w_sum = w_aligned.sum()
    if abs(w_sum) < 1e-12:
        leg_value = pd.Series(0.0, index=prices.index)
    else:
        # Normalize so first row = LEG_SCALE (long) or -LEG_SCALE (short); divide by |w_sum| to preserve sign of (growth*w).sum()
        leg_value = LEG_SCALE * (growth * w_aligned).sum(axis=1) / abs(w_sum)
    return leg_value


def build_portfolio_sheet(
    long_weights: pd.DataFrame,
    short_weights: pd.DataFrame,
    long_prices: pd.DataFrame,
    short_prices: pd.DataFrame,
) -> pd.DataFrame:
    """One region: long leg from PORTFOLIO prices (match sheet), short leg from DATA prices (has short tickers)."""
    long_leg = leg_performance(long_weights, long_prices)
    short_leg = leg_performance(short_weights, short_prices)
    short_leg = short_leg.reindex(long_leg.index).fillna(0.0)
    long_leg_change = long_leg.diff()
    short_leg_change = short_leg.diff()
    total = long_leg_change + short_leg_change
    out = pd.DataFrame({
        "long_leg": long_leg,
        "long_leg_change": long_leg_change,
        "short_leg": short_leg,
        "short_leg_change": short_leg_change,
        "total": total,
    })
    return out


def load_eurusd(path: Path) -> pd.Series | None:
    """Load EURUSD from Performance workbook if sheet exists; index = date, value = rate (USD per 1 EUR)."""
    xl = pd.ExcelFile(path)
    if "EURUSD" not in xl.sheet_names:
        return None
    df = pd.read_excel(path, sheet_name="EURUSD", header=None)
    # First row is header (#NAME?, EUR=); data from row 1: col 0 = date, col 1 = rate
    df = df.iloc[1:]
    dates = pd.to_datetime(df.iloc[:, 0], errors="coerce")
    vals = pd.to_numeric(df.iloc[:, 1], errors="coerce")
    s = pd.Series(vals.values, index=dates)
    s = s.loc[s.index.notna() & s.notna()]
    return s[~s.index.duplicated(keep="first")]


def run_performance(weights_path: Path, out_path: Path) -> bool:
    """
    Build performance workbook from one weight file and write one output Excel.
    Returns True if written, False if skipped (e.g. weights file missing).
    """
    if not weights_path.exists():
        print(f"Weights not found: {weights_path}")
        return False
    if not DATA_PATH.exists():
        print(f"Data file not found: {DATA_PATH}")
        return False

    weights = load_weights(weights_path)
    long_prices_eu = load_prices_from_portfolio_sheet(DATA_PATH, "EU PORTFOLIO")
    short_prices_eu = load_prices_from_data_sheet(DATA_PATH, "EU DATA")
    long_prices_us = load_prices_from_portfolio_sheet(DATA_PATH, "US PORTFOLIO")
    short_prices_us = load_prices_from_data_sheet(DATA_PATH, "US DATA")
    eu_sheet = build_portfolio_sheet(weights["long_eu"], weights["short_eu"], long_prices_eu, short_prices_eu)
    us_sheet = build_portfolio_sheet(weights["long_us"], weights["short_us"], long_prices_us, short_prices_us)

    eurusd = load_eurusd(DATA_PATH)
    if eurusd is not None and not eurusd.empty:
        us_total = us_sheet["total"]
        fx = us_total.index.map(lambda d: eurusd.reindex([d]).iloc[0] if d in eurusd.index else np.nan)
        fx = pd.Series(fx, index=us_total.index).ffill().bfill()
        us_total_eur = us_total / fx
    else:
        us_total_eur = us_sheet["total"]

    total_sheet = pd.DataFrame({
        "eu_total": eu_sheet["total"],
        "us_total": us_sheet["total"],
        "us_total_eur": us_total_eur,
        "total_eur": eu_sheet["total"] + us_total_eur,
    })

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        eu_sheet.index.name = "Date"
        us_sheet.index.name = "Date"
        total_sheet.index.name = "Date"
        eu_sheet.to_excel(w, sheet_name="EU PORTFOLIO")
        us_sheet.to_excel(w, sheet_name="US PORTFOLIO")
        total_sheet.to_excel(w, sheet_name="Total")
    print(f"Wrote {out_path}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Build performance from weight files.")
    parser.add_argument("--weights", type=Path, help="Single weights file (run only this one)")
    parser.add_argument("--output", type=Path, help="Output Excel path (use with --weights)")
    args = parser.parse_args()

    if args.weights is not None:
        out_path = args.output if args.output is not None else OUT_DIR / "performance_from_weights.xlsx"
        run_performance(Path(args.weights), Path(out_path))
        return

    for weights_path, out_path in PORTFOLIO_CONFIGS:
        run_performance(weights_path, out_path)


if __name__ == "__main__":
    main()
