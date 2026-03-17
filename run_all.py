"""
Run everything in one script: all factors + portfolio build.

Usage:
    python run_all.py                    # combined region (default)
    python run_all.py --region us
    python run_all.py --region eu
"""

import sys
import os
import argparse
from pathlib import Path

import pandas as pd
import numpy as np

# Project root and path setup (so we can import from src and write outputs in one place)
import config
ROOT = config.PROJECT_ROOT
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))  # so src modules can "from data_loader import ..."

# Import factor modules (they use ROOT as cwd now)
from src.data_loader import load_stock_returns_us, load_stock_returns_eu
from src.value import calculate_value_factor_monthly
from src.momentum import calculate_momentum_factor_monthly
from src.quality import calculate_quality_factor
from src.liquidity import calculate_liquidity_factor_monthly
from src.yield_factor import calculate_yield_factors_monthly
from src.lowvol import calculate_lowvol_factor_monthly

OUTPUT_FACTORS = config.get_output_path("factors")


def _pool_and_write_tickers(val, mom, liq, qlt, lvol):
    """
    Pool long/short tickers from all factors (union: one stock can be selected
    by several factors but appears once per leg) and write data/Tickers.xlsx
    for hrp_allocation (sheets: long_us, short_us, long_eu, short_eu).
    """
    long_us, short_us = set(), set()
    long_eu, short_eu = set(), set()
    for name, res in [("VAL", val), ("MOM", mom), ("LIQ", liq), ("QLT", qlt), ("LVOL", lvol)]:
        if not isinstance(res, dict):
            continue
        if "picks_us" in res and res["picks_us"]:
            l, s = res["picks_us"]
            long_us.update(l)
            short_us.update(s)
        if "picks_eu" in res and res["picks_eu"]:
            l, s = res["picks_eu"]
            long_eu.update(l)
            short_eu.update(s)

    tickers_path = config.get_data_path("Tickers.xlsx")
    tickers_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(tickers_path, engine="openpyxl") as w:
        pd.DataFrame({"TICKER": sorted(long_us)}).to_excel(w, sheet_name="long_us", index=False)
        pd.DataFrame({"TICKER": sorted(short_us)}).to_excel(w, sheet_name="short_us", index=False)
        pd.DataFrame({"TICKER": sorted(long_eu)}).to_excel(w, sheet_name="long_eu", index=False)
        pd.DataFrame({"TICKER": sorted(short_eu)}).to_excel(w, sheet_name="short_eu", index=False)
    print(f"\n  ✓ Pooled tickers written to {tickers_path} (long_us: {len(long_us)}, short_us: {len(short_us)}, long_eu: {len(long_eu)}, short_eu: {len(short_eu)})")


def run_all_factors():
    """Run all factor calculations and return combined DataFrames for portfolio."""
    OUTPUT_FACTORS.mkdir(parents=True, exist_ok=True)
    out = str(OUTPUT_FACTORS)

    # 1. Value (Book-to-Market)
    print("\n" + "=" * 60)
    print("1/6 VALUE FACTOR (VAL)")
    print("=" * 60)
    val = calculate_value_factor_monthly(save_outputs=True, output_dir=out)

    # 2. Load returns once for momentum
    print("\nLoading stock returns (cached if files exist)...")
    us_returns = load_stock_returns_us()
    eu_returns = load_stock_returns_eu()

    # 3. Momentum (12‑1)
    print("\n" + "=" * 60)
    print("2/6 MOMENTUM FACTOR (MOM)")
    print("=" * 60)
    mom = calculate_momentum_factor_monthly(
        us_returns, eu_returns, save_outputs=True, output_dir=out
    )

    # 4. Liquidity (Amihud illiquidity 10–1)
    print("\n" + "=" * 60)
    print("3/6 LIQUIDITY FACTOR (LIQ)")
    print("=" * 60)
    try:
        from src.liquidity import LIQUIDITY_FILE_PATH as _liq_file
    except Exception:
        _liq_file = "Minerva_Size_Factor.xlsx"
    liq_path = Path(_liq_file)
    if not liq_path.is_absolute():
        liq_path = config.get_data_path(_liq_file)
    if liq_path.exists():
        liq = calculate_liquidity_factor_monthly(
            file_path=str(liq_path),
            save_outputs=True,
            output_dir=str(OUTPUT_FACTORS),
        )
    else:
        # No liquidity data file: try cached output or use placeholder
        liq_cache = OUTPUT_FACTORS / "liquidity_regional.xlsx"
        if liq_cache.exists():
            print(f"  ⚠ Liquidity file not found: {liq_path}")
            print(f"  → Using cached {liq_cache}")
            _liq_df = pd.read_excel(liq_cache, index_col=0)
            _liq_df.index = pd.to_datetime(_liq_df.index)
            liq = {"combined": _liq_df}
        else:
            print(f"  ⚠ Liquidity file not found: {liq_path}")
            print("  → Skipping LIQ; using NaN placeholder (add Minerva_Size_Factor.xlsx to run LIQ).")
            _idx = val["combined"].index
            liq = {
                "combined": pd.DataFrame(
                    np.nan, index=_idx, columns=["LIQ_US", "LIQ_EU", "LIQ"]
                )
            }

    # 5. Quality (ROE / growth / leverage / EVOL)
    print("\n" + "=" * 60)
    print("4/6 QUALITY FACTOR (QLT)")
    print("=" * 60)
    qlt = calculate_quality_factor(save_outputs=True, output_dir=out)

    # 6. Yield curve factors (Nelson‑Siegel: BETA0 level, BETA1 slope, BETA2 curvature)
    print("\n" + "=" * 60)
    print("5/6 YIELD FACTORS (BETA0, BETA1, BETA2)")
    print("=" * 60)
    yld = calculate_yield_factors_monthly(save_outputs=True, output_dir=out)
    yld_combined = yld["combined"]

    # 7. Low volatility factor
    print("\n" + "=" * 60)
    print("6/6 LOW VOLATILITY FACTOR (LVOL)")
    print("=" * 60)
    lvol = calculate_lowvol_factor_monthly(
        us_returns, eu_returns, save_outputs=True, output_dir=out
    )

    # Pool factor picks (union: a stock can be selected by several factors, appear once)
    # and write Tickers.xlsx for hrp_allocation
    _pool_and_write_tickers(val=val, mom=mom, liq=liq, qlt=qlt, lvol=lvol)

    return {
        "VAL": val["combined"],
        "MOM": mom["combined"],
        "LIQ": liq["combined"],
        "QLT": qlt["combined"],
        "BETA0": yld_combined[["BETA0_US", "BETA0_EU", "BETA0"]].copy(),
        "BETA1": yld_combined[["BETA1_US", "BETA1_EU", "BETA1"]].copy(),
        "BETA2": yld_combined[["BETA2_US", "BETA2_EU", "BETA2"]].copy(),
        "LVOL": lvol["combined"],
    }


def build_factor_matrix(factor_dfs, region):
    """Build one DataFrame of factor returns for the chosen region."""
    cols = ["VAL", "MOM", "LIQ", "QLT", "LVOL", "BETA0", "BETA1", "BETA2"]
    if region == "combined":
        return pd.DataFrame({c: factor_dfs[c][c] for c in cols})
    suffix = "_US" if region == "us" else "_EU"
    return pd.DataFrame({c: factor_dfs[c][c + suffix] for c in cols})


def performance_stats(factor_returns):
    """Annualized return, vol, Sharpe, cum return, max drawdown per factor."""
    stats = {}
    for col in factor_returns.columns:
        ret = factor_returns[col].dropna()
        if len(ret) < 2:
            continue
        ann_ret = ret.mean() * 12
        ann_vol = ret.std() * np.sqrt(12)
        sharpe = ann_ret / ann_vol if ann_vol > 0 else 0
        cum = (1 + ret).cumprod()
        dd = (cum - cum.cummax()) / cum.cummax()
        stats[col] = {
            "Ann. Return": ann_ret,
            "Ann. Vol": ann_vol,
            "Sharpe": sharpe,
            "Cum. Return": cum.iloc[-1] - 1,
            "Max DD": dd.min(),
        }
    return pd.DataFrame(stats).T


def main():
    parser = argparse.ArgumentParser(description="Run all factors and build portfolio")
    parser.add_argument(
        "--region",
        type=str,
        default="combined",
        choices=["us", "eu", "combined"],
        help="Portfolio region (default: combined)",
    )
    args = parser.parse_args()
    region = args.region.lower()

    print("=" * 60)
    print("RUN ALL: FACTORS + PORTFOLIO")
    print("=" * 60)
    print(f"Region: {region.upper()}")

    # Run all factors (saves to outputs/factors)
    factor_dfs = run_all_factors()

    # Build factor return matrix for selected region
    ff = build_factor_matrix(factor_dfs, region)
    ff = ff.dropna(how="all")
    if ff.empty:
        print("\nNo factor data for this region.")
        return

    # Performance
    perf = performance_stats(ff)

    # Save portfolio outputs
    out_port = config.get_output_path(f"portfolio_{region}")
    out_port.mkdir(parents=True, exist_ok=True)
    ff.to_excel(out_port / "factor_returns.xlsx")
    perf.to_excel(out_port / "performance_stats.xlsx")
    (1 + ff).cumprod().to_excel(out_port / "cumulative_returns.xlsx")

    print("\n" + "=" * 60)
    print(f"PORTFOLIO ({region.upper()}) – SUMMARY")
    print("=" * 60)
    print(perf.to_string(float_format=lambda x: f"{x:.4f}"))
    best = perf["Sharpe"].idxmax()
    print(f"\nBest Sharpe: {best} ({perf.loc[best, 'Sharpe']:.3f})")
    print(f"\nResults: {out_port}/")

    # Final performance from HRP weights + Performance workbook
    try:
        from performance_from_hrp import main as run_performance_from_hrp
        perf_path = config.get_data_path("Performance_SPRING_2026.xlsx")
        if perf_path.exists():
            run_performance_from_hrp()
        else:
            print(f"\n(Skip final performance: {perf_path} not found)")
    except Exception as e:
        print(f"\n(Skip final performance: {e})")


if __name__ == "__main__":
    main()
