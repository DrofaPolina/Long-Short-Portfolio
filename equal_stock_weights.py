"""
Equal factor weights (1/K) stock weights

Same pipeline as TSFM (equal weight within each factor long/short leg, then aggregate,
normalize long +1 / short −1, caps), but factor weights are uniform: 1/K for each
active factor from config (same factor universe as TSFM).

Does not use factor_momentum_weights.xlsx.

Inputs:
  outputs/factors/*_positions.xlsx  (one per factor, four sheets each)

Output:
  outputs/hrp_weights/equal_stock_weights.xlsx
    Sheets: long_eu, short_eu, long_us, short_us  (Ticker, Weight — same as hrp / TSFM)

Usage:
    python equal_stock_weights.py [--date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse

import pandas as pd

import config
from tsfm_stock_weights import (
    HRP_DIR,
    LEGS,
    REGIONS,
    _get_active_position_files,
    compute_stock_weights,
    get_active_factors_for_stock_weights,
    load_positions,
    split_and_normalize,
)

OUT_FILE = HRP_DIR / "equal_stock_weights.xlsx"


def run(date: pd.Timestamp | None = None) -> dict[str, "pd.DataFrame"]:
    print("=" * 60)
    print("EQUAL FACTOR WEIGHTS (1/K) — STOCK WEIGHTS")
    print("=" * 60)

    active_factors = get_active_factors_for_stock_weights()
    K = len(active_factors)
    if K == 0:
        print("No active factors with position stems in config — nothing to do.")
        return {}

    factor_weights = {f: 1.0 / K for f in active_factors}
    print(f"\n[1/4] Equal factor weights (K={K}):")
    for f, w in factor_weights.items():
        print(f"  {f:6s}: {w:+.4f}")

    print("\n[2/4] Loading stock positions...")
    active_position_files = _get_active_position_files()
    all_positions: dict[str, dict[str, dict[str, list[str]]]] = {}
    for factor in active_factors:
        filename = active_position_files[factor]
        positions = load_positions(factor, filename, date)
        all_positions[factor] = positions
        for region in REGIONS:
            for leg in LEGS:
                n = len(positions[region][leg])
                print(f"  {factor} {leg}_{region}: {n} stocks")

    print("\n[3/4] Computing stock weights...")
    raw = compute_stock_weights(factor_weights, all_positions)
    total_stocks = sum(len(v) for v in raw.values())
    print(f"  {total_stocks} unique stocks across all regions")

    print("\n[4/4] Normalizing and capping weights...")
    sheets = split_and_normalize(raw)

    for sheet_name, df in sheets.items():
        n = len(df)
        if n > 0:
            w_sum = df["Weight"].sum()
            print(f"  {sheet_name:12s}: {n:3d} stocks, weight sum = {w_sum:+.4f}")
        else:
            print(f"  {sheet_name:12s}: 0 stocks")

    HRP_DIR.mkdir(parents=True, exist_ok=True)
    sheet_order = ["long_eu", "short_eu", "long_us", "short_us"]
    with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as writer:
        for sheet_name in sheet_order:
            df = sheets.get(sheet_name, pd.DataFrame(columns=["Ticker", "Weight"]))
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"\n  ✓ Written: {OUT_FILE}")
    print(f"  Sheets: {sheet_order}")
    return sheets


def main():
    parser = argparse.ArgumentParser(
        description="Equal (1/K) factor weights → stock weights (TSFM-style pipeline)"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Rebalance date YYYY-MM-DD (default: latest available)",
    )
    args = parser.parse_args()

    date = pd.Timestamp(args.date) if args.date else None
    run(date)


if __name__ == "__main__":
    main()
