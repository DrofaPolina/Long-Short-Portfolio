"""
Debug script: compare performance_2026_automation output to original workbook (config.PERFORMANCE_WORKBOOK_FILENAME).

Run from project root:
  python tests/debug_compare.py
  python tests/debug_compare.py --first 20    # show first 20 dates with big diffs
  python tests/debug_compare.py --col AR      # focus on one column
  python tests/debug_compare.py --save-diff   # write diff report CSV to outputs/

Use this after tests fail (or anytime) to see WHERE and HOW MUCH script vs Excel differ.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(TESTS_DIR))

import numpy as np
import pandas as pd

# Reuse test helpers (same column layout and read functions)
from test_performance_2026_vs_excel import (
    PERFORMANCE_XLSX,
    HRP_WEIGHTS_XLSX,
    read_portfolio_sheet_from_excel,
    read_total_daily_from_excel,
    _AR_AX_COLS,
    RTOL,
    ATOL,
)
import config as _config
_Root = PROJECT_ROOT
_PERF_OUT = PROJECT_ROOT / _config.OUTPUT_DIRS["performance"]


def run_script():
    """Run automation and return eu_portfolio, us_portfolio, total_daily."""
    import performance_2026_automation as p26
    return p26.run_performance_2026(
        performance_xlsx=PERFORMANCE_XLSX,
        hrp_weights_xlsx=HRP_WEIGHTS_XLSX,
    )


def diff_report(script_series: pd.Series, excel_series: pd.Series, name: str, rtol=RTOL, atol=ATOL):
    """Print and return diff stats; return a small DataFrame of worst rows."""
    common = script_series.index.intersection(excel_series.index).sort_values()
    if len(common) == 0:
        print(f"  [{name}] No common dates.")
        return None
    s = script_series.loc[common].astype(float)
    e = excel_series.loc[common].astype(float)
    diff = s - e
    abs_diff = np.abs(diff)
    # Mask where both are NaN
    both_nan = s.isna() & e.isna()
    abs_diff = abs_diff.where(~both_nan)
    n = abs_diff.notna().sum()
    if n == 0:
        print(f"  [{name}] All NaN or identical.")
        return None
    max_abs = abs_diff.max()
    mean_abs = abs_diff.mean()
    # Count failures vs tolerance
    fails = (abs_diff > atol) & (abs_diff > rtol * np.abs(e).replace(0, np.nan))
    n_fail = fails.sum()
    print(f"  [{name}] n={n}, max_abs_diff={max_abs:.6g}, mean_abs_diff={mean_abs:.6g}, fails(rtol/atol)={n_fail}")
    # Worst rows
    worst_idx = abs_diff.nlargest(min(10, len(abs_diff))).index
    report = pd.DataFrame({
        "script": s.loc[worst_idx],
        "excel": e.loc[worst_idx],
        "diff": diff.loc[worst_idx],
        "abs_diff": abs_diff.loc[worst_idx],
    }, index=worst_idx)
    return report


def main():
    ap = argparse.ArgumentParser(description="Compare script output to Excel for debugging.")
    ap.add_argument("--first", type=int, default=10, help="Show first N dates with large diff (default 10)")
    ap.add_argument("--col", type=str, default=None, help="Only compare this column (e.g. AR, AX, A, B, C)")
    ap.add_argument("--save-diff", action="store_true", help="Save diff report CSV to config output dir")
    args = ap.parse_args()

    if not PERFORMANCE_XLSX.exists():
        print(f"Missing: {PERFORMANCE_XLSX}")
        return 1
    if not HRP_WEIGHTS_XLSX.exists():
        print(f"Missing: {HRP_WEIGHTS_XLSX}. Run: python hrp_allocation.py")
        return 1

    print("Running performance_2026_automation...")
    out = run_script()
    eu_script = out["eu_portfolio"]
    us_script = out["us_portfolio"]
    total_script = out["total_daily"]

    print("Loading Excel sheets...")
    eu_excel = read_portfolio_sheet_from_excel(PERFORMANCE_XLSX, "EU PORTFOLIO")
    us_excel = read_portfolio_sheet_from_excel(PERFORMANCE_XLSX, "US PORTFOLIO")
    total_excel = read_total_daily_from_excel(PERFORMANCE_XLSX)

    n_portfolio_cols = min(7, eu_script.shape[1], us_excel.shape[1])
    col_idx = None
    if args.col and args.col in _AR_AX_COLS:
        col_idx = _AR_AX_COLS.index(args.col)

    reports = []

    print("\n--- EU PORTFOLIO ---")
    for i in range(n_portfolio_cols):
        if col_idx is not None and i != col_idx:
            continue
        name = eu_script.columns[i] if i < len(eu_script.columns) else f"col{i}"
        r = diff_report(eu_script.iloc[:, i], eu_excel.iloc[:, i], f"EU {name}", RTOL, ATOL)
        if r is not None:
            reports.append((f"eu_col{i}", r))
            print(r.head(args.first).to_string())
            print()

    print("--- US PORTFOLIO ---")
    for i in range(n_portfolio_cols):
        if col_idx is not None and i != col_idx:
            continue
        name = us_script.columns[i] if i < len(us_script.columns) else f"col{i}"
        r = diff_report(us_script.iloc[:, i], us_excel.iloc[:, i], f"US {name}", RTOL, ATOL)
        if r is not None:
            reports.append((f"us_col{i}", r))
            print(r.head(args.first).to_string())
            print()

    print("--- TOTAL DAILY (EU Daily, US Daily, Total Daily) ---")
    if not total_excel.empty:
        for col, label in [("A", "EU Daily"), ("B", "US Daily"), ("C", "Total Daily")]:
            if args.col and args.col != col:
                continue
            if label not in total_script.columns or col not in total_excel.columns:
                continue
            r = diff_report(total_script[label], total_excel[col], f"Total {label}", RTOL, ATOL)
            if r is not None:
                reports.append((f"total_{col}", r))
                print(r.head(args.first).to_string())
                print()
    else:
        print("  TOTAL DAILY sheet empty or unreadable.")

    if args.save_diff and reports:
        out_dir = _PERF_OUT
        out_dir.mkdir(parents=True, exist_ok=True)
        for name, df in reports:
            path = out_dir / f"debug_diff_{name}.csv"
            df.to_csv(path)
            print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
