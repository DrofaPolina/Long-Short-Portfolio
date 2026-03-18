"""
Check that pooling tickers from *_positions.xlsx yields the same sets as Tickers.xlsx.

Run after run_all.py has produced:
  - outputs/factors/{value,momentum,quality,liquidity,lowvol,beta0,beta1,beta2}_positions.xlsx
  - data/Tickers.xlsx

Logic:
  - Positions files: four sheets each (long_us, short_us, long_eu, short_eu), columns Date, Ticker.
    For each sheet we take tickers on the latest date; then union across all position files per leg.
  - Tickers.xlsx: four sheets (long_us, short_us, long_eu, short_eu), column TICKER (or first column).
  - Compare the four pooled sets to the four Tickers.xlsx sets.

Usage:
  python check_tickers_pool.py
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import pandas as pd

import config

# #region agent log
DEBUG_LOG_PATH = Path("/Users/polina/alberblanc/.cursor/debug-0b625c.log")
def _debug_log(message: str, data: dict, hypothesis_id: str = ""):
    try:
        payload = {"id": f"log_{int(time.time()*1000)}", "timestamp": int(time.time() * 1000), "location": "check_tickers_pool.py", "message": message, "data": data, "hypothesisId": hypothesis_id}
        with open(DEBUG_LOG_PATH, "a") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        pass
# #endregion

FACTORS_DIR = config.get_output_path("factors")
TICKERS_PATH = config.get_data_path("Tickers.xlsx")

# Factors that produce *_positions.xlsx (run_all order; yield adds beta0/1/2)
POSITION_FILES = [
    "value_positions.xlsx",
    "momentum_positions.xlsx",
    "quality_positions.xlsx",
    "liquidity_positions.xlsx",
    "lowvol_positions.xlsx",
    "beta0_positions.xlsx",
    "beta1_positions.xlsx",
    "beta2_positions.xlsx",
]

SHEET_ORDER = ["long_us", "short_us", "long_eu", "short_eu"]


def _ticker_col(df: pd.DataFrame) -> str | None:
    """Column name for tickers: prefer TICKER, else first column."""
    if df is None or df.empty:
        return None
    for c in df.columns:
        if str(c).strip().upper() == "TICKER":
            return c
    return str(df.columns[0])


def load_tickers_xlsx(path: Path) -> dict[str, set[str]]:
    """Load Tickers.xlsx into four sets. Keys: long_us, short_us, long_eu, short_eu."""
    out = {s: set() for s in SHEET_ORDER}
    for sheet in SHEET_ORDER:
        try:
            df = pd.read_excel(path, sheet_name=sheet, header=0)
        except Exception:
            continue
        col = _ticker_col(df)
        if col:
            raw = df[col].astype(str).str.strip().replace("", pd.NA).dropna().unique().tolist()
            out[sheet] = set(raw)
            # #region agent log
            unnamed_in_sheet = [t for t in raw if (re.match(r"Unnamed\s*:\s*\d+", t, re.I) or "unnamed" in t.lower())]
            if unnamed_in_sheet:
                _debug_log("load_tickers_xlsx: unnamed values in Tickers.xlsx", {"sheet": sheet, "col": col, "unnamed_count": len(unnamed_in_sheet), "unnamed_sample": unnamed_in_sheet[:15]}, "H4")
            # #endregion
    return out


def pool_from_positions(factors_dir: Path, filenames: list[str]) -> dict[str, set[str]]:
    """
    Pool tickers from all *_positions.xlsx: for each sheet, take tickers on latest date
    in that sheet, then union across files. Returns same keys as load_tickers_xlsx.
    """
    pooled = {s: set() for s in SHEET_ORDER}
    for filename in filenames:
        path = factors_dir / filename
        if not path.exists():
            continue
        for sheet in SHEET_ORDER:
            try:
                df = pd.read_excel(path, sheet_name=sheet)
            except Exception:
                continue
            if df.empty:
                continue
            df.columns = [str(c).strip() for c in df.columns]
            if "Date" not in df.columns or "Ticker" not in df.columns:
                continue
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"])
            if df.empty:
                continue
            use_date = df["Date"].max()
            tickers = (
                df.loc[df["Date"] == use_date, "Ticker"]
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .unique()
                .tolist()
            )
            # #region agent log
            unnamed_here = [t for t in tickers if (re.match(r"Unnamed\s*:\s*\d+", t, re.I) or "unnamed" in t.lower()) or t.lower() == "nan"]
            if unnamed_here:
                _debug_log("pool_from_positions: unnamed/nan in position file", {"file": filename, "sheet": sheet, "unnamed_sample": unnamed_here[:15], "all_columns": list(df.columns)}, "H1")
            # #endregion
            pooled[sheet].update(tickers)
    return pooled


def write_tickers_from_positions(factors_dir: Path, tickers_path: Path) -> bool:
    """
    Build Tickers.xlsx by pooling from *_positions.xlsx. Same logic as run_all's
    intended ticker set. Returns True if at least one position file was found and written.
    """
    pooled = pool_from_positions(factors_dir, POSITION_FILES)
    found_any = any((factors_dir / f).exists() for f in POSITION_FILES)
    if not found_any:
        return False
    # #region agent log
    for sheet in SHEET_ORDER:
        bad = [t for t in pooled[sheet] if (re.match(r"Unnamed\s*:\s*\d+", t, re.I) or "unnamed" in t.lower()) or t.lower() == "nan"]
        if bad:
            _debug_log("write_tickers_from_positions: would write unnamed/nan", {"sheet": sheet, "bad_count": len(bad), "bad_sample": bad[:15]}, "H3")
    # #endregion
    tickers_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(tickers_path, engine="openpyxl") as w:
        for sheet in SHEET_ORDER:
            pd.DataFrame({"TICKER": sorted(pooled[sheet])}).to_excel(
                w, sheet_name=sheet, index=False
            )
    return True


def main():
    print("=" * 60)
    print("TICKERS POOL CHECK: positions.xlsx vs Tickers.xlsx")
    print("=" * 60)

    if not TICKERS_PATH.exists():
        print(f"\nTickers.xlsx not found: {TICKERS_PATH}")
        print("Run run_all.py first to generate it.")
        return

    from_file = load_tickers_xlsx(TICKERS_PATH)
    print(f"\nTickers.xlsx: {TICKERS_PATH}")
    for s in SHEET_ORDER:
        print(f"  {s}: {len(from_file[s])} tickers")

    if not FACTORS_DIR.exists():
        print(f"\nFactors output dir not found: {FACTORS_DIR}")
        return

    pooled = pool_from_positions(FACTORS_DIR, POSITION_FILES)
    found = [f for f in POSITION_FILES if (FACTORS_DIR / f).exists()]
    print(f"\nPooled from {len(found)} position files: {found}")
    for s in SHEET_ORDER:
        print(f"  {s}: {len(pooled[s])} tickers")

    # Compare
    all_match = True
    print("\n--- Comparison ---")
    for sheet in SHEET_ORDER:
        a, b = from_file[sheet], pooled[sheet]
        only_in_tickers = a - b
        only_in_pooled = b - a
        match = a == b
        all_match = all_match and match
        status = "OK" if match else "DIFF"
        print(f"  {sheet}: {status}")
        if not match:
            if only_in_tickers:
                print(f"    Only in Tickers.xlsx ({len(only_in_tickers)}): {sorted(only_in_tickers)[:10]}{'...' if len(only_in_tickers) > 10 else ''}")
            if only_in_pooled:
                print(f"    Only in pooled positions ({len(only_in_pooled)}): {sorted(only_in_pooled)[:10]}{'...' if len(only_in_pooled) > 10 else ''}")

    if all_match:
        print("\n  Result: Pooled tickers from *_positions.xlsx match Tickers.xlsx.")
    else:
        print("\n  Result: Mismatch. Check that run_all wrote both from the same factor run.")


if __name__ == "__main__":
    main()
