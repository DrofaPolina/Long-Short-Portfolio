"""
TSFM Stock Weights

Combines TSFM factor weights (from factor_momentum.py) with per-factor stock
positions (from *_positions.xlsx) to produce final stock-level weights in the
same four-sheet format as hrp_weights.xlsx.

Logic:
  stock_weight = TSFM_factor_weight × (1 / N_stocks_in_leg) × leg_direction
  where leg_direction = +1 for long leg, -1 for short leg

  Negative TSFM weight flips the factor:
    long leg stocks  → become short contributors
    short leg stocks → become long contributors

  Contributions from the same stock across multiple factors are summed.
  Final weights are normalized so long sums to +1, short sums to -1.
  Weights are capped at ±MAX_WEIGHT per stock.

Inputs:
  outputs/factors/*_positions.xlsx      (one per factor, four sheets each)
  outputs/hrp_weights/factor_momentum_weights.xlsx  (current sheet)

Output:
  outputs/hrp_weights/tsfm_stock_weights.xlsx
    Sheets: long_us, short_us, long_eu, short_eu
    Columns: Ticker, Weight   (matching hrp_weights.xlsx format exactly)

Usage:
    python tsfm_stock_weights.py [--date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import sys
import json
import re
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

import config

ROOT        = config.PROJECT_ROOT
FACTORS_DIR = config.get_output_path("factors")
HRP_DIR     = config.get_output_path("hrp_weights")
OUT_FILE    = HRP_DIR / "tsfm_stock_weights.xlsx"

TSFM_WEIGHTS_FILE = HRP_DIR / "factor_momentum_weights.xlsx"

# Position filename pattern: factor name → outputs/factors/{stem}_positions.xlsx
# Stems are derived from factor names in config.ACTIVE_FACTORS
_FACTOR_STEMS = {
    "VAL":   "value",
    "MOM":   "momentum",
    "LVOL":  "lowvol",
    "QLT":   "quality",
    "SIZE":  "size",
    "BETA0": "beta0",
    "BETA1": "beta1",
    "BETA2": "beta2",
}

def _get_active_position_files() -> dict[str, str]:
    """
    Build {FACTOR: filename} for all active factors that have a positions file stem.
    Driven entirely by config.ACTIVE_FACTORS — add new factors there, not here.
    """
    return {
        factor: f"{_FACTOR_STEMS[factor]}_positions.xlsx"
        for factor in config.get_active_factors()
        if factor in _FACTOR_STEMS
    }

REGIONS    = ["us", "eu"]
LEGS       = ["long", "short"]

# Weight cap per stock — taken from config MVO_CONFIG
MAX_WEIGHT = config.MVO_CONFIG.get("max_weight", 0.05)

# #region agent log
DEBUG_LOG_PATH = Path("/Users/polina/alberblanc/.cursor/debug-0b625c.log")
def _debug_log(message: str, data: dict, hypothesis_id: str = "", run_id: str = "pre-fix"):
    try:
        payload = {
            "sessionId": "0b625c",
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "id": f"log_{int(time.time()*1000)}",
            "timestamp": int(time.time() * 1000),
            "location": "tsfm_stock_weights.py",
            "message": message,
            "data": data,
        }
        with open(DEBUG_LOG_PATH, "a") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        pass

_UNNAMED_RE = re.compile(r"^\s*Unnamed\s*:\s*\d+\s*$", re.IGNORECASE)
def _is_bad_ticker(t: str) -> bool:
    s = str(t).strip()
    if not s:
        return True
    if s.lower() in {"nan", "none"}:
        return True
    if _UNNAMED_RE.match(s):
        return True
    return False
# #endregion


# ---------------------------------------------------------------------------
# 1. Load TSFM factor weights
# ---------------------------------------------------------------------------

def load_tsfm_weights(date: pd.Timestamp | None = None) -> dict[str, float]:
    """
    Load factor weights from factor_momentum_weights.xlsx.

    If date is None → use the 'current' sheet (latest rebalance).
    If date is given → look up that date in the 'history' sheet.

    Returns dict {factor: weight}, e.g. {'VAL': 0.32, 'MOM': 0.40, ...}
    """
    if not TSFM_WEIGHTS_FILE.exists():
        raise FileNotFoundError(
            f"TSFM weights not found: {TSFM_WEIGHTS_FILE}\n"
            "Run factor_momentum.py first."
        )

    if date is None:
        df = pd.read_excel(TSFM_WEIGHTS_FILE, sheet_name="current")
        # Expects columns: Factor, Weight
        return dict(zip(df["Factor"].str.strip().str.upper(), df["Weight"]))

    history = pd.read_excel(TSFM_WEIGHTS_FILE, sheet_name="history")
    history["Date"] = pd.to_datetime(history["Date"])
    row = history[history["Date"] == date]
    if row.empty:
        # Fall back to most recent date before the requested date
        past = history[history["Date"] <= date]
        if past.empty:
            raise ValueError(f"No TSFM weights available on or before {date}")
        row = past.iloc[[-1]]
    factor_cols = [c for c in row.columns if c != "Date"]
    return {c.upper(): float(row.iloc[0][c]) for c in factor_cols}


# ---------------------------------------------------------------------------
# 2. Load stock positions for one factor
# ---------------------------------------------------------------------------

def load_positions(
    factor: str,
    filename: str,
    date: pd.Timestamp | None = None,
) -> dict[str, dict[str, list[str]]]:
    """
    Load stock positions from *_positions.xlsx for the given factor.

    Returns nested dict:
        { region: { leg: [ticker, ...] } }
        e.g. { 'us': { 'long': ['AAPL', 'MSFT'], 'short': ['JPM'] },
               'eu': { 'long': ['ASML.AS'],        'short': ['VOD.L'] } }

    If date is None → uses the last available rebalance date in the file.
    """
    path = FACTORS_DIR / filename
    if not path.exists():
        print(f"  Warning: positions file not found for {factor}: {path}")
        # #region agent log
        _debug_log(
            "load_positions: positions file missing",
            {"factor": factor, "filename": filename, "path": str(path)},
            hypothesis_id="H_missing_positions_file",
        )
        # #endregion
        return {r: {l: [] for l in LEGS} for r in REGIONS}

    result: dict[str, dict[str, list[str]]] = {
        r: {l: [] for l in LEGS} for r in REGIONS
    }

    for region in REGIONS:
        for leg in LEGS:
            sheet = f"{leg}_{region}"
            try:
                df = pd.read_excel(path, sheet_name=sheet)
            except Exception:
                # #region agent log
                _debug_log(
                    "load_positions: missing sheet or read error",
                    {"factor": factor, "file": str(path), "sheet": sheet},
                    hypothesis_id="H_missing_sheet",
                )
                # #endregion
                continue  # sheet may not exist for this factor/region

            if df.empty:
                # #region agent log
                _debug_log(
                    "load_positions: sheet empty",
                    {"factor": factor, "file": str(path), "sheet": sheet},
                    hypothesis_id="H_empty_sheet",
                )
                # #endregion
                continue

            df.columns = [c.strip() for c in df.columns]
            if "Date" not in df.columns or "Ticker" not in df.columns:
                print(f"  Warning: {factor} {sheet} missing Date/Ticker columns")
                continue

            df["Date"] = pd.to_datetime(df["Date"])

            # #region agent log
            if factor == "QLT":
                try:
                    _debug_log(
                        "load_positions: QLT date inventory",
                        {
                            "factor": factor,
                            "file": str(path),
                            "sheet": sheet,
                            "date_arg": (str(date) if date is not None else None),
                            "min_date": (str(df["Date"].min()) if not df["Date"].empty else None),
                            "max_date": (str(df["Date"].max()) if not df["Date"].empty else None),
                            "n_rows": int(len(df)),
                            "n_unique_dates": int(df["Date"].nunique(dropna=True)),
                        },
                        hypothesis_id="H_qlt_date_mismatch",
                    )
                except Exception:
                    pass
            # #endregion

            if date is None:
                use_date = df["Date"].max()
            else:
                available = df["Date"][df["Date"] <= date]
                if available.empty:
                    # #region agent log
                    if factor == "QLT":
                        _debug_log(
                            "load_positions: QLT no available dates <= date_arg",
                            {
                                "factor": factor,
                                "file": str(path),
                                "sheet": sheet,
                                "date_arg": str(date),
                                "min_date": (str(df["Date"].min()) if not df["Date"].empty else None),
                            },
                            hypothesis_id="H_qlt_date_mismatch",
                        )
                    # #endregion
                    continue
                use_date = available.max()

            tickers_raw = (
                df[df["Date"] == use_date]["Ticker"]
                .dropna()
                .astype(str)
                .str.strip()
                .tolist()
            )
            tickers = [t for t in tickers_raw if not _is_bad_ticker(t)]
            # #region agent log
            if factor == "QLT":
                _debug_log(
                    "load_positions: QLT tickers loaded",
                    {
                        "factor": factor,
                        "file": str(path),
                        "sheet": sheet,
                        "use_date": str(use_date),
                        "raw_count": int(len(tickers_raw)),
                        "kept_count": int(len(tickers)),
                        "raw_sample": tickers_raw[:10],
                    },
                    hypothesis_id="H_qlt_empty_after_filter",
                )
            bad = [t for t in tickers_raw if _is_bad_ticker(t)]
            if bad:
                _debug_log(
                    "load_positions: bad tickers filtered out",
                    {
                        "factor": factor,
                        "file": str(path),
                        "sheet": sheet,
                        "use_date": str(use_date),
                        "bad_count": len(bad),
                        "kept_count": len(tickers),
                        "bad_sample": bad[:20],
                    },
                    hypothesis_id="H_src_positions",
                )
            # #endregion
            result[region][leg] = tickers

    return result


# ---------------------------------------------------------------------------
# 3. Compute stock weights
# ---------------------------------------------------------------------------

def compute_stock_weights(
    tsfm_weights: dict[str, float],
    all_positions: dict[str, dict[str, dict[str, list[str]]]],
) -> dict[str, dict[str, float]]:
    """
    Combine TSFM factor weights with stock positions to produce final weights.

    Parameters
    ----------
    tsfm_weights  : {factor: weight}  e.g. {'VAL': 0.32, 'MOM': -0.18}
    all_positions : {factor: {region: {leg: [tickers]}}}

    Returns
    -------
    raw_weights : {sheet_name: {ticker: weight}}
        sheet_name in ['long_us', 'short_us', 'long_eu', 'short_eu']
        weights before normalization and capping — can be positive or negative

    Algorithm
    ---------
    For each factor f, region r, leg l:
        leg_direction = +1 if leg == 'long', -1 if leg == 'short'
        contribution  = tsfm_weight_f × leg_direction / N_stocks

    A negative TSFM weight flips the sign → long-leg stocks get negative
    contribution (they move to the short pool) and vice versa.

    Contributions for the same ticker are summed across factors.
    """
    # accumulate raw contributions per (region, ticker)
    raw: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for factor, tsfm_w in tsfm_weights.items():
        positions = all_positions.get(factor, {})

        for region in REGIONS:
            for leg in LEGS:
                tickers = positions.get(region, {}).get(leg, [])
                if not tickers:
                    continue

                leg_direction = 1.0 if leg == "long" else -1.0
                n = len(tickers)
                contribution = tsfm_w * leg_direction / n

                for ticker in tickers:
                    raw[region][ticker] += contribution

    return raw


def split_and_normalize(
    raw: dict[str, dict[str, float]],
) -> dict[str, pd.DataFrame]:
    """
    Split raw per-region weights into long/short sheets, normalize and cap.

    Returns four DataFrames keyed by sheet name:
        long_us, short_us, long_eu, short_eu
    Each DataFrame has columns [Ticker, Weight].
    Long weights  → positive, sum to 1.0, capped at +MAX_WEIGHT
    Short weights → negative, sum to -1.0, capped at -MAX_WEIGHT
    """
    sheets: dict[str, pd.DataFrame] = {}

    for region in REGIONS:
        region_raw = raw.get(region, {})
        if not region_raw:
            for leg in LEGS:
                sheets[f"{leg}_{region}"] = pd.DataFrame(
                    columns=["Ticker", "Weight"]
                )
            continue

        longs  = {t: w for t, w in region_raw.items() if w > 0 and not _is_bad_ticker(t)}
        shorts = {t: w for t, w in region_raw.items() if w < 0 and not _is_bad_ticker(t)}

        def normalize_and_cap(
            pool: dict[str, float], target_sum: float
        ) -> pd.DataFrame:
            if not pool:
                return pd.DataFrame(columns=["Ticker", "Weight"])

            s = pd.Series(pool)
            total = s.abs().sum()
            if total < 1e-12:
                n = len(s)
                s = pd.Series(
                    [target_sum / n] * n, index=s.index
                )
            else:
                s = s / total * abs(target_sum)

            # Iterative capping: redistribute excess weight
            cap = MAX_WEIGHT * np.sign(target_sum)
            for _ in range(100):
                capped   = s.clip(
                    lower=min(cap, 0) if target_sum < 0 else None,
                    upper=max(cap, 0) if target_sum > 0 else None,
                )
                excess   = (s - capped).sum()
                uncapped = capped[capped.abs() < MAX_WEIGHT - 1e-9]
                if abs(excess) < 1e-9 or uncapped.empty:
                    s = capped
                    break
                s = capped.copy()
                s[uncapped.index] += excess * (
                    uncapped.abs() / uncapped.abs().sum()
                )
            else:
                s = capped

            df = s.reset_index()
            df.columns = ["Ticker", "Weight"]
            return df.sort_values("Weight", ascending=(target_sum < 0)).reset_index(
                drop=True
            )

        sheets[f"long_{region}"]  = normalize_and_cap(longs,  +1.0)
        sheets[f"short_{region}"] = normalize_and_cap(shorts, -1.0)

    return sheets


# ---------------------------------------------------------------------------
# 4. Main
# ---------------------------------------------------------------------------

def run(date: pd.Timestamp | None = None) -> dict[str, pd.DataFrame]:
    print("=" * 60)
    print("TSFM STOCK WEIGHTS")
    print("=" * 60)

    # #region agent log
    _debug_log(
        "run: TSFM started",
        {"date_arg": (str(date) if date is not None else None)},
        hypothesis_id="H_tsfm_not_running",
    )
    # #endregion

    # Load TSFM factor weights
    print("\n[1/4] Loading TSFM factor weights...")
    tsfm_weights = load_tsfm_weights(date)
    for f, w in tsfm_weights.items():
        print(f"  {f:6s}: {w:+.4f}")

    # Load stock positions for each factor
    print("\n[2/4] Loading stock positions...")
    active_position_files = _get_active_position_files()
    all_positions: dict[str, dict[str, dict[str, list[str]]]] = {}
    for factor in tsfm_weights:
        if factor not in active_position_files:
            print(f"  Warning: no position file configured for {factor} (not in ACTIVE_FACTORS or no stem), skipping")
            continue
        filename = active_position_files[factor]
        positions = load_positions(factor, filename, date)
        all_positions[factor] = positions
        for region in REGIONS:
            for leg in LEGS:
                n = len(positions[region][leg])
                print(f"  {factor} {leg}_{region}: {n} stocks")

    # Compute raw stock weights
    print("\n[3/4] Computing stock weights...")
    raw = compute_stock_weights(tsfm_weights, all_positions)

    total_stocks = sum(len(v) for v in raw.values())
    print(f"  {total_stocks} unique stocks across all regions")

    # Normalize, cap, split into sheets
    print("\n[4/4] Normalizing and capping weights...")
    sheets = split_and_normalize(raw)

    for sheet_name, df in sheets.items():
        n = len(df)
        if n > 0:
            w_sum = df["Weight"].sum()
            print(f"  {sheet_name:12s}: {n:3d} stocks, weight sum = {w_sum:+.4f}")
        else:
            print(f"  {sheet_name:12s}: 0 stocks")

    # Save output
    HRP_DIR.mkdir(parents=True, exist_ok=True)
    sheet_order = ["long_eu", "short_eu", "long_us", "short_us"]
    # #region agent log
    for sheet_name in sheet_order:
        df = sheets.get(sheet_name, pd.DataFrame(columns=["Ticker", "Weight"]))
        if not df.empty and "Ticker" in df.columns:
            bad = [t for t in df["Ticker"].astype(str).tolist() if _is_bad_ticker(t)]
            if bad:
                _debug_log(
                    "run: bad tickers present before write",
                    {"sheet": sheet_name, "bad_count": len(bad), "bad_sample": bad[:20]},
                    hypothesis_id="H_before_write",
                )
    # #endregion
    with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as writer:
        for sheet_name in sheet_order:
            df = sheets.get(sheet_name, pd.DataFrame(columns=["Ticker", "Weight"]))
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"\n  ✓ Written: {OUT_FILE}")
    print(f"  Sheets: {sheet_order}")
    return sheets


def main():
    parser = argparse.ArgumentParser(description="Compute TSFM stock weights")
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
