"""
Liquidity Factor (Amihud Illiquidity, long–short 10–1)
======================================================

This is a script version of `Liquidity_Factor.ipynb`.
It keeps the original Amihud (2002) illiquidity logic:

- Read monthly prices and monthly dollar value traded from a wide Excel file
- Prefilter tickers by data coverage
- Compute |return| / dollar_value (Amihud illiquidity)
- Form cross‑sectional deciles each month
- Build a long–short factor: long decile 10, short decile 1, equal‑weighted

Here we:
- Wrap the original Colab pipeline into reusable functions
- Remove Colab‑specific pieces (`drive.mount`, `input()`)
- Expose a `calculate_liquidity_factor_monthly` entry point consistent with
  the other factor scripts in `src/`.

You MUST set `LIQUIDITY_FILE_PATH` to the path of the Excel file that
contains the SP500 / STOXX600 price and dollar‑value sheets, in the same
format as used in the original notebook.
"""

import gc
import os
from pathlib import Path

import numpy as np
import pandas as pd


# =============================================================================
# CONFIG – EDIT THESE TO MATCH YOUR DATA FILE
# =============================================================================

# Path to the Excel file used in the original Liquidity_Factor.ipynb
# It must contain (at least) the sheets:
#   - "SP500"       : monthly prices for US universe
#   - "STOXX600"    : monthly prices for EU universe
#   - "Dollar Value US" or "Dollar Volume US"
#   - "Dollar Value EU" or "Dollar Volume EU"
# Default: data/Minerva_Size_Factor.xlsx (override or pass file_path to calculate_liquidity_factor_monthly)
LIQUIDITY_FILE_PATH = str(Path(__file__).resolve().parent.parent / "data" / "Minerva_Size_Factor.xlsx")

# Minimum non‑null months of data for both prices and dollar value
MIN_MONTHS = 24

# Optional cap on number of tickers (None = no cap)
MAX_TICKERS = 700

# Number of columns per batch for illiquidity computation
BATCH_SIZE = 300


# =============================================================================
# HELPERS (adapted from Liquidity_Factor.ipynb)
# =============================================================================

def read_wide_row0_header(path, sheet, dayfirst=True):
    """
    Read an Excel sheet where row 0 contains headers and there may be
    an empty first column.

    Returns a tidy DataFrame with a 'date' column and numeric columns
    per ticker. Numeric data are downcast to float32 to save memory.
    """
    raw = pd.read_excel(path, sheet_name=sheet, header=None)
    header = raw.iloc[0].tolist()

    # Sometimes the first column is empty: drop it and recompute header
    if pd.isna(header[0]):
        raw = raw.drop(columns=[0])
        header = raw.iloc[0].tolist()

    df = raw.iloc[1:].copy()
    del raw
    df.columns = header

    # Ensure the first column is named 'date'
    if df.columns[0] != "date":
        for c in df.columns:
            if str(c).strip().lower() == "date":
                df = df.rename(columns={c: "date"})
                break

    # Parse dates and coerce numeric columns
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=dayfirst)
    num_cols = []
    for c in df.columns[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        num_cols.append(c)

    # Downcast numeric to float32
    if num_cols:
        df[num_cols] = df[num_cols].astype("float32")

    # Clean‑up
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df = df.dropna(axis=1, how="all")
    gc.collect()
    return df


def prefilter_universe(prices_wide, dval_wide, min_months=24, max_tickers=None):
    """
    Keep only tickers that:
      - appear in both sheets
      - have at least `min_months` non‑null price months AND strictly
        positive dollar‑value months.

    Optionally cap to the top `max_tickers` by combined coverage.

    Returns: (px_filtered, dv_filtered, kept_tickers_list)
    """
    px = prices_wide.copy()
    px["date"] = pd.to_datetime(px["date"])
    px = px.set_index("date").sort_index()

    dv = dval_wide.copy()
    dv["date"] = pd.to_datetime(dv["date"])
    dv = dv.set_index("date").sort_index()

    # Common tickers only
    common = sorted(set(px.columns).intersection(dv.columns))
    if not common:
        raise ValueError("No common tickers between prices and dollar‑value sheets.")

    px = px[common].astype("float32")
    dv = dv[common].astype("float32")

    # Coverage: valid months for price and strictly positive months for dollar value
    px_valid = px.notna().sum()
    dv_valid = (dv > 0).sum()
    coverage = (px_valid.add(dv_valid)).sort_values(ascending=False)

    keep = coverage[(px_valid >= min_months) & (dv_valid >= min_months)].index.tolist()

    # Optional cap to reduce memory
    if max_tickers is not None and len(keep) > max_tickers:
        keep = coverage.loc[keep].head(max_tickers).index.tolist()

    # Reduce matrices early
    px = px[keep]
    dv = dv[keep]

    gc.collect()
    return px, dv, keep


def illiq_exposures_monthly_batched(px_indexed, dv_indexed, batch_size=300):
    """
    Compute Amihud illiquidity in column batches to cap peak memory.

    illiq_{m,i} = |ret_{m,i}| / dollar_volume_{m,i}

    Inputs must be indexed by 'date' and share identical columns.
    Returns a wide DataFrame (float32) with same index and (subset of)
    columns.
    """
    cols = list(px_indexed.columns)
    n = len(cols)
    out_chunks = []

    # Pre‑compute monthly returns once (float32 in -> float32 out)
    ret_m = px_indexed.pct_change()
    dv_pos = dv_indexed.copy()
    dv_pos[dv_pos <= 0] = np.nan

    for i in range(0, n, batch_size):
        sub = cols[i : i + batch_size]
        r_sub = ret_m[sub]
        dv_sub = dv_pos[sub]
        illiq_sub = (r_sub.abs() / dv_sub).astype("float32")
        illiq_sub = illiq_sub.dropna(axis=1, how="all")  # drop all‑empty columns
        out_chunks.append(illiq_sub)

        # Free intermediates
        del r_sub, dv_sub, illiq_sub
        gc.collect()

    if not out_chunks:
        return pd.DataFrame(index=px_indexed.index)

    illiq = pd.concat(out_chunks, axis=1)
    illiq = illiq.dropna(axis=0, how="all").dropna(axis=1, how="all")
    gc.collect()
    return illiq


def winsorize_monthly(x, p=0.01):
    """
    Clip a cross‑section to the [p, 1-p] quantiles.
    Safe on empty/constant series. (From Liquidity_Factor.ipynb add‑ons.)
    """
    if x is None or len(x) == 0:
        return x
    q = x.quantile([p, 1 - p])
    lo = q.iloc[0] if not np.isnan(q.iloc[0]) else x.min()
    hi = q.iloc[1] if not np.isnan(q.iloc[1]) else x.max()
    if lo > hi:
        lo, hi = hi, lo
    return x.clip(lo, hi)


def deciles_from_exposures_fast(exposures_wide, q=10, min_names=20):
    """
    Assign deciles per month in a memory‑conscious loop:
      - For each month, rank cross‑sectionally (method='first')
      - qcut the ranks into q buckets

    Returns a long DataFrame with columns: date, ticker, illiq, decile
    """
    exp = exposures_wide.copy()
    exp = exp.loc[:, exp.columns.notna()]
    dates = exp.index

    out_rows = []
    for dt in dates:
        s = exp.loc[dt].dropna()
        if s.size < min_names:
            continue
        ranked = s.rank(method="first")
        dec = pd.qcut(ranked, q, labels=range(1, q + 1))
        tmp = pd.DataFrame(
            {
                "date": dt,
                "ticker": s.index,
                "illiq": s.values,
                "decile": dec.astype(int).values,
            }
        )
        out_rows.append(tmp)

    if out_rows:
        res = pd.concat(out_rows, ignore_index=True)
        del out_rows
        gc.collect()
        return res
    else:
        return pd.DataFrame(columns=["date", "ticker", "illiq", "decile"])


def deciles_from_exposures(exposures_wide, q=10, min_names=60):
    """
    Convert wide exposures (date x ticker) to a tidy decile table.
    Winsorizes exposures each month; requires at least min_names per month.
    (From Liquidity_Factor.ipynb add‑ons.)
    Returns columns: date, ticker, illiq, decile
    """
    if exposures_wide is None or exposures_wide.empty:
        return pd.DataFrame(columns=["date", "ticker", "illiq", "decile"])
    out = []
    for dt, row in exposures_wide.iterrows():
        s = row.dropna()
        if len(s) < min_names:
            continue
        s = winsorize_monthly(s, 0.01)
        ranks = s.rank(method="first")
        try:
            dec = pd.qcut(ranks, q, labels=range(1, q + 1))
        except Exception:
            dec = pd.cut(
                ranks.rank(method="first"), q,
                labels=range(1, q + 1), include_lowest=True
            )
        out.append(
            pd.DataFrame({
                "date": dt,
                "ticker": s.index,
                "illiq": s.values,
                "decile": dec.astype(int).values,
            })
        )
    return (
        pd.concat(out, ignore_index=True)
        if out
        else pd.DataFrame(columns=["date", "ticker", "illiq", "decile"])
    )


def long_short_table(wide_w):
    """
    Convert wide weights (date x ticker) to a tidy table with LONG/SHORT flags.
    (From Liquidity_Factor.ipynb add‑ons.)
    """
    if wide_w is None or wide_w.empty:
        return pd.DataFrame(columns=["date", "side", "ticker", "weight", "abs_weight"])
    df = wide_w.stack().rename("weight").reset_index()
    df.columns = ["date", "ticker", "weight"]
    df = df[df["weight"] != 0]
    df["side"] = np.where(df["weight"] > 0, "LONG", "SHORT")
    df["abs_weight"] = df["weight"].abs()
    df = df.sort_values(["date", "side", "abs_weight"], ascending=[True, True, False])
    df["side"] = pd.Categorical(df["side"], categories=["LONG", "SHORT"], ordered=True)
    return df[["date", "side", "ticker", "weight", "abs_weight"]]


def tidy_picks(picks_df):
    """Tidy picks (no weights) from a picks DataFrame with date, longs, shorts."""
    if picks_df is None or picks_df.empty:
        return pd.DataFrame(columns=["date", "side", "ticker"])
    rows = []
    for _, r in picks_df.iterrows():
        dt = pd.to_datetime(r["date"])
        for t in str(r.get("longs", "")).split(","):
            t = t.strip()
            if t:
                rows.append((dt, "LONG", t))
        for t in str(r.get("shorts", "")).split(","):
            t = t.strip()
            if t:
                rows.append((dt, "SHORT", t))
    res = pd.DataFrame(rows, columns=["date", "side", "ticker"])
    return res.sort_values(["date", "side", "ticker"]).reset_index(drop=True) if not res.empty else pd.DataFrame(columns=["date", "side", "ticker"])


def longshort_from_deciles(
    deciles_long, monthly_returns, long_dec=10, short_dec=1, hold_next=True
):
    """
    Build a monthly long‑short factor:
      - Long the highest illiquidity decile (default: 10)
      - Short the lowest illiquidity decile (default: 1)
      - Equal‑weight within each side
      - If hold_next=True, apply signals formed at month t to returns at t+1

    Returns:
      factor (Series), picks (DataFrame), weights (DataFrame)
    """
    sig = deciles_long.copy()
    sig["date"] = pd.to_datetime(sig["date"])
    if hold_next:
        sig["date"] = sig["date"] + pd.offsets.MonthEnd(1)

    mr = monthly_returns.copy()
    mr.index = pd.to_datetime(mr.index)
    months = sorted(set(sig["date"]).intersection(set(mr.index)))

    ls, picks, weights = [], [], []
    for dt in months:
        sel = sig[sig["date"] == dt]
        L = [
            t
            for t in sel.loc[sel["decile"] == long_dec, "ticker"]
            if t in mr.columns
        ]
        S = [
            t
            for t in sel.loc[sel["decile"] == short_dec, "ticker"]
            if t in mr.columns
        ]
        if len(L) < 5 or len(S) < 5:
            continue

        ls.append((dt, mr.loc[dt, L].mean() - mr.loc[dt, S].mean()))
        picks.append(
            {
                "date": dt,
                "long_count": len(L),
                "short_count": len(S),
                "longs": ",".join(sorted(L)),
                "shorts": ",".join(sorted(S)),
            }
        )
        weights.append(
            {
                "date": dt,
                **{t: 1 / len(L) for t in L},
                **{t: -1 / len(S) for t in S},
            }
        )

    factor = pd.Series(
        {dt: v for dt, v in ls}, name="ILLIQ_LS_return"
    ).sort_index()
    picks = pd.DataFrame(picks).sort_values("date")
    weights = pd.DataFrame(weights).set_index("date").sort_index().fillna(0.0)
    gc.collect()
    return factor, picks, weights


# =============================================================================
# PIPELINE FOR A SINGLE DATASET (SP500 or STOXX600)
# =============================================================================

def _run_illiq_pipeline_for_dataset(
    file_path,
    dataset="SP500",
    min_months=MIN_MONTHS,
    max_tickers=MAX_TICKERS,
    batch_size=BATCH_SIZE,
):
    """
    Run the full Amihud illiquidity pipeline for a single dataset.

    Parameters
    ----------
    file_path : str or Path
        Excel file path.
    dataset : {'SP500', 'STOXX600'}
        Which sheet pair to use.

    Returns
    -------
    factor_ls : pd.Series
        Long–short illiquidity factor (monthly)
    deciles : pd.DataFrame
        Long table of monthly decile assignments.
    picks : pd.DataFrame
        Long/short selections per month.
    weights : pd.DataFrame
        Long/short weights per month (rows = dates, cols = tickers).
    region_tag : {'US', 'EU'}
        Region label.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Liquidity Excel file not found: {file_path}")

    dataset = dataset.upper()
    if dataset not in {"SP500", "STOXX600"}:
        raise ValueError(f"Unknown dataset '{dataset}'. Choose 'SP500' or 'STOXX600'.")

    xls = pd.ExcelFile(file_path)
    available_sheets = set(xls.sheet_names)

    price_sheet = dataset
    if price_sheet not in available_sheets:
        raise ValueError(
            f"Price sheet '{price_sheet}' not found. "
            f"Available: {sorted(available_sheets)}"
        )

    if dataset == "SP500":
        dv_candidates = ["Dollar Value US", "Dollar Volume US"]
        region_tag = "US"
    else:
        dv_candidates = ["Dollar Value EU", "Dollar Volume EU"]
        region_tag = "EU"

    dv_sheet = next((s for s in dv_candidates if s in available_sheets), None)
    if dv_sheet is None:
        raise ValueError(
            f"No dollar‑value sheet found for {dataset}. "
            f"Tried: {dv_candidates}. Available: {sorted(available_sheets)}"
        )

    print(f"→ [{region_tag}] Using price sheet: '{price_sheet}'")
    print(f"→ [{region_tag}] Using dollar‑value sheet: '{dv_sheet}'")

    # 1) Read raw sheets
    prices_raw = read_wide_row0_header(file_path, price_sheet)
    dval_raw = read_wide_row0_header(file_path, dv_sheet)

    # 2) Prefilter universe, then compute monthly returns BEFORE dropping px
    px, dv, kept = prefilter_universe(
        prices_raw, dval_raw, min_months=min_months, max_tickers=max_tickers
    )
    print(
        f"[{region_tag}] Kept {len(kept)} tickers "
        f"(min_months={min_months}, max_tickers={max_tickers})."
    )

    # Free the big raw DataFrames early
    del prices_raw, dval_raw
    gc.collect()

    # 3) Monthly returns from filtered prices (keep as float32)
    monthly_ret = px.pct_change().astype("float32")

    # 4) Illiquidity in batches on the same filtered universe
    illiq_wide = illiq_exposures_monthly_batched(px, dv, batch_size=batch_size)

    # Free px/dv if not needed further
    del px, dv
    gc.collect()

    # 5) Build deciles (winsorized version from notebook for robustness) and L/S factor
    deciles = deciles_from_exposures(illiq_wide, q=10, min_names=60)
    if deciles.empty:
        deciles = deciles_from_exposures_fast(illiq_wide, q=10, min_names=20)
    factor_ls, picks, weights = longshort_from_deciles(
        deciles, monthly_ret, long_dec=10, short_dec=1, hold_next=True
    )

    return factor_ls, deciles, picks, weights, region_tag


# =============================================================================
# Default: write to project root outputs/factors (same regardless of cwd)
_DEFAULT_FACTOR_OUTPUT = str(Path(__file__).resolve().parent.parent / "outputs" / "factors")

# PUBLIC ENTRY POINT (like other factor scripts)
# =============================================================================

def calculate_liquidity_factor_monthly(
    file_path: str = None,
    save_outputs: bool = True,
    output_dir: str = None,
) -> dict:
    """
    Main function to calculate liquidity factor (Amihud illiquidity).

    Computes separate factors for US (SP500) and EU (STOXX600) and
    returns both, plus a combined series.

    Parameters
    ----------
    file_path : str, optional
        Excel file path. If None, uses LIQUIDITY_FILE_PATH.
    save_outputs : bool
        Whether to save results under `output_dir`.
    output_dir : str
        Directory where Excel outputs will be written.

    Returns
    -------
    dict
        {
          'US'      : pd.Series (LIQ_US),
          'EU'      : pd.Series (LIQ_EU),
          'combined': pd.DataFrame with columns ['LIQ_US', 'LIQ_EU', 'LIQ']
        }
    """
    if output_dir is None:
        output_dir = _DEFAULT_FACTOR_OUTPUT
    print("=" * 60)
    print("LIQUIDITY FACTOR CALCULATION (Amihud 10–1)")
    print("=" * 60)

    if file_path is None:
        file_path = LIQUIDITY_FILE_PATH

    # Run pipeline for US and EU
    print("\n[1/4] Running US (SP500) pipeline...")
    factor_us, dec_us, picks_us, weights_us, tag_us = _run_illiq_pipeline_for_dataset(
        file_path, dataset="SP500"
    )

    print("\n[2/4] Running EU (STOXX600) pipeline...")
    factor_eu, dec_eu, picks_eu, weights_eu, tag_eu = _run_illiq_pipeline_for_dataset(
        file_path, dataset="STOXX600"
    )

    # Build regional and combined series
    liq_us = factor_us.rename("LIQ_US")
    liq_eu = factor_eu.rename("LIQ_EU")

    combined = pd.DataFrame({"LIQ_US": liq_us, "LIQ_EU": liq_eu})
    combined["LIQ"] = combined.mean(axis=1, skipna=True)

    results = {
        "US": liq_us,
        "EU": liq_eu,
        "combined": combined,
    }

    # Save outputs
    if save_outputs:
        print(f"\n[3/4] Saving outputs to {output_dir}/...")
        os.makedirs(output_dir, exist_ok=True)

        # Combined factor returns (for portfolio builder)
        liq_returns = pd.DataFrame({"LIQ": combined["LIQ"]})
        liq_returns.to_excel(f"{output_dir}/liquidity_returns.xlsx")
        print("  ✓ Saved liquidity_returns.xlsx")

        # Regional breakdown
        combined.to_excel(f"{output_dir}/liquidity_regional.xlsx")
        print("  ✓ Saved liquidity_regional.xlsx")

        # Optionally also export the detailed CSVs from original notebook
        # (deciles, picks, weights) for US/EU, with region tags.
        us_dir = Path(output_dir) / "liquidity_US_detail"
        eu_dir = Path(output_dir) / "liquidity_EU_detail"
        us_dir.mkdir(parents=True, exist_ok=True)
        eu_dir.mkdir(parents=True, exist_ok=True)

        dec_us.to_csv(us_dir / "illiq_deciles_US.csv", index=False)
        picks_us.to_csv(us_dir / "illiq_portfolios_US.csv", index=False)
        weights_us.to_csv(us_dir / "illiq_weights_US.csv")

        dec_eu.to_csv(eu_dir / "illiq_deciles_EU.csv", index=False)
        picks_eu.to_csv(eu_dir / "illiq_portfolios_EU.csv", index=False)
        weights_eu.to_csv(eu_dir / "illiq_weights_EU.csv")

        print("  ✓ Saved detailed illiquidity CSVs (US/EU)")

    print("\n" + "=" * 60)
    print("LIQUIDITY FACTOR CALCULATION COMPLETE")
    print("=" * 60)

    # Quick summary
    for name, series in [("LIQ_US", liq_us), ("LIQ_EU", liq_eu), ("LIQ", combined["LIQ"])]:
        s = series.dropna()
        if len(s) > 0:
            sharpe = s.mean() / s.std() * np.sqrt(12) if s.std() > 0 else np.nan
            print(f"  {name:6s}: obs={len(s):4d}, mean={s.mean():+.4f}, std={s.std():.4f}, Sharpe={sharpe: .3f}")

    return results


if __name__ == "__main__":
    # Run directly: python liquidity.py
    _ = calculate_liquidity_factor_monthly(
        file_path=LIQUIDITY_FILE_PATH,
        save_outputs=True,
        output_dir=_DEFAULT_FACTOR_OUTPUT,
    )

