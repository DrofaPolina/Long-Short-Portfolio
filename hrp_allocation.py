"""
HRP (Hierarchical Risk Parity) allocation script.

Follows HRP_Allocation.ipynb workflow: reads data from Google (Drive or Sheets),
runs HRP for long/short US and EU legs, and saves weights to file.

Data source (only from Google):
  - By default: download Tickers.xlsx, US_Returns.xlsx, EU_Returns.xlsx from Google Drive (gdown).
  - With --use-sheets: read from Google Sheets export URLs (no local files; set env or pass URLs).

Outputs:
  - hrp_weights.xlsx: all four legs (long_eu, long_us, short_eu, short_us) in one workbook
  - weights_long_us.csv, weights_short_us.csv: for use with us_portfolio_performance.py

Usage:
  python hrp_allocation.py
  python hrp_allocation.py --download
  python hrp_allocation.py --use-sheets [--tickers-url url] [--us-returns-url url] [--eu-returns-url url]
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import linkage, leaves_list
from scipy.spatial.distance import squareform


# Default paths / file IDs (from notebook)
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_TICKERS_FILE = PROJECT_ROOT / "Tickers.xlsx"
DEFAULT_US_RETURNS_FILE = PROJECT_ROOT / "US_Returns.xlsx"
DEFAULT_EU_RETURNS_FILE = PROJECT_ROOT / "EU_Returns.xlsx"
DEFAULT_OUT_DIR = PROJECT_ROOT / "outputs" / "hrp_weights"
TICKERS_DRIVE_ID = "1l6Ms10hLmhAveuuUaWM2JRn1GVVr1wbN"
EU_RETURNS_DRIVE_ID = "1oy0zrqGXpOW7rM1XQ5WU_9HlX6Spkr8f"
US_RETURNS_DRIVE_ID = "1jDIZKHsSfAKk61TJauSfhFSnTgx8nZgi"

# Google Sheets export (optional): set env TICKERS_SHEET_ID, US_RETURNS_SHEET_ID, EU_RETURNS_SHEET_ID and gids if using --use-sheets
HRP_CAP = 0.10
HRP_TARGET_SUM = 0.5


def norm(series: pd.Series) -> list[str]:
    """Normalize ticker column: strip, drop empty, unique list."""
    return (
        series.astype(str)
        .str.strip()
        .replace({"": pd.NA})
        .dropna()
        .unique()
        .tolist()
    )


def cap_proportional(
    w: np.ndarray,
    cap: float = HRP_CAP,
    target_sum: float = HRP_TARGET_SUM,
) -> np.ndarray:
    """Cap each weight at `cap` and redistribute excess. Normalize so sum = target_sum."""
    w = np.asarray(w, dtype=float).clip(min=0)
    if w.sum() == 0:
        return w
    w = w / w.sum()
    for _ in range(100):
        over = w > cap
        if not over.any():
            break
        excess = (w[over] - cap).sum()
        w[over] = cap
        under = ~over
        if under.sum() == 0 or excess <= 0:
            break
        share = w[under]
        w[under] += excess * (share / share.sum())
    w = np.clip(w, 0, cap)
    w = w / (w.sum() or 1)
    return w * target_sum


def recursive_bisection(V: np.ndarray, l: int, r: int, W: np.ndarray) -> np.ndarray:
    """HRP recursive bisection; updates W in place. V is sorted covariance matrix."""
    if r - l <= 1:
        return W
    mid = l + (r - l) // 2
    V1 = V[l:mid, l:mid]
    V2 = V[mid:r, mid:r]
    diag1 = np.diag(V1)
    diag2 = np.diag(V2)
    if diag1.size == 0 or diag2.size == 0:
        return W
    inv1 = 1.0 / np.maximum(diag1, 1e-12)
    inv2 = 1.0 / np.maximum(diag2, 1e-12)
    w1 = inv1 / inv1.sum()
    w2 = inv2 / inv2.sum()
    var1 = float(w1 @ V1 @ w1)
    var2 = float(w2 @ V2 @ w2)
    total = var1 + var2
    if total <= 0:
        a1, a2 = 0.5, 0.5
    else:
        a2 = var1 / total
        a1 = 1.0 - a2
    W[l:mid] *= a1
    W[mid:r] *= a2
    recursive_bisection(V, l, mid, W)
    recursive_bisection(V, mid, r, W)
    return W


def hrp_weights_for_leg(
    returns: pd.DataFrame,
    tickers: list[str],
    cap: float = HRP_CAP,
    target_sum: float = HRP_TARGET_SUM,
    skip_rows: int = 1,
) -> tuple[list[str], np.ndarray]:
    """
    Compute HRP weights for one leg (long or short).
    returns: DataFrame with columns = ticker symbols.
    tickers: list of tickers to use (order preserved in output).
    skip_rows: rows to drop after pct_change (notebook: long=2, short=1).
    Returns (tickers, weights) with weights summing to target_sum.
    """
    selected = [t for t in tickers if t in returns.columns]
    if not selected:
        return [], np.array([])
    data = returns[selected].copy()
    data = data.apply(pd.to_numeric, errors="coerce").ffill().bfill()
    mv_returns = data.pct_change(fill_method=None).iloc[skip_rows:]
    mv_returns = mv_returns.loc[:, mv_returns.std(skipna=True) > 0]
    tickers_used = mv_returns.columns.tolist()
    if len(tickers_used) < 2:
        w = np.ones(len(tickers_used)) / len(tickers_used) if tickers_used else np.array([])
        return tickers_used, (w * target_sum if w.size else w)

    log_ret = np.log1p(mv_returns)
    log_ret = log_ret.replace([np.inf, -np.inf], np.nan).dropna(how="all").fillna(0)
    corr = np.corrcoef(log_ret, rowvar=False)
    corr = np.clip(corr, -1.0, 1.0)
    np.fill_diagonal(corr, 1.0)
    cov = np.cov(log_ret, rowvar=False) * 252
    D = np.sqrt(0.5 * (1 - corr))
    np.fill_diagonal(D, 0.0)
    if not np.isfinite(D).all():
        col_means = np.nanmean(np.where(np.isfinite(D), D, np.nan), axis=0)
        D = np.where(np.isfinite(D), D, col_means)
        D = np.where(np.isfinite(D), D, 0.0)
        np.fill_diagonal(D, 0.0)
    D_condensed = squareform(D, checks=False)
    Z = linkage(D_condensed, method="single")
    res_order = leaves_list(Z).astype(int)
    N = len(res_order)
    V_sorted = cov[np.ix_(res_order, res_order)]
    W_sorted = recursive_bisection(V_sorted.copy(), 0, N, np.ones(N, dtype=float))
    W = np.zeros_like(W_sorted)
    W[res_order] = W_sorted
    W = np.maximum(W, 0)
    W = W / (W.sum() or 1)
    W = cap_proportional(W, cap=cap, target_sum=target_sum)
    return tickers_used, W


def _ticker_column(df: pd.DataFrame) -> Optional[str]:
    """Notebook uses column 'TICKER'. Prefer that (case-insensitive), else first column."""
    if df is None or df.empty or len(df.columns) == 0:
        return None
    for c in df.columns:
        if isinstance(c, str) and c.strip().upper() == "TICKER":
            return c
    return str(df.columns[0])


def load_tickers(tickers_path: Path) -> tuple[list[str], list[str], list[str], list[str]]:
    """Load long/short US and EU ticker lists from Tickers.xlsx (sheets 0..3). Column TICKER as in notebook."""
    df0 = pd.read_excel(tickers_path, sheet_name=0, header=0, usecols="A")
    df1 = pd.read_excel(tickers_path, sheet_name=1, header=0, usecols="A")
    df2 = pd.read_excel(tickers_path, sheet_name=2, header=0, usecols="A")
    df3 = pd.read_excel(tickers_path, sheet_name=3, header=0, usecols="A")
    t0, t1, t2, t3 = _ticker_column(df0), _ticker_column(df1), _ticker_column(df2), _ticker_column(df3)
    long_us = norm(df0[t0]) if t0 else []
    short_us = norm(df1[t1]) if t1 else []
    long_eu = norm(df2[t2]) if t2 else []
    short_eu = norm(df3[t3]) if t3 else []
    return long_us, short_us, long_eu, short_eu


def load_returns(us_path: Path, eu_path: Path) -> pd.DataFrame:
    """Load US and EU returns and merge (align on index)."""
    us = pd.read_excel(us_path, sheet_name=0, index_col=0)
    eu = pd.read_excel(eu_path, sheet_name=0, index_col=0)
    data = pd.concat([us, eu], axis=1)
    data = data.apply(pd.to_numeric, errors="coerce").ffill().bfill()
    return data


def _read_sheets_csv(url: str) -> pd.DataFrame:
    """Read a single sheet from Google Sheets export URL (CSV)."""
    return pd.read_csv(url, index_col=0)

def load_tickers_from_sheets(
    tickers_urls: list[str],
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Load ticker lists from Google Sheets CSV export URLs. Order: long_us, short_us, long_eu, short_eu (4 URLs)."""
    if len(tickers_urls) < 4:
        raise ValueError("Need 4 ticker sheet URLs: long_us, short_us, long_eu, short_eu")
    out = []
    for url in tickers_urls[:4]:
        df = pd.read_csv(url)
        tcol = _ticker_column(df) or df.columns[0]
        out.append(norm(df[tcol]))
    return tuple(out)


def load_returns_from_sheets(us_url: str, eu_url: str) -> pd.DataFrame:
    """Load US and EU returns from Google Sheets CSV export URLs; first column = index (dates)."""
    us = _read_sheets_csv(us_url)
    eu = _read_sheets_csv(eu_url)
    us.index = pd.to_datetime(us.index, errors="coerce")
    eu.index = pd.to_datetime(eu.index, errors="coerce")
    data = pd.concat([us, eu], axis=1)
    data = data.apply(pd.to_numeric, errors="coerce").ffill().bfill()
    return data


def _run_hrp_core(
    long_us_names: list[str],
    short_us_names: list[str],
    long_eu_names: list[str],
    short_eu_names: list[str],
    data: pd.DataFrame,
    out_dir: Path,
    cap: float = HRP_CAP,
    target_sum: float = HRP_TARGET_SUM,
) -> dict[str, pd.DataFrame]:
    """Run HRP for all four legs and save weights. Returns dict leg_name -> DataFrame (Ticker, Weight)."""
    results = {}
    for leg_name, names, is_short, skip in [
        ("long_eu", long_eu_names, False, 2),
        ("short_eu", short_eu_names, True, 1),
        ("long_us", long_us_names, False, 2),
        ("short_us", short_us_names, True, 1),
    ]:
        tickers_used, W = hrp_weights_for_leg(data, names, cap=cap, target_sum=target_sum, skip_rows=skip)
        if is_short and len(W):
            W = -1.0 * W
        results[leg_name] = pd.DataFrame({"Ticker": tickers_used, "Weight": W})

    out_dir.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_dir / "hrp_weights.xlsx", engine="openpyxl") as w:
        for leg_name, df in results.items():
            df.to_excel(w, sheet_name=leg_name, index=False)
    for leg in ("long_us", "short_us"):
        df = results[leg]
        if not df.empty:
            out_name = "weights_long_us.csv" if leg == "long_us" else "weights_short_us.csv"
            df.rename(columns={"Ticker": "ticker", "Weight": "weight"}).to_csv(
                out_dir / out_name, index=False
            )
    print(f"Saved HRP weights to {out_dir}")
    print(f"  hrp_weights.xlsx (sheets: long_eu, short_eu, long_us, short_us)")
    print(f"  weights_long_us.csv, weights_short_us.csv (for us_portfolio_performance.py)")
    return results


def run_hrp_and_save(
    tickers_path: Path,
    us_returns_path: Path,
    eu_returns_path: Path,
    out_dir: Path,
    cap: float = HRP_CAP,
    target_sum: float = HRP_TARGET_SUM,
) -> dict[str, pd.DataFrame]:
    """Load tickers and returns from local files, run HRP, save weights."""
    long_us_names, short_us_names, long_eu_names, short_eu_names = load_tickers(tickers_path)
    data = load_returns(us_returns_path, eu_returns_path)
    return _run_hrp_core(
        long_us_names, short_us_names, long_eu_names, short_eu_names, data, out_dir, cap, target_sum
    )


def run_hrp_from_sheets(
    tickers_urls: list[str],
    us_returns_url: str,
    eu_returns_url: str,
    out_dir: Path,
    cap: float = HRP_CAP,
    target_sum: float = HRP_TARGET_SUM,
) -> dict[str, pd.DataFrame]:
    """Load tickers and returns from Google Sheets (CSV export URLs), run HRP, save weights."""
    long_us_names, short_us_names, long_eu_names, short_eu_names = load_tickers_from_sheets(tickers_urls)
    data = load_returns_from_sheets(us_returns_url, eu_returns_url)
    return _run_hrp_core(
        long_us_names, short_us_names, long_eu_names, short_eu_names, data, out_dir, cap, target_sum
    )


def download_if_missing(path: Path, drive_id: str) -> Path:
    """Download file from Google Drive if path does not exist."""
    if path.exists():
        return path
    try:
        import gdown
        gdown.download(f"https://drive.google.com/uc?id={drive_id}", str(path), quiet=True)
    except Exception as e:
        raise FileNotFoundError(
            f"Missing {path}. Install gdown and run with network, or place file manually."
        ) from e
    return path


def main():
    parser = argparse.ArgumentParser(description="HRP allocation: compute weights (data from Google only).")
    parser.add_argument("--tickers", type=Path, default=DEFAULT_TICKERS_FILE, help="Tickers.xlsx path (if local)")
    parser.add_argument("--us-returns", type=Path, default=DEFAULT_US_RETURNS_FILE, help="US_Returns.xlsx path (if local)")
    parser.add_argument("--eu-returns", type=Path, default=DEFAULT_EU_RETURNS_FILE, help="EU_Returns.xlsx path (if local)")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help="Output directory")
    parser.add_argument("--download", action="store_true", help="Force download from Google Drive (default: download if files missing)")
    parser.add_argument("--use-sheets", action="store_true", help="Read from Google Sheets export URLs only (no Drive/local)")
    parser.add_argument("--tickers-urls", nargs=4, metavar="URL", help="4 CSV export URLs: long_us short_us long_eu short_eu (with --use-sheets)")
    parser.add_argument("--us-returns-url", help="US returns sheet CSV export URL (with --use-sheets)")
    parser.add_argument("--eu-returns-url", help="EU returns sheet CSV export URL (with --use-sheets)")
    parser.add_argument("--cap", type=float, default=HRP_CAP, help="Max weight per asset")
    parser.add_argument("--target-sum", type=float, default=HRP_TARGET_SUM, help="Target sum per leg (e.g. 0.5)")
    args = parser.parse_args()

    if args.use_sheets:
        urls = args.tickers_urls or os.environ.get("HRP_TICKERS_URLS", "").split()
        if len(urls) < 4:
            raise SystemExit(
                "With --use-sheets provide --tickers-urls URL URL URL URL (4 URLs) or set HRP_TICKERS_URLS env."
            )
        us_url = args.us_returns_url or os.environ.get("HRP_US_RETURNS_URL", "")
        eu_url = args.eu_returns_url or os.environ.get("HRP_EU_RETURNS_URL", "")
        if not us_url or not eu_url:
            raise SystemExit("With --use-sheets provide --us-returns-url and --eu-returns-url (or set env HRP_US_RETURNS_URL, HRP_EU_RETURNS_URL).")
        run_hrp_from_sheets(
            urls[:4], us_url, eu_url, args.out_dir, cap=args.cap, target_sum=args.target_sum
        )
        return

    tickers_path = args.tickers
    us_path = args.us_returns
    eu_path = args.eu_returns

    # Notebook workflow: only read from Google — download from Drive if local files missing
    if not tickers_path.exists() or args.download:
        download_if_missing(tickers_path, TICKERS_DRIVE_ID)
    if not us_path.exists() or args.download:
        download_if_missing(us_path, US_RETURNS_DRIVE_ID)
    if not eu_path.exists() or args.download:
        download_if_missing(eu_path, EU_RETURNS_DRIVE_ID)

    if not tickers_path.exists():
        raise FileNotFoundError(f"Tickers file not found: {tickers_path}")
    if not us_path.exists():
        raise FileNotFoundError(f"US returns not found: {us_path}")
    if not eu_path.exists():
        raise FileNotFoundError(f"EU returns not found: {eu_path}")

    run_hrp_and_save(
        tickers_path,
        us_path,
        eu_path,
        args.out_dir,
        cap=args.cap,
        target_sum=args.target_sum,
    )


if __name__ == "__main__":
    main()
