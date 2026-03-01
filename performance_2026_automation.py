"""
Performance workbook automation: read DATA sheets and hrp_weights, compute EU/US portfolio and Total Daily. Outputs use meaningful column names; single Excel file only.

Uses config.PERFORMANCE_WORKBOOK_FILENAME and config.OUTPUT_DIRS. Saves CSV and Excel to output dir.
"""

from __future__ import annotations

import re
import zipfile
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

import config

PROJECT_ROOT = Path(__file__).resolve().parent
PERFORMANCE_XLSX = PROJECT_ROOT / config.PERFORMANCE_WORKBOOK_FILENAME
HRP_WEIGHTS_XLSX = PROJECT_ROOT / config.OUTPUT_DIRS["hrp_weights"] / "hrp_weights.xlsx"
OUTPUT_DIR = PROJECT_ROOT / config.OUTPUT_DIRS["performance"]

# Default total notional; must match sheet (row-162 initial sums). Set in config.PERFORMANCE_TOTAL_AMOUNT.
def _total_amount():
    return getattr(config, "PERFORMANCE_TOTAL_AMOUNT", 5000)
LONG_COLS = 20
SHORT_COLS = 20

# Meaningful column names: EU PORTFOLIO sheet = EU names, US PORTFOLIO sheet = US names
COLUMNS_EU = [
    "EU Long Performance",
    "EU Long Change Daily",
    "EU Long Change Percentage Daily",
    "EU Short Performance Treated as LONG",
    "EU Short Portfolio Daily",
    "EU Short Correctly",
    "EU Daily",
    "EU Daily EUR",
]
COLUMNS_US = [
    "US Long Performance",
    "US Long Change Daily",
    "US Long Change Percentage Daily",
    "US Short Performance Treated as LONG",
    "US Short Portfolio Daily",
    "US Short Correctly",
    "US Daily",
]
# DATA layout: col B = date, cols C:V = 20 long, col W = blank, cols X:AQ = 20 short
# Tickers are in ROW 4 (not row 1): C4=first long ticker (e.g. APP.OQ), X4=first short ticker.
DATA_DATE_COL = 1
DATA_LONG_START = 2
DATA_LONG_END = 21
DATA_SHORT_START = 23
DATA_SHORT_END = 42
DATA_FIRST_ROW = 5
DATA_TICKER_ROW = 4  # row where column tickers live (C4=APP, so column C data = APP)


def _col_letter_to_idx(s: str) -> int:
    return sum((ord(ch) - ord("A") + 1) * (26 ** i) for i, ch in enumerate(reversed(s))) - 1


def _read_shared_strings(z: zipfile.ZipFile) -> list[str]:
    import xml.etree.ElementTree as ET
    NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    root = ET.fromstring(z.read("xl/sharedStrings.xml"))
    out = []
    for si in root.findall(f".//{{{NS}}}si"):
        t = si.find(f"{{{NS}}}t")
        if t is not None and t.text:
            out.append(t.text)
        else:
            r = si.find(f".//{{{NS}}}t")
            out.append(r.text if r is not None and r.text else "")
    return out


def _read_sheet_cells(z: zipfile.ZipFile, sheet_path: str, shared: list[str]) -> dict[tuple[int, int], object]:
    import xml.etree.ElementTree as ET
    NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ET.fromstring(z.read(sheet_path))
    out = {}
    for row in root.findall(".//m:row", NS):
        for c in row.findall("m:c", NS):
            ref = c.get("r")
            m = re.match(r"([A-Z]+)(\d+)", ref)
            if not m:
                continue
            col = _col_letter_to_idx(m.group(1))
            r = int(m.group(2))
            v = c.find("m:v", NS)
            t = c.get("t")
            if v is not None and v.text:
                if t == "s":
                    out[(r, col)] = shared[int(v.text)]
                else:
                    try:
                        out[(r, col)] = float(v.text)
                    except ValueError:
                        out[(r, col)] = v.text
            else:
                f = c.find("m:f", NS)
                if f is not None and f.text:
                    out[(r, col)] = None
    return out


def load_data_sheet_prices(xlsx_path: Path, sheet_xml: str) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Read a DATA sheet (EU or US). Tickers in row 4 (C4=first long, X4=first short); row 5+ = data, date in col B.
    Long = cols C:V (20), short = cols X:AQ (20) with one blank in between.
    Returns (prices_df, tickers_long, tickers_short). prices_df index=date, columns=ticker (long then short).
    """
    with zipfile.ZipFile(xlsx_path, "r") as z:
        shared = _read_shared_strings(z)
        cells = _read_sheet_cells(z, sheet_xml, shared)

    tickers_long = []
    for c in range(DATA_LONG_START, DATA_LONG_END + 1):
        v = cells.get((DATA_TICKER_ROW, c))
        if v and isinstance(v, str) and str(v).strip():
            tickers_long.append(str(v).strip())
        else:
            tickers_long.append(f"_L{c}")
    tickers_short = []
    for c in range(DATA_SHORT_START, DATA_SHORT_END + 1):
        v = cells.get((DATA_TICKER_ROW, c))
        if v and isinstance(v, str) and str(v).strip():
            tickers_short.append(str(v).strip())
        else:
            tickers_short.append(f"_S{c}")

    rows = []
    for r in range(DATA_FIRST_ROW, 200):
        date_val = cells.get((r, DATA_DATE_COL))
        if date_val is None:
            continue
        if isinstance(date_val, (int, float)):
            try:
                dt = pd.Timestamp(1899, 12, 30) + pd.Timedelta(days=float(date_val))
                date = dt.date()
            except Exception:
                date = date_val
        else:
            date = date_val
        row = [date]
        for c in range(DATA_LONG_START, DATA_LONG_END + 1):
            row.append(cells.get((r, c)))
        for c in range(DATA_SHORT_START, DATA_SHORT_END + 1):
            row.append(cells.get((r, c)))
        rows.append(row)
    if not rows:
        return pd.DataFrame(), tickers_long, tickers_short
    columns = ["Date"] + tickers_long + tickers_short
    df = pd.DataFrame(rows, columns=columns)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    df = df.set_index("Date").sort_index()
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df, tickers_long, tickers_short


def load_hrp_weights(hrp_xlsx_path: Path) -> dict[str, tuple[list[str], list[float]]]:
    """
    Read hrp_weights.xlsx. Sheets: long_eu, short_eu, long_us, short_us.
    Each sheet has columns Ticker, Weight (or first col = ticker, second = weight).
    Returns dict[sheet_name] -> (tickers, weights).
    """
    out = {}
    for sheet in ("long_eu", "short_eu", "long_us", "short_us"):
        try:
            df = pd.read_excel(hrp_xlsx_path, sheet_name=sheet)
        except Exception:
            out[sheet] = ([], [])
            continue
        if df is None or df.empty:
            out[sheet] = ([], [])
            continue
        cols = [c for c in df.columns if isinstance(c, str)]
        wcol = next((c for c in cols if c.lower() == "weight"), df.columns[1] if len(df.columns) > 1 else df.columns[0])
        tcol = next((c for c in cols if c.lower() == "ticker"), df.columns[0])
        tickers = df[tcol].astype(str).str.strip().tolist()
        weights = df[wcol].astype(float).tolist()
        out[sheet] = (tickers, weights)
    return out


def _ticker_lookup(ticker: str, w_by_ticker: dict[str, float]) -> float:
    """
    Match DATA ticker to HRP weight: exact match first, then base symbol (strip .EXCHANGE).
    Case-insensitive for base match. Handles DATA "FBK.MI" vs HRP "FBK" and vice versa.
    """
    t = ticker.strip()
    # Normalized keys (strip) so HRP " FBK " still matches
    w_norm = {k.strip(): v for k, v in w_by_ticker.items()}
    if t in w_norm:
        return w_norm[t]
    base_t = t.rsplit(".", 1)[0].strip() if "." in t else t
    base_t_upper = base_t.upper()
    # DATA has "FBK.MI", HRP has "FBK" -> match by base
    if base_t in w_norm:
        return w_norm[base_t]
    # Case-insensitive base match
    for k, v in w_norm.items():
        k_base = k.rsplit(".", 1)[0] if "." in k else k
        if base_t_upper == k_base.upper():
            return v
    return 0.0


def align_weights_to_tickers(
    tickers_in_data: list[str],
    tickers_from_hrp: list[str],
    weights_from_hrp: list[float],
    normalize_positive: bool = True,
) -> list[float]:
    """
    Return weights in same order as tickers_in_data; if ticker missing in HRP, use 0.
    Matching: exact string first, then base symbol (without .EXCHANGE) so DATA "FBK.MI" matches HRP "FBK".
    If normalize_positive and weights sum to > 0: normalize to sum 1.
    If weights sum to < 0 (short leg): return as-is so they keep sum -0.5.
    """
    w_by_ticker = dict(zip(tickers_from_hrp, weights_from_hrp))
    weights = [_ticker_lookup(t, w_by_ticker) for t in tickers_in_data]
    w = np.asarray(weights, dtype=float)
    if normalize_positive and w.sum() > 0:
        w = w / w.sum()
    return w.tolist()


def build_portfolio_one_region(
    prices: pd.DataFrame,
    tickers_long: list[str],
    tickers_short: list[str],
    weights_long: list[float],
    weights_short: list[float],
    columns: list[str],
    total_amount: float = 5000,
    at_daily_return: bool = True,
    region_label: str = "",
    debug: bool = True,
) -> pd.DataFrame:
    """
    Compute long/short portfolio series for one region.
    Long weights are positive (sum 0.5). Short weights from HRP sum to -0.5; for "Short Performance
    Treated as LONG" we use |short weights| normalized to sum 0.5 so allocation = amount_per_side (2500).
    """
    amount_per_side = total_amount / 2
    pl = prices.reindex(columns=tickers_long).dropna(how="all")
    if pl.empty:
        raise ValueError("No long price data.")
    dates = pl.index.sort_values()
    pl = pl.loc[dates].ffill().bfill()
    n_long = len(tickers_long)
    wl = np.asarray(weights_long, dtype=float)
    if wl.sum() > 0:
        wl = wl / wl.sum()
    else:
        wl = np.ones(n_long) / n_long
    initial_long = wl * amount_per_side
    P0_long = np.where(pl.iloc[0].values == 0, np.nan, pl.iloc[0].values.astype(float))
    ret_long = pl.values.astype(float) / P0_long
    long_perf = np.nansum(initial_long * ret_long, axis=1)

    if tickers_short and weights_short:
        ps = prices.reindex(columns=tickers_short).dropna(how="all")
        dates = dates.intersection(ps.index).sort_values()
        pl = pl.loc[dates].ffill().bfill()
        ps = ps.loc[dates].ffill().bfill()
        n_short = len(tickers_short)
        ws = np.asarray(weights_short, dtype=float)
        # Short weights from HRP sum to -0.5. "Short Performance Treated as LONG" = gross value
        # with positive allocation (sheet row 162): use |weights|, normalize to sum 0.5 → total 2500.
        ws_abs = np.abs(ws)
        ws_abs_sum = ws_abs.sum()
        if ws_abs_sum > 0:
            alloc_short = (ws_abs / ws_abs_sum) * amount_per_side  # positive, sums to amount_per_side
        else:
            alloc_short = np.ones(n_short) / n_short * amount_per_side
        P0_short = np.where(ps.iloc[0].values == 0, np.nan, ps.iloc[0].values.astype(float))
        ret_short = ps.values.astype(float) / P0_short
        short_perf = np.nansum(alloc_short * ret_short, axis=1)

        if debug and region_label:
            print(f"[DEBUG {region_label}] total_amount={total_amount}, amount_per_side={amount_per_side}")
            print(f"  long: sum(weights)={wl.sum():.4f}, sum(initial_long)={initial_long.sum():.2f}, P0_long[:3]={P0_long[:3]}, long_perf[0]={long_perf[0]:.2f}")
            print(f"  short: raw sum(weights)={ws.sum():.4f}, |weights| sum={ws_abs_sum:.4f}, sum(alloc_short)={alloc_short.sum():.2f}, P0_short[:3]={P0_short[:3]}, short_perf[0]={short_perf[0]:.2f}")
    else:
        short_perf = np.zeros_like(long_perf)
        if debug and region_label:
            print(f"[DEBUG {region_label}] total_amount={total_amount}, amount_per_side={amount_per_side}")
            print(f"  long: sum(initial_long)={initial_long.sum():.2f}, long_perf[0]={long_perf[0]:.2f}; no short leg.")

    long_change_daily = np.diff(long_perf, prepend=long_perf[0])
    prev_long = np.concatenate([[np.nan], long_perf[:-1]])
    ratio = np.where(np.isfinite(prev_long) & (prev_long != 0), long_perf / prev_long, np.nan)
    long_pct_daily = (ratio - 1.0) if at_daily_return else ratio
    short_portfolio_daily = np.diff(short_perf, prepend=short_perf[0])
    short_correctly = -short_portfolio_daily
    daily_combined = long_change_daily + short_correctly

    data = {
        columns[0]: long_perf,
        columns[1]: long_change_daily,
        columns[2]: long_pct_daily,
        columns[3]: short_perf,
        columns[4]: short_portfolio_daily,
        columns[5]: short_correctly,
        columns[6]: daily_combined,
    }
    if len(columns) >= 8:
        data[columns[7]] = daily_combined  # EU Daily EUR = same as EU Daily
    return pd.DataFrame(data, index=dates)


def build_total_daily(
    eu_portfolio: pd.DataFrame,
    us_portfolio: pd.DataFrame,
    c2_initial: Optional[float] = None,
) -> pd.DataFrame:
    """
    Build Total Daily sheet. Uses column index 6 (daily combined) and 0+3 (long+short perf) for initial.
    """
    dates = eu_portfolio.index.intersection(us_portfolio.index).sort_values()
    eu_daily_col = eu_portfolio.columns[6]
    us_daily_col = us_portfolio.columns[6]
    eu_ax = eu_portfolio.loc[dates, eu_daily_col].reindex(dates).ffill().bfill()
    us_ax = us_portfolio.loc[dates, us_daily_col].reindex(dates).ffill().bfill()
    if c2_initial is None:
        c2_initial = (
            float(eu_portfolio.loc[dates[0], eu_portfolio.columns[0]])
            + float(eu_portfolio.loc[dates[0], eu_portfolio.columns[3]])
            + float(us_portfolio.loc[dates[0], us_portfolio.columns[0]])
            + float(us_portfolio.loc[dates[0], us_portfolio.columns[3]])
        )
    a, b = eu_ax.values, us_ax.values
    p = a + b
    c = np.empty(len(dates) + 1)
    c[0] = c2_initial
    for i in range(len(dates)):
        c[i + 1] = c[i] + a[i] + b[i]
    c = c[1:]
    sharpe = (np.nanmean(p) / np.nanstd(p)) * np.sqrt(252) if len(p) >= 2 and np.nanstd(p) > 0 else np.nan
    out = pd.DataFrame(
        {
            "EU Daily": a,
            "US Daily": b,
            "Total Daily": c,
            "C minus C2": c - c2_initial,
            "C over C2": c / c2_initial,
            "Daily PnL": p,
        },
        index=dates,
    )
    out.index.name = "Date"
    out.attrs["C2_initial"] = c2_initial
    out.attrs["Sharpe_Ratio"] = sharpe
    out.attrs["Total_PnL"] = np.nansum(a) + np.nansum(b)
    return out


def run_performance_2026(
    performance_xlsx: Optional[Path] = None,
    hrp_weights_xlsx: Optional[Path] = None,
    total_amount: Optional[float] = None,
) -> dict[str, pd.DataFrame]:
    """
    Full pipeline: read EU DATA, US DATA, hrp_weights.xlsx; compute EU/US portfolio and Total Daily.
    Saves single Excel to config output dir. Returns dict with keys: eu_portfolio, us_portfolio, total_daily.
    """
    performance_xlsx = performance_xlsx or PERFORMANCE_XLSX
    hrp_weights_xlsx = hrp_weights_xlsx or HRP_WEIGHTS_XLSX
    total_amount = total_amount if total_amount is not None else _total_amount()
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1) Load DATA sheets (US DATA = sheet1, EU DATA = sheet2)
    if not performance_xlsx.exists():
        raise FileNotFoundError(f"Performance workbook not found: {performance_xlsx}")
    eu_prices, eu_tickers_long, eu_tickers_short = load_data_sheet_prices(
        performance_xlsx, "xl/worksheets/sheet2.xml"
    )
    us_prices, us_tickers_long, us_tickers_short = load_data_sheet_prices(
        performance_xlsx, "xl/worksheets/sheet1.xml"
    )
    if eu_prices.empty or us_prices.empty:
        raise ValueError("EU DATA or US DATA has no price rows.")

    # 2) Load HRP weights
    if not hrp_weights_xlsx.exists():
        raise FileNotFoundError(f"hrp_weights.xlsx not found: {hrp_weights_xlsx}. Run hrp_allocation.py first.")
    hrp = load_hrp_weights(hrp_weights_xlsx)
    eu_w_long_t, eu_w_long_w = hrp.get("long_eu", ([], []))
    eu_w_short_t, eu_w_short_w = hrp.get("short_eu", ([], []))
    us_w_long_t, us_w_long_w = hrp.get("long_us", ([], []))
    us_w_short_t, us_w_short_w = hrp.get("short_us", ([], []))

    # Long weights sum to 0.5 (normalize to 1 in builder). Short weights sum to -0.5 (do not normalize).
    eu_weights_long = align_weights_to_tickers(eu_tickers_long, eu_w_long_t, eu_w_long_w)
    eu_weights_short = align_weights_to_tickers(eu_tickers_short, eu_w_short_t, eu_w_short_w, normalize_positive=False)
    us_weights_long = align_weights_to_tickers(us_tickers_long, us_w_long_t, us_w_long_w)
    us_weights_short = align_weights_to_tickers(us_tickers_short, us_w_short_t, us_w_short_w, normalize_positive=False)

    print(f"[DEBUG] total_amount={total_amount}, EU long tickers={len(eu_tickers_long)}, short={len(eu_tickers_short)}, US long={len(us_tickers_long)}, short={len(us_tickers_short)}")
    print(f"[DEBUG] Tickers from row 4: US long C4={us_tickers_long[0] if us_tickers_long else '?'}, US short X4={us_tickers_short[0] if us_tickers_short else '?'}; EU long C4={eu_tickers_long[0] if eu_tickers_long else '?'}, EU short X4={eu_tickers_short[0] if eu_tickers_short else '?'}")
    print(f"[DEBUG] EU short weights sum={sum(eu_weights_short):.4f}, US short weights sum={sum(us_weights_short):.4f}")
    # Match counts and unmatched tickers for all legs (same _ticker_lookup used everywhere)
    for label, tickers, weights in [
        ("EU long", eu_tickers_long, eu_weights_long),
        ("EU short", eu_tickers_short, eu_weights_short),
        ("US long", us_tickers_long, us_weights_long),
        ("US short", us_tickers_short, us_weights_short),
    ]:
        matched = sum(1 for w in weights if w != 0)
        unmatched = [t for t, w in zip(tickers, weights) if w == 0]
        print(f"[DEBUG] {label}: matched {matched}/{len(weights)}")
        if unmatched:
            print(f"        unmatched (0 weight): {unmatched[:10]}{'...' if len(unmatched) > 10 else ''}")

    # 3) Build EU and US portfolio DataFrames (meaningful column names)
    eu_portfolio = build_portfolio_one_region(
        eu_prices, eu_tickers_long, eu_tickers_short,
        eu_weights_long, eu_weights_short,
        columns=COLUMNS_EU, total_amount=total_amount, at_daily_return=True, region_label="EU", debug=True,
    )
    us_portfolio = build_portfolio_one_region(
        us_prices, us_tickers_long, us_tickers_short,
        us_weights_long, us_weights_short,
        columns=COLUMNS_US, total_amount=total_amount, at_daily_return=True, region_label="US", debug=True,
    )

    # 4) Total Daily
    total_daily = build_total_daily(eu_portfolio, us_portfolio, c2_initial=None)

    # 5) Save single Excel: portfolios, total daily, and ticker lists for checking
    output_path = output_dir / "performance_output.xlsx"
    with pd.ExcelWriter(output_path, engine="openpyxl") as w:
        eu_portfolio.to_excel(w, sheet_name="EU_PORTFOLIO")
        us_portfolio.to_excel(w, sheet_name="US_PORTFOLIO")
        total_daily.to_excel(w, sheet_name="TOTAL_DAILY")
        max_us = max(len(us_tickers_long), len(us_tickers_short))
        us_tickers_df = pd.DataFrame({
            "Long Ticker": us_tickers_long + [""] * (max_us - len(us_tickers_long)),
            "Short Ticker": us_tickers_short + [""] * (max_us - len(us_tickers_short)),
        })
        us_tickers_df.to_excel(w, sheet_name="US_Tickers", index=False)
        max_eu = max(len(eu_tickers_long), len(eu_tickers_short))
        eu_tickers_df = pd.DataFrame({
            "Long Ticker": eu_tickers_long + [""] * (max_eu - len(eu_tickers_long)),
            "Short Ticker": eu_tickers_short + [""] * (max_eu - len(eu_tickers_short)),
        })
        eu_tickers_df.to_excel(w, sheet_name="EU_Tickers", index=False)
    print(f"Saved to {output_path}")

    return {"eu_portfolio": eu_portfolio, "us_portfolio": us_portfolio, "total_daily": total_daily}


def main():
    run_performance_2026()


if __name__ == "__main__":
    main()
