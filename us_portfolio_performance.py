"""
US Portfolio performance (AR–AX columns) from weights and prices.

Replicates the logic of the "US PORTFOLIO" sheet in the performance workbook (default: Performance_SPRING_2026.xlsx):
- Weights: row 8 (B:U long, X:AQ short); tickers row 10.
- Prices: from "US DATA" sheet (same workbook) or from a separate price file.
- Output: AR (long value), AU (short value), AS, AT, AV, AW, AX (daily changes and combined PnL).

Usage:
  python us_portfolio_performance.py [path_to_performance.xlsx]
  # Output: outputs/portfolio_us/us_portfolio_ar_ax.csv (and optionally .xlsx)

Or import and call build_us_portfolio_ar_ax(weights_long, weights_short, prices_df, amount=50000).
"""

from __future__ import annotations

import re
import zipfile
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# Default paths: input from data/, output from config
import config as _config
PROJECT_ROOT = _config.PROJECT_ROOT
DEFAULT_XLSX = _config.get_data_path("Performance_SPRING_2026.xlsx")  # override via CLI or argument
OUTPUT_DIR = _config.get_output_path("portfolio_us")
OUTPUT_CSV = OUTPUT_DIR / "us_portfolio_ar_ax.csv"
OUTPUT_XLSX = OUTPUT_DIR / "us_portfolio_ar_ax.xlsx"

# Column layout in US PORTFOLIO / US DATA
LONG_COLS = 20   # B:U
SHORT_COLS = 20  # X:AQ
TOTAL_AMOUNT = 50_000
AMOUNT_PER_SIDE = TOTAL_AMOUNT / 2  # 25k long, 25k short


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
                    out[(r, col)] = None  # formula: we'll use cached value from a saved workbook
    return out


def load_weights_and_tickers_from_xlsx(xlsx_path: Path) -> tuple[list[float], list[str], list[float], list[str]]:
    """Read US PORTFOLIO sheet: row 8 = weights, row 10 = tickers. Returns (weights_long, tickers_long, weights_short, tickers_short)."""
    # US PORTFOLIO is sheet6 (rId6 -> sheet6.xml)
    with zipfile.ZipFile(xlsx_path, "r") as z:
        shared = _read_shared_strings(z)
        cells = _read_sheet_cells(z, "xl/worksheets/sheet6.xml", shared)

    # Row 8: weights. B=1..U=20 long, X=23..AQ=42 short
    weights_long = [cells.get((8, c)) for c in range(1, 1 + LONG_COLS)]
    weights_short = [cells.get((8, c)) for c in range(23, 23 + SHORT_COLS)]
    # Row 10: tickers
    tickers_long = [cells.get((10, c)) for c in range(1, 1 + LONG_COLS)]
    tickers_short = [cells.get((10, c)) for c in range(23, 23 + SHORT_COLS)]

    # Filter None and convert weights to float
    def clean_weights(w, t):
        return [(float(w) if w is not None else 0.0, str(t) if t else "") for w, t in zip(w, t)]

    wl = [x[0] for x in clean_weights(weights_long, tickers_long) if x[1]]
    tl = [x[1] for x in clean_weights(weights_long, tickers_long) if x[1]]
    ws = [x[0] for x in clean_weights(weights_short, tickers_short) if x[1]]
    ts = [x[1] for x in clean_weights(weights_short, tickers_short) if x[1]]
    return (wl, tl, ws, ts)


def load_prices_from_us_data_sheet(xlsx_path: Path) -> pd.DataFrame:
    """Read US DATA sheet: row 1 = tickers, row 5+ = date (col B), prices (cols C onwards). Returns DataFrame index=date, columns=tickers."""
    with zipfile.ZipFile(xlsx_path, "r") as z:
        shared = _read_shared_strings(z)
        cells = _read_sheet_cells(z, "xl/worksheets/sheet1.xml", shared)

    # Row 1: ticker names in cols 0..39 (A through AN)
    tickers = []
    for c in range(40):
        v = cells.get((1, c))
        if v and isinstance(v, str):
            tickers.append(v)
        else:
            tickers.append(f"Col{c}")
    # Row 5+: col 1 = Excel date, col 2..41 = prices (20 long + 20 short)
    rows = []
    for r in range(5, 200):
        date_val = cells.get((r, 1))
        if date_val is None:
            continue
        if isinstance(date_val, float):
            # Excel serial date
            from datetime import datetime
            try:
                dt = datetime(1899, 12, 30).replace(tzinfo=None) + pd.Timedelta(days=float(date_val))
                date = dt.date()
            except Exception:
                date = date_val
        else:
            date = date_val
        row = [date]
        for c in range(2, 2 + 40):
            row.append(cells.get((r, c)))
        rows.append(row)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["Date"] + tickers)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    df = df.set_index("Date").sort_index()
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def build_ar_ax(
    weights_long: list[float],
    tickers_long: list[str],
    weights_short: list[float],
    tickers_short: list[str],
    prices: pd.DataFrame,
    amount_long: float = AMOUNT_PER_SIDE,
    amount_short: float = AMOUNT_PER_SIDE,
) -> pd.DataFrame:
    """
    Compute AR, AU, AS, AT, AV, AW, AX from weights and price DataFrame.

    prices: index = date, columns = ticker (must include all tickers_long and tickers_short).
    Weights are normalized per side; initial investment per name = weight_i * amount_side.
    """
    if not tickers_long:
        raise ValueError("At least one long ticker is required.")
    # Align price columns to long and short order
    pl = prices.reindex(columns=tickers_long).dropna(how="all")
    dates = pl.index.sort_values()
    pl = pl.loc[dates].ffill().bfill()

    n_long = len(tickers_long)
    wl = np.asarray(weights_long, dtype=float)
    wl = wl / wl.sum() if wl.sum() else np.ones(n_long) / n_long
    initial_long = wl * amount_long
    P0_long = pl.iloc[0].values.astype(float)
    P0_long = np.where(P0_long == 0, np.nan, P0_long)
    ret_long = pl.values.astype(float) / P0_long
    AR = np.nansum(initial_long * ret_long, axis=1)

    if tickers_short and weights_short:
        ps = prices.reindex(columns=tickers_short).dropna(how="all")
        dates = dates.intersection(ps.index).sort_values()
        pl = pl.loc[dates].ffill().bfill()
        ps = ps.loc[dates].ffill().bfill()
        n_short = len(tickers_short)
        ws = np.asarray(weights_short, dtype=float)
        ws = ws / ws.sum() if ws.sum() else np.ones(n_short) / n_short
        initial_short = ws * amount_short
        P0_short = ps.iloc[0].values.astype(float)
        P0_short = np.where(P0_short == 0, np.nan, P0_short)
        ret_short = ps.values.astype(float) / P0_short
        AU = np.nansum(initial_short * ret_short, axis=1)
    else:
        AU = np.zeros_like(AR)

    AS = np.diff(AR, prepend=AR[0])
    prev_AR = np.concatenate([[np.nan], AR[:-1]])
    AT = np.where(np.isfinite(prev_AR) & (prev_AR != 0), AR / prev_AR, np.nan)
    AV = np.diff(AU, prepend=AU[0])
    AW = -AV  # short PnL "as long": IF(AV>0,-AV,ABS(AV)) = -AV
    AX = AS + AW

    out = pd.DataFrame(
        {
            "AR": AR,
            "AS": AS,
            "AT": AT,
            "AU": AU,
            "AV": AV,
            "AW": AW,
            "AX": AX,
        },
        index=dates,
    )
    out.index.name = "Date"
    return out


def main(
    xlsx_path: Optional[Path] = None,
    weights_long: Optional[list[float]] = None,
    weights_short: Optional[list[float]] = None,
    tickers_long: Optional[list[str]] = None,
    tickers_short: Optional[list[str]] = None,
    prices_path: Optional[Path] = None,
    amount: float = TOTAL_AMOUNT,
    output_csv: Optional[Path] = None,
    output_xlsx: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Build AR–AX output. Sources (in order of use):
    1) If xlsx_path set: load weights and tickers from US PORTFOLIO sheet.
    2) Else use passed weights_long, weights_short, tickers_long, tickers_short (must all be set).

    Prices: from xlsx_path "US DATA" sheet if xlsx_path set and prices_path not set; else from prices_path (CSV/Excel with Date index and ticker columns).
    """
    xlsx_path = xlsx_path or DEFAULT_XLSX
    output_csv = output_csv or OUTPUT_CSV
    output_xlsx = output_xlsx or OUTPUT_XLSX
    amount_per_side = amount / 2

    if xlsx_path.exists():
        wl, tl, ws, ts = load_weights_and_tickers_from_xlsx(xlsx_path)
        if weights_long is not None:
            wl = list(weights_long)
        if weights_short is not None:
            ws = list(weights_short)
        if tickers_long is not None:
            tl = list(tickers_long)
        if tickers_short is not None:
            ts = list(tickers_short)
    else:
        if not all([weights_long is not None, weights_short is not None, tickers_long is not None, tickers_short is not None]):
            raise ValueError("Provide xlsx_path or (weights_long, weights_short, tickers_long, tickers_short).")
        wl, ws, tl, ts = list(weights_long), list(weights_short), list(tickers_long), list(tickers_short)

    if prices_path is not None:
        if str(prices_path).lower().endswith(".csv"):
            prices = pd.read_csv(prices_path, index_col=0, parse_dates=True)
        else:
            prices = pd.read_excel(prices_path, index_col=0)
            prices.index = pd.to_datetime(prices.index, errors="coerce")
    else:
        if not xlsx_path.exists():
            raise FileNotFoundError(f"Prices source not set and xlsx not found: {xlsx_path}")
        prices = load_prices_from_us_data_sheet(xlsx_path)

    result = build_ar_ax(wl, tl, ws, ts, prices, amount_long=amount_per_side, amount_short=amount_per_side)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_csv)
    print(f"Wrote AR–AX to {output_csv}")
    try:
        result.to_excel(output_xlsx)
        print(f"Wrote AR–AX to {output_xlsx}")
    except Exception as e:
        print(f"Skip Excel output (install openpyxl if needed): {e}")

    return result


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Build US portfolio AR–AX from Performance xlsx (weights and tickers from US PORTFOLIO sheet).")
    p.add_argument("xlsx", nargs="?", default=None, help="Path to Performance workbook (default: data/Performance_SPRING_2026.xlsx)")
    p.add_argument("--prices", type=Path, help="CSV or Excel with Date index and ticker columns (default: US DATA sheet)")
    p.add_argument("--output-csv", type=Path, default=None)
    p.add_argument("--output-xlsx", type=Path, default=None)
    args = p.parse_args()
    main(
        xlsx_path=Path(args.xlsx) if args.xlsx else None,
        prices_path=args.prices,
        output_csv=args.output_csv,
        output_xlsx=args.output_xlsx,
    )
