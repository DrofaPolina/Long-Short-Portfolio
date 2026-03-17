"""
One-time: copy data/Tickers_HRP.xlsx → data/Tickers.xlsx.

Run once to make data/Tickers.xlsx match Tickers_HRP. Sheets are written as
long_us, short_us, long_eu, short_eu (sheet index 0..3 for hrp_allocation).
If Tickers_HRP uses different sheet names, we copy by position (first 4 sheets).
"""

from pathlib import Path

import pandas as pd

import config

DATA_DIR = config.get_data_path(".")
SRC = DATA_DIR / "Tickers_HRP.xlsx"
DST = config.get_data_path("Tickers.xlsx")
SHEET_NAMES = ["long_us", "short_us", "long_eu", "short_eu"]


def main():
    if not SRC.exists():
        print(f"Not found: {SRC}")
        print("Place Tickers_HRP.xlsx in data/ and run again.")
        return
    xl = pd.ExcelFile(SRC)
    if len(xl.sheet_names) < 4:
        print(f"Tickers_HRP has {len(xl.sheet_names)} sheets; need at least 4.")
        return
    DST.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(DST, engine="openpyxl") as w:
        for i, out_name in enumerate(SHEET_NAMES):
            df = pd.read_excel(xl, sheet_name=i, header=0)
            # Use first column as TICKER (hrp_allocation uses usecols="A" / first col)
            if len(df.columns) == 0:
                df = pd.DataFrame(columns=["TICKER"])
            else:
                df = df.iloc[:, [0]].rename(columns={df.columns[0]: "TICKER"})
            df.to_excel(w, sheet_name=out_name, index=False)
    print(f"Copied {SRC} → {DST}")


if __name__ == "__main__":
    main()
