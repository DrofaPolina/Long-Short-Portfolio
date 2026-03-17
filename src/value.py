"""
Value Factor

Book-to-Market (B/M) value strategy based on Fama-French.
Logic matches Value_Factor.ipynb exactly (data load, number parsing, month_end, signals, factor return).

Strategy:
- Calculate Book Value / Market Cap ratio for each stock
- Split universe by median market cap (small vs big)
- Within each group: Long top 20% B/M, short bottom 20% B/M
- Hold positions for 12 months with monthly rebalancing
- Equal-weighted within each leg

Data required:
- Book Value Per Share (BVPS)
- Outstanding Shares
- Market Cap
- Stock Prices/Returns
"""

import re
import numpy as np
import pandas as pd
import sys
import os
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config

# Default: write to project root outputs/factors (same regardless of cwd)
_DEFAULT_FACTOR_OUTPUT = str(Path(__file__).resolve().parent.parent / "outputs" / "factors")
from factor_positions_io import save_positions_excel


def _to_num(x):
    """Parse numeric strings like Value_Factor.ipynb: European decimal (1,5 -> 1.5) or US thousands."""
    if not isinstance(x, str):
        return x
    x = x.strip()
    if x == "" or x.upper() == "NULL":
        return np.nan
    x = x.replace("\u00A0", "").replace(" ", "")
    if re.search(r"\d,\d", x) and x.count(",") == 1:
        x = x.replace(",", ".")
    else:
        x = x.replace(",", "")
    try:
        return float(x)
    except Exception:
        return np.nan


def _month_end(df):
    """Month-end aggregation; same as Value_Factor.ipynb (freq ME)."""
    try:
        grouper = pd.Grouper(freq="ME")
    except Exception:
        grouper = pd.Grouper(freq="M")
    out = df.groupby(grouper).apply(lambda x: x.ffill().iloc[-1])
    if isinstance(out.index, pd.MultiIndex):
        out.index = out.index.get_level_values(0)
    return out


def fetch_value_data_us():
    """
    Fetch US value factor data from Google Sheets.
    
    Returns:
        dict with 'bvps', 'shares', 'mcap', 'prices'
    """
    import requests
    import io
    
    FILE_ID = "1P26cx04a_1KqJvBbP3ueBGp-05ea835q"
    TABS_US = {
        "bvps":   345849336,   # BOOK VALUE PER SHARE US
        "shares": 427235033,   # OUTSTANDING SHARES US
        "mcap":   1661952652,  # MARKET CAP US
        "prices": 128801732,   # SP500 prices
    }
    
    def fetch_tab(file_id, gid):
        url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid={gid}"
        r = requests.get(url)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), dtype=str)
        df.rename(columns={df.columns[0]: "date"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for c in df.columns[1:]:
            df[c] = df[c].map(_to_num)
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)
        return df

    print("Fetching US value data...")
    data = {}
    for name, gid in TABS_US.items():
        print(f"  Loading {name}...")
        df = fetch_tab(FILE_ID, gid)
        data[name] = _month_end(df)
    return data


def fetch_value_data_eu():
    """
    Fetch EU value factor data from Google Sheets.
    
    Returns:
        dict with 'bvps', 'shares', 'mcap', 'returns'
    """
    import requests
    import io
    
    FILE_ID = "1P26cx04a_1KqJvBbP3ueBGp-05ea835q"
    TABS_EU = {
        "bvps":    1376806582,  # BOOK VALUE PER SHARE EU
        "shares":  1853626146,  # OUTSTANDING SHARES EU
        "mcap":    1241718120,  # MARKET CAP EU
        "returns": 457903805,   # STOXX600 returns (in %)
    }
    
    def fetch_tab(file_id, gid):
        url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid={gid}"
        r = requests.get(url)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), dtype=str)
        df.rename(columns={df.columns[0]: "date"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for c in df.columns[1:]:
            df[c] = df[c].map(_to_num)
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)
        return df

    print("Fetching EU value data...")
    data = {}
    for name, gid in TABS_EU.items():
        print(f"  Loading {name}...")
        df = fetch_tab(FILE_ID, gid)
        data[name] = _month_end(df)
    # EU returns sheet is in % -> decimal (same as notebook)
    data['returns'] = data['returns'] / 100.0
    return data


def calculate_book_to_market(bvps, shares, mcap):
    """
    Calculate Book-to-Market ratio.
    
    B/M = Book Equity / Market Cap
    where Book Equity = BVPS * Shares
    
    Parameters:
        bvps: pd.DataFrame - Book value per share
        shares: pd.DataFrame - Outstanding shares
        mcap: pd.DataFrame - Market capitalization
    
    Returns:
        tuple: (BM, mcap) aligned DataFrames
    """
    # Align all dataframes
    idx = bvps.index.union(shares.index).union(mcap.index)
    cols = sorted(set(bvps.columns) | set(shares.columns) | set(mcap.columns))
    
    bvps = bvps.reindex(idx, columns=cols).ffill()
    shares = shares.reindex(idx, columns=cols).ffill()
    mcap = mcap.reindex(idx, columns=cols).ffill()
    
    # Calculate book equity
    book_equity = (bvps * shares).replace(0, np.nan)
    
    # Calculate B/M
    BM = book_equity / mcap.replace(0, np.nan)
    BM.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    return BM, mcap


def generate_value_signal(BM, MCAP, q=None):
    """
    Generate long-short signals based on B/M ratio.
    
    Strategy:
    - Split stocks by median market cap
    - Within each group (small/big):
      - Long top q% by B/M (high B/M = value stocks)
      - Short bottom q% by B/M (low B/M = growth stocks)
    
    Parameters:
        BM: pd.DataFrame - Book-to-market ratios
        MCAP: pd.DataFrame - Market caps
        q: float - Quantile for long/short (default from config.LONG_SHORT_CONFIG['long_percentile'])
    
    Returns:
        pd.DataFrame - Signals (+1 = long, -1 = short, 0 = neutral)
    """
    if q is None:
        q = config.LONG_SHORT_CONFIG['long_percentile']
    sig = pd.DataFrame(0.0, index=BM.index, columns=BM.columns)
    
    for dt in BM.index:
        b = BM.loc[dt].dropna()
        m = MCAP.loc[dt].reindex(b.index).dropna()
        
        if len(b) < 10 or len(m) < 10:
            continue
        
        # Split by median market cap
        med = m.median()
        b_small = b[m <= med]
        b_big = b[m > med]
        
        def pick_long_short(group):
            if group.empty:
                return [], []
            lo = group.quantile(q)
            hi = group.quantile(1 - q)
            longs = group.index[group >= hi].tolist()
            shorts = group.index[group <= lo].tolist()
            return longs, shorts
        
        # Pick from each group
        Ls, Ss = pick_long_short(b_small)
        Lb, Sb = pick_long_short(b_big)
        
        # Set signals
        sig.loc[dt, Ls + Lb] = 1.0
        sig.loc[dt, Ss + Sb] = -1.0
    
    # Shift signals forward (use t to trade at t+1)
    return sig.shift(1)


def hold_positions(sig, hold_months=12):
    """
    Create overlapping portfolios with monthly rebalancing.
    
    Holds each position for `hold_months`, creating overlapping portfolios.
    
    Parameters:
        sig: pd.DataFrame - Monthly signals
        hold_months: int - Holding period
    
    Returns:
        pd.DataFrame - Active positions (sum of overlapping portfolios)
    """
    active = pd.DataFrame(0.0, index=sig.index, columns=sig.columns)
    
    for k in range(hold_months):
        active += sig.shift(k).fillna(0.0)
    
    return active


def calculate_value_returns(returns, active):
    """
    Calculate value factor returns from active positions.
    
    Parameters:
        returns: pd.DataFrame - Stock returns
        active: pd.DataFrame - Active positions
    
    Returns:
        pd.Series - Value factor returns (long - short)
    """
    R = returns.reindex_like(active)
    
    # Separate long and short positions
    L = (active > 0).astype(float)
    S = (active < 0).astype(float)
    
    # Equal-weight within each leg
    wL = L.div(L.sum(axis=1).replace(0, np.nan), axis=0)
    wS = S.div(S.sum(axis=1).replace(0, np.nan), axis=0)
    
    # Calculate returns
    long_ret = (R * wL).sum(axis=1, min_count=1)
    short_ret = (R * wS).sum(axis=1, min_count=1)
    
    return (long_ret - short_ret)


def calculate_value_factor_monthly(save_outputs=True, output_dir=None):
    """
    Main function to calculate value factor.
    
    Parameters:
        save_outputs: bool - Whether to save results
        output_dir: str - Output directory
    
    Returns:
        dict with 'US', 'EU', 'combined' value factor returns
    """
    if output_dir is None:
        output_dir = _DEFAULT_FACTOR_OUTPUT
    print("=" * 60)
    print("VALUE FACTOR CALCULATION (Book-to-Market)")
    print("=" * 60)
    
    # Fetch data
    print("\n[1/6] Fetching US data...")
    us_data = fetch_value_data_us()
    
    print("\n[2/6] Fetching EU data...")
    eu_data = fetch_value_data_eu()
    
    # Calculate B/M ratios
    print("\n[3/6] Calculating Book-to-Market ratios...")
    BM_us, mcap_us = calculate_book_to_market(
        us_data['bvps'], us_data['shares'], us_data['mcap']
    )
    BM_eu, mcap_eu = calculate_book_to_market(
        eu_data['bvps'], eu_data['shares'], eu_data['mcap']
    )
    
    # Generate signals (quantile from config.LONG_SHORT_CONFIG)
    print("\n[4/6] Generating value signals...")
    q = config.LONG_SHORT_CONFIG['long_percentile']
    sig_us = generate_value_signal(BM_us, mcap_us, q=q)
    sig_eu = generate_value_signal(BM_eu, mcap_eu, q=q)
    
    # Create overlapping portfolios
    print("\n[5/6] Creating 12-month overlapping portfolios...")
    act_us = hold_positions(sig_us, hold_months=12)
    act_eu = hold_positions(sig_eu, hold_months=12)
    
    # Calculate returns (same as Value_Factor.ipynb: prices -> pct_change, EU already %/100)
    print("\n[6/6] Calculating value factor returns...")
    prices_us = us_data['prices'].replace(0, np.nan)
    returns_us = prices_us.pct_change(fill_method=None)
    returns_us = returns_us.replace([np.inf, -np.inf], np.nan).dropna(how="all")

    returns_eu = eu_data['returns'].replace([np.inf, -np.inf], np.nan).dropna(how="all")

    val_us = calculate_value_returns(returns_us, act_us)
    val_us.name = 'VAL_US'

    val_eu = calculate_value_returns(returns_eu, act_eu)
    val_eu.name = 'VAL_EU'
    
    # Combine
    combined = pd.DataFrame({
        'VAL_US': val_us,
        'VAL_EU': val_eu
    })
    combined['VAL'] = combined.mean(axis=1, skipna=True)
    
    # Picks for pooling (last rebalance date): long/short tickers per region
    def _picks_from_active(active):
        if active is None or active.empty:
            return [], []
        last = active.dropna(how="all").iloc[-1] if not active.dropna(how="all").empty else active.iloc[-1]
        long_tickers = last.index[last > 0].tolist()
        short_tickers = last.index[last < 0].tolist()
        return long_tickers, short_tickers

    def _positions_jan_jul_from_active(active):
        """List of (date, long_tickers) and (date, short_tickers) at Jan/Jul only."""
        if active is None or active.empty:
            return [], []
        long_rows, short_rows = [], []
        for dt in active.index:
            if dt.month not in (1, 7):
                continue
            row = active.loc[dt]
            long_t = row.index[row > 0].tolist()
            short_t = row.index[row < 0].tolist()
            if long_t:
                long_rows.append((dt, long_t))
            if short_t:
                short_rows.append((dt, short_t))
        return long_rows, short_rows

    long_us, short_us = _positions_jan_jul_from_active(act_us)
    long_eu, short_eu = _positions_jan_jul_from_active(act_eu)

    results = {
        'US': val_us,
        'EU': val_eu,
        'combined': combined,
        'picks_us': _picks_from_active(act_us),
        'picks_eu': _picks_from_active(act_eu),
    }
    
    # Save outputs
    if save_outputs:
        print(f"\nSaving outputs to {output_dir}/...")
        os.makedirs(output_dir, exist_ok=True)
        
        # Save combined value
        value_returns = pd.DataFrame({'VAL': combined['VAL']})
        value_returns.to_excel(f'{output_dir}/value_returns.xlsx')
        print(f"  ✓ Saved value_returns.xlsx")
        
        # Save regional breakdown
        combined.to_excel(f'{output_dir}/value_regional.xlsx')
        print(f"  ✓ Saved value_regional.xlsx")
        save_positions_excel(long_us, short_us, long_eu, short_eu, Path(output_dir) / "value_positions.xlsx")
        print(f"  ✓ Saved value_positions.xlsx")
    
    print("\n" + "=" * 60)
    print("VALUE FACTOR COMPLETE")
    print("=" * 60)
    
    # Performance stats
    for name in ['VAL_US', 'VAL_EU', 'VAL']:
        ret = combined[name].dropna()
        if len(ret) > 0:
            sharpe = ret.mean() / ret.std() * np.sqrt(12)
            print(f"{name:8s} Sharpe: {sharpe:.3f}")
    
    return results


if __name__ == '__main__':
    # Can run directly: python value.py
    results = calculate_value_factor_monthly(
        save_outputs=True,
        output_dir=_DEFAULT_FACTOR_OUTPUT
    )
    
    print("\n✓ Value factor ready!")
    print("\nOutput files:")
    print("  - outputs/factors/value_returns.xlsx")
    print("  - outputs/factors/value_regional.xlsx")
