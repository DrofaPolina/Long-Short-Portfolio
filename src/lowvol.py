"""
Low Volatility Factor

Stocks with lower historical volatility tend to outperform (low-vol anomaly).

Strategy:
- Calculate rolling volatility for each stock
- Long stocks with lowest volatility (bottom decile)
- Short stocks with highest volatility (top decile)
- Equal-weighted within each leg

Note: The original Liquidity_Factor.ipynb used Amihud illiquidity, which is more complex
and requires volume data. This implementation uses a simpler volatility-based approach.
"""

import numpy as np
import pandas as pd
import sys
import os
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config

# Default: write to project root outputs/factors (same regardless of cwd)
_DEFAULT_FACTOR_OUTPUT = str(Path(__file__).resolve().parent.parent / "outputs" / "factors")
from data_loader import load_stock_returns_us, load_stock_returns_eu
from factor_positions_io import save_positions_excel


def calculate_rolling_volatility(returns, window=None, min_periods=None):
    """
    Calculate rolling volatility for each stock.
    
    Parameters:
        returns: pd.DataFrame - Stock returns
        window: int - Rolling window in days (default from config.VOL_WINDOW)
        min_periods: int - Minimum periods required (default from config.VOL_MIN_PERIODS)
    
    Returns:
        pd.DataFrame - Rolling volatility
    """
    if window is None:
        window = config.VOL_WINDOW
    if min_periods is None:
        min_periods = config.VOL_MIN_PERIODS
    return returns.rolling(window=window, min_periods=min_periods).std()


def calculate_lowvol_factor(returns, volatility, long_short_pct=0.10, min_start_idx=60):
    """
    Calculate low volatility factor.

    Long stocks with LOW volatility, short stocks with HIGH volatility.

    Parameters:
        returns: pd.DataFrame - Daily returns
        volatility: pd.DataFrame - Rolling volatility
        long_short_pct: float - Percentage for long/short
        min_start_idx: int - Minimum index to start

    Returns:
        tuple: (pd.Series of daily factor returns, last_long_tickers, last_short_tickers, history)
        history: list of (date, long_list, short_list) at Jan/Jul rebalance dates
    """
    vol = volatility.reindex(columns=returns.columns)

    dates = vol.index
    T, N = vol.shape

    first_idx = max(min_start_idx, 60)
    lowvol_factor = pd.Series(np.nan, index=dates, dtype=float)
    last_long, last_short = [], []
    history = []

    t = first_idx
    while t < T - 1:
        dt = dates[t]
        x = vol.loc[dt].dropna()

        if len(x) < 20:
            t += 1
            continue

        # Select low volatility (long) and high volatility (short)
        cutoff_low = x.quantile(long_short_pct)      # Bottom 10% = low vol
        cutoff_high = x.quantile(1 - long_short_pct) # Top 10% = high vol

        low_vol_names = x[x <= cutoff_low].index.tolist()   # Long these
        high_vol_names = x[x >= cutoff_high].index.tolist() # Short these

        if len(low_vol_names) < 5 or len(high_vol_names) < 5:
            t += 1
            continue

        last_long, last_short = low_vol_names, high_vol_names
        if dt.month in (1, 7) and (t + 1 >= T or dates[t + 1].month != dt.month):
            history.append((dt, low_vol_names, high_vol_names))

        # Calculate next period's return
        dt_next = dates[t + 1]
        r_next = returns.loc[dt_next]

        low_vol_ret = r_next.reindex(low_vol_names).mean()
        high_vol_ret = r_next.reindex(high_vol_names).mean()

        if pd.notna(low_vol_ret) and pd.notna(high_vol_ret):
            # Long low-vol, short high-vol
            lowvol_factor.loc[dt_next] = low_vol_ret - high_vol_ret

        t += 1

    return lowvol_factor.dropna(), last_long, last_short, history


def calculate_lowvol_factor_monthly(us_returns, eu_returns, vol_window=None,
                                     save_outputs=True, output_dir=None):
    """
    Main function to calculate low volatility factor.
    
    Parameters:
        us_returns: pd.DataFrame - US stock returns
        eu_returns: pd.DataFrame - EU stock returns
        vol_window: int - Volatility calculation window in days (default from config.VOL_WINDOW)
        save_outputs: bool - Whether to save results
        output_dir: str - Output directory
    
    Returns:
        dict with 'US', 'EU', 'combined' factor returns
    """
    if vol_window is None:
        vol_window = config.VOL_WINDOW
    if output_dir is None:
        output_dir = _DEFAULT_FACTOR_OUTPUT
    print("=" * 60)
    print("LOW VOLATILITY FACTOR CALCULATION")
    print("=" * 60)
    
    print(f"\n[1/3] Calculating US low-vol factor (window={vol_window} days)...")
    us_vol = calculate_rolling_volatility(us_returns, window=vol_window)
    us_factor, lvol_last_long_us, lvol_last_short_us, history_us = calculate_lowvol_factor(us_returns, us_vol)
    
    # Convert to monthly
    us_factor_monthly = (1 + us_factor).resample('M').prod() - 1
    us_factor_monthly.name = 'LVOL_US'
    
    print(f"  ✓ US low-vol: {len(us_factor_monthly)} monthly observations")
    print(f"    Sharpe: {us_factor_monthly.mean() / us_factor_monthly.std() * np.sqrt(12):.3f}")
    
    print(f"\n[2/3] Calculating EU low-vol factor...")
    eu_vol = calculate_rolling_volatility(eu_returns, window=vol_window)
    eu_factor, lvol_last_long_eu, lvol_last_short_eu, history_eu = calculate_lowvol_factor(eu_returns, eu_vol)
    
    # Convert to monthly
    eu_factor_monthly = (1 + eu_factor).resample('M').prod() - 1
    eu_factor_monthly.name = 'LVOL_EU'
    
    print(f"  ✓ EU low-vol: {len(eu_factor_monthly)} monthly observations")
    print(f"    Sharpe: {eu_factor_monthly.mean() / eu_factor_monthly.std() * np.sqrt(12):.3f}")
    
    print("\n[3/3] Combining regions...")
    combined = pd.DataFrame({
        'LVOL_US': us_factor_monthly,
        'LVOL_EU': eu_factor_monthly
    })
    combined['LVOL'] = combined.mean(axis=1)
    
    results = {
        'US': us_factor_monthly,
        'EU': eu_factor_monthly,
        'combined': combined,
        'picks_us': (lvol_last_long_us, lvol_last_short_us),
        'picks_eu': (lvol_last_long_eu, lvol_last_short_eu),
    }
    
    # Save outputs
    if save_outputs:
        print(f"\nSaving outputs to {output_dir}/...")
        os.makedirs(output_dir, exist_ok=True)
        
        # Save combined factor
        lowvol_returns = pd.DataFrame({'LVOL': combined['LVOL']})
        lowvol_returns.to_excel(f'{output_dir}/lowvol_returns.xlsx')
        print(f"  ✓ Saved lowvol_returns.xlsx")
        
        # Save regional breakdown
        combined.to_excel(f'{output_dir}/lowvol_regional.xlsx')
        print(f"  ✓ Saved lowvol_regional.xlsx")
        long_us = [(d, long_l) for d, long_l, _ in history_us]
        short_us = [(d, short_l) for d, _, short_l in history_us]
        long_eu = [(d, long_l) for d, long_l, _ in history_eu]
        short_eu = [(d, short_l) for d, _, short_l in history_eu]
        save_positions_excel(long_us, short_us, long_eu, short_eu, Path(output_dir) / "lowvol_positions.xlsx")
        print(f"  ✓ Saved lowvol_positions.xlsx")
    
    print("\n" + "=" * 60)
    print("LOW VOLATILITY FACTOR COMPLETE")
    print("=" * 60)
    print(f"Combined Low-Vol Sharpe: {combined['LVOL'].mean() / combined['LVOL'].std() * np.sqrt(12):.3f}")
    
    return results


if __name__ == '__main__':
    print("Loading stock returns...")
    us_returns = load_stock_returns_us()
    eu_returns = load_stock_returns_eu()
    
    # Calculate low-vol factor (uses config.VOL_WINDOW by default)
    results = calculate_lowvol_factor_monthly(
        us_returns,
        eu_returns,
        save_outputs=True
    )
    
    print("\n✓ Low volatility factor ready!")
