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

sys.path.append('src')
from data_loader import load_stock_returns_us, load_stock_returns_eu


def calculate_rolling_volatility(returns, window=60, min_periods=30):
    """
    Calculate rolling volatility for each stock.
    
    Parameters:
        returns: pd.DataFrame - Stock returns
        window: int - Rolling window in days
        min_periods: int - Minimum periods required
    
    Returns:
        pd.DataFrame - Rolling volatility
    """
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
        pd.Series - Daily low-vol factor returns
    """
    vol = volatility.reindex(columns=returns.columns)
    
    dates = vol.index
    T, N = vol.shape
    
    first_idx = max(min_start_idx, 60)
    lowvol_factor = pd.Series(np.nan, index=dates, dtype=float)
    
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
        
        # Calculate next period's return
        dt_next = dates[t + 1]
        r_next = returns.loc[dt_next]
        
        low_vol_ret = r_next.reindex(low_vol_names).mean()
        high_vol_ret = r_next.reindex(high_vol_names).mean()
        
        if pd.notna(low_vol_ret) and pd.notna(high_vol_ret):
            # Long low-vol, short high-vol
            lowvol_factor.loc[dt_next] = low_vol_ret - high_vol_ret
        
        t += 1
    
    return lowvol_factor.dropna()


def calculate_lowvol_factor_monthly(us_returns, eu_returns, vol_window=60, 
                                     save_outputs=True, output_dir='outputs/factors'):
    """
    Main function to calculate low volatility factor.
    
    Parameters:
        us_returns: pd.DataFrame - US stock returns
        eu_returns: pd.DataFrame - EU stock returns
        vol_window: int - Volatility calculation window (days)
        save_outputs: bool - Whether to save results
        output_dir: str - Output directory
    
    Returns:
        dict with 'US', 'EU', 'combined' factor returns
    """
    print("=" * 60)
    print("LOW VOLATILITY FACTOR CALCULATION")
    print("=" * 60)
    
    print(f"\n[1/3] Calculating US low-vol factor (window={vol_window} days)...")
    us_vol = calculate_rolling_volatility(us_returns, window=vol_window)
    us_factor = calculate_lowvol_factor(us_returns, us_vol)
    
    # Convert to monthly
    us_factor_monthly = (1 + us_factor).resample('M').prod() - 1
    us_factor_monthly.name = 'LVOL_US'
    
    print(f"  ✓ US low-vol: {len(us_factor_monthly)} monthly observations")
    print(f"    Sharpe: {us_factor_monthly.mean() / us_factor_monthly.std() * np.sqrt(12):.3f}")
    
    print(f"\n[2/3] Calculating EU low-vol factor...")
    eu_vol = calculate_rolling_volatility(eu_returns, window=vol_window)
    eu_factor = calculate_lowvol_factor(eu_returns, eu_vol)
    
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
        'combined': combined
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
    
    print("\n" + "=" * 60)
    print("LOW VOLATILITY FACTOR COMPLETE")
    print("=" * 60)
    print(f"Combined Low-Vol Sharpe: {combined['LVOL'].mean() / combined['LVOL'].std() * np.sqrt(12):.3f}")
    
    return results


if __name__ == '__main__':
    print("Loading stock returns...")
    us_returns = load_stock_returns_us()
    eu_returns = load_stock_returns_eu()
    
    # Calculate low-vol factor
    results = calculate_lowvol_factor_monthly(
        us_returns,
        eu_returns,
        vol_window=60,
        save_outputs=True
    )
    
    print("\n✓ Low volatility factor ready!")
