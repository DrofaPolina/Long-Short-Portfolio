"""
Momentum Factor (12-1 Strategy)

Calculates momentum based on prior 12-month returns, skipping the most recent month
to avoid short-term reversal effects.

Strategy:
- Long top decile (10%) of stocks by momentum
- Short bottom decile (10%) of stocks by momentum
- Equal-weighted within each leg
"""

import numpy as np
import pandas as pd
import sys
import os

sys.path.append('src')
from data_loader import load_stock_returns_us, load_stock_returns_eu


def calculate_momentum_signal(returns, lookback=12, skip=1):
    """
    Calculate 12-1 momentum signal.
    
    Parameters:
        returns: pd.DataFrame - Daily returns (time x stocks)
        lookback: int - Lookback period in months (default 12)
        skip: int - Months to skip (default 1, skips most recent month)
    
    Returns:
        pd.DataFrame - Momentum signals (cumulative returns over lookback)
    """
    # Skip most recent month to avoid reversal
    r = returns.shift(skip)
    
    # Calculate cumulative return using log returns
    log_cum = np.log1p(r).rolling(lookback, min_periods=lookback).sum()
    
    # Convert back to simple returns
    momentum_signal = np.expm1(log_cum)
    
    return momentum_signal


def calculate_momentum_factor(returns, momentum_signal, long_short_pct=0.10, min_start_idx=60):
    """
    Calculate momentum factor using long-short strategy.
    
    Parameters:
        returns: pd.DataFrame - Daily returns
        momentum_signal: pd.DataFrame - Momentum signals
        long_short_pct: float - Percentage for long/short (0.10 = top/bottom deciles)
        min_start_idx: int - Minimum index to start (for data history)
    
    Returns:
        pd.Series - Daily momentum factor returns
    """
    signal = momentum_signal.reindex(columns=returns.columns)
    
    dates = signal.index
    T, N = signal.shape
    
    first_idx = max(min_start_idx, 12 + 1)
    momentum_factor = pd.Series(np.nan, index=dates, dtype=float)
    
    t = first_idx
    while t < T - 1:
        dt = dates[t]
        x = signal.loc[dt].dropna()
        
        if len(x) < 20:
            t += 1
            continue
        
        # Rank and select top/bottom
        cutoff_high = x.quantile(1 - long_short_pct)
        cutoff_low = x.quantile(long_short_pct)
        
        long_names = x[x >= cutoff_high].index.tolist()
        short_names = x[x <= cutoff_low].index.tolist()
        
        if len(long_names) < 5 or len(short_names) < 5:
            t += 1
            continue
        
        # Calculate next period's return
        dt_next = dates[t + 1]
        r_next = returns.loc[dt_next]
        
        long_ret = r_next.reindex(long_names).mean()
        short_ret = r_next.reindex(short_names).mean()
        
        if pd.notna(long_ret) and pd.notna(short_ret):
            momentum_factor.loc[dt_next] = long_ret - short_ret
        
        t += 1
    
    return momentum_factor.dropna()


def calculate_momentum_factor_monthly(us_returns, eu_returns, save_outputs=True, output_dir='outputs/factors'):
    """
    Main function to calculate momentum factor for both US and EU.
    
    Parameters:
        us_returns: pd.DataFrame - US stock returns
        eu_returns: pd.DataFrame - EU stock returns
        save_outputs: bool - Whether to save results
        output_dir: str - Output directory
    
    Returns:
        dict with 'US', 'EU', 'combined' factor returns
    """
    print("=" * 60)
    print("MOMENTUM FACTOR CALCULATION (12-1)")
    print("=" * 60)
    
    print("\n[1/3] Calculating US momentum...")
    us_signal = calculate_momentum_signal(us_returns, lookback=12, skip=1)
    us_factor = calculate_momentum_factor(us_returns, us_signal)
    
    # Convert to monthly
    us_factor_monthly = (1 + us_factor).resample('M').prod() - 1
    us_factor_monthly.name = 'MOM_US'
    
    print(f"  ✓ US momentum: {len(us_factor_monthly)} monthly observations")
    print(f"    Sharpe: {us_factor_monthly.mean() / us_factor_monthly.std() * np.sqrt(12):.3f}")
    
    print("\n[2/3] Calculating EU momentum...")
    eu_signal = calculate_momentum_signal(eu_returns, lookback=12, skip=1)
    eu_factor = calculate_momentum_factor(eu_returns, eu_signal)
    
    # Convert to monthly
    eu_factor_monthly = (1 + eu_factor).resample('M').prod() - 1
    eu_factor_monthly.name = 'MOM_EU'
    
    print(f"  ✓ EU momentum: {len(eu_factor_monthly)} monthly observations")
    print(f"    Sharpe: {eu_factor_monthly.mean() / eu_factor_monthly.std() * np.sqrt(12):.3f}")
    
    print("\n[3/3] Combining regions...")
    # Combine both regions (average)
    combined = pd.DataFrame({
        'MOM_US': us_factor_monthly,
        'MOM_EU': eu_factor_monthly
    })
    combined['MOM'] = combined.mean(axis=1)
    
    results = {
        'US': us_factor_monthly,
        'EU': eu_factor_monthly,
        'combined': combined
    }
    
    # Save outputs
    if save_outputs:
        print(f"\nSaving outputs to {output_dir}/...")
        os.makedirs(output_dir, exist_ok=True)
        
        # Save combined momentum
        momentum_returns = pd.DataFrame({'MOM': combined['MOM']})
        momentum_returns.to_excel(f'{output_dir}/momentum_returns.xlsx')
        print(f"  ✓ Saved momentum_returns.xlsx")
        
        # Save regional breakdown
        combined.to_excel(f'{output_dir}/momentum_regional.xlsx')
        print(f"  ✓ Saved momentum_regional.xlsx")
    
    print("\n" + "=" * 60)
    print("MOMENTUM FACTOR COMPLETE")
    print("=" * 60)
    print(f"Combined Momentum Sharpe: {combined['MOM'].mean() / combined['MOM'].std() * np.sqrt(12):.3f}")
    
    return results


if __name__ == '__main__':
    print("Loading stock returns...")
    us_returns = load_stock_returns_us()
    eu_returns = load_stock_returns_eu()
    
    # Calculate momentum factor
    results = calculate_momentum_factor_monthly(
        us_returns,
        eu_returns,
        save_outputs=True
    )
    
    print("\n✓ Momentum factor ready!")
