"""
Quality Factor Calculation

Combines multiple financial metrics:
- ROE (Return on Equity) - Profitability
- ROE Growth - Earnings growth
- Debt/Equity - Financial leverage (inverted)
# - Safety Score - Financial stability
- EVOL (Earnings Volatility) - Earnings consistency (inverted)

Higher quality score = better quality company
"""

import numpy as np
import pandas as pd
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_loader import (
    load_financial_data_us,
    load_financial_data_eu,
    load_stock_returns_us,
    load_stock_returns_eu,
)


def _z_scores_cross_sectional(df):
    """Z-scores across stocks (columns) at each time point (row). Same shape as input."""
    numeric = df.select_dtypes(include="number")
    if numeric.empty:
        return df.astype("float64") * np.nan
    z = numeric.sub(numeric.mean(axis=1), axis=0).div(numeric.std(axis=1), axis=0)
    out = df.copy()
    for col in out.columns:
        out[col] = z[col] if col in z.columns else np.nan
    return out


def __align_dataframes(*dfs):
    """Align multiple DataFrames to common index and columns. Returns list of aligned DataFrames."""
    cleaned = []
    for df in dfs:
        d = df.copy()
        if not d.index.is_unique:
            d = d[~d.index.duplicated(keep="first")]
        if not d.columns.is_unique:
            d = d.loc[:, ~d.columns.duplicated(keep="first")]
        cleaned.append(d)
    common_index = cleaned[0].index
    common_columns = cleaned[0].columns
    for d in cleaned[1:]:
        common_index = common_index.union(d.index)
        common_columns = common_columns.union(d.columns)
    return [d.reindex(index=common_index, columns=common_columns) for d in cleaned]


def _clean_time_index(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """
    Ensure a sorted, unique DateTimeIndex and column index.
    
    This avoids pandas InvalidIndexError and reindexing issues when
    concatenating or aligning multiple DataFrames.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
    # Drop non-parsable dates
    df = df[~df.index.isna()]
    # De-duplicate dates (keep first occurrence)
    if not df.index.is_unique:
        df = df[~df.index.duplicated(keep="first")]
    # Drop duplicate columns (keep first occurrence)
    if not df.columns.is_unique:
        df = df.loc[:, ~df.columns.duplicated(keep="first")]
    return df.sort_index()


def calculate_evol(roe_quarterly, window=20, min_periods=12):
    """
    Calculate earnings volatility (EVOL).
    
    Measures consistency of earnings - lower is better.
    
    Parameters:
        roe_quarterly: pd.DataFrame - Quarterly ROE data
        window: int - Rolling window size
        min_periods: int - Minimum periods for calculation
    
    Returns:
        pd.DataFrame - Earnings volatility scores
    """
    # Ensure numeric values and sorted unique DateTimeIndex to avoid
    # deprecation warnings about nuisance columns and non-unique indexes.
    df = roe_quarterly.copy()
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()]
    if not df.index.is_unique:
        df = df[~df.index.duplicated(keep="first")]
    df = df.apply(pd.to_numeric, errors="coerce")
    roe_sorted = df.sort_index()
    evol = roe_sorted.rolling(window=window, min_periods=min_periods).std()
    return evol


def calculate_quality_score_region(roe, roe_growth, debt_eq, roe_quarterly,
                                   weights=None, region_name=""):
    """
    Calculate quality scores for a single region (US or EU).
    
    Parameters:
        roe: pd.DataFrame - Return on Equity (monthly)
        roe_growth: pd.DataFrame - ROE growth rates
        debt_eq: pd.DataFrame - Debt to equity ratios
        roe_quarterly: pd.DataFrame - Quarterly ROE for EVOL
        weights: dict - Custom weights for each metric
    
    Returns:
        pd.DataFrame - Quality scores (time x stocks) for this region
    """
    # Default weights
    if weights is None:
        weights = {
            'roe': 0.24,
            'roe_growth': 0.22,
            'debt_eq': -0.16,   # Negative: less debt = better quality
            #'safety': 0.16,
            'evol': -0.22       # Negative: less volatility = better quality
        }
    
    print(f"Cleaning indices for {region_name}...")
    roe = _clean_time_index(roe, f"roe_{region_name}")
    roe_growth = _clean_time_index(roe_growth, f"roe_growth_{region_name}")
    debt_eq = _clean_time_index(debt_eq, f"debt_eq_{region_name}")
    roe_quarterly = _clean_time_index(roe_quarterly, f"roe_quarterly_{region_name}")

    print(f"Calculating earnings volatility for {region_name}...")
    evol = calculate_evol(roe_quarterly)
    
    # Ensure numeric values
    roe = roe.apply(pd.to_numeric, errors="coerce")
    roe_growth = roe_growth.apply(pd.to_numeric, errors="coerce")
    debt_eq = debt_eq.apply(pd.to_numeric, errors="coerce")
    evol = evol.apply(pd.to_numeric, errors="coerce")
    
    print(f"Calculating cross-sectional z-scores for {region_name}...")
    z_scores_evol = _z_scores_cross_sectional(evol)
    z_scores_roe = _z_scores_cross_sectional(roe)
    z_scores_roe_growth = _z_scores_cross_sectional(roe_growth)
    z_scores_debt_eq = _z_scores_cross_sectional(debt_eq)
    
    print(f"Aligning metrics for {region_name}...")
    aligned = _align_dataframes(
        z_scores_roe,
        z_scores_roe_growth,
        z_scores_debt_eq,
        # z_scores_safety,
        z_scores_evol
    )
    
    z_roe, z_roe_growth, z_debt_eq, z_evol = aligned
    
    print(f"Calculating weighted quality score for {region_name}...")
    quality_score = (
        (weights['roe'] * z_roe)
        .add(weights['roe_growth'] * z_roe_growth, fill_value=0)
        .add(weights['debt_eq'] * z_debt_eq, fill_value=0)
        # .add(weights['safety'] * z_safety, fill_value=0)
        .add(weights['evol'] * z_evol, fill_value=0)
    )
    
    quality_score.fillna(0, inplace=True)
    quality_score.index = pd.to_datetime(quality_score.index)
    
    return quality_score


def calculate_quality_portfolio_returns(quality_score, returns, region_name=""):
    """
    Calculate quality factor long-short portfolio returns for one region.
    
    Strategy:
    - Long top 20% of stocks by quality score
    - Short bottom 20% of stocks by quality score
    - Equal-weighted within each leg
    
    Parameters:
        quality_score: pd.DataFrame - Quality scores (time x stocks)
        returns: pd.DataFrame - Stock returns (time x stocks) for the same region
    
    Returns:
        pd.Series - Monthly portfolio returns
    """
    print(f"Assigning long/short positions for {region_name}...")
    def assign_positions(df):
        """Top 20% = 1 (long), Bottom 20% = -1 (short), Middle = 0"""
        deciles = df.rank(axis=1, method='first', pct=True).apply(lambda x: x * 10)
        positions = deciles.applymap(lambda x: 1 if x <= 2 else -1 if x >= 9 else 0)
        return positions
    
    positions = assign_positions(quality_score)
    
    print(f"Aligning returns and positions for {region_name}...")
    # Get common tickers
    common_tickers = returns.columns.intersection(positions.columns)
    aligned_returns = returns[common_tickers]
    aligned_positions = positions[common_tickers]
    
    # Convert to numeric and resample to monthly (month-end)
    aligned_returns = aligned_returns.apply(pd.to_numeric, errors='coerce')
    monthly_returns = aligned_returns.resample('M').sum()
    
    # Shift positions forward (use previous month's signal)
    shifted_positions = aligned_positions.shift(1)
    
    print(f"Calculating portfolio returns for {region_name}...")
    # Portfolio return = average return of positioned stocks
    portfolio_returns = (shifted_positions * monthly_returns).mean(axis=1)
    portfolio_returns.dropna(inplace=True)
    
    return portfolio_returns


def calculate_quality_factor(save_outputs=True, output_dir='outputs/factors'):
    """
    Main function to calculate quality factor.
    
    Loads all required data, calculates quality scores and returns,
    and optionally saves to Excel files.
    
    Parameters:
        save_outputs: bool - Whether to save results to disk
        output_dir: str - Directory to save outputs
    
    Returns:
        dict with keys:
            - 'returns': pd.Series - Monthly factor returns
            - 'scores': pd.DataFrame - Stock-level quality scores
    """
    print("=" * 60)
    print("QUALITY FACTOR CALCULATION")
    print("=" * 60)
    
    print("\n[1/4] Loading financial data...")
    roe_us = load_financial_data_us('roe')
    roe_eu = load_financial_data_eu('roe')
    roe_growth_us = load_financial_data_us('roe_growth')
    roe_growth_eu = load_financial_data_eu('roe_growth')
    debt_eq_us = load_financial_data_us('debt_eq')
    debt_eq_eu = load_financial_data_eu('debt_eq')
    roe_quarterly_us = load_financial_data_us('roe_quarterly')
    roe_quarterly_eu = load_financial_data_eu('roe_quarterly')
    
    print("\n[2/4] Calculating regional quality scores...")
    quality_scores_us = calculate_quality_score_region(
        roe_us, roe_growth_us, debt_eq_us, roe_quarterly_us, region_name="US"
    )
    quality_scores_eu = calculate_quality_score_region(
        roe_eu, roe_growth_eu, debt_eq_eu, roe_quarterly_eu, region_name="EU"
    )
    
    print("\n[3/4] Loading stock returns...")
    us_returns = load_stock_returns_us()
    eu_returns = load_stock_returns_eu()
    
    print("\n[4/4] Calculating portfolio returns...")
    quality_us = calculate_quality_portfolio_returns(
        quality_scores_us, us_returns, region_name="US"
    )
    quality_us.name = "QLT_US"
    
    quality_eu = calculate_quality_portfolio_returns(
        quality_scores_eu, eu_returns, region_name="EU"
    )
    quality_eu.name = "QLT_EU"
    
    combined = pd.DataFrame({
        'QLT_US': quality_us,
        'QLT_EU': quality_eu
    })
    combined['QLT'] = combined.mean(axis=1, skipna=True)
    
    # Prepare results
    results = {
        'US': quality_us,
        'EU': quality_eu,
        'combined': combined,
        'returns': combined['QLT'],
        'scores_us': quality_scores_us,
        'scores_eu': quality_scores_eu,
    }
    
    # Save outputs if requested
    if save_outputs:
        print(f"\nSaving outputs to {output_dir}/...")
        os.makedirs(output_dir, exist_ok=True)
        
        # Save combined returns (for unified portfolio builder)
        returns_df = pd.DataFrame({'QLT': combined['QLT']})
        returns_df.to_excel(f'{output_dir}/quality_returns.xlsx')
        print(f"  ✓ Saved quality_returns.xlsx")
        
        # Save regional breakdown
        combined.to_excel(f'{output_dir}/quality_regional.xlsx')
        print(f"  ✓ Saved quality_regional.xlsx")
        
        # Save scores (optional, combined universe)
        all_scores = pd.concat(
            [quality_scores_us, quality_scores_eu],
            axis=1
        )
        all_scores.to_excel(f'{output_dir}/quality_scores.xlsx')
        print(f"  ✓ Saved quality_scores.xlsx")
    
    print("\n" + "=" * 60)
    print("QUALITY FACTOR CALCULATION COMPLETE")
    print("=" * 60)
    qlt = combined['QLT'].dropna()
    print(f"Period: {qlt.index[0].strftime('%Y-%m-%d')} to {qlt.index[-1].strftime('%Y-%m-%d')}")
    print(f"Monthly observations: {len(qlt)}")
    print(f"Annualized Sharpe: {qlt.mean() / qlt.std() * np.sqrt(12):.3f}")
    
    return results


if __name__ == '__main__':
    # Run quality factor calculation
    results = calculate_quality_factor(save_outputs=True)
    
    print("\n✓ Quality factor ready!")
    print(f"  Combined returns shape: {results['returns'].shape}")
