"""
Yield Curve Factor (Nelson-Siegel Model)

Logic aligned with Yield_Factor.ipynb: same data source (Google Sheets),
maturities in years, lambda=1.5, OLS fit, standardization on first 144 months.

Extracts latent factors from the yield curve using Nelson-Siegel parametrization:
- BETA0: Level (long-term rate)
- BETA1: Slope (short-term vs long-term)
- BETA2: Curvature (medium-term hump)

Data required:
- Treasury yields at different maturities (US and EU)
"""

import numpy as np
import pandas as pd
import statsmodels.api as sm
import sys
import os
from pathlib import Path

from factor_positions_io import save_positions_excel

# Default: write to project root outputs/factors (same regardless of cwd)
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent
_DEFAULT_FACTOR_OUTPUT = str(_PROJECT_ROOT / "outputs" / "factors")

# Tickers.xlsx defines the investable universe for beta position files (yield is macro, no stock picks)
_TICKERS_PATH = _PROJECT_ROOT / "data" / "Tickers.xlsx"


def nelson_siegel_design_matrix(t, lb=1.5):
    """
    Create Nelson-Siegel design matrix.
    
    The Nelson-Siegel model:
    y(t) = β0 + β1 * ((1-exp(-t/λ))/(t/λ)) + β2 * (((1-exp(-t/λ))/(t/λ)) - exp(-t/λ))
    
    Parameters:
        t: np.array - Maturities in years
        lb: float - Lambda parameter (controls decay rate)
    
    Returns:
        np.array - Design matrix [1, A, B] where:
            - Column 0: β0 (level)
            - Column 1: β1 (slope)  
            - Column 2: β2 (curvature)
    """
    A = (1 - np.exp(-t / lb)) / (t / lb)
    B = A - np.exp(-t / lb)
    X = np.column_stack([np.ones_like(t), A, B])
    return X


def fit_nelson_siegel(maturities, yields, lb=1.5):
    """
    Fit Nelson-Siegel model to yield curve.
    
    Parameters:
        maturities: np.array - Maturities in years
        yields: np.array - Yield rates (as decimals, e.g., 0.03 for 3%)
        lb: float - Lambda parameter
    
    Returns:
        tuple: (beta0, beta1, beta2) - Fitted parameters
    """
    X = nelson_siegel_design_matrix(maturities, lb)
    model = sm.OLS(yields, X)
    results = model.fit()
    return results.params  # [β0, β1, β2]


def extract_yield_factors(rates_df, maturities, lb=1.5, region='US'):
    """
    Extract Nelson-Siegel factors from yield curve data.
    
    Parameters:
        rates_df: pd.DataFrame - Yield data (rows=dates, cols=maturities)
        maturities: np.array - Maturity periods in years
        lb: float - Lambda parameter
        region: str - 'US' or 'EU'
    
    Returns:
        pd.DataFrame with columns [BETA0_{region}, BETA1_{region}, BETA2_{region}]
    """
    # Skip header row: first column = dates, rest = yields (in %)
    dates = pd.to_datetime(rates_df.iloc[1:, 0])
    yield_data = rates_df.iloc[1:, 1:].astype(float).values / 100.0
    
    # Fit Nelson-Siegel for each date
    betas_0, betas_1, betas_2 = [], [], []
    
    for i in range(yield_data.shape[0]):
        y = yield_data[i, :]
        
        # Skip if too many NaNs
        if np.isnan(y).sum() > len(y) / 2:
            betas_0.append(np.nan)
            betas_1.append(np.nan)
            betas_2.append(np.nan)
            continue
        
        try:
            beta0, beta1, beta2 = fit_nelson_siegel(maturities, y, lb)
            betas_0.append(beta0)
            betas_1.append(beta1)
            betas_2.append(beta2)
        except Exception as e:
            betas_0.append(np.nan)
            betas_1.append(np.nan)
            betas_2.append(np.nan)
    
    # Create DataFrame
    df = pd.DataFrame({
        f'BETA0_{region}': betas_0,
        f'BETA1_{region}': betas_1,
        f'BETA2_{region}': betas_2
    }, index=dates)
    
    return df


def standardize_factors(factors_df):
    """
    Standardize factors (subtract mean, divide by std).
    
    Parameters:
        factors_df: pd.DataFrame - Raw beta factors
    
    Returns:
        pd.DataFrame - Standardized factors
    """
    return (factors_df - factors_df.mean()) / factors_df.std()


def _load_tickers_universe():
    """
    Load long_us, short_us, long_eu, short_eu ticker lists from data/Tickers.xlsx.
    Returns dict with keys long_us, short_us, long_eu, short_eu, each a list of ticker strings.
    Yield factors are macro (no stock picks); we use the same universe as other factors.
    """
    if not _TICKERS_PATH.exists():
        return None
    sheet_order = ["long_us", "short_us", "long_eu", "short_eu"]
    out = {s: [] for s in sheet_order}
    for sheet in sheet_order:
        try:
            df = pd.read_excel(_TICKERS_PATH, sheet_name=sheet, header=0)
        except Exception:
            continue
        if df.empty:
            continue
        col = df.columns[0] if len(df.columns) else None
        for c in df.columns:
            if str(c).strip().upper() == "TICKER":
                col = c
                break
        if col is None:
            continue
        tickers = (
            df[col].astype(str).str.strip().replace("", pd.NA).dropna().unique().tolist()
        )
        # Drop bad tickers (Unnamed, nan)
        out[sheet] = [
            t
            for t in tickers
            if t
            and not (str(t).startswith("Unnamed") or "nan" in str(t).lower())
        ]
    return out


def load_yield_curves():
    """
    Load US and EU yield curve data from Google Sheets.
    
    Returns:
        tuple: (rates_us, rates_eu) DataFrames
    """
    sheet_id = '1QgQ4-WxhQliistZiTGe9jMZQH-9vWpeF'
    gid_us = '617929213'
    gid_eu = '1308664179'
    
    url_us = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid_us}'
    url_eu = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid_eu}'
    
    print("Loading yield curve data...")
    rates_us = pd.read_csv(url_us)
    rates_eu = pd.read_csv(url_eu)
    
    print(f"  ✓ US yields: {rates_us.shape}")
    print(f"  ✓ EU yields: {rates_eu.shape}")
    
    return rates_us, rates_eu


def calculate_yield_factors_monthly(save_outputs=True, output_dir=None):
    """
    Main function to calculate yield curve factors.
    
    Parameters:
        save_outputs: bool - Whether to save results
        output_dir: str - Output directory
    
    Returns:
        dict with 'US', 'EU', 'combined' yield factors
    """
    if output_dir is None:
        output_dir = _DEFAULT_FACTOR_OUTPUT
    print("=" * 60)
    print("YIELD CURVE FACTOR CALCULATION (Nelson-Siegel)")
    print("=" * 60)
    
    # Load data
    print("\n[1/4] Loading yield curve data...")
    rates_us, rates_eu = load_yield_curves()
    
    # Define maturities (in years)
    # US: 1m, 3m, 6m, 1y, 2y, 5y, 7y, 10y, 30y
    maturities_us = np.array([1, 3, 6, 12, 24, 60, 84, 120, 360]) / 12.0
    
    # EU: 1m, 3m, 6m, 1y, 2y, 5y, 7y, 10y, 20y, 30y
    maturities_eu = np.array([1, 3, 6, 12, 24, 60, 84, 120, 240, 360]) / 12.0
    
    # Extract factors
    print("\n[2/4] Extracting Nelson-Siegel factors...")
    print("  Fitting US yield curves...")
    factors_us = extract_yield_factors(rates_us, maturities_us, lb=1.5, region='US')
    print(f"    ✓ Extracted {len(factors_us)} US factor observations")
    
    print("  Fitting EU yield curves...")
    factors_eu = extract_yield_factors(rates_eu, maturities_eu, lb=1.5, region='EU')
    print(f"    ✓ Extracted {len(factors_eu)} EU factor observations")
    
    # Standardize factors
    print("\n[3/4] Standardizing factors...")
    # Use first 144 months for mean/std (aligned with Yield_Factor.ipynb)
    win = min(144, len(factors_us))
    factors_us_std = standardize_factors(factors_us.iloc[:win])
    factors_eu_std = standardize_factors(factors_eu.iloc[:win])
    
    # Reindex to original dates
    factors_us_std = factors_us_std.reindex(factors_us.index)
    factors_eu_std = factors_eu_std.reindex(factors_eu.index)
    
    print(f"  ✓ Standardized US factors: {factors_us_std.shape}")
    print(f"  ✓ Standardized EU factors: {factors_eu_std.shape}")
    
    # Combine both regions
    combined = pd.concat([factors_us_std, factors_eu_std], axis=1)
    
    # Calculate average across regions for each beta
    combined['BETA0'] = combined[['BETA0_US', 'BETA0_EU']].mean(axis=1, skipna=True)
    combined['BETA1'] = combined[['BETA1_US', 'BETA1_EU']].mean(axis=1, skipna=True)
    combined['BETA2'] = combined[['BETA2_US', 'BETA2_EU']].mean(axis=1, skipna=True)
    
    results = {
        'US': factors_us_std,
        'EU': factors_eu_std,
        'combined': combined
    }
    
    # Save outputs
    if save_outputs:
        print(f"\n[4/4] Saving outputs to {output_dir}/...")
        os.makedirs(output_dir, exist_ok=True)
        
        # Save each beta separately (for portfolio builder)
        for beta_num in [0, 1, 2]:
            beta_df = pd.DataFrame({
                f'BETA{beta_num}_US': factors_us_std[f'BETA{beta_num}_US'],
                f'BETA{beta_num}_EU': factors_eu_std[f'BETA{beta_num}_EU'],
                f'BETA{beta_num}': combined[f'BETA{beta_num}']
            })
            beta_df.to_excel(f'{output_dir}/beta{beta_num}_regional.xlsx')
            print(f"  ✓ Saved beta{beta_num}_regional.xlsx")
            
            # Also save simple version
            simple_df = pd.DataFrame({f'BETA{beta_num}': combined[f'BETA{beta_num}']})
            simple_df.to_excel(f'{output_dir}/beta{beta_num}_returns.xlsx')

        # Yield is a macro factor (no stock picks). Create beta0/1/2_positions.xlsx using
        # the same universe as other factors (data/Tickers.xlsx) so TSFM can apply beta weights.
        universe = _load_tickers_universe()
        if universe is not None:
            # Rebalance dates: one month-end per Jan/Jul in the yield factor index
            jan_jul = combined.index[combined.index.month.isin([1, 7])]
            rebalance_dates = sorted(
                set(
                    (pd.Timestamp(d).normalize() + pd.offsets.MonthEnd(0))
                    for d in jan_jul
                )
            )
            if rebalance_dates:
                long_us = [(d, universe["long_us"]) for d in rebalance_dates]
                short_us = [(d, universe["short_us"]) for d in rebalance_dates]
                long_eu = [(d, universe["long_eu"]) for d in rebalance_dates]
                short_eu = [(d, universe["short_eu"]) for d in rebalance_dates]
                for beta_num in [0, 1, 2]:
                    out_path = Path(output_dir) / f"beta{beta_num}_positions.xlsx"
                    save_positions_excel(long_us, short_us, long_eu, short_eu, out_path)
                    print(f"  ✓ Saved beta{beta_num}_positions.xlsx")
            else:
                print("  ⚠ No Jan/Jul dates in yield index; skipping beta *_positions.xlsx")
        else:
            print(f"  ⚠ Tickers.xlsx not found at {_TICKERS_PATH}; skipping beta *_positions.xlsx")
        
        # Save all factors together
        combined.to_excel(f'{output_dir}/yield_factors_all.xlsx')
        print(f"  ✓ Saved yield_factors_all.xlsx")
    
    print("\n" + "=" * 60)
    print("YIELD FACTOR CALCULATION COMPLETE")
    print("=" * 60)
    
    # Show summary stats
    print("\nFactor Summary (Combined):")
    for beta in ['BETA0', 'BETA1', 'BETA2']:
        series = combined[beta].dropna()
        if len(series) > 0:
            print(f"  {beta:6s}: mean={series.mean():7.4f}, std={series.std():7.4f}, obs={len(series)}")
    
    return results


if __name__ == '__main__':
    # Run directly: python yield.py
    results = calculate_yield_factors_monthly(
        save_outputs=True,
        output_dir=_DEFAULT_FACTOR_OUTPUT
    )
    
    print("\n✓ Yield factors ready!")
    print("\nOutput files:")
    print("  - outputs/factors/beta0_regional.xlsx, beta0_returns.xlsx, beta0_positions.xlsx (Level)")
    print("  - outputs/factors/beta1_regional.xlsx, beta1_returns.xlsx, beta1_positions.xlsx (Slope)")
    print("  - outputs/factors/beta2_regional.xlsx, beta2_returns.xlsx, beta2_positions.xlsx (Curvature)")
    print("  - outputs/factors/yield_factors_all.xlsx (Combined)")
    
    print("\nInterpretation:")
    print("  BETA0 (Level)     - Long-term interest rate level")
    print("  BETA1 (Slope)     - Short vs long rates (yield curve slope)")
    print("  BETA2 (Curvature) - Medium-term hump (curvature)")
