"""
Configuration for Factor Portfolio

This file contains all configurable parameters:
"""

from pathlib import Path

# Project root (directory containing config.py, run_all.py, src/)
PROJECT_ROOT = Path(__file__).resolve().parent

# Input data directory (Excel files: Tickers, US_Returns, EU_Returns, Performance_*.xlsx, Minerva_Size_Factor.xlsx)
DATA_DIR = PROJECT_ROOT / "data"


def get_data_path(*parts):
    """Return path under data/ (e.g. get_data_path('Tickers.xlsx'))."""
    return DATA_DIR.joinpath(*parts)

# ============================================
# FACTOR SELECTION
# ============================================

# Which factors to include in the portfolio
ACTIVE_FACTORS = {
    'BETA0': True,   # Yield curve level
    'BETA1': True,   # Yield curve slope  
    'BETA2': True,   # Yield curve curvature
    'MOM': True,     # Momentum
    'QLT': True,     # Quality
    'SIZE': True,    # Size
    'VAL': True,     # Value
    'LVOL': False,   # Low volatility (optional)
}

# ============================================
# QUALITY FACTOR WEIGHTS
# ============================================

# Weights for quality factor components
QUALITY_WEIGHTS = {
    'roe': 0.24,           # Return on Equity (profitability)
    'roe_growth': 0.22,    # ROE growth (earnings growth)
    'debt_eq': -0.16,      # Debt/Equity (leverage, negative = less debt is better)
    'safety': 0.16,        # Safety score (financial stability)
    'evol': -0.22          # Earnings volatility (negative = less volatile is better)
}

# ============================================
# PORTFOLIO CONSTRUCTION WEIGHTS
# (These will come from HRP optimization)
# ============================================

# NOTE: These are example weights - actual weights should be generated
# by your HRP_Allocation.ipynb script and saved to weights.xlsx

PORTFOLIO_WEIGHTS = {
    'BETA0': 0.15,
    'BETA1': 0.15,
    'BETA2': 0.00,  # Can be set to 0 to exclude
    'MOM': 0.10,
    'QLT': 0.25,
    'SIZE': -0.10,  # Negative = short small caps
    'VAL': 0.15,
    'LVOL': 0.25
}

# Factor-level weights source (for analysis, reporting, combined factor return)
# Priority: TSFM file > HRP file > static PORTFOLIO_WEIGHTS
USE_TSFM_WEIGHTS = True   # If True, load from outputs/hrp_weights/factor_momentum_weights.xlsx (from factor_momentum.py)
USE_HRP_WEIGHTS = False   # If True and no TSFM, load from hrp_weights.xlsx first sheet (legacy)

# ============================================
# LONG-SHORT PORTFOLIO PARAMETERS
# ============================================

# For factor construction (long-short portfolios)
LONG_SHORT_CONFIG = {
    'long_percentile': 0.20,   # Top 20% = long
    'short_percentile': 0.80,  # Bottom 20% = short
    'equal_weighted': True,     # Equal weight within each leg
    'rebalance_frequency': 'M'  # Monthly rebalancing
}

# ============================================
# OPTIMIZATION PARAMETERS
# ============================================

# HRP (Hierarchical Risk Parity) settings
HRP_CONFIG = {
    'linkage_method': 'single',  # single, complete, average, ward
    'distance_metric': 'correlation'  # correlation, euclidean
}

# TSFM (Time-Series Factor Momentum) — used by factor_momentum.py
TSFM_CONFIG = {
    'formation_months': 12,   # Formation return lookback (months)
    'vol_months': 36,        # Volatility lookback (months)
    'signal_cap': 2.0,       # Cap signal in [-signal_cap, +signal_cap]
    'rebalance_months': (1, 7),  # Jan and Jul
}

# Mean-Variance optimization settings
MVO_CONFIG = {
    'long_exposure': 0.5,     # Total long exposure
    'short_exposure': -0.5,   # Total short exposure
    'min_weight': 0.02,       # Minimum weight per stock (long leg)
    'max_weight': 0.05,       # Maximum weight per stock (long leg)
    'min_weight_short': -0.05,  # Minimum weight per stock (short leg)
    'max_weight_short': -0.02   # Maximum weight per stock (short leg)
}

# ============================================
# DATA PARAMETERS
# ============================================

# Rolling window parameters (used by quality.py, lowvol.py)
EVOL_WINDOW = 20           # Earnings volatility rolling window (quarters)
EVOL_MIN_PERIODS = 12      # Minimum periods for EVOL calculation

VOL_WINDOW = 60            # Volatility rolling window (days)
VOL_MIN_PERIODS = 30       # Minimum periods for volatility

# ============================================
# OUTPUT SETTINGS
# ============================================

OUTPUT_DIRS = {
    'factors': 'outputs/factors',
    'portfolio_us': 'outputs/portfolio_us',
    'portfolio_eu': 'outputs/portfolio_eu',
    'portfolio_combined': 'outputs/portfolio_combined',
    'hrp_weights': 'outputs/hrp_weights',
}


def get_output_path(name):
    """Return absolute path for a named output directory (e.g. 'factors', 'hrp_weights')."""
    return PROJECT_ROOT / OUTPUT_DIRS[name]

# ============================================
# HELPER FUNCTIONS
# ============================================

def get_active_factors():
    """Return list of active factor names."""
    return [k for k, v in ACTIVE_FACTORS.items() if v]


def get_portfolio_weights(factors=None):
    """
    Get portfolio weights for specified factors.

    Priority: TSFM file (factor_momentum_weights.xlsx) > HRP file > static PORTFOLIO_WEIGHTS.
    Used for analysis, reporting, and combined factor return (not for stock-level HRP weights).
    """
    import pandas as pd

    # 1) TSFM factor weights (from factor_momentum.py)
    if USE_TSFM_WEIGHTS:
        tsfm_file = get_output_path('hrp_weights') / 'factor_momentum_weights.xlsx'
        if tsfm_file.exists():
            try:
                current = pd.read_excel(tsfm_file, sheet_name='current')
                if 'Factor' in current.columns and 'Weight' in current.columns:
                    out = current.set_index('Factor')['Weight'].to_dict()
                    if factors is not None:
                        out = {k: out[k] for k in factors if k in out}
                    return out
            except Exception as e:
                print(f"Warning: Could not read TSFM weights from {tsfm_file}: {e}")
                print("Falling back to configured weights")
        else:
            print(f"Warning: TSFM weights file not found at {tsfm_file}. Run: python factor_momentum.py")
            print("Falling back to configured weights")

    # 2) Legacy: HRP file first sheet (stock-level weights interpreted as factor; rarely used)
    if USE_HRP_WEIGHTS:
        weights_file = get_output_path('hrp_weights') / 'hrp_weights.xlsx'
        if weights_file.exists():
            weights_df = pd.read_excel(weights_file, index_col=0)
            if 'Weight' in weights_df.columns:
                return weights_df['Weight'].to_dict()
        print("Falling back to configured weights")

    # 3) Static config
    if factors is None:
        factors = get_active_factors()
    return {k: PORTFOLIO_WEIGHTS[k] for k in factors if k in PORTFOLIO_WEIGHTS}


def print_config():
    """Print current configuration."""
    print("=" * 60)
    print("CURRENT CONFIGURATION")
    print("=" * 60)
    
    print("\nActive Factors:")
    for factor, active in ACTIVE_FACTORS.items():
        status = "✓" if active else "✗"
        print(f"  {status} {factor}")
    
    print("\nQuality Factor Weights:")
    for metric, weight in QUALITY_WEIGHTS.items():
        print(f"  {metric:12s}: {weight:+.2f}")
    
    print("\nPortfolio Weights:")
    weights = get_portfolio_weights()
    for factor, weight in weights.items():
        print(f"  {factor:8s}: {weight:+.2f}")
    
    print("\nOutput Directories:")
    for name, path in OUTPUT_DIRS.items():
        print(f"  {name:12s}: {path}")


if __name__ == '__main__':
    # Print configuration when run directly
    print_config()
