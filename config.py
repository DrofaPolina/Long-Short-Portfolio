"""
Configuration for Factor Portfolio

This file contains all configurable parameters:
- Factor weights
- Quality factor metric weights
- Optimization parameters
- Data sources

Modify these settings to test different portfolio configurations.
"""

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

# Default weights per region (used by build_portfolio_unified for weighted portfolio return)
# Yield factor = three betas: BETA0 (level), BETA1 (slope), BETA2 (curvature)
DEFAULT_WEIGHTS_US = {
    'SIZE_US': 0.1,
    'VAL_US': 0.15,
    'MOM_US': 0.1,
    'QLT_US': 0.2,
    'LVOL_US': 0.25,
    'BETA0_US': -0.10,
    'BETA1_US': 0.05,
    'BETA2_US': 0.05,
}
DEFAULT_WEIGHTS_EU = {
    'SIZE_EU': 0.1,
    'VAL_EU': 0.1,
    'MOM_EU': 0.1,
    'QLT_EU': 0.25,
    'LVOL_EU': 0.25,
    'BETA0_EU': -0.10,
    'BETA1_EU': 0.05,
    'BETA2_EU': 0.05,
}
DEFAULT_WEIGHTS_COMBINED = {
    'SIZE': 0.1,
    'VAL': 0.125,
    'MOM': 0.1,
    'QLT': 0.225,
    'LVOL': 0.25,
    'BETA0': -0.10,
    'BETA1': 0.05,
    'BETA2': 0.05,
}

# Alternative: Load weights from HRP optimization
USE_HRP_WEIGHTS = True  # If True, load from outputs/weights/hrp_weights.xlsx

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

# Rolling window parameters
EVOL_WINDOW = 20           # Earnings volatility rolling window (quarters)
EVOL_MIN_PERIODS = 12      # Minimum periods for EVOL calculation

VOL_WINDOW = 60            # Volatility rolling window (days)
VOL_MIN_PERIODS = 30       # Minimum periods for volatility

MOMENTUM_WINDOW = 252      # Momentum lookback (1 year)

# Industry neutralization
INDUSTRY_NEUTRAL = True
INDUSTRY_TRANSFORM = 'yeo'  # 'yeo' or 'normal'
WINSORIZE_LIMITS = (0.05, 0.05)  # (lower, upper) percentiles to winsorize

# ============================================
# OUTPUT SETTINGS
# ============================================

OUTPUT_DIRS = {
    'factors': 'outputs/factors',
    'portfolio': 'outputs/portfolio',
    'weights': 'outputs/weights',
    'analysis': 'outputs/analysis'
}

# Save intermediate results
SAVE_FACTOR_SCORES = True   # Save stock-level factor scores
SAVE_FACTOR_RETURNS = True  # Save factor returns
SAVE_DIAGNOSTICS = True     # Save diagnostic plots and stats

# ============================================
# HELPER FUNCTIONS
# ============================================

def get_active_factors():
    """Return list of active factor names."""
    return [k for k, v in ACTIVE_FACTORS.items() if v]


def get_portfolio_weights(factors=None):
    """
    Get portfolio weights for specified factors.
    
    If USE_HRP_WEIGHTS is True, loads from HRP output file.
    Otherwise uses PORTFOLIO_WEIGHTS dict.
    """
    if USE_HRP_WEIGHTS:
        import pandas as pd
        import os
        
        weights_file = os.path.join(OUTPUT_DIRS['weights'], 'hrp_weights.xlsx')
        if os.path.exists(weights_file):
            weights_df = pd.read_excel(weights_file, index_col=0)
            return weights_df['Weight'].to_dict()
        else:
            print(f"Warning: HRP weights file not found at {weights_file}")
            print("Falling back to configured weights")
    
    # Use configured weights
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
