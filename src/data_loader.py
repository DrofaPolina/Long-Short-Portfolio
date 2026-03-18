"""
Data loading utilities for factor portfolio construction.

This module provides functions to load data from Google Sheets/Drive.
Each function loads a specific dataset and returns a clean pandas DataFrame.

Temp/cache files are written to src/temp/ so all cache paths are in one place.
"""

import os
from pathlib import Path
import pandas as pd
import gdown

# Temp/cache folder lives inside src/ (src/temp/)
_SRC_DIR = Path(__file__).resolve().parent
_CACHE_DIR = _SRC_DIR / "temp"

STOCKS_SHEET_ID = '1oCUB1exeFq3AnxRcBppZNsbOxhBQyiW5'


def _cache_path(filename):
    """Path for a cache/temp file in the dedicated temp/ folder. Creates temp/ if needed."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return _CACHE_DIR / filename


def load_stock_prices_us():
    """
    Load US stock price data from Google Sheets.
    
    Returns:
        pd.DataFrame: Stock prices with Date index and ticker columns
    """
    sheet_id = STOCKS_SHEET_ID
    
    # Download the Excel file (simple on-disk cache)
    file_id = sheet_id
    download_url = f'https://drive.google.com/uc?id={file_id}'
    output = _cache_path('temp_prices_us.xlsx')
    if not output.exists():
        gdown.download(download_url, str(output), quiet=True)
    # Load US sheet
    df = pd.read_excel(output, sheet_name='Converted Data US', index_col=0)
    df.index = pd.to_datetime(df.index, errors='coerce')
    
    return df


def load_stock_prices_eu():
    """
    Load EU stock price data from Google Sheets.
    
    Returns:
        pd.DataFrame: Stock prices with Date index and ticker columns
    """
    sheet_id = STOCKS_SHEET_ID
    
    # Download the Excel file (simple on-disk cache)
    file_id = sheet_id
    download_url = f'https://drive.google.com/uc?id={file_id}'
    output = _cache_path('temp_prices_eu.xlsx')
    if not output.exists():
        gdown.download(download_url, str(output), quiet=True)
    # Load EU sheet
    df = pd.read_excel(output, sheet_name='Converted Data EU', index_col=0)
    df.index = pd.to_datetime(df.index, errors='coerce')
    
    return df


def load_financial_data_us(metric):
    """
    Load US financial data for quality factor.
    
    Parameters:
        metric: str - One of: 'roe', 'roe_growth', 'debt_eq', 'roe_quarterly'
    
    Returns:
        pd.DataFrame: Financial metric data with Date index
    """
    sheet_id = '1LVTrqEG0yY-S-sB1wkMByV5FTUJzvAq-'
    
    sheet_mapping = {
        'roe': 'Monthly ROE US',
        'roe_growth': 'ROE Growth 5 Year US',
        'debt_eq': 'Monthly Debt to Equity US',
        'roe_quarterly': 'Quarterly ROE US',
    }
    
    if metric not in sheet_mapping:
        raise ValueError(f"Unknown metric: {metric}. Choose from {list(sheet_mapping.keys())}")
    
    # Download (simple on-disk cache)
    download_url = f'https://drive.google.com/uc?id={sheet_id}'
    output = _cache_path('temp_financial.xlsx')
    if not output.exists():
        gdown.download(download_url, str(output), quiet=True)
    # Load specific sheet
    df = pd.read_excel(output, sheet_name=sheet_mapping[metric], index_col=0)
    df.index = pd.to_datetime(df.index, errors='coerce')

    # Clean common Excel artifacts: "Unnamed:*" columns and fully-empty columns
    df.columns = [str(c).strip() for c in df.columns]
    df = df.loc[:, ~pd.Series(df.columns).str.match(r"^Unnamed\s*:\s*\d+$", case=False).to_numpy()]
    df = df.dropna(axis=1, how="all")

    return df


def load_financial_data_eu(metric):
    """
    Load EU financial data for quality factor.
    
    Parameters:
        metric: str - One of: 'roe', 'roe_growth', 'debt_eq', 'roe_quarterly', 'safety'
    
    Returns:
        pd.DataFrame: Financial metric data with Date index
    """
    sheet_id = '1LVTrqEG0yY-S-sB1wkMByV5FTUJzvAq-'
    
    sheet_mapping = {
        'roe': 'Monthly ROE EU',
        'roe_growth': 'ROE Growth 5 Year EU',
        'debt_eq': 'Monthly Debt to Equity EU',
        'roe_quarterly': 'Quarterly ROE EU',
        # 'safety': 'Monthly Safety EU'
    }
    
    if metric not in sheet_mapping:
        raise ValueError(f"Unknown metric: {metric}. Choose from {list(sheet_mapping.keys())}")
    
    # Download (simple on-disk cache)
    download_url = f'https://drive.google.com/uc?id={sheet_id}'
    output = _cache_path('temp_financial_eu.xlsx')
    if not output.exists():
        gdown.download(download_url, str(output), quiet=True)
    # Load specific sheet
    df = pd.read_excel(output, sheet_name=sheet_mapping[metric], index_col=0)
    df.index = pd.to_datetime(df.index, errors='coerce')

    # Clean common Excel artifacts: "Unnamed:*" columns and fully-empty columns
    df.columns = [str(c).strip() for c in df.columns]
    df = df.loc[:, ~pd.Series(df.columns).str.match(r"^Unnamed\s*:\s*\d+$", case=False).to_numpy()]
    df = df.dropna(axis=1, how="all")

    return df


def load_yield_curves():
    """
    Load US and EU yield curve data.
    
    Returns:
        tuple: (rates_us, rates_eu) DataFrames
    """
    sheet_id = '1QgQ4-WxhQliistZiTGe9jMZQH-9vWpeF'
    gid_us = '617929213'
    gid_eu = '1308664179'
    
    url_us = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid_us}'
    url_eu = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid_eu}'
    
    rates_us = pd.read_csv(url_us)
    rates_eu = pd.read_csv(url_eu)
    
    return rates_us, rates_eu


def load_industry_mapping():
    """
    Load ticker to industry sector mapping.
    
    Returns:
        pd.DataFrame: with columns ['Ticker', 'Industry']
    """
    file_id = '1tkm01r0L9Gs2Di82mHaTnjJvibzE_NBE'
    file_url = f'https://drive.google.com/uc?id={file_id}'
    output = _cache_path('industry_name.csv')
    if not output.exists():
        gdown.download(file_url, str(output), quiet=True)
    
    industry_data = pd.read_csv(output, sep=';')
    industry_data.columns = ['Ticker', 'Industry']
    
    return industry_data


def load_stock_returns_us():
    """
    Calculate US stock returns from prices.
    
    Returns:
        pd.DataFrame: Daily returns
    """
    prices = load_stock_prices_us()
    returns = prices.pct_change(fill_method=None)[1:]
    return returns


def load_stock_returns_eu():
    """
    Calculate EU stock returns from prices.
    
    Returns:
        pd.DataFrame: Daily returns
    """
    prices = load_stock_prices_eu()
    returns = prices.pct_change(fill_method=None)[1:]
    return returns


if __name__ == '__main__':
    # Test the data loader
    print("Testing data loader...")
    
    print("\n1. Loading US stock prices...")
    us_prices = load_stock_prices_us()
    print(f"   Shape: {us_prices.shape}")
    print(f"   Date range: {us_prices.index[0]} to {us_prices.index[-1]}")
    
    print("\n2. Loading US ROE data...")
    roe_us = load_financial_data_us('roe')
    print(f"   Shape: {roe_us.shape}")
    
    print("\n3. Loading yield curves...")
    rates_us, rates_eu = load_yield_curves()
    print(f"   US rates shape: {rates_us.shape}")
    print(f"   EU rates shape: {rates_eu.shape}")
    
    print("\n✓ Data loader working!")
