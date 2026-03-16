# Project structure

## Current layout

```
Minerva Code/
├── config.py                    # Paths, factor flags, weights, get_output_path() / get_data_path()
├── run_all.py                   # Pipeline: all factors → factor returns → outputs/portfolio_{region}
├── hrp_allocation.py            # HRP stock weights → outputs/hrp_weights/hrp_weights.xlsx
├── us_portfolio_performance.py  # US portfolio AR–AX from Performance xlsx → outputs/portfolio_us/
├── verify_output_paths.py       # Sanity check: outputs under project root only
├── README.md
├── requirements.txt
├── .gitignore
├── HOW_THE_CODE_WORKS.md        # How the code works and how to add a new weighting algorithm
│
├── data/                        # Input Excel files (gitignored)
│   ├── Tickers.xlsx
│   ├── US_Returns.xlsx
│   ├── EU_Returns.xlsx
│   ├── Performance_SPRING_2026.xlsx
│   └── Minerva_Size_Factor.xlsx (optional)
│
├── notebooks/
│   ├── HRP_Allocation.ipynb
│   ├── Factor_Portfolio_US.ipynb
│   ├── Factor_Portfolio_EU.ipynb
│   ├── portfolio_analysis.ipynb
│   └── factor_performance_backtest.ipynb
│
├── src/                         # Factor and data code only
│   ├── data_loader.py           # Loads/caches from Sheets/Drive → src/temp/
│   ├── value.py, momentum.py, quality.py, liquidity.py, yield_factor.py, lowvol.py
│   └── temp/                    # Cache (gitignored)
│
└── outputs/                     # All generated files (gitignored)
    ├── factors/                 # Factor returns, regional xlsx
    ├── hrp_weights/            # hrp_weights.xlsx (long_eu, short_eu, long_us, short_us)
    ├── portfolio_us/, portfolio_eu/, portfolio_combined/
    └── plots/                   # Heatmaps, cumulative returns, etc.
```

## Path rules

- **Outputs:** All under project root `outputs/`. Use `config.get_output_path('factors')`, `get_output_path('hrp_weights')`, etc.
- **Data:** All inputs under `data/`. Use `config.get_data_path('Tickers.xlsx')`, etc.
- **Cache:** Only `src/temp/` (data_loader). No outputs under `src/`.

## Quick reference

| Task              | Command / note                                      |
|-------------------|-----------------------------------------------------|
| Check paths       | `python verify_output_paths.py`                     |
| Full pipeline     | `python run_all.py [--region us\|eu\|combined]`     |
| HRP weights       | `python hrp_allocation.py` → `outputs/hrp_weights/` |
| US performance    | `python us_portfolio_performance.py [path.xlsx]`     |
| Config            | `python config.py` to print config                  |
