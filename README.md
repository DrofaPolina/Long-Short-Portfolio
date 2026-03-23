# Minerva Code – Factor portfolio pipeline

Multi-factor pipeline (Value, Momentum, Liquidity, Quality, Yield, Low Vol) with three stock-level weighting schemes: **HRP** (stock-level), **TSFM** (factor momentum then equal weight within factor legs), and **equal factor weights** (1/K per active factor, same within-leg logic as TSFM). Outputs feed into performance workbooks.

---

## Data files (local, not from remote)

All paths below are under the **`data/`** folder at the project root. The repo is set up so only `Tickers.xlsx` (and optionally `Tickers_HRP.xlsx`) are tracked; everything else in `data/` is gitignored.

| File | Required? | Used by |
|------|-----------|--------|
| **`data/Tickers.xlsx`** | **Yes** | run_all (ticker list), HRP with `--local`. Intended to be in the repo so the project runs out of the box. |
| **`data/Performance_SPRING_2026.xlsx`** | **Yes** (for performance step) | `performance_from_hrp.py` and run_all’s final performance step. Add this file locally; it is not in the repo. |
| `data/Tickers_HRP.xlsx` | Optional | One-time source for `copy_tickers_hrp_to_tickers.py` to refresh `Tickers.xlsx`. |
| `data/Minerva_Size_Factor.xlsx` | Optional | Liquidity factor in run_all. If missing, LIQ uses cached output or placeholder. |
| `data/US_Returns.xlsx`, `data/EU_Returns.xlsx` | Only for HRP `--local` | `hrp_allocation.py --local`. With default (Google Sheets) they are not needed. |

**Summary:** Put **`Performance_SPRING_2026.xlsx`** in `data/` if you want the performance step and run_all end-to-end. Keep **`Tickers.xlsx`** in the repo. Other files are optional or only for `hrp_allocation.py --local`.

---

## How to use

**From the project root** (folder containing `run_all.py` and `src/`):

```bash
pip install -r requirements.txt
```

### 1. Factors and portfolio

```bash
python run_all.py
```

- Runs all factor modules, writes `outputs/factors/*` (returns, positions).
- Builds regional/combined factor returns and portfolio stats.
- Option: `--region us` or `--region eu`.
- **Note:** `data/Tickers.xlsx` is **not** overwritten by default (see [Tickers](#tickers) below).

### 2. Stock weights (HRP)

**Default: Google Sheets.** Set env vars `HRP_TICKERS_URLS` (4 space-separated CSV export URLs), `HRP_US_RETURNS_URL`, `HRP_EU_RETURNS_URL`, then:

```bash
python hrp_allocation.py
```

To use local files instead (and download from Drive if missing):

```bash
python hrp_allocation.py --local
```

- **Output:** `outputs/hrp_weights/hrp_weights.xlsx` (sheets: long_us, short_us, long_eu, short_eu).

### 3. Factor weights (TSFM) and TSFM stock weights

```bash
python factor_momentum.py
python tsfm_stock_weights.py
```

- **factor_momentum.py** → `outputs/hrp_weights/factor_momentum_weights.xlsx` (factor weights from time-series momentum).
- **tsfm_stock_weights.py** → `outputs/hrp_weights/tsfm_stock_weights.xlsx` (stock-level weights using TSFM factor weights and factor positions).

### 3b. Equal factor weights (1/K) — same sheet format as TSFM

Does **not** require `factor_momentum_weights.xlsx`. Uses only `config.ACTIVE_FACTORS` (factors with a `*_positions.xlsx` stem) and assigns **1/K** to each factor; within each factor, stocks are equal-weighted on the long and short legs (same pipeline as TSFM).

```bash
python equal_stock_weights.py
```

- **Output:** `outputs/hrp_weights/equal_stock_weights.xlsx` (same four sheets as `hrp_weights.xlsx` / `tsfm_stock_weights.xlsx`).

### 4. Performance from weight files

Requires **`data/Performance_SPRING_2026.xlsx`** (see [Data files](#data-files-local-not-from-remote)).

```bash
python performance_from_hrp.py
```

- Reads `hrp_weights.xlsx`, `tsfm_stock_weights.xlsx`, and `equal_stock_weights.xlsx` (skips any missing file), plus `data/Performance_SPRING_2026.xlsx`.
- **Output:** **`performance_from_all.xlsx`** only — sheet **`PERFORMANCE`** has **HRP | TSFM | EQUAL** side by side (each method: EU | US | Total), plus weight sheets `HRP_W_*`, `TSFM_W_*`, `EQUAL_W_*`.

### Tickers

- **HRP** and **run_all** expect `data/Tickers.xlsx` with four sheets: **long_us**, **short_us**, **long_eu**, **short_eu** (one ticker per row, column `TICKER` or first column).
- **Current state (important):** For HRP, the ticker universe is effectively **hardcoded by `data/Tickers.xlsx`** (this file is intended to be committed so the repo is runnable out of the box).
- To align with a canonical set: put `Tickers_HRP.xlsx` in `data/` and run once:
  ```bash
  python copy_tickers_hrp_to_tickers.py
  ```
- The step that would overwrite `Tickers.xlsx` from factor positions is **disabled** in `run_all.py` so the file is not modified by the pipeline; re-enable when pooling logic is fixed.

---

## Logic (short)

### HRP (Hierarchical Risk Parity) — `hrp_allocation.py`

- **Input:** Ticker lists (long/short US/EU) and a returns matrix (e.g. from US_Returns + EU_Returns).
- **Per leg:**  
  1. Use returns of the leg’s tickers to compute a **correlation matrix**, then turn it into a **distance** (e.g. \(\sqrt{0.5(1-\rho)}\)).  
  2. **Hierarchical clustering** (e.g. single linkage) on that distance; get asset order from the dendrogram leaves.  
  3. **Recursive bisection:** sort the covariance matrix by that order, split into two blocks, assign inverse-variance weights within each block and combine so that risk is balanced between blocks; recurse.  
  4. **Cap** weights (e.g. 10%) and **normalize** so the leg sums to a target (e.g. 0.5).  
- **Output:** One weight vector per leg → `hrp_weights.xlsx` (four sheets).

### Factor momentum (TSFM) — `factor_momentum.py`

- **Input:** Monthly factor returns in `outputs/factors/*_regional.xlsx` (from run_all).
- **At each rebalance date** (e.g. Jan and Jul):  
  1. **Formation return** = compounded return over the last 12 months.  
  2. **Volatility** = annualized vol over the last 36 months.  
  3. **Signal** = formation return / vol, **capped** (e.g. ±2).  
  4. **Weights** = proportional to absolute signal, normalized to sum to 1 (long/short by sign of signal).  
- **Output:** Current factor weights and history → `factor_momentum_weights.xlsx`. These weights are used by `config.get_portfolio_weights()` and by `tsfm_stock_weights.py` to build stock-level TSFM weights.

---

## Layout

```
Minerva Code/
├── config.py           # Active factors, paths, TSFM/HRP params
├── run_all.py          # Run all factors + portfolio (does not overwrite Tickers)
├── hrp_allocation.py   # HRP stock weights
├── factor_momentum.py  # TSFM factor weights
├── tsfm_stock_weights.py  # TSFM → stock weights (same format as hrp_weights)
├── equal_stock_weights.py # Equal 1/K factor weights → stock weights (TSFM-style pipeline)
├── performance_from_hrp.py # Performance from hrp + tsfm + equal weight files
├── copy_tickers_hrp_to_tickers.py  # One-time: Tickers_HRP → Tickers.xlsx
├── data/               # Tickers.xlsx, US_Returns, EU_Returns, Performance_*.xlsx, etc.
├── src/                 # Factor modules (value, momentum, quality, liquidity, yield, lowvol)
└── outputs/
    ├── factors/         # Factor returns, *_positions.xlsx
    ├── hrp_weights/     # hrp_weights.xlsx, factor_momentum_weights.xlsx, tsfm_stock_weights.xlsx, equal_stock_weights.xlsx
    └── portfolio_combined/  # performance_from_all.xlsx (+ run_all factor_returns, etc.)
```

- **data/** and **outputs/** are typically in `.gitignore`.

For more on layout and path rules, see [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md).

---

## Git: push to GitHub

1. Create an empty repo on GitHub (no README/.gitignore).
2. From project root:

```bash
git init
git add .
git commit -m "Initial commit: factor pipeline and notebooks"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git push -u origin main
```

Use a **Personal access token** (not account password) when `git push` asks for credentials.
