# How the code works and how the project is organised

This document describes the data flow, where weights come from, and how to add a new weighting algorithm.

---

## 1. Project organisation

- **Config:** `config.py` is the single source for paths and main parameters. It defines:
  - `PROJECT_ROOT`, `DATA_DIR`, `get_data_path(...)` for input data under `data/`
  - `OUTPUT_DIRS`, `get_output_path(...)` for output directories under `outputs/`
  - Factor toggles (`ACTIVE_FACTORS`), quality weights, **factor-level** portfolio weights (`PORTFOLIO_WEIGHTS`), and `USE_HRP_WEIGHTS` (see below).

- **Data:** All input Excel files live in **`data/`**: `Tickers.xlsx`, `US_Returns.xlsx`, `EU_Returns.xlsx`, `Performance_SPRING_2026.xlsx`, and optionally `Minerva_Size_Factor.xlsx`. Scripts use `config.get_data_path("filename.xlsx")`.

- **Outputs:** Everything generated goes under **`outputs/`**, in subfolders such as `factors/`, `hrp_weights/`, `portfolio_us/`, `portfolio_eu/`, `portfolio_combined/`. Use `config.get_output_path('hrp_weights')` etc.; do not write under `src/`.

- **Source code:** Factor logic and data loading live in **`src/`** (e.g. `value.py`, `momentum.py`, `data_loader.py`). Cache only in `src/temp/`.

- **Notebooks:** All notebooks are in **`notebooks/`**. Their first cell sets the project root (so they work when run from `notebooks/` or from the repo root) and adds `src` to `sys.path`.

---

## 2. Two main pipelines

### Pipeline A: Factors → factor returns → portfolio stats

1. **`run_all.py`** (entry point):
   - Runs all factor modules in order: Value, Momentum, Liquidity, Quality, Yield (BETA0/BETA1/BETA2), Low Vol.
   - Each factor reads data (from `data/` or data_loader cache), computes factor returns, and writes to `outputs/factors/`.
   - Builds a factor-return matrix for the chosen region (`us`, `eu`, or `combined`).
   - Writes to `outputs/portfolio_{region}/`: `factor_returns.xlsx`, `performance_stats.xlsx`, `cumulative_returns.xlsx`.

2. **Factor modules** (`src/value.py`, `momentum.py`, etc.):
   - Expose a function like `calculate_*_factor_monthly(..., save_outputs=True, output_dir=...)`.
   - Return DataFrames of factor returns (often with regional columns, e.g. `VAL_US`, `VAL_EU`, `VAL` for combined).
   - `run_all.py` aggregates these into one matrix and saves under `outputs/portfolio_*`.

3. **Factor-level weights** (how much to allocate to each factor, e.g. VAL 0.15, MOM 0.10):
   - In **config**: `PORTFOLIO_WEIGHTS` dict and `config.get_portfolio_weights()`. If `USE_HRP_WEIGHTS` is True, `get_portfolio_weights()` tries to load from `outputs/hrp_weights/hrp_weights.xlsx` (first sheet, index + `Weight` column); otherwise it uses `PORTFOLIO_WEIGHTS`. These factor weights are used by code that combines factor returns into a single portfolio return (e.g. in notebooks or future pipeline steps).

### Pipeline B: Tickers + returns → weighting algorithm → stock weights

1. **Inputs:**
   - **Tickers:** `data/Tickers.xlsx` with four sheets: long US, short US, long EU, short EU (column `TICKER` or first column).
   - **Returns:** `data/US_Returns.xlsx` and `data/EU_Returns.xlsx` (one sheet each, index = dates, columns = tickers).

2. **Current weighting script: `hrp_allocation.py`**
   - Loads tickers and returns (from local files or, with `--use-sheets`, from Google Sheets).
   - For each of the four legs (long_eu, short_eu, long_us, short_us), computes **stock-level** weights using HRP (hierarchical risk parity): correlation → distance → linkage → recursive bisection → cap and normalize.
   - Writes **`outputs/hrp_weights/hrp_weights.xlsx`** with four sheets: `long_eu`, `short_eu`, `long_us`, `short_us`. Each sheet has columns **`Ticker`**, **`Weight`**. Short legs store negative weights.

3. **Who uses these stock weights?**
   - **US portfolio performance:** `us_portfolio_performance.py` does **not** read `hrp_weights.xlsx` directly. It reads the **Performance workbook** (default `data/Performance_SPRING_2026.xlsx`), specifically the “US PORTFOLIO” sheet (weights in row 8, tickers in row 10). In practice you would copy or export the long_us/short_us weights from `hrp_weights.xlsx` into that workbook (or another process updates the workbook), then run `us_portfolio_performance.py` to get AR–AX columns and CSV/xlsx in `outputs/portfolio_us/`.
   - **Notebooks** may read `hrp_weights.xlsx` or the Performance workbook for analysis and reporting.
   - **Config** `get_portfolio_weights()` reads the first sheet of `hrp_weights.xlsx` (long_eu) and returns a ticker→weight dict; this is used when factor-level weights are taken from HRP output (see above).

---

## 3. Summary: two kinds of weights

| Kind | Where defined / stored | Used by |
|------|------------------------|--------|
| **Factor weights** | `config.PORTFOLIO_WEIGHTS` or first sheet of `hrp_weights.xlsx` via `get_portfolio_weights()` | Combining factor returns (e.g. in notebooks or extended pipeline) |
| **Stock weights (per leg)** | `outputs/hrp_weights/hrp_weights.xlsx` (sheets long_eu, short_eu, long_us, short_us) | Performance workbook “US PORTFOLIO” (then `us_portfolio_performance.py`), notebooks, reporting |

---

## 4. How to add a new weighting algorithm

You can add a new method in two places: (1) **stock-level** weights (replacing or alongside HRP), or (2) **factor-level** weights (replacing or alongside `PORTFOLIO_WEIGHTS`).

### Option A: New stock-level weighting (e.g. equal weight, risk parity, custom)

1. **Reuse the same inputs as HRP:**
   - Tickers: `config.get_data_path("Tickers.xlsx")` (four sheets: long_us, short_us, long_eu, short_eu).
   - Returns: `config.get_data_path("US_Returns.xlsx")`, `config.get_data_path("EU_Returns.xlsx")`.

2. **Implement your algorithm** so that for each of the four legs you produce:
   - A list of tickers (subset of the leg’s tickers that have valid data).
   - A 1D array of weights (same length), non‑negative per leg; for short legs you can store negative in the file as HRP does.

3. **Match the output format expected by the rest of the project:**
   - Write an Excel file with four sheets: **`long_eu`**, **`short_eu`**, **`long_us`**, **`short_us`**.
   - Each sheet: columns **`Ticker`**, **`Weight`** (and optionally index). Short legs: negative weights if downstream expects that.

4. **Where to write:**
   - **Option (i):** Write to `outputs/hrp_weights/` (e.g. `hrp_weights.xlsx` or a new file like `my_weights.xlsx`). Then anything that currently reads `hrp_weights.xlsx` can be pointed to your file (e.g. config or a single env/flag), or you keep both and choose by filename.
   - **Option (ii):** Add a new key in `config.OUTPUT_DIRS`, e.g. `'my_weights': 'outputs/my_weights'`, and write there. Downstream (e.g. a script that fills the Performance workbook, or notebooks) would then read from `config.get_output_path('my_weights') / 'my_weights.xlsx'`.

5. **Practical ways to integrate:**
   - **New script** (e.g. `my_allocation.py`): same CLI pattern as `hrp_allocation.py` (read tickers + returns from `data/`, write to a chosen output dir). Optionally call the same helpers (`load_tickers`, `load_returns`) from `hrp_allocation.py` to avoid duplication.
   - **New function in `hrp_allocation.py`:** e.g. `run_my_weights_and_save(...)` that produces the same four-sheet Excel; then either replace the default run or add a CLI flag to choose HRP vs your method.

6. **Downstream:** Update the Performance workbook (or whatever builds “US PORTFOLIO”) to use your new file when you want US performance to reflect your algorithm. `us_portfolio_performance.py` does not need to change as long as the Performance workbook has the usual layout (row 8 = weights, row 10 = tickers).

### Option B: New factor-level weighting

- **Option (i):** Add a new dict in `config.py` (e.g. `MY_FACTOR_WEIGHTS`) and a flag or function that returns it; then in the code that combines factor returns, call that instead of `get_portfolio_weights()` when you want to use your weights.
- **Option (ii):** Your algorithm writes a one-sheet Excel (or CSV): index = factor name (VAL, MOM, QLT, …), column `Weight`. Then extend `config.get_portfolio_weights()` to optionally read that file (e.g. via a config path or env var) and return the same dict shape `{factor_name: weight}`.

---

## 5. Quick reference: key files

| File | Role |
|------|------|
| `config.py` | Paths (`data/`, `outputs/`), factor flags, factor weights, `get_portfolio_weights()` |
| `run_all.py` | Runs all factors, builds factor-return matrix, writes `outputs/portfolio_{region}/` |
| `hrp_allocation.py` | Loads tickers + returns, runs HRP per leg, writes `outputs/hrp_weights/hrp_weights.xlsx` |
| `us_portfolio_performance.py` | Reads Performance xlsx (US PORTFOLIO sheet), writes AR–AX to `outputs/portfolio_us/` |
| `data/Tickers.xlsx` | Four sheets: long US, short US, long EU, short EU tickers |
| `data/US_Returns.xlsx`, `data/EU_Returns.xlsx` | Used by HRP and factor modules |
| `data/Performance_*.xlsx` | Used by `us_portfolio_performance.py` (weights + tickers + prices) |
| `outputs/hrp_weights/hrp_weights.xlsx` | Four sheets: stock weights per leg; also read by `get_portfolio_weights()` (first sheet) |

Using this, you can add a new weighting algorithm (stock-level or factor-level) and plug it into the existing data paths and output layout without changing the rest of the project except where you choose to switch to the new weights.
