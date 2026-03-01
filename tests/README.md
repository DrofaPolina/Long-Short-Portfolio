# Tests: script output vs original Excel

## Run tests

```bash
# From project root (Minerva Code/)
pip install -r requirements.txt   # includes pytest
python -m pytest tests/ -v
```

## How to debug when tests fail (or numbers look wrong)

1. **See which comparison failed**  
   Run pytest with full tracebacks and stop on first failure:
   ```bash
   python -m pytest tests/test_performance_2026_vs_excel.py -v --tb=long -x
   ```
   The assertion message shows which column (e.g. `AR`, `AX`) failed.

2. **Run the debug comparison script**  
   This loads the Excel sheets and script output, then prints per-column diff stats and the worst rows:
   ```bash
   python tests/debug_compare.py
   ```
   You get:
   - For each column (AR, AS, AT, … and A, B, C): `n`, `max_abs_diff`, `mean_abs_diff`, and how many values fail the tolerance.
   - The first 10 dates with largest absolute difference (script vs Excel).

3. **Narrow down by column or save diffs**  
   ```bash
   python tests/debug_compare.py --col AR          # only EU/US AR and Total Daily A,B,C
   python tests/debug_compare.py --first 20        # show 20 worst rows per column
   python tests/debug_compare.py --save-diff       # write debug_diff_*.csv to outputs/performance_2026/
   ```
   Open the CSV in Excel or a notebook to inspect exact script vs Excel values and dates.

4. **Check Excel layout**  
   If the script assumes wrong column positions (e.g. date or AR:AX in different columns), the test file reads by position. Inspect the workbook:
   - EU PORTFOLIO / US PORTFOLIO: which column is the date? Which columns are AR, AU, AX?
   - TOTAL DAILY: which columns are EU daily (A), US daily (B), Total (C), Date (D)?
   Edit the constants at the top of `tests/test_performance_2026_vs_excel.py` (e.g. `_COL_AR`, `_TOTAL_DAILY_DATE_COL`) to match your Excel, then re-run tests and `debug_compare.py`.

5. **Inspect in a notebook**  
   Load script output and Excel in pandas and compare manually:
   ```python
   import performance_2026_automation as p26
   out = p26.run_performance_2026()
   # out["eu_portfolio"], out["us_portfolio"], out["total_daily"]
   import pandas as pd
   excel_eu = pd.read_excel("Performance_SPRING_2026.xlsx", sheet_name="EU PORTFOLIO")  # or config.PERFORMANCE_WORKBOOK_FILENAME
   # compare out["eu_portfolio"] with excel_eu columns you care about
   ```

## What is tested

### `test_performance_2026_vs_excel.py`

Compares **performance_2026_automation** output to the original workbook (**config.PERFORMANCE_WORKBOOK_FILENAME**, e.g. Performance_SPRING_2026.xlsx):

- **EU PORTFOLIO**: script `eu_portfolio` (meaningful column names) vs Excel sheet "EU PORTFOLIO" by position.
- **US PORTFOLIO**: script `us_portfolio` vs Excel sheet "US PORTFOLIO" by position.
- **TOTAL DAILY**: script `total_daily` (EU Daily, US Daily, Total Daily) vs Excel sheet "TOTAL DAILY" columns A, B, C.

DataFrames are aligned by date; numeric columns are compared with `rtol=1e-4`, `atol=0.01`.

**Requires:**

- Performance workbook (e.g. `Performance_SPRING_2026.xlsx`) in the project root; filename set in **config.PERFORMANCE_WORKBOOK_FILENAME**.
- `outputs/hrp_weights/hrp_weights.xlsx` (run `python hrp_allocation.py` first).

Tests are skipped if either file is missing.

### `test_hrp_allocation.py`

- **Structure**: `outputs/hrp_weights/hrp_weights.xlsx` has sheets `long_eu`, `short_eu`, `long_us`, `short_us` with Ticker/Weight columns.
- **US CSVs**: `weights_long_us.csv` and/or `weights_short_us.csv` exist when hrp_weights.xlsx exists.
- **Leg sign**: short legs have non-positive weight sum; long legs have non-negative sum.
- **Optional reference**: If `tests/fixtures/hrp_weights_reference.xlsx` exists (e.g. export from the HRP notebook), tickers and weights are compared with tolerance.

## Optional reference file

To compare HRP script output to a “golden” Excel from the notebook:

1. Export `hrp_weights.xlsx` from the HRP notebook (or save a known-good run).
2. Save it as `tests/fixtures/hrp_weights_reference.xlsx`.
3. Run tests; `test_hrp_vs_reference_excel` will compare tickers and weights per sheet.
