"""
Compare performance_2026_automation script output to original workbook (config.PERFORMANCE_WORKBOOK_FILENAME).

Reads EU PORTFOLIO, US PORTFOLIO, and TOTAL DAILY sheets from the workbook and compares
to the DataFrames produced by run_performance_2026(). Uses relative/absolute tolerance.
"""

from __future__ import annotations

import pytest
from pathlib import Path
import numpy as np
import pandas as pd

# Excel column letters to 0-based index: AR=43, AS=44, ..., AX=49
_COL_AR, _COL_AS, _COL_AT, _COL_AU, _COL_AV, _COL_AW, _COL_AX = 43, 44, 45, 46, 47, 48, 49
_AR_AX_COLS = ["AR", "AS", "AT", "AU", "AV", "AW", "AX"]
# Total Daily: A=0 (EU daily), B=1 (US daily), C=2 (Total), D=3 (date)
_TOTAL_DAILY_DATE_COL = 3
_TOTAL_DAILY_EU_COL = 0
_TOTAL_DAILY_US_COL = 1
_TOTAL_DAILY_C_COL = 2

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Use config so path stays in sync with performance_2026_automation
import config as _config
PERFORMANCE_XLSX = PROJECT_ROOT / _config.PERFORMANCE_WORKBOOK_FILENAME
HRP_WEIGHTS_XLSX = PROJECT_ROOT / _config.OUTPUT_DIRS["hrp_weights"] / "hrp_weights.xlsx"

# Tolerances
RTOL = 1e-4
ATOL = 0.01


def read_portfolio_sheet_from_excel(xlsx_path: Path, sheet_name: str) -> pd.DataFrame:
    """
    Read EU PORTFOLIO or US PORTFOLIO sheet. Col B (index 1) = date, cols AR:AX = 43:50 (0-based).
    Uses position-based reading so header row does not need to match.
    """
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=0)
    ncol = len(df.columns)
    # Date: column B = index 1
    date_idx = min(1, ncol - 1)
    dates = pd.to_datetime(df.iloc[:, date_idx], errors="coerce")
    valid = dates.notna()
    df = df.loc[valid].copy()
    dates = dates.loc[valid]
    # AR:AX at 0-based columns 43..49
    ar_ax_list = []
    for name in _AR_AX_COLS:
        j = _COL_AR + len(ar_ax_list)
        if j < ncol:
            ar_ax_list.append(pd.to_numeric(df.iloc[:, j], errors="coerce").values)
        else:
            ar_ax_list.append(np.full(len(df), np.nan))
    out = pd.DataFrame(
        np.column_stack(ar_ax_list),
        columns=_AR_AX_COLS,
        index=pd.DatetimeIndex(dates.values),
    )
    out.index.name = "Date"
    return out


def read_total_daily_from_excel(xlsx_path: Path) -> pd.DataFrame:
    """Read TOTAL DAILY sheet. A=0 (EU daily), B=1 (US daily), C=2 (Total), D=3 (date)."""
    df = pd.read_excel(xlsx_path, sheet_name="TOTAL DAILY", header=0)
    if df.shape[1] <= _TOTAL_DAILY_DATE_COL:
        return pd.DataFrame()
    dates = pd.to_datetime(df.iloc[:, _TOTAL_DAILY_DATE_COL], errors="coerce")
    valid = dates.notna()
    df = df.loc[valid]
    dates = dates.loc[valid]
    out = pd.DataFrame(
        {
            "A": pd.to_numeric(df.iloc[:, _TOTAL_DAILY_EU_COL], errors="coerce"),
            "B": pd.to_numeric(df.iloc[:, _TOTAL_DAILY_US_COL], errors="coerce"),
            "C": pd.to_numeric(df.iloc[:, _TOTAL_DAILY_C_COL], errors="coerce"),
        },
        index=pd.DatetimeIndex(dates.values),
    )
    out.index.name = "Date"
    return out


@pytest.fixture(scope="module")
def performance_script_output():
    """Run performance_2026 automation and return eu_portfolio, us_portfolio, total_daily."""
    if not PERFORMANCE_XLSX.exists():
        pytest.skip(f"Performance workbook not found: {PERFORMANCE_XLSX}")
    if not HRP_WEIGHTS_XLSX.exists():
        pytest.skip(f"hrp_weights.xlsx not found: {HRP_WEIGHTS_XLSX}. Run hrp_allocation.py first.")
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    import performance_2026_automation as p26
    result = p26.run_performance_2026(
        performance_xlsx=PERFORMANCE_XLSX,
        hrp_weights_xlsx=HRP_WEIGHTS_XLSX,
    )
    # Script always writes; tests only need the returned DataFrames
    return result


@pytest.fixture(scope="module")
def excel_eu_portfolio():
    if not PERFORMANCE_XLSX.exists():
        pytest.skip(f"Performance workbook not found: {PERFORMANCE_XLSX}")
    return read_portfolio_sheet_from_excel(PERFORMANCE_XLSX, "EU PORTFOLIO")


@pytest.fixture(scope="module")
def excel_us_portfolio():
    if not PERFORMANCE_XLSX.exists():
        pytest.skip(f"Performance workbook not found: {PERFORMANCE_XLSX}")
    return read_portfolio_sheet_from_excel(PERFORMANCE_XLSX, "US PORTFOLIO")


@pytest.fixture(scope="module")
def excel_total_daily():
    if not PERFORMANCE_XLSX.exists():
        pytest.skip(f"Performance workbook not found: {PERFORMANCE_XLSX}")
    return read_total_daily_from_excel(PERFORMANCE_XLSX)


def _align_and_compare(
    script_df: pd.DataFrame,
    excel_df: pd.DataFrame,
    columns: list[str],
    rtol: float = RTOL,
    atol: float = ATOL,
) -> None:
    """Align two DataFrames on index (date) and assert close for given columns."""
    common = script_df.index.intersection(excel_df.index).sort_values()
    if len(common) == 0:
        pytest.skip("No common dates between script and Excel")
    for col in columns:
        if col not in script_df.columns or col not in excel_df.columns:
            continue
        s = script_df.loc[common, col].astype(float)
        e = excel_df.loc[common, col].astype(float)
        mask = s.notna() | e.notna()
        if not mask.any():
            continue
        s, e = s[mask], e[mask]
        np.testing.assert_allclose(s, e, rtol=rtol, atol=atol, err_msg=f"Column {col}")


def _align_and_compare_by_position(
    script_df: pd.DataFrame,
    excel_df: pd.DataFrame,
    n_cols: int,
    rtol: float = RTOL,
    atol: float = ATOL,
) -> None:
    """Compare script and Excel by column position (script uses meaningful names; Excel has AR:AX)."""
    common = script_df.index.intersection(excel_df.index).sort_values()
    if len(common) == 0:
        pytest.skip("No common dates between script and Excel")
    n = min(n_cols, script_df.shape[1], excel_df.shape[1])
    for i in range(n):
        s = script_df.iloc[:, i].loc[common].astype(float)
        e = excel_df.iloc[:, i].loc[common].astype(float)
        mask = s.notna() | e.notna()
        if not mask.any():
            continue
        np.testing.assert_allclose(s[mask], e[mask], rtol=rtol, atol=atol, err_msg=f"Column index {i}")


class TestPerformance2026VsExcel:
    """Compare script output to original Excel sheets (Excel has AR:AX; script has meaningful names)."""

    def test_eu_portfolio_vs_excel(
        self, performance_script_output, excel_eu_portfolio
    ):
        eu_script = performance_script_output["eu_portfolio"]
        _align_and_compare_by_position(eu_script, excel_eu_portfolio, len(_AR_AX_COLS))

    def test_us_portfolio_vs_excel(
        self, performance_script_output, excel_us_portfolio
    ):
        us_script = performance_script_output["us_portfolio"]
        _align_and_compare_by_position(us_script, excel_us_portfolio, len(_AR_AX_COLS))

    def test_total_daily_vs_excel(
        self, performance_script_output, excel_total_daily
    ):
        if excel_total_daily.empty:
            pytest.skip("TOTAL DAILY sheet has no usable data")
        total_script = performance_script_output["total_daily"]
        script_abc = pd.DataFrame({
            "A": total_script["EU Daily"].values,
            "B": total_script["US Daily"].values,
            "C": total_script["Total Daily"].values,
        }, index=total_script.index)
        _align_and_compare(script_abc, excel_total_daily, ["A", "B", "C"])

    def test_script_produces_same_shape_as_excel(
        self, performance_script_output, excel_eu_portfolio, excel_us_portfolio
    ):
        eu_script = performance_script_output["eu_portfolio"]
        us_script = performance_script_output["us_portfolio"]
        assert eu_script.shape[1] >= len(_AR_AX_COLS)
        assert us_script.shape[1] == len(_AR_AX_COLS)
        common_eu = eu_script.index.intersection(excel_eu_portfolio.index)
        common_us = us_script.index.intersection(excel_us_portfolio.index)
        assert len(common_eu) > 0 or len(common_us) > 0, "At least one region should have common dates"
