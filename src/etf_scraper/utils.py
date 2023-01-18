import logging
from typing import Sequence

import pandas as pd
import pandas_market_calendars as mcal

logger = logging.getLogger(__name__)


def check_missing_cols(
    exp_cols: Sequence, returned_cols: Sequence, raise_error: bool = False
) -> None:
    """Convenience function to log if we are missing columns from a request.

    raises: ValueError if raise_error=True and if we are missing expected columns
    """
    missing_cols = [k for k in exp_cols if k not in returned_cols]

    if missing_cols:
        logger.error(f"Missing expectd columns {missing_cols}")

        if raise_error:
            raise ValueError(
                f"Missing required columns from response. Got {returned_cols}"
                f"Was expecting at least all of {exp_cols}"
            )


def check_data_mismatch(
    expected, returned, item_name: str, raise_error: bool = False
) -> None:
    """Checks for equality for expected and returned items, if not equal then
    log an error and optionally raise an error.
    """
    if expected != returned:
        msg = f"Mismatching {item_name}: queried {expected}, returned {returned}"
        logger.warning(msg)

        if raise_error:
            raise ValueError(msg)


def safe_urljoin(host, endpoint):
    """Same as urljoin but leading/trailing '/' makes no difference"""
    return f"{host.rstrip('/')}/{endpoint.lstrip('/')}"


def set_numeric_cols(df: pd.DataFrame, cols: Sequence):
    """Apply pd.to_numeric in-place to the given dataframe for cols"""

    for col in cols:
        df.loc[:, col] = pd.to_numeric(df[col])


def strip_str_cols(df: pd.DataFrame, cols: Sequence):
    """Apply str.strip in place to the given columns"""

    for col in cols:
        df.loc[:, col] = df[col].str.strip()


def get_interval_dates(
    start_date: str, end_date: str, month_ends: bool = False, trading_days=False
):
    """ """
