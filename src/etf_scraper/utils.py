from datetime import date
from typing import Sequence
import logging
import os

import pandas as pd
import numpy as np
from pandas.tseries.offsets import BDay


logger = logging.getLogger(__name__)


def parse_bool_env(env_var: str) -> bool:
    """Returns True if the environment variable env_var is set (up to upper/lower case)
    to one of "true", "1" or "t" , otherwise returns False.
    """
    return os.getenv(env_var, "False").lower() in ("true", "1", "t")


def check_missing_cols(
    exp_cols: Sequence, returned_cols: Sequence, raise_error: bool = False
) -> None:
    """Convenience function to log if we are missing columns from a request.

    raises: ValueError if raise_error=True and if we are missing expected columns
    """
    missing_cols = [k for k in exp_cols if k not in returned_cols]

    if missing_cols:
        logger.error(f"Missing expected columns {missing_cols}")

        if raise_error:
            raise ValueError(
                f"Missing required columns from response. Got {returned_cols}"
                f"Was expecting at least all of {exp_cols}"
            )


def check_dupl_cols(
    exp_cols: Sequence[str], returned_cols: Sequence[str], col_name: str
) -> str:
    """Check and raise an error if >1 item in exp_cols appears in returned_cols,
    otherwise returns that single element.

    Useful for when the same column appears under different names, eg date is returned
    as "Date" or "PositionDate" for Invesco funds.
    """

    matched_cols = [k for k in returned_cols if k in exp_cols]

    if len(matched_cols) != 1:
        raise ValueError(
            f"Received {len(matched_cols)} {col_name} columns: {matched_cols}, "
            f"was expecting exactly one of {exp_cols}"
        )

    return matched_cols[0]


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
    """Try to apply str.strip in place to the given columns,
    on failure returns the original item.

    # FIXME: this is bug prone, eg if we accidentally call this on
    a non-string column, (but convenient, eg if we have missing tickers)
    """

    def strip_(s):
        try:
            return s.strip()
        except:
            return s

    for col in cols:
        df.loc[:, col] = df[col].apply(strip_)


def _get_trd_dates(start_date: str, end_date: str, exchange: str) -> pd.DatetimeIndex:
    import pandas_market_calendars as mcal

    schedule = mcal.get_calendar(exchange).schedule(start_date, end_date)
    return mcal.date_range(schedule, frequency="1D").normalize().drop_duplicates()


def get_interval_query_dates(
    start_date: str,
    end_date: str,
    month_ends: bool = False,
    trading_days: bool = False,
    exchange: str = "NYSE",
) -> Sequence[date]:
    """Return a list of days to query holdings data for. Expects date strings
    parseable by pandas.

    Args:
    - month_ends: if True, then return only month end days within the specified range
    - trading_days: if True, then also subset to trading days. If month_ends=True too then
    selects the last trading day of each month.
    - exchange: The exchange schedule to use when trading_days=True
    """
    if not trading_days:
        freq = "BM" if month_ends else "B"
        return pd.date_range(start_date, end_date, freq=freq).date

    if not month_ends:
        return _get_trd_dates(start_date, end_date, exchange).date

    # buffer in case last day is a trading month end
    end_date_ = pd.to_datetime(end_date) + BDay(1)

    date_range = _get_trd_dates(start_date, end_date_, exchange)
    day_series = pd.Series(date_range, index=date_range)

    month_end_trd_days = day_series.groupby(pd.Grouper(freq="M")).last()
    month_end_trd_days_ = pd.Series(month_end_trd_days, index=month_end_trd_days)
    return month_end_trd_days_.loc[start_date:end_date].index.date


def get_list_chunk(list_: list[str], task_index: int, num_tasks: int) -> list[str]:
    """Split the given list into into num_tasks blocks and returns the
    block at task_index.
    """
    if len(list_) == 0:
        return []

    if num_tasks == 1:
        return list_

    block_size = int(np.ceil(len(list_) / int(num_tasks)))
    return list_[block_size * task_index : block_size * (task_index + 1)]
