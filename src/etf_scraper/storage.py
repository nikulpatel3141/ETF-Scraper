"""
Module for helper functions to store retrieved data.

Defauly saving here is done using Pandas, so should work seamlessly between
local and cloud infrastructure.
"""
import logging
import os
from datetime import date, datetime
from pathlib import Path
from itertools import product
from typing import Any, Callable, Dict, List, Sequence, Tuple, Union
from traceback import format_exc
from multiprocessing.pool import ThreadPool
from enum import Enum

from tenacity import (
    retry,
    retry_if_not_exception_type,
    wait_random_exponential,
    stop_after_attempt,
    before_sleep_log,
)
import pandas as pd
from pandas.io.common import is_fsspec_url

from etf_scraper.base import InvalidParameterError
from etf_scraper.api import ETFScraper

DATE_FMT = "%Y_%m_%d"

logger = logging.getLogger(__name__)


class SaveFormat(str, Enum):
    csv = "csv"
    parquet = "parquet"
    pickle = "pickle"


def list_files(path: str, extension: str) -> List[str]:
    """Recursively list all files ending with the given extension.

    Currently works for local filesystems and GCS buckets only.
    """
    if not is_fsspec_url(path):
        logger.debug(f"Given path {path} looks local")
        return [str(x) for x in Path(path).rglob("*" + extension)]
    elif path.startswith(("gs://", "gcs://")):
        logger.debug(f"Found a GCS path {path}")

        import gcsfs

        fs = gcsfs.GCSFileSystem()
        glob_uri = os.path.join(path, "*" + extension)
        return ["gs://" + x for x in fs.glob(glob_uri)]
    else:
        raise NotImplementedError(
            f"Can only list local and GCS filesystems, not for {path}"
        )


def holdings_filename(ticker: str, holdings_date: date, file_extension: str) -> str:
    """Hardcoded default save name for files"""
    return f"{ticker}_{holdings_date.strftime(DATE_FMT)}{file_extension}"


def parse_holdings_filename(filename: str) -> Tuple[str, date, str]:
    """Go from a filename output by `holdings_filename` to the ticker + date + extension."""
    base, ext = filename.rsplit(".", 1)
    ext_ = "." + ext

    ticker, date_str = base.split("_", 1)
    date_ = datetime.strptime(date_str, DATE_FMT).date()

    return ticker, date_, ext_


def list_unqueried_data(
    existing_files: Sequence[str],
    expected_dates: Sequence[date],
    expected_tickers: Sequence[str],
) -> List[Tuple[str, date]]:
    """Find the list of dates + tickers we are missing data for given an existing
    list of files. Expects filenames to be in the format returned by `holdings_filename`

    #TODO: allow users to pass their own filename parser
    """
    missing_data = {tuple(x) for x in product(expected_tickers, expected_dates)}

    for x in existing_files:
        ticker, date_, _ = parse_holdings_filename(Path(x).name)
        if (date_ in expected_dates) and (ticker in expected_tickers):
            missing_data.remove((ticker, date_))

    return list(missing_data)


def default_save_func(
    holdings_df: pd.DataFrame,
    ticker: str,
    holdings_date: date,
    out_dir: str,
    out_fmt: SaveFormat = SaveFormat.csv,
    existing_filenames: Sequence[str] = (),
) -> Union[str, None]:
    """Example function to pass to `query_range`. Saves output data to
    a file in out_dir (can be any filesystem supported by Pandas, eg local or
    a bucket on the cloud).

    You may pass a list of existing files to avoid unnecessary writes, eg when querying
    latest holdings data which hasn't updated since the initial write. This is useful
    eg in cloud scenarios to avoid incurring write costs.

    Args:
    - out_fmt: determines the file format + extension used to save the data.
    Valid values are eg "csv", "parquet", "pickle".
    Uses Pandas to_{{out_fmt}} to save data and appends .{{out_fmt}} to the filename, eg
    out_fmt="csv" will use df.to_csv and save to a file {ticker}_{date}.csv
    - existing_filenames: Don't write if the target filename is in this list of
    existing filenames. If this behaviour is not desired, then pass existing_filenames = []
    """
    filename = holdings_filename(ticker, holdings_date, "." + out_fmt)

    if filename in existing_filenames:
        logger.info(f"File {filename} already exists, not saving again.")
        return None

    out_path = os.path.join(out_dir, filename)
    logger.info(f"Saving holdings to {out_path}")
    getattr(holdings_df, f"to_{out_fmt}")(out_path, index=False)
    return out_path


def query_hist_ticker_dates(
    query_ticker_dates: Sequence[Tuple[str, Union[date, None]]],
    etf_scraper: ETFScraper,
    save_func: Callable[[pd.DataFrame, str, date], Any],
    num_threads: int = 10,
) -> Dict[Tuple[str, date], dict]:
    """Query the given dates + tickers using the passed ETFScraper and save the return
    value of save_func + # of returned holdings in a dict. Will return error messages
    in a dict if unable to query.

    Args:
        - save_func: a callable taking args (holdings_df, ticker, query_date) to save the output
        df. Should also return the save path if applicable.
        - num_threads: # of threads to parallelise over in Python's ThreadPool

    Returns: output as a dict with (ticker, date) as the keys.
    """
    assert num_threads >= 1

    # FIXME: put these retry parameters in a comfig file
    @retry(
        reraise=True,
        retry=retry_if_not_exception_type((InvalidParameterError, ImportError)),
        wait=wait_random_exponential(multiplier=1, max=60),
        stop=stop_after_attempt(3),
        before_sleep=before_sleep_log(logger, logging.INFO),
    )
    def _query_holdings(ticker, query_date):
        holdings_df = etf_scraper.query_holdings(ticker, query_date)

        if not query_date:
            holdings_date = holdings_df["as_of_date"].iloc[0]
        else:
            holdings_date = query_date

        save_path = save_func(
            holdings_df=holdings_df, ticker=ticker, holdings_date=holdings_date
        )
        return save_path, holdings_date, len(holdings_df)

    def query_holdings(ticker, query_date):
        try:
            save_path, holdings_date, n_holdings = _query_holdings(ticker, query_date)
            return_rpt = {
                "save_path": save_path,
                "holdings_date": holdings_date,
                "n_holdings": n_holdings,
            }
        except Exception as e:
            return_rpt = {"error": format_exc(), "error_class": type(e).__name__}
        return return_rpt

    with ThreadPool(num_threads) as p:
        from_pool = p.starmap(query_holdings, query_ticker_dates)

    return dict(zip(query_ticker_dates, from_pool))


def format_hist_query_output(query_output) -> pd.DataFrame:
    """Formats the output of `query_hist_ticker_dates` as a dataframe"""
    df = (
        pd.DataFrame(query_output)
        .T.rename_axis(index=["ticker", "query_date"])
        .reset_index()
    )
    for col in ["query_date", "holdings_date"]:
        df.loc[:, col] = pd.to_datetime(df[col], errors="coerce")
    return df
